package dht

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log"
	todoctr "github.com/ipfs/go-todocounter"
	process "github.com/jbenet/goprocess"
	ctxproc "github.com/jbenet/goprocess/context"
	inet "github.com/libp2p/go-libp2p-net"
	peer "github.com/libp2p/go-libp2p-peer"
	pset "github.com/libp2p/go-libp2p-peer/peerset"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	queue "github.com/libp2p/go-libp2p-peerstore/queue"
	routing "github.com/libp2p/go-libp2p-routing"
	notif "github.com/libp2p/go-libp2p-routing/notifications"
	errors "github.com/pkg/errors"
)

var maxQueryConcurrency = AlphaValue

type dhtQuery struct {
	dht         *IpfsDHT
	key         string    // the key we're querying for
	qfunc       queryFunc // the function to execute per peer
	concurrency int       // the concurrency parameter
}

type dhtQueryResult struct {
	value         []byte             // GetValue
	peer          *pstore.PeerInfo   // FindPeer
	providerPeers []pstore.PeerInfo  // GetProviders
	closerPeers   []*pstore.PeerInfo // *
	success       bool

	finalSet   *pset.PeerSet
	queriedSet *pset.PeerSet
}

// constructs query
func (dht *IpfsDHT) newQuery(k string, f queryFunc) *dhtQuery {
	return &dhtQuery{
		key:         k,
		dht:         dht,
		qfunc:       f,
		concurrency: maxQueryConcurrency,
	}
}

// QueryFunc is a function that runs a particular query with a given peer.
// It returns either:
// - the value
// - a list of peers potentially better able to serve the query
// - an error
type queryFunc func(context.Context, peer.ID) (*dhtQueryResult, error)

// Run runs the query at hand. pass in a list of peers to use first.
func (q *dhtQuery) Run(ctx context.Context, peers []peer.ID) (*dhtQueryResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	runner := newQueryRunner(q)
	return runner.Run(ctx, peers)
}

type dialQueue struct {
	ctx    context.Context
	dialFn func(context.Context, peer.ID) error

	lk             sync.Mutex
	nWorkers       int
	scalingFactor  float64
	lastScalingEvt time.Time

	in  *queue.ChanQueue
	out *queue.ChanQueue

	waiting chan chan<- peer.ID
	dieCh   chan struct{}
}

var DialQueueMinParallelism = 6
var DialQueueMaxParallelism = 20
var DialQueueMaxIdle = 5 * time.Second
var DialQueueScalingMutePeriod = 1 * time.Second

var ErrContextClosed = errors.New("context closed")

// newDialQueue returns an adaptive dial queue that spawns a dynamically sized set of goroutines to preemptively
// stage dials for later handoff to the DHT protocol for RPC.
//
// Why? Dialing is expensive. It's orders of magnitude slower than running an RPC on an already-established
// connection, as it requires establishing a TCP connection, multistream handshake, crypto handshake, mux handshake,
// and protocol negotiation.
//
// We start with DialQueueMinParallelism number of workers, and scale up and down based on demand and supply of
// dialled peers.
//
// The following events trigger scaling:
//  - there are no successful dials to return immediately when requested (i.e. consumer stalls) => scale up.
//  - there are no consumers to hand off a successful dial to when requested (i.e. producer stalls) => scale down.
//
// We ought to watch out for dialler throttling (e.g. FD limit exceeded), to avoid adding fuel to the fire. Since
// we have no deterministic way to detect this, for now we are hard-limiting concurrency by a max factor.
func newDialQueue(ctx context.Context, target string, in *queue.ChanQueue, dialFn func(context.Context, peer.ID) error, nConsumers int) *dialQueue {
	sq := &dialQueue{
		ctx:           ctx,
		dialFn:        dialFn,
		nWorkers:      DialQueueMinParallelism,
		scalingFactor: 1.5,

		in:      in,
		out:     queue.NewChanQueue(ctx, queue.NewXORDistancePQ(target)),
		waiting: make(chan chan<- peer.ID, nConsumers),
		dieCh:   make(chan struct{}, DialQueueMinParallelism),
	}
	for i := 0; i < DialQueueMinParallelism; i++ {
		go sq.worker()
	}
	go sq.feedWaiting()
	return sq
}

func (dq *dialQueue) feedWaiting() {
	var out chan<- peer.ID
	var t time.Time // for logging purposes
	for {
		select {
		case <-dq.ctx.Done():
			return
		case out = <-dq.waiting: // got a channel that's waiting for a peer.
			t = time.Now()
		}

		select {
		case <-dq.ctx.Done():
			return
		case p := <-dq.out.DeqChan:
			log.Debugf("delivering dialled peer to DHT; took %dms.", time.Now().Sub(t)/time.Millisecond)
			out <- p
		}
		close(out)
	}
}

func (dq *dialQueue) Consume() (<-chan peer.ID, error) {
	ch := make(chan peer.ID, 1)
	select {
	case <-dq.ctx.Done():
		return nil, ErrContextClosed
	case p := <-dq.out.DeqChan:
		ch <- p
		close(ch)
		return ch, nil
	default:
	}

	// we have no finished dials to return, trigger a scale up.
	dq.hintGrow()

	// let feedWaiters handle returning a peer.
	select {
	case dq.waiting <- ch:
	default:
		return nil, errors.New("more consumers that declared upfront")
	}
	return ch, nil
}

func (dq *dialQueue) hintGrow() {
	dq.lk.Lock()
	defer dq.lk.Unlock()
	if dq.nWorkers == DialQueueMaxParallelism || time.Now().Sub(dq.lastScalingEvt) < DialQueueScalingMutePeriod {
		return
	}
	prev := dq.nWorkers
	tgt := int(math.Floor(float64(prev) * dq.scalingFactor))
	if tgt > DialQueueMaxParallelism {
		tgt = DialQueueMinParallelism
	}
	for ; prev < tgt; prev++ {
		go dq.worker()
	}
	dq.nWorkers = tgt

	// scale the buffer of dieCh accordingly; taking into account that we'll never kill workers beyond minParallelism.
	dieCh := dq.dieCh
	dq.dieCh = make(chan struct{}, tgt-DialQueueMinParallelism)
	close(dieCh)

	dq.lastScalingEvt = time.Now()
}

func (dq *dialQueue) hintShrink() {
	dq.lk.Lock()
	defer dq.lk.Unlock()

	if dq.nWorkers == DialQueueMinParallelism || time.Now().Sub(dq.lastScalingEvt) < DialQueueScalingMutePeriod {
		return
	}
	prev := dq.nWorkers
	tgt := int(math.Floor(float64(prev) / dq.scalingFactor))
	if tgt < DialQueueMinParallelism {
		tgt = DialQueueMinParallelism
	}

	// send as many die signals as workers we have to prune. we don't scale down the buffer of dieCh,
	// as we might need to grow the pool again.
	for ; prev > tgt; prev-- {
		select {
		case dq.dieCh <- struct{}{}:
		default:
			log.Debugf("too many die signals queued up.")
		}
	}
	dq.nWorkers = tgt

	dq.lastScalingEvt = time.Now()
}

func (dq *dialQueue) worker() {
	timer := time.NewTimer(0)

	for {
		// trap exit signals first.
		select {
		case <-dq.ctx.Done():
			return
		case _, more := <-dq.dieCh:
			if more {
				// if channel is still open, we're being told to die.
				// if channel is closed, it's just being replaced by a new one.
				return
			}
		default:
		}

		// This idle timer tracks if the environment is slow. This could happen if we're waiting too long before
		// dequeuing a peers to dial (input), or if the DHT controller consumes successful dials too slowly (output).
		//
		// We send scaleDown signals everytime this timer triggers.
		timer.Reset(DialQueueMaxIdle)
		select {
		case <-timer.C:
			// no new dial requests during our idle period; time to scale down.
			dq.hintShrink()
			continue
		case p := <-dq.in.DeqChan:
			if !timer.Stop() {
				<-timer.C
			}
			if err := dq.dialFn(dq.ctx, p); err != nil {
				log.Debugf("discarding dialled peer because of error: %v", err)
				continue
			}
			timer.Reset(DialQueueMaxIdle)

			dq.out.EnqChan <- p
			if len(dq.waiting) == 0 {
				dq.hintShrink()
			}
		}
	}
}

type dhtQueryRunner struct {
	query          *dhtQuery        // query to run
	peersSeen      *pset.PeerSet    // all peers queried. prevent querying same peer 2x
	peersQueried   *pset.PeerSet    // peers successfully connected to and queried
	peersDialed    *dialQueue       // peers we have dialed to
	peersToQuery   *queue.ChanQueue // peers remaining to be queried
	peersRemaining todoctr.Counter  // peersToQuery + currently processing

	result *dhtQueryResult // query result
	errs   u.MultiErr      // result errors. maybe should be a map[peer.ID]error

	rateLimit chan struct{} // processing semaphore
	log       logging.EventLogger

	runCtx context.Context

	proc process.Process
	sync.RWMutex
}

func newQueryRunner(q *dhtQuery) *dhtQueryRunner {
	proc := process.WithParent(process.Background())
	ctx := ctxproc.OnClosingContext(proc)
	peersToQuery := queue.NewChanQueue(ctx, queue.NewXORDistancePQ(string(q.key)))
	r := &dhtQueryRunner{
		query:          q,
		peersRemaining: todoctr.NewSyncCounter(),
		peersSeen:      pset.New(),
		peersQueried:   pset.New(),
		rateLimit:      make(chan struct{}, q.concurrency),
		peersToQuery:   peersToQuery,
		proc:           proc,
	}
	r.peersDialed = newDialQueue(ctx, q.key, peersToQuery, r.dialPeer, AlphaValue)
	return r
}

func (r *dhtQueryRunner) Run(ctx context.Context, peers []peer.ID) (*dhtQueryResult, error) {
	r.log = log
	r.runCtx = ctx

	if len(peers) == 0 {
		log.Warning("Running query with no peers!")
		return nil, nil
	}

	// setup concurrency rate limiting
	for i := 0; i < r.query.concurrency; i++ {
		r.rateLimit <- struct{}{}
	}

	// add all the peers we got first.
	for _, p := range peers {
		r.addPeerToQuery(p)
	}

	// go do this thing.
	// do it as a child proc to make sure Run exits
	// ONLY AFTER spawn workers has exited.
	r.proc.Go(r.spawnWorkers)

	// so workers are working.

	// wait until they're done.
	err := routing.ErrNotFound

	// now, if the context finishes, close the proc.
	// we have to do it here because the logic before is setup, which
	// should run without closing the proc.
	ctxproc.CloseAfterContext(r.proc, ctx)

	select {
	case <-r.peersRemaining.Done():
		r.proc.Close()
		r.RLock()
		defer r.RUnlock()

		err = routing.ErrNotFound

		// if every query to every peer failed, something must be very wrong.
		if len(r.errs) > 0 && len(r.errs) == r.peersSeen.Size() {
			log.Debugf("query errs: %s", r.errs)
			err = r.errs[0]
		}

	case <-r.proc.Closed():
		r.RLock()
		defer r.RUnlock()
		err = context.DeadlineExceeded
	}

	if r.result != nil && r.result.success {
		return r.result, nil
	}

	return &dhtQueryResult{
		finalSet:   r.peersSeen,
		queriedSet: r.peersQueried,
	}, err
}

func (r *dhtQueryRunner) addPeerToQuery(next peer.ID) {
	// if new peer is ourselves...
	if next == r.query.dht.self {
		r.log.Debug("addPeerToQuery skip self")
		return
	}

	if !r.peersSeen.TryAdd(next) {
		return
	}

	notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
		Type: notif.AddingPeer,
		ID:   next,
	})

	r.peersRemaining.Increment(1)
	select {
	case r.peersToQuery.EnqChan <- next:
	case <-r.proc.Closing():
	}
}

func (r *dhtQueryRunner) spawnWorkers(proc process.Process) {
	for {
		select {
		case <-r.peersRemaining.Done():
			return

		case <-r.proc.Closing():
			return

		case <-r.rateLimit:
			ch, err := r.peersDialed.Consume()
			if err != nil {
				log.Warningf("error while fetching a dialled peer: %v", err)
				r.rateLimit <- struct{}{}
				continue
			}
			select {
			case p, _ := <-ch:
				// do it as a child func to make sure Run exits
				// ONLY AFTER spawn workers has exited.
				proc.Go(func(proc process.Process) {
					r.queryPeer(proc, p)
				})
			case <-r.proc.Closing():
				return
			case <-r.peersRemaining.Done():
				return
			}
		}
	}
}

func (r *dhtQueryRunner) dialPeer(ctx context.Context, p peer.ID) error {
	// short-circuit if we're already connected.
	if r.query.dht.host.Network().Connectedness(p) == inet.Connected {
		return nil
	}

	log.Debug("not connected. dialing.")
	notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
		Type: notif.DialingPeer,
		ID:   p,
	})

	pi := pstore.PeerInfo{ID: p}
	if err := r.query.dht.host.Connect(ctx, pi); err != nil {
		log.Debugf("error connecting: %s", err)
		notif.PublishQueryEvent(r.runCtx, &notif.QueryEvent{
			Type:  notif.QueryError,
			Extra: err.Error(),
			ID:    p,
		})

		r.Lock()
		r.errs = append(r.errs, err)
		r.Unlock()
		return err
	}
	log.Debugf("connected. dial success.")
	return nil
}

func (r *dhtQueryRunner) queryPeer(proc process.Process, p peer.ID) {
	// ok let's do this!

	// create a context from our proc.
	ctx := ctxproc.OnClosingContext(proc)

	// make sure we do this when we exit
	defer func() {
		// signal we're done processing peer p
		r.peersRemaining.Decrement(1)
		r.rateLimit <- struct{}{}
	}()

	// finally, run the query against this peer
	res, err := r.query.qfunc(ctx, p)

	r.peersQueried.Add(p)

	if err != nil {
		log.Debugf("ERROR worker for: %v %v", p, err)
		r.Lock()
		r.errs = append(r.errs, err)
		r.Unlock()

	} else if res.success {
		log.Debugf("SUCCESS worker for: %v %s", p, res)
		r.Lock()
		r.result = res
		r.Unlock()
		go r.proc.Close() // signal to everyone that we're done.
		// must be async, as we're one of the children, and Close blocks.

	} else if len(res.closerPeers) > 0 {
		log.Debugf("PEERS CLOSER -- worker for: %v (%d closer peers)", p, len(res.closerPeers))
		for _, next := range res.closerPeers {
			if next.ID == r.query.dht.self { // don't add self.
				log.Debugf("PEERS CLOSER -- worker for: %v found self", p)
				continue
			}

			// add their addresses to the dialer's peerstore
			r.query.dht.peerstore.AddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
			r.addPeerToQuery(next.ID)
			log.Debugf("PEERS CLOSER -- worker for: %v added %v (%v)", p, next.ID, next.Addrs)
		}
	} else {
		log.Debugf("QUERY worker for: %v - not found, and no closer peers.", p)
	}
}
