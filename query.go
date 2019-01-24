package dht

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
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

type stagingQueue struct {
	dialFn func(context.Context, peer.ID) error
	ctx    context.Context
	// TODO maxParallelism int

	parLk         sync.Mutex
	parallelism   int
	scalingFactor float64

	in          *queue.ChanQueue
	completedCh chan peer.ID

	dieCh    chan struct{}
	notifyCh chan struct{}

	retChs   []chan dialResult
	retChIdx int32
}

var StagingQueueMinParallelism = 6
var StagingQueueMaxIdle = 5 * time.Second

var ErrContextClosed = errors.New("context closed")

func newStagingQueue(ctx context.Context, in *queue.ChanQueue, dialFn func(context.Context, peer.ID) error, nConsumers int) *stagingQueue {
	sq := &stagingQueue{
		dialFn:        dialFn,
		ctx:           ctx,
		scalingFactor: 1.5,
		parallelism:   StagingQueueMinParallelism,
		in:            in,

		// TODO: we should really re-sort dials as they come back based on XORKeySpace, instead of queuing them in the
		// order they come back.
		completedCh: make(chan peer.ID),
		dieCh:       make(chan struct{}, StagingQueueMinParallelism),
		notifyCh:    make(chan struct{}),
	}

	sq.retChIdx = int32(nConsumers)
	for ; nConsumers > 0; nConsumers-- {
		sq.retChs = append(sq.retChs, make(chan dialResult))
	}
	for i := 0; i < StagingQueueMinParallelism; i++ {
		go sq.worker()
	}
	return sq
}

type dialResult struct {
	p   peer.ID
	err error
}

func (sq *stagingQueue) Consume() (<-chan dialResult, error) {
	// TODO: not super happy with this approach and the extra goroutine per consumption.
	// Need to think of a better way.
	chIdx := atomic.AddInt32(&sq.retChIdx, -1)
	if chIdx < 0 {
		atomic.AddInt32(&sq.retChIdx, 1)
		return nil, errors.New("more concurrent calls to Consume() than declared")
	}

	ch := sq.retChs[chIdx]
	go func() {
		defer atomic.AddInt32(&sq.retChIdx, 1)
		for {
			select {
			case <-sq.ctx.Done():
				ch <- dialResult{"", ErrContextClosed}
			case p := <-sq.completedCh:
				ch <- dialResult{p, nil}
			default:
			}

			// We have no finished dials to return, so this means it's time to scale up our
			// parallelism, as demand is higher than supply.
			sq.scaleUp()
			<-sq.notifyCh

			select {
			case <-sq.ctx.Done():
				ch <- dialResult{"", ErrContextClosed}
			case p := <-sq.completedCh:
				ch <- dialResult{p, nil}
			}
		}
	}()
	return ch, nil
}

func (sq *stagingQueue) scaleUp() {
	// TODO: we need to debounce scaling, as we can easily end up with a swamp of scaleup calls if the entire DHT
	// is progressing slowly.
	sq.parLk.Lock()
	defer sq.parLk.Unlock()

	prev := sq.parallelism
	tgt := int(math.Floor(float64(prev) * sq.scalingFactor))
	for ; prev < tgt; prev++ {
		go sq.worker()
	}
	sq.parallelism = tgt

	// scale the buffer of dieCh accordingly; taking into account that we'll never kill workers beyond minParallelism.
	dieCh := sq.dieCh
	sq.dieCh = make(chan struct{}, tgt-StagingQueueMinParallelism)
	close(dieCh)
}

func (sq *stagingQueue) scaleDown() {
	// TODO: we need to debounce scaling, as we can easily end up with a swamp of scaledown calls if the entire DHT
	// is progressing slowly.
	sq.parLk.Lock()
	defer sq.parLk.Unlock()

	prev := sq.parallelism
	tgt := int(math.Floor(float64(prev) / sq.scalingFactor))
	if tgt < StagingQueueMinParallelism {
		tgt = StagingQueueMinParallelism
	}

	// send as many die signals as workers we have to prune; we don't scale down the buffer of dieCh, as we might
	// end up growing the pool again.
	for ; prev > tgt; prev-- {
		select {
		case sq.dieCh <- struct{}{}:
		default:
			log.Debugf("too many die signals queued up.")
		}
	}
	sq.parallelism = tgt
}

func (sq *stagingQueue) notify() {
	select {
	case sq.notifyCh <- struct{}{}:
	default:
	}
}

func (sq *stagingQueue) worker() {
	timer := time.NewTimer(0)

	for {
		// trap exit signals first.
		select {
		case <-sq.ctx.Done():
			return
		case _, more := <-sq.dieCh:
			// if channel is closed, it's being replaced by a new one, so we're not being told to die.
			if more {
				return
			}
		default:
		}

		// The idle timer tracks if the environment is slow, i.e.:
		// (a) we're waiting too long before dequeing new peers (input), which is an indication that the DHT lookup
		//     progressing slowly, therefore we can scale down.
		// (b) the DHT controller consumes successful dials too slowly (output).
		//
		// We send scaleDown signals everytime this timer triggers.
		timer.Reset(StagingQueueMaxIdle)
		select {
		case <-timer.C:
			// we received no new dials to schedule during our idle period, so it's time to scale down.
			sq.scaleDown()
			continue
		case p := <-sq.in.DeqChan:
			if err := sq.dialFn(sq.ctx, p); err != nil {
				log.Debugf("discarding dialled peer because of error: %v", err)
				continue
			}
			timer.Reset(StagingQueueMaxIdle)

		Handoff:
			select {
			case <-sq.ctx.Done():
				return
			case sq.completedCh <- p:
				if !timer.Stop() {
					<-timer.C
				}
				// all good; peer was handed off.
				sq.notify()
			case <-timer.C:
				sq.scaleDown()
				// keep trying to publish on the channel, but having sent the signal that the downstream goroutine
				// has not read from us.
				goto Handoff
			}
		}
	}
}

type dhtQueryRunner struct {
	query          *dhtQuery        // query to run
	peersSeen      *pset.PeerSet    // all peers queried. prevent querying same peer 2x
	peersQueried   *pset.PeerSet    // peers successfully connected to and queried
	peersDialed    *stagingQueue    // peers we have dialed to
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
		proc:           proc,
	}
	r.peersDialed = newStagingQueue(ctx, peersToQuery, r.dialPeer, AlphaValue)
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
				// TODO an error here can only be thrown if we are consuming with a higher concurrency than we
				//  declared upfront. Needs to be thought out better.
				continue
			}
			select {
			case p, more := <-ch:
				if !more {
					// Put this back so we can finish any outstanding queries.
					r.rateLimit <- struct{}{}
					return // channel closed.
				}

				// do it as a child func to make sure Run exits
				// ONLY AFTER spawn workers has exited.
				proc.Go(func(proc process.Process) {
					r.queryPeer(proc, p.p)
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
	// make sure we're connected to the peer.
	// FIXME abstract away into the network layer
	// Note: Failure to connect in this block will cause the function to
	// short circuit.
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
