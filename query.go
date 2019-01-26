package dht

import (
	"context"
	"sync"

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
