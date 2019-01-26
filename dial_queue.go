package dht

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-peerstore/queue"
)

var DialQueueMinParallelism = 6
var DialQueueMaxParallelism = 20
var DialQueueMaxIdle = 5 * time.Second
var DialQueueScalingMutePeriod = 1 * time.Second

var ErrContextClosed = errors.New("context closed")

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
	if dq.in.Queue.Len() > 0 {
		dq.hintGrow()
	}

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
	if !timer.Stop() {
		<-timer.C
	}

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
		case <-dq.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
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
