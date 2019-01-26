package dht

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-peer"
	"github.com/libp2p/go-libp2p-peerstore/queue"
)

func TestDialQueueErrorsWithTooManyConsumers(t *testing.T) {
	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})
	dialFn := func(ctx context.Context, p peer.ID) error {
		<-hang
		return nil
	}
	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)

	for i := 0; i < 3; i++ {
		if _, err := dq.Consume(); err != nil {
			t.Errorf("expected nil err, got: %v", err)
		}
	}
	if _, err := dq.Consume(); err == nil {
		t.Error("expected err but got nil")
	}
}

func TestDialQueueGrowsOnSlowDials(t *testing.T) {
	in := queue.NewChanQueue(context.Background(), queue.NewXORDistancePQ("test"))
	hang := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(19) // we expect 19 workers
	dialFn := func(ctx context.Context, p peer.ID) error {
		wg.Done()
		<-hang
		return nil
	}

	// Enqueue 20 jobs.
	for i := 0; i < 20; i++ {
		in.EnqChan <- peer.ID(i)
	}

	// remove the mute period to grow faster.
	DialQueueScalingMutePeriod = 0
	dq := newDialQueue(context.Background(), "test", in, dialFn, 3)

	for i := 0; i < 3; i++ {
		_, _ = dq.Consume()
		time.Sleep(100 * time.Millisecond)
	}

	doneCh := make(chan struct{})

	// wait in a goroutine in case the test fails and we block.
	go func() {
		defer close(doneCh)
		wg.Wait()
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Error("expected 19 concurrent dials, got less")
	}
}

func TestDialQueueShrinksWithInactivity(t *testing.T) {

}

func TestDialQueueShrinksWithSlowDelivery(t *testing.T) {

}

func TestDialQueueScalingMutePeriod(t *testing.T) {

}
