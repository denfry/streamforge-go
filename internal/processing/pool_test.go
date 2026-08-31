package processing

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestPoolBlocksSubmitWhenQueueIsFull(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	pool := NewPool(1, 1, func(context.Context, Job) error {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Run(ctx)
	if err := pool.Submit(ctx, Job{}); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := pool.Submit(ctx, Job{}); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { blocked <- pool.Submit(ctx, Job{}) }()
	select {
	case err := <-blocked:
		t.Fatalf("third job bypassed bounded queue: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
}

func TestPoolStopsSubmissionOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := NewPool(1, 1, func(context.Context, Job) error { return nil })
	done := make(chan struct{})
	go func() { pool.Run(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pool did not stop")
	}
	if err := pool.Submit(context.Background(), Job{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("submit error=%v, want context.Canceled", err)
	}
}
