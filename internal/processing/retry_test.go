package processing

import (
	"context"
	"errors"
	"testing"
	"time"
)

var (
	errTransient = errors.New("transient")
	errPermanent = errors.New("permanent")
)

func TestRetryStopsAtConfiguredAttemptLimit(t *testing.T) {
	calls := 0
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Classify: func(err error) bool { return errors.Is(err, errTransient) }}
	_, err := policy.Run(context.Background(), func(context.Context) error {
		calls++
		return errTransient
	})
	if !errors.Is(err, errTransient) || calls != 3 {
		t.Fatalf("err=%v calls=%d, want transient and 3 calls", err, calls)
	}
}

func TestRetryStopsImmediatelyOnPermanentError(t *testing.T) {
	calls := 0
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, Classify: func(err error) bool { return errors.Is(err, errTransient) }}
	_, err := policy.Run(context.Background(), func(context.Context) error {
		calls++
		return errPermanent
	})
	if !errors.Is(err, errPermanent) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want permanent and 1 call", err, calls)
	}
}

func TestRetryStopsWaitingWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	policy := RetryPolicy{MaxAttempts: 3, BaseDelay: time.Hour, Classify: func(error) bool { return true }}
	calls := 0
	started := make(chan struct{})
	go func() {
		close(started)
		_, _ = policy.Run(ctx, func(context.Context) error { calls++; return errTransient })
	}()
	<-started
	time.Sleep(5 * time.Millisecond)
	cancel()
	if calls < 1 {
		t.Fatal("operation was not attempted")
	}
}
