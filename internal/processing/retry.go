package processing

import (
	"context"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	Classify    func(error) bool
}

func (p RetryPolicy) Run(ctx context.Context, operation func(context.Context) error) (int, error) {
	maxAttempts := p.MaxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	baseDelay := p.BaseDelay
	if baseDelay <= 0 {
		baseDelay = time.Millisecond
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attempt - 1, err
		}
		err := operation(ctx)
		if err == nil {
			return attempt, nil
		}
		if ctx.Err() != nil {
			return attempt, ctx.Err()
		}
		if p.Classify == nil || !p.Classify(err) || attempt == maxAttempts {
			return attempt, err
		}
		delay := backoff(baseDelay, attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return attempt, ctx.Err()
		case <-timer.C:
		}
	}
	return maxAttempts, ctx.Err()
}

func backoff(base time.Duration, attempt int) time.Duration {
	if attempt > 10 {
		attempt = 10
	}
	delay := base * time.Duration(1<<(attempt-1))
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}
