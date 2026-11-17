package main

import (
	"context"
	"time"
)

// retryApplier decorates a RouteApplier with bounded retries and
// exponential backoff. Retry policy lives here so backends stay
// policy-free: swapping proxies never means re-implementing retry
// semantics, and tuning backoff never means touching a backend.
type retryApplier struct {
	inner    RouteApplier
	attempts int           // total attempts, >= 1
	backoff  time.Duration // sleep before retry n is backoff<<(n-1)
}

func (r retryApplier) Apply(ctx context.Context, doc []byte) error {
	var lastErr error
	for attempt := 0; attempt < r.attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return lastErr
			case <-time.After(r.backoff << (attempt - 1)):
			}
		}
		if err := r.inner.Apply(ctx, doc); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}
