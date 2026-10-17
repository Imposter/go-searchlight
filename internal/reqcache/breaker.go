package reqcache

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrOpen is a call a [Breaker] did not make: its store failed too often lately.
var ErrOpen = errors.New("reqcache: the store is failing; skipped")

// Breaker defaults.
const (
	DefaultBreakerFailures = 5
	DefaultBreakerBackoff  = time.Second
	DefaultBreakerMax      = 30 * time.Second
)

// Breaker is a [Store] in front of another that stops asking it once it fails: after
// Failures failures in a row (errors and timeouts; ErrTooLarge and calls whose own
// context ended are not the store's) every call is skipped with ErrOpen for a backoff,
// Backoff at first and doubling, up to Max, at each probe that fails again. Once the
// backoff has passed, one call is let through as a probe: its success closes the
// breaker, its failure opens it again.
type Breaker struct {
	Store
	failures int
	base     time.Duration
	max      time.Duration
	now      func() time.Time

	mu        sync.Mutex
	failed    int           // failures in a row
	openUntil time.Time     // zero: closed
	backoff   time.Duration // the next opening's
	probing   bool          // a probe is in flight
}

// NewBreaker returns s behind a breaker; zero arguments take the defaults.
func NewBreaker(s Store, failures int, backoff, maxBackoff time.Duration) *Breaker {
	if failures <= 0 {
		failures = DefaultBreakerFailures
	}
	if backoff <= 0 {
		backoff = DefaultBreakerBackoff
	}
	if maxBackoff < backoff {
		maxBackoff = max(DefaultBreakerMax, backoff)
	}
	return &Breaker{Store: s, failures: failures, base: backoff, max: maxBackoff, now: time.Now, backoff: backoff}
}

// Open reports whether calls are being skipped.
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openUntil.IsZero()
}

// Get implements [Store].
func (b *Breaker) Get(ctx context.Context, key string) ([]byte, error) {
	probe, ok := b.allow()
	if !ok {
		return nil, ErrOpen
	}
	v, err := b.Store.Get(ctx, key)
	b.done(ctx, probe, err)
	return v, err
}

// Set implements [Store].
func (b *Breaker) Set(ctx context.Context, key string, value []byte) error {
	probe, ok := b.allow()
	if !ok {
		return ErrOpen
	}
	err := b.Store.Set(ctx, key, value)
	b.done(ctx, probe, err)
	return err
}

// allow reports whether a call may go to the store, and whether it is the probe.
func (b *Breaker) allow() (probe, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.openUntil.IsZero():
		return false, true
	case b.probing || b.now().Before(b.openUntil):
		return false, false
	}
	b.probing = true
	return true, true
}

func (b *Breaker) done(ctx context.Context, probe bool, err error) {
	failed := err != nil && !errors.Is(err, ErrTooLarge)
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
	}
	switch {
	case failed && ctx.Err() != nil:
		// The caller gave up: it tells nothing of the store.
	case !failed && (probe || b.openUntil.IsZero()):
		b.failed, b.openUntil, b.backoff = 0, time.Time{}, b.base
	case !failed:
	case probe:
		b.backoff = min(2*b.backoff, b.max)
		b.openUntil = b.now().Add(b.backoff)
	default:
		b.failed++
		if b.failed >= b.failures && b.openUntil.IsZero() {
			b.openUntil = b.now().Add(b.backoff)
		}
	}
}
