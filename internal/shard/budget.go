package shard

import (
	"context"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// MergeBudget bounds the CPU and I/O background merges use (spec section 12's
// merge_budget, split by config into MergeThreads and MergeBudget). Share one across
// every shard on a node.
//
//   - CPU: Threads tokens. A merge takes one, waiting if none is free, then whatever
//     others are free at that moment, and runs with that many goroutines; it gives them
//     back when done. So merges never use more than Threads cores between them, a lone
//     merge uses all of them, and many merges share them.
//   - I/O: a byte rate, enforced as each ~64 KiB chunk of a merged segment is written
//     (segment.MergeOptions.Throttle). 0 means unlimited.
//
// Refreshes are not budgeted: they are on the write-to-visible path.
type MergeBudget struct {
	tokens chan struct{}
	rate   float64 // bytes per second; 0: unlimited
	clock  clock.Clock

	mu   sync.Mutex
	next time.Time // when the next byte may be written
}

// NewMergeBudget returns a budget of threads concurrent merge goroutines (at least 1)
// and bytesPerSec merge writes per second (0: unlimited), throttled by clk.
func NewMergeBudget(threads int, bytesPerSec int64, clk clock.Clock) *MergeBudget {
	threads = max(1, threads)
	b := &MergeBudget{tokens: make(chan struct{}, threads), rate: float64(max(0, bytesPerSec)), clock: clk}
	for range threads {
		b.tokens <- struct{}{}
	}
	return b
}

// Threads is the budget's CPU tokens.
func (b *MergeBudget) Threads() int { return cap(b.tokens) }

// acquire takes one token, waiting for it, then up to want-1 more that are free now.
func (b *MergeBudget) acquire(ctx context.Context, want int) (int, error) {
	select {
	case <-b.tokens:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	n := 1
	for n < want {
		select {
		case <-b.tokens:
			n++
		default:
			return n, nil
		}
	}
	return n, nil
}

func (b *MergeBudget) release(n int) {
	for range n {
		b.tokens <- struct{}{}
	}
}

// throttle waits until n more bytes fit the rate, or ctx ends.
func (b *MergeBudget) throttle(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.rate == 0 {
		return nil
	}
	b.mu.Lock()
	now := b.clock.Now()
	if b.next.Before(now) {
		b.next = now
	}
	wait := b.next.Sub(now)
	b.next = b.next.Add(time.Duration(float64(n) / b.rate * float64(time.Second)))
	b.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	return b.clock.Sleep(ctx, wait)
}
