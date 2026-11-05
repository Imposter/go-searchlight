package reqcache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// flaky is a Store that fails while down is set, counting its calls.
type flaky struct {
	down  atomic.Bool
	calls atomic.Int64
}

func (f *flaky) Name() string         { return "flaky" }
func (f *flaky) MaxEntryBytes() int64 { return 10 }
func (f *flaky) Close() error         { return nil }

func (f *flaky) Get(context.Context, string) ([]byte, error) {
	f.calls.Add(1)
	if f.down.Load() {
		return nil, errors.New("down")
	}
	return []byte("v"), nil
}

func (f *flaky) Set(_ context.Context, _ string, v []byte) error {
	f.calls.Add(1)
	if len(v) > 10 {
		return ErrTooLarge
	}
	if f.down.Load() {
		return errors.New("down")
	}
	return nil
}

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	t.Parallel()
	f := &flaky{}
	b := NewBreaker(f, 3, time.Second, 4*time.Second)
	now := time.Unix(1000, 0)
	b.now = func() time.Time { return now }
	ctx := context.Background()
	f.down.Store(true)
	for range 3 {
		if _, err := b.Get(ctx, "k"); err == nil || errors.Is(err, ErrOpen) {
			t.Fatalf("Get = %v, want the store's error", err)
		}
	}
	if !b.Open() {
		t.Fatal("three failures in a row did not open the breaker")
	}
	if _, err := b.Get(ctx, "k"); !errors.Is(err, ErrOpen) || f.calls.Load() != 3 {
		t.Fatalf("Get = %v after %d calls: want ErrOpen, the store not asked", err, f.calls.Load())
	}
	if err := b.Set(ctx, "k", []byte("v")); !errors.Is(err, ErrOpen) {
		t.Fatalf("Set = %v, want ErrOpen", err)
	}
	// The probe fails: open again, for twice as long.
	now = now.Add(time.Second)
	if _, err := b.Get(ctx, "k"); err == nil || errors.Is(err, ErrOpen) {
		t.Fatalf("probe = %v, want the store's error", err)
	}
	now = now.Add(1500 * time.Millisecond)
	if _, err := b.Get(ctx, "k"); !errors.Is(err, ErrOpen) {
		t.Fatalf("Get = %v inside the doubled backoff, want ErrOpen", err)
	}
	// The next probe succeeds: closed.
	f.down.Store(false)
	now = now.Add(time.Second)
	if _, err := b.Get(ctx, "k"); err != nil {
		t.Fatalf("probe = %v", err)
	}
	if b.Open() {
		t.Fatal("a successful probe left the breaker open")
	}
	if err := b.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestBreakerIgnoresWhatIsNotTheStoresFault(t *testing.T) {
	t.Parallel()
	f := &flaky{}
	b := NewBreaker(f, 2, time.Second, time.Second)
	for range 5 {
		if err := b.Set(context.Background(), "k", make([]byte, 20)); !errors.Is(err, ErrTooLarge) {
			t.Fatalf("Set = %v", err)
		}
	}
	f.down.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 5 {
		_, _ = b.Get(ctx, "k")
	}
	if b.Open() {
		t.Fatal("refused values or callers that gave up opened the breaker")
	}
	// A success between failures resets the count.
	_, _ = b.Get(context.Background(), "k")
	f.down.Store(false)
	_, _ = b.Get(context.Background(), "k")
	f.down.Store(true)
	_, _ = b.Get(context.Background(), "k")
	if b.Open() {
		t.Fatal("failures with a success between them opened the breaker")
	}
}
