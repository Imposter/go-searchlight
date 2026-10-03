package replica

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

func meteredOptions() (Options, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	opts := testOptions()
	opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	return opts, reader
}

// postingsHold reports whether some term of field lists doc in v.
func postingsHold(v view, field, doc string) bool {
	for k, ids := range v.terms {
		if strings.HasPrefix(k, field+"/") && strings.Contains(","+ids+",", ","+doc+",") {
			return true
		}
	}
	return false
}

// TestRemapReanalysis is the review's divergence repro: copy A tails d1
// {"brand":"Acme"} while the mapping (dynamic false) does not map brand; a mapping
// change adds brand; copy B is rebuilt after it. A rebuilds at the mapping change, so
// both index d1 under brand. A mapping change that maps a field no document holds,
// and dynamic-mode changes, rebuild nothing.
func TestRemapReanalysis(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "rm", `{"dynamic":false,"fields":{"title":"text"}}`)
		id := ShardID{Index: "rm", Shard: 0}
		optsA, readerA := meteredOptions()
		a := newCopy(t, d.open(t), id, optsA)
		a.start()
		a.waitApplied(mustApply(t, st, upsert("rm", 0, "d1", `{"title":"x","brand":"Acme"}`), upsert("rm", 0, "d2", `{"title":"y"}`)))
		remaps := func() int64 { return counterSum(t, readerA, telemetry.MetricReplicaRecoveries, "reason", reasonRemap) }

		updateMapping(t, st, "rm", func(m *schema.Mapping) { m.Fields["color"] = schema.Keyword })
		updateMapping(t, st, "rm", func(m *schema.Mapping) { m.Dynamic = schema.DynamicStrict })
		updateMapping(t, st, "rm", func(m *schema.Mapping) { m.Dynamic = schema.DynamicFalse })
		a.waitApplied(mustApply(t, st, upsert("rm", 0, "d3", `{"title":"z","color":"red"}`)))
		if n := remaps(); n != 0 {
			t.Fatalf("%d rebuilds for mapping changes no document is touched by", n)
		}

		updateMapping(t, st, "rm", func(m *schema.Mapping) { m.Fields["brand"] = schema.Keyword })
		head := mustApply(t, st, upsert("rm", 0, "d4", `{"title":"w","brand":"Beta"}`))
		va := viewOf(t, a.waitApplied(head))
		if n := remaps(); n != 1 {
			t.Fatalf("%d rebuilds for mapping brand, which d1 holds", n)
		}
		b := newCopy(t, d.open(t), id, testOptions())
		b.start()
		vb := viewOf(t, b.waitApplied(head))
		if dd := diff(va, vb, true); dd != "" {
			t.Fatalf("the tailed and the rebuilt copy differ:\n%s", dd)
		}
		if !postingsHold(va, "brand", "d1") || !postingsHold(va, "color", "d3") {
			t.Fatalf("brand or color postings miss documents: %v", va.terms)
		}
		if dd := diff(va, truthOf(t, st, id), false); dd != "" || va.mapping != truthOf(t, st, id).mapping {
			t.Fatalf("copy differs from the store:\n%s", dd)
		}
	})
}

// TestBootFaultsDoNotRebuild: a store that fails while a copy starts (catalogue, head)
// delays the start but never makes the tailer take a healthy copy for a stale one.
func TestBootFaultsDoNotRebuild(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "boot", testMapping)
		id := ShardID{Index: "boot", Shard: 0}
		first := newCopy(t, d.open(t), id, testOptions())
		first.start()
		first.waitApplied(mustApply(t, st, upsert("boot", 0, "a", docBody("a", 1))))
		if err := first.stop(); err != nil {
			t.Fatal(err)
		}
		if err := first.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}

		fs := newFaultStore(d.open(t))
		fs.inject("get", 3)
		fs.inject("head", 2)
		fs.inject("changes", 2)
		opts, reader := meteredOptions()
		var swaps atomic.Int32
		opts.OnShard = func(*shard.Shard) { swaps.Add(1) }
		c := newCopy(t, fs, id, opts)
		c.dir = first.dir
		c.start()
		sh := c.waitApplied(mustApply(t, st, upsert("boot", 0, "b", docBody("b", 2))))
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", ""); n != 0 || swaps.Load() != 0 {
			t.Fatalf("%d recoveries and %d swaps for a healthy copy", n, swaps.Load())
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("copy differs:\n%s", dd)
		}
	})
}

// TestStoreFaults: failing polls, scans and a scan cut short mid-way are retried
// (the half-loaded copy wiped) until the copy converges.
func TestStoreFaults(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "sf", testMapping)
		id := ShardID{Index: "sf", Shard: 0}
		var batch []store.Change
		for i := range 40 {
			batch = append(batch, upsert("sf", 0, "d"+strings.Repeat("x", i%3)+string(rune('a'+i%26)), docBody("x", i)))
		}
		mustApply(t, st, batch...)
		fs := newFaultStore(d.open(t))
		fs.inject("scan", 2)
		var cut atomic.Bool
		fs.onRecord = func(_ context.Context, n int, _ store.Record) error {
			if n == 15 && cut.CompareAndSwap(false, true) {
				return errInjected
			}
			return nil
		}
		c := newCopy(t, fs, id, testOptions())
		c.sopt.FlushBytes = 1 << 10 // commits mid-load: the cut-short load leaves segments
		c.start()
		c.waitApplied(mustApply(t, st, upsert("sf", 0, "late", `{}`)))
		fs.inject("changes", 5)
		fs.inject("head", 3)
		head := mustApply(t, st, del("sf", 0, "late"), upsert("sf", 0, "later", `{}`))
		if dd := diff(viewOf(t, c.waitApplied(head)), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("copy differs:\n%s", dd)
		}
		if !cut.Load() {
			t.Fatal("the scan was never cut short")
		}
	})
}

// TestCrashPoints kills a copy in the middle of a snapshot load, of a wipe, and of a
// fetch; started again over the same directory, it converges.
func TestCrashPoints(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "cp", testMapping)
		id := ShardID{Index: "cp", Shard: 0}
		var batch []store.Change
		for i := range 60 {
			batch = append(batch, upsert("cp", 0, "d"+string(rune('A'+i%26))+string(rune('a'+i/26)), docBody("x", i)))
		}
		mustApply(t, st, batch...)

		// crashAt starts c with its hooks armed, waits for reached, kills it, and
		// starts it again over the same directory with no hooks.
		crashAt := func(c *copyRunner, reached <-chan struct{}) {
			t.Helper()
			c.start()
			select {
			case <-reached:
			case <-time.After(30 * time.Second):
				t.Fatal("the crash point was never reached")
			}
			c.crashOnly()
		}
		check := func(c *copyRunner) {
			t.Helper()
			head := mustApply(t, st, upsert("cp", 0, "after", docBody("after", 1)))
			if dd := diff(viewOf(t, c.waitApplied(head)), truthOf(t, st, id), false); dd != "" {
				t.Fatalf("copy differs after the crash:\n%s", dd)
			}
		}

		t.Run("mid-load", func(t *testing.T) {
			fs := newFaultStore(d.open(t))
			reached := make(chan struct{})
			var once sync.Once
			fs.onRecord = func(ctx context.Context, n int, _ store.Record) error {
				if n == 30 {
					once.Do(func() { close(reached) })
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			}
			c := newCopy(t, fs, id, testOptions())
			c.sopt.FlushBytes = 1 << 10 // part of the load is committed, at seq 0
			crashAt(c, reached)
			fs.onRecord = nil
			c.start()
			check(c)
		})

		t.Run("mid-wipe", func(t *testing.T) {
			c := newCopy(t, d.open(t), id, testOptions())
			c.start()
			c.waitApplied(mustApply(t, st, upsert("cp", 0, "w", `{}`)))
			if err := c.stop(); err != nil {
				t.Fatal(err)
			}
			if err := c.shard().Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			head, _, err := st.HeadSeq(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			mustApply(t, st, upsert("cp", 0, "w2", `{}`))
			if err := st.Prune(context.Background(), id, head+2); err != nil { // past the copy: it must rebuild
				t.Fatal(err)
			}
			reached := make(chan struct{})
			var once sync.Once
			c.opts.hooks = &testHooks{afterDiscard: func(ctx context.Context, _ string) {
				once.Do(func() { close(reached) })
				<-ctx.Done()
			}}
			crashAt(c, reached)
			c.opts.hooks = nil
			c.start()
			check(c)
		})

		t.Run("mid-fetch", func(t *testing.T) {
			seed := newCopy(t, d.open(t), id, testOptions())
			seed.start()
			seed.waitApplied(mustApply(t, st, upsert("cp", 0, "f", `{}`)))
			if err := seed.stop(); err != nil {
				t.Fatal(err)
			}
			if err := seed.shard().Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			reached := make(chan struct{})
			opts := testOptions()
			opts.Fetcher = &partialFetcher{from: seed.dir, reached: reached}
			c := newCopy(t, d.open(t), id, opts)
			crashAt(c, reached)
			c.opts.Fetcher = nil
			c.start()
			check(c)
		})
	})
}

// partialFetcher copies a copy's files but the manifest, then hangs until cancelled:
// a fetch killed before its commit point.
type partialFetcher struct {
	from    string
	reached chan struct{}
	once    sync.Once
}

func (f *partialFetcher) Fetch(ctx context.Context, _ ShardID, dir string) error {
	entries, err := os.ReadDir(f.from)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "manifest" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(f.from, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o600); err != nil {
			return err
		}
	}
	f.once.Do(func() { close(f.reached) })
	<-ctx.Done()
	return ctx.Err()
}

// TestPruneDuringLoad: the changelog pruned past a snapshot while it loads makes the
// copy rebuild again, and it converges.
func TestPruneDuringLoad(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "pl", testMapping)
		id := ShardID{Index: "pl", Shard: 0}
		var batch []store.Change
		for i := range 30 {
			batch = append(batch, upsert("pl", 0, "d"+string(rune('a'+i)), docBody("x", i)))
		}
		mustApply(t, st, batch...)
		fs := newFaultStore(d.open(t))
		var pruned atomic.Bool
		fs.onRecord = func(_ context.Context, n int, _ store.Record) error {
			if n == 10 && pruned.CompareAndSwap(false, true) {
				_, last, err := st.Apply(ctx, []store.Change{del("pl", 0, "da"), upsert("pl", 0, "new", `{}`)})
				if err != nil {
					return err
				}
				return st.Prune(ctx, id, last+1)
			}
			return nil
		}
		opts, reader := meteredOptions()
		c := newCopy(t, fs, id, opts)
		c.start()
		head := mustApply(t, st, upsert("pl", 0, "end", `{}`))
		if dd := diff(viewOf(t, c.waitApplied(head)), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("copy differs:\n%s", dd)
		}
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "reason", reasonPruned); n == 0 || !pruned.Load() {
			t.Fatalf("%d rebuilds after the prune", n)
		}
	})
}
