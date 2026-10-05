package replica

import (
	"context"
	"errors"
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
		c.sopt.RefreshBytes = 1 << 10 // refreshes mid-load, and flushes persist them: the cut-short load leaves segments
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
			c.sopt.RefreshBytes = 1 << 10 // part of the load is refreshed, and flushed at seq 0
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
			c.opts.hooks = &testHooks{inPlace: true, afterDiscard: func(ctx context.Context, _ string) {
				once.Do(func() { close(reached) })
				<-ctx.Done()
			}}
			crashAt(c, reached)
			c.opts.hooks = nil
			c.start()
			check(c)
		})

		t.Run("mid-aside", func(t *testing.T) {
			// A copy pruned past is rebuilt aside: a crash just before the swap
			// leaves the old copy current; the next open removes the half-built
			// one, and the copy rebuilds again and converges.
			c := newCopy(t, d.open(t), id, testOptions())
			c.start()
			c.waitApplied(mustApply(t, st, upsert("cp", 0, "a", `{}`)))
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
			mustApply(t, st, upsert("cp", 0, "a2", `{}`))
			if err := st.Prune(context.Background(), id, head+2); err != nil {
				t.Fatal(err)
			}
			reached := make(chan struct{})
			var once sync.Once
			var built string
			c.opts.hooks = &testHooks{beforeSwap: func(ctx context.Context, dir string) error {
				built = dir
				once.Do(func() { close(reached) })
				<-ctx.Done()
				return ctx.Err()
			}}
			crashAt(c, reached)
			if cur, err := CopyDir(c.dir); err != nil || cur != c.dir {
				t.Fatalf("after a crash mid-build the current copy is %q (%v), want the old one", cur, err)
			}
			c.opts.hooks = nil
			c.start()
			if _, err := os.Stat(built); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the half-built copy %s survived the reopen: %v", built, err)
			}
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

func (f *partialFetcher) Fetch(ctx context.Context, _ ShardID, dir string) (string, error) {
	entries, err := os.ReadDir(f.from)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "manifest" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(f.from, e.Name()))
		if err != nil {
			return "", err
		}
		if err := os.WriteFile(filepath.Join(dir, e.Name()), data, 0o600); err != nil {
			return "", err
		}
	}
	f.once.Do(func() { close(f.reached) })
	<-ctx.Done()
	return "", ctx.Err()
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
		mustApply(t, st, upsert("pl", 0, "end", `{}`))
		// The hook writes too: wait for it, then for everything there is.
		deadline := time.Now().Add(30 * time.Second)
		for !pruned.Load() {
			if time.Now().After(deadline) {
				t.Fatal("the load never reached the prune")
			}
			time.Sleep(5 * time.Millisecond)
		}
		head, _, err := st.HeadSeq(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if dd := diff(viewOf(t, c.waitApplied(head)), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("copy differs:\n%s", dd)
		}
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "reason", reasonPruned); n == 0 || !pruned.Load() {
			t.Fatalf("%d rebuilds after the prune", n)
		}
	})
}

// TestLagWhileTheDatabaseIsUnreachable is the partition case: a caught-up copy loses
// the database while other nodes may keep writing. It cannot see what it misses, so
// its lag in time grows from its last good poll, past max_lag, and the poll.failing
// gauge says why; reachable again, both go back to zero.
func TestLagWhileTheDatabaseIsUnreachable(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "part", testMapping)
		id := ShardID{Index: "part", Shard: 0}
		fs := newFaultStore(d.open(t))
		opts, reader := meteredOptions()
		opts.MaxLag = 200 * time.Millisecond
		c := newCopy(t, fs, id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("part", 0, "a", `{}`)))
		if _, age := c.tailer.Lag(); age != 0 {
			t.Fatalf("lag %s when caught up", age)
		}

		fs.down("head", true)
		fs.down("changes", true)
		mustApply(t, st, upsert("part", 0, "b", `{}`)) // another node writes
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, age := c.tailer.Lag()
			if age > 2*opts.MaxLag && c.tailer.PollFailing() {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("lag %s with the database unreachable; want past max_lag %s", age, opts.MaxLag)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaLagTime); v <= opts.MaxLag.Seconds() {
			t.Fatalf("lag.time gauge %g", v)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaPollFailing); v != 1 {
			t.Fatalf("poll.failing gauge %g", v)
		}

		fs.down("head", false)
		fs.down("changes", false)
		head, _, err := st.HeadSeq(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		c.waitApplied(head)
		deadline = time.Now().Add(30 * time.Second)
		for c.tailer.PollFailing() || lagAge(c.tailer) != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("lag %s, failing %v once reachable", lagAge(c.tailer), c.tailer.PollFailing())
			}
			c.tailer.Wake()
			time.Sleep(10 * time.Millisecond)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaPollFailing); v != 0 {
			t.Fatalf("poll.failing gauge %g once reachable", v)
		}
	})
}

func lagAge(tl *Tailer) time.Duration {
	_, age := tl.Lag()
	return age
}

// TestRemapDebounce: a burst of mapping changes that each map a field live documents
// hold costs one rebuild, at the end of the burst, and the copy ends right. Each change
// after the first lands while the copy waits out its debounce window, which the test
// ends by advancing the tailer's clock.
func TestRemapDebounce(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "db", `{"dynamic":false,"fields":{"title":"text"}}`)
		id := ShardID{Index: "db", Shard: 0}
		opts, reader := meteredOptions()
		const debounce = 400 * time.Millisecond
		opts.RemapDebounce = debounce
		c := newCopy(t, d.open(t), id, opts)
		clk := c.withFakeClock(opts.PollInterval)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("db", 0, "d1", `{"title":"x","brand":"A","color":"red","size":3,"weight":1}`)))
		for i, f := range []struct {
			name string
			typ  schema.FieldType
		}{{"brand", schema.Keyword}, {"color", schema.Keyword}, {"size", schema.Number}, {"weight", schema.Number}} {
			updateMapping(t, st, "db", func(m *schema.Mapping) { m.Fields[f.name] = f.typ })
			if i == 0 {
				c.current().Wake()
			} else {
				clk.Advance(debounce)
			}
			if err := clk.BlockUntilArmed(tctx(t), debounce); err != nil {
				t.Fatalf("mapping change %d: the copy is not waiting out a debounce window: %v", i+1, err)
			}
		}
		clk.Advance(debounce)
		head := mustApply(t, st, upsert("db", 0, "d2", `{"title":"y","brand":"B"}`))
		v := viewOf(t, c.waitApplied(head))
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "reason", reasonRemap); n != 1 {
			t.Fatalf("%d rebuilds for a burst of 4 mapping changes; want 1", n)
		}
		fresh := newCopy(t, d.open(t), id, testOptions())
		fresh.start()
		if dd := diff(v, viewOf(t, fresh.waitApplied(head)), true); dd != "" {
			t.Fatalf("the debounced copy differs from a fresh one:\n%s", dd)
		}
		if !postingsHold(v, "brand", "d1") || !postingsHold(v, "color", "d1") ||
			!strings.Contains(v.terms["num/size"], "d1=3") || !strings.Contains(v.terms["num/weight"], "d1=1") {
			t.Fatalf("d1 is not indexed under every field the burst mapped: %v", v.terms)
		}
	})
}
