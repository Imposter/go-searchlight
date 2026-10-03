package replica

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestTailerAppliesTheChangelog: documents and saved queries reach the copy, other
// shards' changes leave gaps it advances past, and the copy ends at the head.
func TestTailerAppliesTheChangelog(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "p", testMapping)
		id := ShardID{Index: "p", Shard: 0}
		mustApply(t, st, upsert("p", 0, "a", docBody("a", 1)), upsert("p", 1, "x", docBody("x", 1)))
		c := newCopy(t, d.open(t), id, testOptions())
		c.start()
		mustApply(t, st,
			upsert("p", 0, "b", docBody("b", 2)),
			queryUpsert(t, "p", 0, "q1", `{"field":"brand","op":"eq","value":"Brand 2"}`, `{"owner":"me"}`),
			queryUpsert(t, "p", 0, "q2", `{"all":[]}`, ""),
			del("p", 0, "a"))
		head := mustApply(t, st, upsert("p", 1, "y", docBody("y", 2)), queryDelete("p", 0, "q2"))
		sh := c.waitApplied(head)
		got, want := viewOf(t, sh), truthOf(t, st, id)
		if d := diff(got, want, false); d != "" {
			t.Fatalf("copy differs from the store:\n%s", d)
		}
		if len(got.docs) != 1 || len(got.queries) != 1 {
			t.Fatalf("copy holds %v and %v", got.docs, got.queries)
		}
		if seq, age := c.tailer.Lag(); seq != 0 || age != 0 {
			t.Fatalf("lag %d, %s once caught up", seq, age)
		}
		if err := c.stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if c.tailer.State() != StateIdle {
			t.Fatalf("state %s after Run", c.tailer.State())
		}
	})
}

// TestConvergence is the replication property: copies tailing one store, under random
// interleavings of document and query writes and deletes (with other shards' changes
// in between), restarts, crash-kills, and pruning that forces rebuilds from the
// store's snapshot, end identical to each other and to the store: the same documents,
// saved queries, index terms and mapping, and the same CommittedSeq.
func TestConvergence(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		seeds := 3
		if testing.Short() {
			seeds = 1
		}
		for seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) { runConvergence(t, d, uint64(seed)) })
		}
	})
}

var testQueries = []string{
	`{"field":"brand","op":"eq","value":"Brand 3"}`,
	`{"all":[{"field":"price","op":"gt","value":10},{"field":"tags","op":"has","value":"t1"}]}`,
	`{"any":[{"field":"title","op":"words_any","value":["item","v3"]},{"not":{"field":"brand","op":"exists","value":true}}]}`,
	`{"all":[]}`,
}

func runConvergence(t *testing.T, d *db, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 99))
	index := fmt.Sprintf("conv%d", seed)
	writer := d.open(t)
	createIndex(t, writer, index, testMapping)
	id := ShardID{Index: index, Shard: 0}

	reader := sdkmetric.NewManualReader()
	opts := testOptions()
	opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	copies := []*copyRunner{newCopy(t, d.open(t), id, opts), newCopy(t, d.open(t), id, opts)}
	for _, c := range copies {
		c.sopt.FlushBytes = 16 << 10 // small buffers: background refreshes, and backpressure
		c.sopt.MaxBufferFactor = 2
		c.start()
	}

	randomBody := func(docID string, n int) string {
		body := docBody(docID, n)
		if rng.IntN(4) == 0 {
			// A field no mapping has yet: the copies settle its type in the store.
			f := fmt.Sprintf("extra%d", rng.IntN(4))
			var v string
			switch rng.IntN(3) {
			case 0:
				v = fmt.Sprintf("%q", fmt.Sprintf("value %d", n))
			case 1:
				v = fmt.Sprint(n)
			default:
				v = `["a", "b"]`
			}
			body = strings.TrimSuffix(body, "}") + fmt.Sprintf(",%q:%s}", f, v)
		}
		return body
	}
	write := func() {
		n := 1 + rng.IntN(6)
		batch := make([]store.Change, 0, n)
		for range n {
			sh := 0
			if rng.IntN(4) == 0 {
				sh = 1 // another shard: a gap for the copies
			}
			k := rng.IntN(100)
			docID := fmt.Sprintf("d%d", rng.IntN(40))
			qID := fmt.Sprintf("q%d", rng.IntN(8))
			switch {
			case k < 55:
				batch = append(batch, upsert(index, sh, docID, randomBody(docID, rng.IntN(1000))))
			case k < 75:
				batch = append(batch, del(index, sh, docID))
			case k < 92:
				meta := ""
				if rng.IntN(2) == 0 {
					meta = fmt.Sprintf(`{"n":%d}`, rng.IntN(5))
				}
				batch = append(batch, queryUpsert(t, index, sh, qID, testQueries[rng.IntN(len(testQueries))], meta))
			default:
				batch = append(batch, queryDelete(index, sh, qID))
			}
		}
		mustApply(t, writer, batch...)
		for _, c := range copies {
			if rng.IntN(2) == 0 {
				c.mu.Lock()
				if c.tailer != nil {
					c.tailer.Wake()
				}
				c.mu.Unlock()
			}
		}
	}
	prune := func(stopped *copyRunner) {
		// With one copy down, the changelog is pruned past it: it must rebuild
		// from the snapshot when it comes back.
		if err := stopped.stop(); err != nil {
			t.Fatalf("tailer: %v", err)
		}
		if err := stopped.shard().Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			write()
		}
		head, err := writer.HeadSeq(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.Prune(context.Background(), id, head+1); err != nil {
			t.Fatal(err)
		}
		stopped.start()
	}

	const steps = 160
	for step := range steps {
		if step == steps/2 {
			prune(copies[rng.IntN(2)])
			continue
		}
		c := copies[rng.IntN(2)]
		switch k := rng.IntN(100); {
		case k < 70:
			write()
		case k < 77:
			c.restart()
		case k < 84:
			c.crash()
		case k < 87:
			prune(c)
		default:
			time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
		}
	}

	head, err := writer.HeadSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	truth := truthOf(t, writer, id)
	var views []view
	for i, c := range copies {
		sh := c.waitApplied(head)
		var v view
		// The mapping may still be settling into the shard from the catalogue.
		deadline := time.Now().Add(10 * time.Second)
		for v = viewOf(t, sh); v.mapping != truth.mapping && time.Now().Before(deadline); v = viewOf(t, sh) {
			c.tailer.Wake()
			_ = sh.Refresh(context.Background())
			time.Sleep(20 * time.Millisecond)
		}
		if dd := diff(v, truth, false); dd != "" {
			t.Fatalf("copy %d differs from the store:\n%s", i, dd)
		}
		if v.mapping != truth.mapping {
			t.Fatalf("copy %d mapping %s, the store's %s", i, v.mapping, truth.mapping)
		}
		views = append(views, v)
	}
	if dd := diff(views[0], views[1], true); dd != "" {
		t.Fatalf("the copies differ:\n%s", dd)
	}
	for i, c := range copies {
		if err := c.stop(); err != nil {
			t.Fatalf("copy %d: Run: %v", i, err)
		}
		sh := c.shard()
		if err := sh.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if sh.CommittedSeq() != head {
			t.Fatalf("copy %d committed at %d, want the head %d", i, sh.CommittedSeq(), head)
		}
	}
	if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "source", "sql"); n < 3 {
		t.Fatalf("%d snapshot recoveries, want at least 3 (two new copies and a prune)", n)
	}
	if n := counterSum(t, reader, telemetry.MetricReplicaHalts, "", ""); n != 0 {
		t.Fatalf("%d halts", n)
	}
}

// counterSum adds up an Int64 counter's points, those with attribute key=value when
// key is set.
func counterSum(t testing.TB, reader *sdkmetric.ManualReader, name, key, value string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T", name, m.Data)
			}
			for _, p := range sum.DataPoints {
				if v, ok := p.Attributes.Value(attrKey(key)); key == "" || (ok && v.AsString() == value) {
					n += p.Value
				}
			}
		}
	}
	return n
}

// gaugeValue returns a Float64 gauge's point (the only one, or the one with
// attribute key=value).
func gaugeValue(t testing.TB, reader *sdkmetric.ManualReader, name string) (float64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[float64])
			if !ok || len(g.DataPoints) == 0 {
				t.Fatalf("%s is %T", name, m.Data)
			}
			return g.DataPoints[len(g.DataPoints)-1].Value, true
		}
	}
	return 0, false
}

// TestIndexRecreatedIsRebuilt: a copy whose index was dropped and recreated under the
// same name is wiped and rebuilt from the new incarnation, both when it sees the new
// incarnation's changes while running and when it starts on an old directory.
func TestIndexRecreatedIsRebuilt(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "r", testMapping)
		id := ShardID{Index: "r", Shard: 0}
		opts := testOptions()
		swapped := make(chan *shard.Shard, 8)
		opts.OnShard = func(sh *shard.Shard) { swapped <- sh }
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		head := mustApply(t, st, upsert("r", 0, "old1", docBody("old1", 1)), upsert("r", 0, "old2", docBody("old2", 2)))
		old := c.waitApplied(head)
		oldUID := old.IndexUID()

		if err := st.Indexes().Drop(context.Background(), "r"); err != nil {
			t.Fatal(err)
		}
		meta := createIndex(t, st, "r", testMapping)
		if meta.UID == oldUID {
			t.Fatal("a recreated index kept its uid")
		}
		head = mustApply(t, st, upsert("r", 0, "new1", docBody("new1", 3)))
		sh := c.waitApplied(head)
		if sh == old {
			t.Fatal("the copy was not replaced")
		}
		select {
		case got := <-swapped:
			if got != sh {
				t.Fatal("OnShard got another shard")
			}
		default:
			t.Fatal("OnShard was not called")
		}
		if sh.IndexUID() != meta.UID {
			t.Fatalf("copy holds incarnation %q, want %q", sh.IndexUID(), meta.UID)
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("rebuilt copy differs:\n%s", dd)
		}

		// A copy that was down across a drop and recreate rebuilds when it starts.
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := st.Indexes().Drop(context.Background(), "r"); err != nil {
			t.Fatal(err)
		}
		meta = createIndex(t, st, "r", testMapping)
		head = mustApply(t, st, upsert("r", 0, "newer", docBody("newer", 4)))
		c.start()
		sh = c.waitApplied(head)
		if sh.IndexUID() != meta.UID {
			t.Fatalf("copy holds incarnation %q, want %q", sh.IndexUID(), meta.UID)
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("rebuilt copy differs:\n%s", dd)
		}
	})
}

// TestIndexDroppedStopsTheTailer: with the index gone and not recreated, Run stops.
func TestIndexDroppedStopsTheTailer(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "gone", testMapping)
		c := newCopy(t, d.open(t), ShardID{Index: "gone", Shard: 0}, testOptions())
		c.start()
		c.waitApplied(mustApply(t, st, upsert("gone", 0, "a", `{}`)))
		if err := st.Indexes().Drop(context.Background(), "gone"); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-c.done:
			if !errors.Is(err, ErrIndexDropped) {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not stop")
		}
		c.mu.Lock()
		c.cancel, c.done = nil, nil
		c.mu.Unlock()
	})
}

// TestApplyErrorHalts: a change the store accepted but the copy cannot apply halts it
// exactly before that change, marks it recovering in the registry, counts a halt,
// and is never skipped: a restarted tailer halts on it again.
func TestApplyErrorHalts(t *testing.T) {
	cases := []struct {
		name   string
		bad    func(t testing.TB) store.Change
		reason string
	}{
		{"strict mapping", func(testing.TB) store.Change { return upsert("h", 0, "bad", `{"title":"x","unmapped":1}`) }, ReasonDocument},
		{"query that does not parse", func(t testing.TB) store.Change {
			return queryUpsert(t, "h", 0, "bad", `{"field":"title","op":"nope","value":1}`, "")
		}, ReasonQuery},
		{"meta the shard refuses", func(t testing.TB) store.Change {
			return queryUpsert(t, "h", 0, "bad", `{"all":[]}`, `{"pad":"`+strings.Repeat("x", shard.MaxMetaBytes)+`"}`)
		}, ReasonRefused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			forEachDialect(t, func(t *testing.T, d *db) {
				ctx := context.Background()
				st := d.open(t)
				createIndex(t, st, "h", `{"dynamic":"strict","fields":{"title":"text"}}`)
				id := ShardID{Index: "h", Shard: 0}
				cp, ok, err := st.Registry().ClaimCopy(ctx, id, "node-a", 1, time.Minute)
				if err != nil || !ok {
					t.Fatalf("claim: %v %v", ok, err)
				}
				reader := sdkmetric.NewManualReader()
				opts := testOptions()
				opts.Copy = &cp
				opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
				c := newCopy(t, d.open(t), id, opts)
				c.start()
				good := mustApply(t, st, upsert("h", 0, "ok1", `{"title":"one"}`))
				c.waitApplied(good)
				waitCopyState(t, st, id, store.CopyServing)

				_, bad, err := st.Apply(ctx, []store.Change{upsert("h", 0, "ok2", `{"title":"two"}`), tc.bad(t), upsert("h", 0, "ok3", `{"title":"three"}`)})
				if err != nil {
					t.Fatal(err)
				}
				badSeq := bad - 1
				c.tailer.Wake()
				var halt *HaltError
				select {
				case err := <-c.done:
					if !errors.As(err, &halt) || !errors.Is(err, ErrHalted) {
						t.Fatalf("Run: %v", err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("the copy did not halt")
				}
				c.mu.Lock()
				c.cancel, c.done = nil, nil
				c.mu.Unlock()
				if halt.Seq != badSeq || halt.ID != "bad" || halt.Reason != tc.reason {
					t.Fatalf("halt %+v, want seq %d id bad reason %s", halt, badSeq, tc.reason)
				}
				if c.tailer.State() != StateHalted || c.tailer.Applied() != badSeq-1 {
					t.Fatalf("state %s applied %d, want halted at %d", c.tailer.State(), c.tailer.Applied(), badSeq-1)
				}
				waitCopyState(t, st, id, store.CopyRecovering)
				if n := counterSum(t, reader, telemetry.MetricReplicaHalts, "reason", tc.reason); n != 1 {
					t.Fatalf("%d halts counted", n)
				}
				sh := c.shard()
				if err := sh.Refresh(ctx); err != nil {
					t.Fatal(err)
				}
				v := viewOf(t, sh)
				if _, ok := v.docs["ok2"]; !ok || len(v.docs) != 2 || len(v.queries) != 0 {
					t.Fatalf("halted copy holds %v %v: want ok1 and ok2 only", v.docs, v.queries)
				}
				// Never skipped: a tailer started again stops on the same change.
				c.run(sh)
				select {
				case err := <-c.done:
					if !errors.As(err, &halt) || halt.Seq != badSeq {
						t.Fatalf("restarted Run: %v", err)
					}
				case <-time.After(30 * time.Second):
					t.Fatal("the restarted copy did not halt")
				}
				c.mu.Lock()
				c.cancel, c.done = nil, nil
				c.mu.Unlock()
			})
		})
	}
}

func waitCopyState(t testing.TB, st store.Store, id ShardID, want store.CopyState) store.Copy {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		copies, err := st.Registry().Copies(context.Background(), id.Index)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range copies {
			if c.Shard == id && c.State == want {
				return c
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("copies %+v, want one %s", copies, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestBackpressureIsRetried: a write buffer that is always full makes the tailer
// refresh and retry, never halt, until everything is applied.
func TestBackpressureIsRetried(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "bp", testMapping)
		id := ShardID{Index: "bp", Shard: 0}
		reader := sdkmetric.NewManualReader()
		opts := testOptions()
		opts.BatchSize = 7
		opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
		c := newCopy(t, d.open(t), id, opts)
		c.sopt.RefreshInterval = -1 // only the tailer's refreshes drain it
		c.sopt.FlushBytes = 1       // full after every batch
		c.sopt.MaxBufferFactor = 1
		c.start()
		var batch []store.Change
		for i := range 60 {
			batch = append(batch, upsert("bp", 0, fmt.Sprintf("d%d", i), docBody(fmt.Sprint(i), i)))
		}
		head := mustApply(t, st, batch...)
		sh := c.waitApplied(head)
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("copy differs:\n%s", dd)
		}
		if n := counterSum(t, reader, telemetry.MetricReplicaBackpressure, "", ""); n == 0 {
			t.Fatal("no backpressure was met")
		}
		if n := counterSum(t, reader, telemetry.MetricReplicaHalts, "", ""); n != 0 {
			t.Fatalf("%d halts", n)
		}
	})
}

// TestLeaseLostStopsTheTailer: the copy reports its durable seq and goes serving
// under its lease; once another node takes the slot, the tailer stops.
func TestLeaseLostStopsTheTailer(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "l", testMapping)
		id := ShardID{Index: "l", Shard: 0}
		cp, ok, err := st.Registry().ClaimCopy(ctx, id, "node-a", 1, 2*time.Second)
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		opts := testOptions()
		opts.Copy = &cp
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		head := mustApply(t, st, upsert("l", 0, "a", docBody("a", 1)))
		c.waitApplied(head)
		got := waitCopyState(t, st, id, store.CopyServing)
		deadline := time.Now().Add(30 * time.Second)
		for got.AppliedSeq < head && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
			got = waitCopyState(t, st, id, store.CopyServing)
		}
		if got.AppliedSeq < head {
			t.Fatalf("reported applied seq %d, want %d", got.AppliedSeq, head)
		}

		// The lease runs out; node-b takes the slot.
		for {
			if _, ok, err := st.Registry().ClaimCopy(ctx, id, "node-b", 1, time.Minute); err != nil {
				t.Fatal(err)
			} else if ok {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		mustApply(t, st, upsert("l", 0, "b", docBody("b", 2)))
		select {
		case err := <-c.done:
			if !errors.Is(err, store.ErrLeaseLost) {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the tailer kept running without its lease")
		}
		c.mu.Lock()
		c.cancel, c.done = nil, nil
		c.mu.Unlock()
		applied := c.tailer.Applied()
		mustApply(t, st, upsert("l", 0, "c", docBody("c", 3)))
		time.Sleep(50 * time.Millisecond)
		if c.tailer.Applied() != applied {
			t.Fatal("the tailer applied after losing its lease")
		}
	})
}

// TestLagMetrics: while behind, the lag in changes and in time is reported; caught
// up, both are zero.
func TestLagMetrics(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "lag", testMapping)
		id := ShardID{Index: "lag", Shard: 0}
		mustApply(t, st, upsert("lag", 0, "first", `{}`))
		sh := openShard(t, t.TempDir(), testShardOptions(id))
		t.Cleanup(func() { _ = sh.Close(context.Background()) })
		reader := sdkmetric.NewManualReader()
		opts := testOptions()
		opts.BatchSize = 10
		opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
		tl := NewTailer(st, sh, id, opts)
		// Recover the empty copy, then drive single polls by hand.
		if err := tl.handle(ctx, tl.start(ctx)); err != nil {
			t.Fatal(err)
		}
		if err := tl.recoverIfNeeded(ctx); err != nil {
			t.Fatal(err)
		}
		var batch []store.Change
		for i := range 35 {
			batch = append(batch, upsert("lag", 0, fmt.Sprintf("d%d", i), `{}`))
		}
		head := mustApply(t, st, batch...)
		time.Sleep(60 * time.Millisecond) // the backlog ages
		caughtUp, err := tl.step(ctx)
		if err != nil || caughtUp {
			t.Fatalf("step: %v, caught up %v", err, caughtUp)
		}
		seq, age := tl.Lag()
		if seq != 35 || age < 50*time.Millisecond {
			t.Fatalf("lag %d, %s; want 35 and at least 50ms", seq, age)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaLagSeq); v != 35 {
			t.Fatalf("lag.seq gauge %g", v)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaLagTime); v < 0.05 {
			t.Fatalf("lag.time gauge %g", v)
		}
		for !caughtUp {
			if caughtUp, err = tl.step(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if seq, age := tl.Lag(); seq != 0 || age != 0 || tl.Applied() != head {
			t.Fatalf("caught up: lag %d %s, applied %d of %d", seq, age, tl.Applied(), head)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaLagSeq); v != 0 {
			t.Fatalf("lag.seq gauge %g once caught up", v)
		}
		if v, _ := gaugeValue(t, reader, telemetry.MetricReplicaLagTime); v != 0 {
			t.Fatalf("lag.time gauge %g once caught up", v)
		}
	})
}

// TestRecover covers the standalone Recover: an empty copy loads the snapshot, a copy
// with changes resumes, and a half-loaded or stale one must be wiped.
func TestRecover(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "rc", testMapping)
		id := ShardID{Index: "rc", Shard: 0}
		mustApply(t, st, upsert("rc", 0, "a", docBody("a", 1)), upsert("rc", 0, "b", docBody("b", 2)),
			queryUpsert(t, "rc", 0, "q", `{"field":"brand","op":"eq","value":"Brand 1"}`, ""))
		head := mustApply(t, st, del("rc", 0, "b"), upsert("rc", 1, "z", `{}`))
		dir := t.TempDir()
		sh := openShard(t, dir, testShardOptions(id))
		seq, err := Recover(ctx, sh, st, id)
		if err != nil || seq != head {
			t.Fatalf("Recover = %d, %v; want %d", seq, err, head)
		}
		if err := sh.WaitRefreshed(ctx, head); err != nil {
			t.Fatal(err)
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("recovered copy differs:\n%s", dd)
		}
		if seq, err := Recover(ctx, sh, st, id); err != nil || seq != head {
			t.Fatalf("Recover of a loaded copy = %d, %v", seq, err)
		}
		if err := sh.Close(ctx); err != nil {
			t.Fatal(err)
		}

		// Half loaded: segments committed at seq 0.
		half := openShard(t, t.TempDir(), testShardOptions(id))
		defer half.Close(ctx)
		doc := upsertChange(t, "x", 5)
		if err := half.Load(ctx, []shard.Change{doc}); err != nil {
			t.Fatal(err)
		}
		if err := half.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := Recover(ctx, half, st, id); !errors.Is(err, ErrWipeNeeded) {
			t.Fatalf("Recover of a half-loaded copy: %v", err)
		}

		// Another incarnation.
		old := openShard(t, dir, testShardOptions(id))
		defer old.Close(ctx)
		if err := st.Indexes().Drop(ctx, "rc"); err != nil {
			t.Fatal(err)
		}
		createIndex(t, st, "rc", testMapping)
		if _, err := Recover(ctx, old, st, id); !errors.Is(err, ErrWipeNeeded) {
			t.Fatalf("Recover of a stale incarnation: %v", err)
		}
	})
}

func upsertChange(t testing.TB, id string, seq int64) shard.Change {
	t.Helper()
	m, err := parseMapping([]byte(testMapping))
	if err != nil {
		t.Fatal(err)
	}
	tl := &Tailer{cat: catalog{mapping: m}}
	out, err := tl.convert(context.Background(), []item{{seq: seq, kind: shard.Upsert, id: id, body: []byte(docBody(id, int(seq)))}})
	if err != nil {
		t.Fatal(err)
	}
	return out[0]
}

// dirFetcher fetches a copy by copying another copy's closed directory, as a peer
// recovery would.
type dirFetcher struct {
	from  string
	fail  bool
	calls int
}

func (f *dirFetcher) Fetch(_ context.Context, _ ShardID, dir string) error {
	f.calls++
	if f.fail {
		return errors.New("no peer")
	}
	return copyDir(f.from, dir)
}

// TestFetcherBeforeSnapshot: a new copy takes a fetched copy and only replays the
// changelog past it; a failing fetch falls back to the snapshot.
func TestFetcherBeforeSnapshot(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "f", testMapping)
		id := ShardID{Index: "f", Shard: 0}
		seed := newCopy(t, d.open(t), id, testOptions())
		seed.start()
		seed.waitApplied(mustApply(t, st, upsert("f", 0, "a", docBody("a", 1)), upsert("f", 0, "b", docBody("b", 2))))
		if err := seed.stop(); err != nil {
			t.Fatal(err)
		}
		if err := seed.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		head := mustApply(t, st, upsert("f", 0, "c", docBody("c", 3)), del("f", 0, "a"))

		for _, fail := range []bool{false, true} {
			reader := sdkmetric.NewManualReader()
			opts := testOptions()
			f := &dirFetcher{from: seed.dir, fail: fail}
			opts.Fetcher = f
			opts.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			c := newCopy(t, d.open(t), id, opts)
			c.start()
			sh := c.waitApplied(head)
			if f.calls != 1 {
				t.Fatalf("fetch called %d times", f.calls)
			}
			if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
				t.Fatalf("fetched copy differs:\n%s", dd)
			}
			peer := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "source", "peer")
			sql := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "source", "sql")
			if (!fail && (peer != 1 || sql != 0)) || (fail && sql != 1) {
				t.Fatalf("fail=%v: %d peer and %d sql recoveries", fail, peer, sql)
			}
		}
	})
}
