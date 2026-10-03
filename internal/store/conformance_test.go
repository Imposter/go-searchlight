package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// The conformance suite runs every test on SQLite, and on Postgres and MySQL
// when SEARCHLIGHT_TEST_PG_URL / SEARCHLIGHT_TEST_MYSQL_URL are set.

func TestMigrate(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		// Several nodes migrating a fresh database at once.
		stores := make([]Store, 4)
		for i := range stores {
			st, err := Open(ctx, h.url, quiet)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			stores[i] = st
		}
		var wg sync.WaitGroup
		errs := make([]error, len(stores))
		for i, st := range stores {
			wg.Go(func() { errs[i] = st.Migrate(ctx) })
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("node %d migrate: %v", i, err)
			}
		}
		st := stores[0]
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("second migrate: %v", err)
		}
		ms, err := loadMigrations(engine(st).d.Migrations)
		if err != nil {
			t.Fatal(err)
		}
		if got := countRows(t, st, "SELECT COUNT(*) FROM sl_schema_migrations"); got != len(ms) {
			t.Fatalf("version table has %d rows, want %d", got, len(ms))
		}
		if got := counterValue(t, st); got != 0 {
			t.Fatalf("fresh counter = %d", got)
		}

		// A database migrated by a newer binary is refused.
		s := engine(st)
		if _, err := s.w.ExecContext(ctx, s.bind("INSERT INTO sl_schema_migrations (version, name, applied_at) VALUES (?, ?, 0)"), 9999, "future"); err != nil {
			t.Fatal(err)
		}
		if err := st.Migrate(ctx); !errors.Is(err, ErrNewerSchema) {
			t.Fatalf("migrate with a newer schema: %v, want ErrNewerSchema", err)
		}
	})
}

func TestApplyAndRead(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "products")
		s0 := ShardID{Index: "products", Shard: 0}
		s1 := ShardID{Index: "products", Shard: 1}

		qp, err := EncodeQueryPayload(json.RawMessage(`{"eq":{"brand":"acme"}}`), json.RawMessage(`{"search_id":7}`))
		if err != nil {
			t.Fatal(err)
		}
		long := strings.Repeat("ß", 256) // 512 bytes of non-ASCII: the longest id
		first, last := mustApply(t, st,
			upsert("products", 0, "a", `{"name":"Straße","n":1}`),
			upsert("products", 0, "b", `{"name":"b"}`),
			upsert("products", 1, "c", `{"name":"c"}`),
			Change{Index: "products", Shard: 0, Kind: KindQueryUpsert, ID: "q1", Payload: qp},
			upsert("products", 0, long, `{"İ":"ﬁ"}`),
		)
		if first != 1 || last != 5 {
			t.Fatalf("first batch got %d..%d, want 1..5", first, last)
		}
		f2, l2 := mustApply(t, st,
			upsert("products", 0, "a", `{"name":"a2"}`),
			del("products", 0, "b"),
			upsert("products", 0, "x", `{}`),
			del("products", 0, "x"), // upsert then delete in one batch: gone
			Change{Index: "products", Shard: 0, Kind: KindQueryDelete, ID: "nope"},
		)
		if f2 != 6 || l2 != 10 {
			t.Fatalf("second batch got %d..%d, want 6..10", f2, l2)
		}

		ch := allChanges(t, st, s0)
		var seqs []int64
		for _, c := range ch {
			seqs = append(seqs, c.Seq)
		}
		if want := []int64{1, 2, 4, 5, 6, 7, 8, 9, 10}; fmt.Sprint(seqs) != fmt.Sprint(want) {
			t.Fatalf("shard 0 seqs %v, want %v", seqs, want)
		}
		if c := ch[0]; c.Kind != KindUpsert || c.ID != "a" || string(c.Payload) != `{"name":"Straße","n":1}` || c.At.IsZero() {
			t.Fatalf("first change %+v", c)
		}
		if c := ch[3]; c.ID != long || string(c.Payload) != `{"İ":"ﬁ"}` {
			t.Fatalf("non-ASCII change %+v", c)
		}
		if c := ch[5]; c.Kind != KindDelete || c.Payload != nil {
			t.Fatalf("delete change %+v", c)
		}
		if at := ch[0].At; time.Since(at).Abs() > time.Hour {
			t.Fatalf("change time %s is not now", at)
		}
		// Paging with a limit.
		page, err := st.ChangesAfter(ctx, s0, 2, 2)
		if err != nil || len(page) != 2 || page[0].Seq != 4 || page[1].Seq != 5 {
			t.Fatalf("page after 2: %v %v", page, err)
		}

		recs, asOf := scanAll(t, st, s0)
		if asOf != 10 {
			t.Fatalf("asOf = %d, want 10", asOf)
		}
		if len(recs) != 3 {
			t.Fatalf("shard 0 records %v", recs)
		}
		if r := recs["d:a"]; string(r.Body) != `{"name":"a2"}` || r.Seq != 6 || r.Kind != RecordDocument {
			t.Fatalf("doc a %+v", r)
		}
		if r := recs["d:"+long]; r.Seq != 5 {
			t.Fatalf("long doc %+v", r)
		}
		if r := recs["q:q1"]; string(r.Body) != `{"eq":{"brand":"acme"}}` || string(r.Meta) != `{"search_id":7}` || r.Seq != 4 {
			t.Fatalf("query q1 %+v", r)
		}
		recs1, _ := scanAll(t, st, s1)
		if len(recs1) != 1 || recs1["d:c"].Seq != 3 {
			t.Fatalf("shard 1 records %v", recs1)
		}

		// Bad input is refused before anything is written.
		for name, c := range map[string]Change{
			"bad json":     upsert("products", 0, "z", `{`),
			"empty id":     upsert("products", 0, "", `{}`),
			"long id":      upsert("products", 0, strings.Repeat("x", 513), `{}`),
			"nul id":       upsert("products", 0, "a\x00b", `{}`),
			"kind":         {Index: "products", Kind: "nope", ID: "z"},
			"delete body":  {Index: "products", Kind: KindDelete, ID: "z", Payload: []byte(`{}`)},
			"query no dsl": {Index: "products", Kind: KindQueryUpsert, ID: "z", Payload: []byte(`{"meta":{}}`)},
			"bad utf8":     upsert("products", 0, "z", "{\"x\":\"\xff\"}"),
		} {
			if _, _, err := st.Apply(ctx, []Change{c}); !errors.Is(err, ErrInvalid) {
				t.Errorf("%s: %v, want ErrInvalid", name, err)
			}
		}
		var nf *IndexNotFoundError
		_, _, err = st.Apply(ctx, []Change{upsert("products", 0, "z", `{}`), upsert("missing", 0, "z", `{}`), upsert("missing", 1, "y", `{}`)})
		if !errors.As(err, &nf) || fmt.Sprint(nf.Positions) != "[1 2]" || nf.Indexes[0] != "missing" || !errors.Is(err, ErrNotFound) {
			t.Errorf("missing index: %v, want an IndexNotFoundError at [1 2]", err)
		}
		// An invalid change is reported by position.
		var che *ChangeError
		if _, _, err := st.Apply(ctx, []Change{upsert("products", 0, "z", `{}`), upsert("products", 0, "", `{}`)}); !errors.As(err, &che) || che.Position != 1 {
			t.Errorf("invalid change: %v, want a ChangeError at 1", err)
		}
		if got := counterValue(t, st); got != 10 {
			t.Fatalf("counter = %d after refused batches, want 10", got)
		}
		if f, l, err := st.Apply(ctx, nil); f != 0 || l != 0 || err != nil {
			t.Fatalf("empty apply: %d %d %v", f, l, err)
		}
	})
}

// TestBigBatch covers multi-statement inserts (more rows than one INSERT
// carries) and in-batch rewrites of the same document.
func TestBigBatch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st := h.open(t)
		mustCreateIndex(t, st, "big")
		var batch []Change
		for i := range 1234 {
			batch = append(batch, upsert("big", 0, fmt.Sprintf("d%d", i%700), fmt.Sprintf(`{"v":%d}`, i)))
		}
		for i := range 100 {
			batch = append(batch, del("big", 0, fmt.Sprintf("d%d", i)))
		}
		first, last := mustApply(t, st, batch...)
		if first != 1 || last != int64(len(batch)) {
			t.Fatalf("got %d..%d", first, last)
		}
		recs, _ := scanAll(t, st, ShardID{Index: "big"})
		if len(recs) != 600 {
			t.Fatalf("%d documents, want 600", len(recs))
		}
		// d500 was written by changes 500 and 1200 (0-based); 1200 wins.
		if r := recs["d:d500"]; string(r.Body) != `{"v":1200}` || r.Seq != 1201 {
			t.Fatalf("d500 = %+v", r)
		}
		if n := len(allChanges(t, st, ShardID{Index: "big"})); n != len(batch) {
			t.Fatalf("%d changes, want %d", n, len(batch))
		}
	})
}

// writeLoad has writers on several nodes apply random batches over three
// shards and returns each batch's seq range and changes.
type applied struct {
	first, last int64
	batch       []Change
}

func writeLoad(t *testing.T, appliers []Applier, writersPerNode, batchesPerWriter int) []applied {
	t.Helper()
	var mu sync.Mutex
	var out []applied
	var wg sync.WaitGroup
	errc := make(chan error, len(appliers)*writersPerNode)
	for n, ap := range appliers {
		for w := range writersPerNode {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(n), uint64(w)))
				for b := range batchesPerWriter {
					size := 1 + rng.IntN(8)
					batch := make([]Change, size)
					for i := range batch {
						id := fmt.Sprintf("n%d-w%d-b%d-i%d", n, w, b, i)
						batch[i] = upsert("load", rng.IntN(3), id, fmt.Sprintf(`{"id":%q}`, id))
					}
					first, last, err := ap.Apply(context.Background(), batch)
					if err != nil {
						errc <- fmt.Errorf("node %d writer %d: %w", n, w, err)
						return
					}
					mu.Lock()
					out = append(out, applied{first, last, batch})
					mu.Unlock()
				}
			})
		}
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	return out
}

// checkLoad verifies that the ranges tile 1..N with no gap or overlap and
// that every shard's changelog holds exactly the changes in seq order.
func checkLoad(t *testing.T, st Store, got []applied) {
	t.Helper()
	sort.Slice(got, func(i, j int) bool { return got[i].first < got[j].first })
	next := int64(1)
	bySeq := make(map[int64]Change)
	for _, a := range got {
		if a.first != next || a.last != a.first+int64(len(a.batch))-1 {
			t.Fatalf("range %d..%d for %d changes, expected to start at %d", a.first, a.last, len(a.batch), next)
		}
		for i := range a.batch {
			bySeq[a.first+int64(i)] = a.batch[i]
		}
		next = a.last + 1
	}
	total := 0
	for shard := range 3 {
		var prev int64
		changes := allChanges(t, st, ShardID{Index: "load", Shard: shard})
		for i := range changes {
			c := &changes[i]
			if c.Seq <= prev {
				t.Fatalf("shard %d: seq %d after %d", shard, c.Seq, prev)
			}
			prev = c.Seq
			want, ok := bySeq[c.Seq]
			if !ok || want.ID != c.ID || want.Shard != shard || !bytes.Equal(want.Payload, c.Payload) {
				t.Fatalf("shard %d seq %d: got %s, want %+v", shard, c.Seq, c.ID, want)
			}
			total++
		}
	}
	if total != len(bySeq) {
		t.Fatalf("changelog holds %d changes, writers applied %d", total, len(bySeq))
	}
}

// TestConcurrentWriters is Review Focus 3: 32 writers on four nodes produce
// contiguous, in-order sequence numbers with nothing lost or duplicated.
func TestConcurrentWriters(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		nodes := make([]Applier, 4)
		var first Store
		for i := range nodes {
			st := h.open(t)
			if first == nil {
				first = st
				mustCreateIndex(t, st, "load")
			}
			nodes[i] = st
		}
		checkLoad(t, first, writeLoad(t, nodes, 8, 15))
	})
}

// TestGroupCommitConcurrentWriters runs the same load through a
// GroupCommitter on each of two nodes.
func TestGroupCommitConcurrentWriters(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		var gcs []*GroupCommitter
		var appliers []Applier
		var first Store
		for range 2 {
			st := h.open(t)
			if first == nil {
				first = st
				mustCreateIndex(t, st, "load")
			}
			gc := NewGroupCommitter(st, GroupCommitOptions{})
			t.Cleanup(func() { _ = gc.Close() })
			gcs = append(gcs, gc)
			appliers = append(appliers, gc)
		}
		got := writeLoad(t, appliers, 16, 15)
		checkLoad(t, first, got)
		flushes := gcs[0].Flushes() + gcs[1].Flushes()
		if flushes >= int64(len(got)) {
			t.Fatalf("%d transactions for %d requests: nothing was coalesced", flushes, len(got))
		}
		t.Logf("%s: %d requests in %d transactions", h.dialect, len(got), flushes)
	})
}

// TestLateCommitNotSkipped holds one transaction open after it has taken
// its seqs, starts a second, faster writer on another node, and tails the
// shard throughout: the tailer must see the slow change before the fast one,
// never skipping it.
func TestLateCommitNotSkipped(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		slow, fast, tail := h.open(t), h.open(t), h.open(t)
		mustCreateIndex(t, slow, "late")
		shard := ShardID{Index: "late"}
		mustApply(t, slow, upsert("late", 0, "seed", `{}`))

		paused, release := make(chan int64), make(chan struct{})
		var pauseNext atomic.Bool
		engine(slow).beforeCommit = func(_ context.Context, first, _ int64) {
			if pauseNext.CompareAndSwap(true, false) {
				paused <- first
				<-release
			}
		}

		// The tailer polls continuously and records every seq it sees.
		var seen []int64
		var tailErr error
		stopTail := make(chan struct{})
		tailDone := make(chan struct{})
		go func() {
			defer close(tailDone)
			after := int64(1)
			for {
				// Decide before polling, so the last poll starts after both
				// commits.
				stopping := false
				select {
				case <-stopTail:
					stopping = true
				default:
				}
				page, err := tail.ChangesAfter(ctx, shard, after, 100)
				if err != nil {
					tailErr = err
					return
				}
				for _, c := range page {
					seen = append(seen, c.Seq)
					after = c.Seq
				}
				if stopping && len(page) == 0 {
					return
				}
			}
		}()

		pauseNext.Store(true)
		slowDone := make(chan [2]int64, 1)
		go func() {
			f, l, err := slow.Apply(ctx, []Change{upsert("late", 0, "slow", `{}`)})
			if err != nil {
				t.Errorf("slow apply: %v", err)
			}
			slowDone <- [2]int64{f, l}
		}()
		slowSeq := <-paused

		fastDone := make(chan [2]int64, 1)
		go func() {
			f, l, err := fast.Apply(ctx, []Change{upsert("late", 0, "fast", `{}`)})
			if err != nil {
				t.Errorf("fast apply: %v", err)
			}
			fastDone <- [2]int64{f, l}
		}()
		select {
		case r := <-fastDone:
			t.Fatalf("fast writer committed seq %d while the slow one held the counter", r[0])
		case <-time.After(300 * time.Millisecond):
		}
		close(release)
		s := <-slowDone
		f := <-fastDone
		if s[0] != slowSeq || f[0] != slowSeq+1 {
			t.Fatalf("slow got %d, fast got %d; want %d then %d", s[0], f[0], slowSeq, slowSeq+1)
		}
		close(stopTail)
		<-tailDone
		if tailErr != nil {
			t.Fatal(tailErr)
		}
		if fmt.Sprint(seen) != fmt.Sprint([]int64{slowSeq, slowSeq + 1}) {
			t.Fatalf("tailer saw %v, want [%d %d]", seen, slowSeq, slowSeq+1)
		}
	})
}

// TestTailerNeverSkips is the randomized form: writers on three nodes hold
// their transactions open for random times before committing while a tailer
// polls one shard, and every seq the tailer sees must follow the previous
// one exactly.
func TestTailerNeverSkips(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		var nodes []Store
		for range 3 {
			st := h.open(t)
			var n atomic.Uint64
			engine(st).beforeCommit = func(context.Context, int64, int64) {
				time.Sleep(time.Duration(n.Add(7)%5) * time.Millisecond)
			}
			nodes = append(nodes, st)
		}
		mustCreateIndex(t, nodes[0], "tail")
		tail := h.open(t)
		shard := ShardID{Index: "tail"}

		const writers, perWriter = 12, 10
		stop := make(chan struct{})
		tailErr := make(chan error, 1)
		go func() {
			after := int64(0)
			for {
				page, err := tail.ChangesAfter(ctx, shard, after, 7)
				if err != nil {
					tailErr <- err
					return
				}
				for _, c := range page {
					if c.Seq != after+1 {
						tailErr <- fmt.Errorf("tailer saw seq %d right after %d", c.Seq, after)
						return
					}
					after = c.Seq
				}
				if after == writers*perWriter {
					tailErr <- nil
					return
				}
				select {
				case <-stop:
					tailErr <- fmt.Errorf("tailer stopped at %d", after)
					return
				default:
				}
			}
		}()
		var wg sync.WaitGroup
		for w := range writers {
			wg.Go(func() {
				for i := range perWriter {
					if _, _, err := nodes[w%3].Apply(ctx, []Change{upsert("tail", 0, fmt.Sprintf("w%d-%d", w, i), `{}`)}); err != nil {
						t.Errorf("apply: %v", err)
						return
					}
				}
			})
		}
		wg.Wait()
		select {
		case err := <-tailErr:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(30 * time.Second):
			close(stop)
			t.Fatal(<-tailErr)
		}
	})
}

func TestScanShardSnapshot(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, writer := h.open(t), h.open(t)
		mustCreateIndex(t, st, "snap")
		shard := ShardID{Index: "snap"}
		for i := range 5 {
			mustApply(t, st, upsert("snap", 0, fmt.Sprintf("d%d", i), `{"v":1}`))
		}
		var lateSeq int64
		var ids []string
		asOf, err := st.ScanShard(ctx, shard, func(r Record) error {
			if r.Kind == RecordMapping {
				return nil
			}
			if lateSeq == 0 {
				// Commit more changes mid-scan; the scan must not see them.
				f, _, err := writer.Apply(ctx, []Change{upsert("snap", 0, "d9", `{}`), upsert("snap", 0, "d0", `{"v":2}`)})
				if err != nil {
					return err
				}
				lateSeq = f
			}
			if r.ID == "d0" && string(r.Body) != `{"v":1}` {
				t.Errorf("scan saw d0 = %s from after its snapshot", r.Body)
			}
			ids = append(ids, r.ID)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if asOf != 5 || lateSeq != 6 {
			t.Fatalf("asOf %d, late seq %d; want 5 and 6", asOf, lateSeq)
		}
		if fmt.Sprint(ids) != "[d0 d1 d2 d3 d4]" {
			t.Fatalf("scan saw %v", ids)
		}
		// Replaying ChangesAfter(asOf) on the snapshot gives the present.
		rest, err := st.ChangesAfter(ctx, shard, asOf, 0)
		if err != nil || len(rest) != 2 || rest[0].ID != "d9" {
			t.Fatalf("changes after the snapshot: %v %v", rest, err)
		}
		// A callback error stops the scan and is returned.
		boom := errors.New("boom")
		if _, err := st.ScanShard(ctx, shard, func(Record) error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("scan error %v", err)
		}
	})
}

func TestConditionalWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "cond")
		c := upsert("cond", 0, "a", `{"v":1}`)
		c.IfSeq = IfAbsent
		s1, _ := mustApply(t, st, c)
		var ce *ConflictError
		if _, _, err := st.Apply(ctx, []Change{upsert("cond", 0, "other", `{}`), c}); !errors.As(err, &ce) ||
			fmt.Sprint(ce.Positions) != "[1]" || ce.Current[0] != s1 || !errors.Is(err, ErrConflict) {
			t.Fatalf("second create: %v", err)
		}
		if got := counterValue(t, st); got != s1 {
			t.Fatalf("a refused batch moved the counter to %d", got)
		}
		c.IfSeq = s1 + 100
		if _, _, err := st.Apply(ctx, []Change{c}); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale if_seq: %v", err)
		}
		// A failed condition does not count as applied for later changes:
		// a stale update of a new key leaves it absent for the next change.
		stale := upsert("cond", 0, "new", `{}`)
		stale.IfSeq = 12345
		create := upsert("cond", 0, "new", `{}`)
		create.IfSeq = IfAbsent
		if _, _, err := st.Apply(ctx, []Change{stale, create}); !errors.As(err, &ce) || fmt.Sprint(ce.Positions) != "[0]" {
			t.Fatalf("stale then create: %v, want a conflict at [0] only", err)
		}
		c.IfSeq = s1
		c.Payload = []byte(`{"v":2}`)
		s2, _ := mustApply(t, st, c)
		// Conditions see earlier changes in the same batch.
		d := del("cond", 0, "a")
		d.IfSeq = s2
		re := upsert("cond", 0, "a", `{"v":3}`)
		re.IfSeq = IfAbsent
		mustApply(t, st, d, re)
		// Documents and queries are separate namespaces.
		qp, _ := EncodeQueryPayload(json.RawMessage(`{"exists":"x"}`), nil)
		q := Change{Index: "cond", Kind: KindQueryUpsert, ID: "a", Payload: qp, IfSeq: IfAbsent}
		mustApply(t, st, q)
		recs, _ := scanAll(t, st, ShardID{Index: "cond"})
		if string(recs["d:a"].Body) != `{"v":3}` || string(recs["q:a"].Meta) != `{}` {
			t.Fatalf("records %+v", recs)
		}
	})
}

func TestCancellation(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st := h.open(t)
		mustCreateIndex(t, st, "cancel")
		shard := ShardID{Index: "cancel"}
		mustApply(t, st, upsert("cancel", 0, "a", `{}`))

		done, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := st.Apply(done, []Change{upsert("cancel", 0, "b", `{}`)}); !errors.Is(err, context.Canceled) {
			t.Fatalf("apply with a cancelled context: %v", err)
		}
		if _, err := st.ChangesAfter(done, shard, 0, 10); !errors.Is(err, context.Canceled) {
			t.Fatalf("changes with a cancelled context: %v", err)
		}
		if _, err := st.ScanShard(done, shard, func(Record) error { return nil }); !errors.Is(err, context.Canceled) {
			t.Fatalf("scan with a cancelled context: %v", err)
		}

		// An Apply waiting for the counter lock gives up with its context
		// and leaves nothing behind.
		paused, release := make(chan struct{}), make(chan struct{})
		var pauseNext atomic.Bool
		engine(st).beforeCommit = func(context.Context, int64, int64) {
			if pauseNext.CompareAndSwap(true, false) {
				close(paused)
				<-release
			}
		}
		pauseNext.Store(true)
		holder := make(chan error, 1)
		go func() {
			_, _, err := st.Apply(context.Background(), []Change{upsert("cancel", 0, "holder", `{}`)})
			holder <- err
		}()
		<-paused
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer waitCancel()
		start := time.Now()
		if _, _, err := st.Apply(waitCtx, []Change{upsert("cancel", 0, "waiter", `{}`)}); err == nil {
			t.Fatal("a waiting apply committed while the counter was held")
		}
		if took := time.Since(start); took > 10*time.Second {
			t.Fatalf("cancelled apply took %s", took)
		}
		close(release)
		if err := <-holder; err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, c := range allChanges(t, st, shard) {
			ids = append(ids, c.ID)
		}
		if fmt.Sprint(ids) != "[a holder]" {
			t.Fatalf("changelog %v", ids)
		}
		// The store still works.
		mustApply(t, st, upsert("cancel", 0, "after", `{}`))
	})
}

func TestPrune(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "pr")
		s0, s1 := ShardID{Index: "pr"}, ShardID{Index: "pr", Shard: 1}
		for i := range 10 {
			mustApply(t, st, upsert("pr", 0, fmt.Sprintf("d%d", i), `{}`), upsert("pr", 1, fmt.Sprintf("e%d", i), `{}`))
		}
		// Shard 0 holds odd seqs 1..19, shard 1 even seqs 2..20.
		if err := st.Prune(ctx, s0, 11); err != nil {
			t.Fatal(err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_changes WHERE index_name = ? AND shard = 0", "pr"); n != 5 {
			t.Fatalf("%d shard 0 changes left, want 5", n)
		}
		got, err := st.ChangesAfter(ctx, s0, 10, 0)
		if err != nil || len(got) != 5 || got[0].Seq != 11 {
			t.Fatalf("changes after the horizon: %v %v", got, err)
		}
		for _, after := range []int64{0, 5, 9} {
			if _, err := st.ChangesAfter(ctx, s0, after, 0); !errors.Is(err, ErrPruned) {
				t.Fatalf("changes after %d: %v, want ErrPruned", after, err)
			}
		}
		if n := len(allChanges(t, st, s1)); n != 10 {
			t.Fatalf("shard 1 lost changes: %d left", n)
		}
		// A lower prune never lowers the horizon; one past the end is clamped.
		if err := st.Prune(ctx, s0, 3); err != nil {
			t.Fatal(err)
		}
		if _, err := st.ChangesAfter(ctx, s0, 5, 0); !errors.Is(err, ErrPruned) {
			t.Fatalf("horizon went down: %v", err)
		}
		if err := st.Prune(ctx, s1, 1_000_000); err != nil {
			t.Fatal(err)
		}
		if got, err := st.ChangesAfter(ctx, s1, 20, 0); err != nil || len(got) != 0 {
			t.Fatalf("shard 1 after a full prune: %v %v", got, err)
		}
		f, _ := mustApply(t, st, upsert("pr", 1, "new", `{}`))
		if got, err := st.ChangesAfter(ctx, s1, 20, 0); err != nil || len(got) != 1 || got[0].Seq != f {
			t.Fatalf("shard 1 after new writes: %v %v", got, err)
		}
		// Pruning keeps the documents.
		if recs, _ := scanAll(t, st, s0); len(recs) != 10 {
			t.Fatalf("pruning removed documents: %d left", len(recs))
		}
	})
}

func TestRegistryNodes(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		reg := h.open(t).Registry()
		if err := reg.Heartbeat(ctx, Node{ID: "n1", Address: "10.0.0.1:9200", Version: "1.0", Capacity: 8}); err != nil {
			t.Fatal(err)
		}
		if err := reg.Heartbeat(ctx, Node{ID: "n2", Address: "10.0.0.2:9200", Version: "1.0"}); err != nil {
			t.Fatal(err)
		}
		nodes, err := reg.Nodes(ctx)
		if err != nil || len(nodes) != 2 {
			t.Fatalf("nodes %v %v", nodes, err)
		}
		started := nodes[0].StartedAt
		time.Sleep(20 * time.Millisecond)
		if err := reg.Heartbeat(ctx, Node{ID: "n1", Address: "10.0.0.9:9200", Version: "1.1", Capacity: 4}); err != nil {
			t.Fatal(err)
		}
		nodes, _ = reg.Nodes(ctx)
		n1 := nodes[0]
		if n1.ID != "n1" || n1.Address != "10.0.0.9:9200" || n1.Version != "1.1" || n1.Capacity != 4 {
			t.Fatalf("n1 = %+v", n1)
		}
		if !n1.StartedAt.Equal(started) || !n1.HeartbeatAt.After(started) {
			t.Fatalf("n1 times: started %s (was %s), heartbeat %s", n1.StartedAt, started, n1.HeartbeatAt)
		}
		if n1.HeartbeatAge < 0 || n1.HeartbeatAge > 5*time.Second {
			t.Fatalf("n1 heartbeat age %s", n1.HeartbeatAge)
		}
		if err := reg.RemoveNode(ctx, "n2"); err != nil {
			t.Fatal(err)
		}
		if nodes, _ = reg.Nodes(ctx); len(nodes) != 1 {
			t.Fatalf("after remove: %v", nodes)
		}
	})
}

func TestRegistryLeases(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "idx")
		mustCreateIndex(t, st, "other")
		reg := st.Registry()
		a := ShardID{Index: "idx", Shard: 0}
		const long = time.Minute

		c, ok, err := reg.ClaimCopy(ctx, a, "n1", 1, long)
		if err != nil || !ok || c.Slot != 0 || c.State != CopyRecovering || c.NodeID != "n1" || c.Expired() || c.Epoch <= 0 {
			t.Fatalf("first claim: %+v %v %v", c, ok, err)
		}
		if _, ok, err := reg.ClaimCopy(ctx, a, "n2", 1, long); err != nil || ok {
			t.Fatalf("claim over target: %v %v", ok, err)
		}
		c2, ok, err := reg.ClaimCopy(ctx, a, "n2", 2, long)
		if err != nil || !ok || c2.Slot != 1 || c2.Epoch == c.Epoch {
			t.Fatalf("claim with target 2: %+v %v %v", c2, ok, err)
		}
		if err := reg.SetCopyState(ctx, c, CopyServing); err != nil {
			t.Fatal(err)
		}
		if err := reg.SetCopyState(ctx, c, CopyServing); err != nil {
			t.Fatalf("setting the same state again: %v", err)
		}
		if err := reg.ReportApplied(ctx, c, 42); err != nil {
			t.Fatal(err)
		}
		// Re-claiming keeps the slot, state, progress and epoch.
		again, ok, err := reg.ClaimCopy(ctx, a, "n1", 3, long)
		if err != nil || !ok || again.Slot != 0 || again.State != CopyServing || again.AppliedSeq != 42 || again.Epoch != c.Epoch {
			t.Fatalf("re-claim: %+v %v %v", again, ok, err)
		}
		if err := reg.SetCopyState(ctx, c, "bogus"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad state: %v", err)
		}
		other := c
		other.NodeID = "n3"
		if err := reg.SetCopyState(ctx, other, CopyServing); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("state for a node without a copy: %v", err)
		}

		// Expiry and steal. The initial claim uses a long lease so the
		// "still alive" checks below (blocking a steal) can never race the
		// clock on a loaded machine; the lease is shortened only right
		// before the wait for it to expire, and that wait polls the
		// database clock (waitFor) instead of guessing a sleep long enough
		// to outrun it.
		b := ShardID{Index: "idx", Shard: 1}
		const short = 300 * time.Millisecond
		old, ok, err := reg.ClaimCopy(ctx, b, "n1", 1, long)
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		if err := reg.SetCopyState(ctx, old, CopyServing); err != nil {
			t.Fatal(err)
		}
		if err := reg.ReportApplied(ctx, old, 7); err != nil {
			t.Fatal(err)
		}
		if _, ok, _ := reg.ClaimCopy(ctx, b, "n2", 1, long); ok {
			t.Fatal("stole a live lease")
		}
		// Shrink only this one slot's lease: re-claiming by its own owner
		// renews just that row (unlike RenewLeases, which would also touch
		// a's still-live lease on the same node).
		old, ok, err = reg.ClaimCopy(ctx, b, "n1", 1, short)
		if err != nil || !ok {
			t.Fatalf("shorten the lease: %v %v", ok, err)
		}
		waitFor(t, func() bool {
			copies, err := reg.Copies(ctx, "idx")
			if err != nil {
				t.Fatal(err)
			}
			for _, cp := range copies {
				if cp.Shard == b {
					return cp.Expired()
				}
			}
			return false
		})
		copies, err := reg.Copies(ctx, "idx")
		if err != nil {
			t.Fatal(err)
		}
		var expired *Copy
		for i := range copies {
			if copies[i].Shard == b {
				expired = &copies[i]
			}
		}
		if expired == nil || !expired.Expired() || expired.NodeID != "n1" {
			t.Fatalf("expired copy %+v", expired)
		}
		if err := reg.SetCopyState(ctx, old, CopyRetiring); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("state on an expired lease: %v", err)
		}
		stolen, ok, err := reg.ClaimCopy(ctx, b, "n2", 1, long)
		if err != nil || !ok || stolen.Slot != 0 || stolen.NodeID != "n2" || stolen.State != CopyRecovering || stolen.AppliedSeq != 0 ||
			stolen.Epoch <= old.Epoch {
			t.Fatalf("steal: %+v %v %v", stolen, ok, err)
		}
		if err := reg.ReportApplied(ctx, old, 8); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("progress from the old owner: %v", err)
		}

		// Renewal covers live leases only. c3's claim uses a long lease too:
		// what is tested here is that renewal reaches every live lease of
		// the node (a and c3), not just one, which does not need c3 to be
		// anywhere near expiring, and claiming it short only reintroduced
		// the same race as b's above (RenewLeases must run before a 300 ms
		// deadline, on a loaded machine, with no margin at all).
		c3 := ShardID{Index: "other", Shard: 0}
		if _, ok, err := reg.ClaimCopy(ctx, c3, "n1", 1, long); err != nil || !ok {
			t.Fatal(ok, err)
		}
		renewed, err := reg.RenewLeases(ctx, "n1", long)
		if err != nil || fmt.Sprint(renewed) != fmt.Sprint([]ShardID{a, c3}) {
			t.Fatalf("renewed %v %v", renewed, err)
		}
		copies, _ = reg.Copies(ctx, "other")
		if len(copies) != 1 || copies[0].Expired() || copies[0].LeaseLeft < long/2 {
			t.Fatalf("renewed lease %+v", copies)
		}
		// An expired, unstolen lease is not renewed but can be re-claimed,
		// keeping its epoch: it is the same incarnation. Here the lease
		// must actually expire, so wait for the database clock to agree
		// instead of guessing a sleep long enough to outrun it.
		d := ShardID{Index: "other", Shard: 1}
		dc, ok, _ := reg.ClaimCopy(ctx, d, "n1", 1, short)
		if !ok {
			t.Fatal("claim d")
		}
		waitFor(t, func() bool {
			copies, err := reg.Copies(ctx, "other")
			if err != nil {
				t.Fatal(err)
			}
			for _, cp := range copies {
				if cp.Shard == d {
					return cp.Expired()
				}
			}
			return false
		})
		renewed, _ = reg.RenewLeases(ctx, "n1", long)
		for _, s := range renewed {
			if s == d {
				t.Fatal("renewed an expired lease")
			}
		}
		if c, ok, err := reg.ClaimCopy(ctx, d, "n1", 1, long); err != nil || !ok || c.Expired() || c.Epoch != dc.Epoch {
			t.Fatalf("re-claim an expired lease: %+v %v %v", c, ok, err)
		}

		// Release frees the slot.
		if err := reg.ReleaseCopy(ctx, c); err != nil {
			t.Fatal(err)
		}
		if err := reg.ReleaseCopy(ctx, c); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("releasing twice: %v", err)
		}
		if _, ok, err := reg.ClaimCopy(ctx, a, "n3", 2, long); err != nil || !ok {
			t.Fatalf("claim a released slot: %v %v", ok, err)
		}
		all, err := reg.Copies(ctx, "")
		if err != nil || len(all) != 5 {
			t.Fatalf("all copies %v %v", all, err)
		}
	})
}

// TestRegistryFencing has a node lose its slot and then reclaim the same
// slot: the old incarnation, still holding its Copy, must not touch the new
// row even though node and slot match.
func TestRegistryFencing(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "fence")
		reg := st.Registry()
		shard := ShardID{Index: "fence"}
		const short = 200 * time.Millisecond

		first, ok, err := reg.ClaimCopy(ctx, shard, "n1", 1, short)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		time.Sleep(short + 150*time.Millisecond)
		// n2 steals the slot, then gives it up; n1 claims it afresh.
		thief, ok, err := reg.ClaimCopy(ctx, shard, "n2", 1, time.Minute)
		if err != nil || !ok || thief.Slot != first.Slot {
			t.Fatal(thief, ok, err)
		}
		if err := reg.ReleaseCopy(ctx, thief); err != nil {
			t.Fatal(err)
		}
		second, ok, err := reg.ClaimCopy(ctx, shard, "n1", 1, time.Minute)
		if err != nil || !ok || second.Slot != first.Slot || second.NodeID != first.NodeID {
			t.Fatal(second, ok, err)
		}
		if second.Epoch == first.Epoch || second.Epoch == thief.Epoch {
			t.Fatalf("epochs: first %d, thief %d, second %d", first.Epoch, thief.Epoch, second.Epoch)
		}
		if err := reg.ReportApplied(ctx, second, 10); err != nil {
			t.Fatal(err)
		}
		// The stale incarnation is fenced off from every update.
		if err := reg.SetCopyState(ctx, first, CopyServing); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale SetCopyState: %v", err)
		}
		if err := reg.ReportApplied(ctx, first, 99); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale ReportApplied: %v", err)
		}
		if err := reg.ReleaseCopy(ctx, first); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("stale ReleaseCopy: %v", err)
		}
		copies, err := reg.Copies(ctx, "fence")
		if err != nil || len(copies) != 1 || copies[0].Epoch != second.Epoch || copies[0].AppliedSeq != 10 || copies[0].State != CopyRecovering {
			t.Fatalf("the new incarnation was touched: %+v %v", copies, err)
		}
		// Epochs never consume changelog sequence numbers.
		if got := counterValue(t, st); got != 0 {
			t.Fatalf("claims moved the seq counter to %d", got)
		}
	})
}

// TestRegistryConcurrentClaims has eight nodes race for three copies.
func TestRegistryConcurrentClaims(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		shard := ShardID{Index: "race", Shard: 0}
		mustCreateIndex(t, h.open(t), "race")
		var wins atomic.Int32
		var wg sync.WaitGroup
		for n := range 8 {
			reg := h.open(t).Registry()
			wg.Go(func() {
				_, ok, err := reg.ClaimCopy(ctx, shard, fmt.Sprintf("n%d", n), 3, time.Minute)
				if err != nil {
					t.Errorf("node %d: %v", n, err)
				}
				if ok {
					wins.Add(1)
				}
			})
		}
		wg.Wait()
		copies, err := h.open(t).Registry().Copies(ctx, "race")
		if err != nil {
			t.Fatal(err)
		}
		if wins.Load() != 3 || len(copies) != 3 {
			t.Fatalf("%d claims won, %d copies; want 3", wins.Load(), len(copies))
		}
		owners := map[string]bool{}
		for _, c := range copies {
			owners[c.NodeID] = true
		}
		if len(owners) != 3 {
			t.Fatalf("copies %v", copies)
		}
	})
}

// TestClaimCopyMissingIndex checks ClaimCopy refuses a shard of an index
// that was never created, or has been dropped, instead of happily leasing a
// slot nothing will ever tail.
func TestClaimCopyMissingIndex(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		reg := st.Registry()
		if _, ok, err := reg.ClaimCopy(ctx, ShardID{Index: "ghost"}, "n1", 1, time.Minute); !errors.Is(err, ErrNotFound) || ok {
			t.Fatalf("claim on a never-created index: ok=%v err=%v", ok, err)
		}
		mustCreateIndex(t, st, "gone")
		if _, ok, err := reg.ClaimCopy(ctx, ShardID{Index: "gone"}, "n1", 1, time.Minute); err != nil || !ok {
			t.Fatalf("claim before drop: ok=%v err=%v", ok, err)
		}
		if err := st.Indexes().Drop(ctx, "gone"); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := reg.ClaimCopy(ctx, ShardID{Index: "gone"}, "n2", 1, time.Minute); !errors.Is(err, ErrNotFound) || ok {
			t.Fatalf("claim on a dropped index: ok=%v err=%v", ok, err)
		}
	})
}

// TestReportAppliedMonotonic checks a late, stale ReportApplied never moves
// applied_seq backwards, and that it and SetCopyState validate their shard
// and node like every other entry point.
func TestReportAppliedMonotonic(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "mono")
		reg := st.Registry()
		shard := ShardID{Index: "mono"}
		c, ok, err := reg.ClaimCopy(ctx, shard, "n1", 1, time.Minute)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		if err := reg.ReportApplied(ctx, c, 10); err != nil {
			t.Fatal(err)
		}
		if err := reg.ReportApplied(ctx, c, 3); err != nil { // stale, arrives late
			t.Fatal(err)
		}
		copies, err := reg.Copies(ctx, "mono")
		if err != nil || len(copies) != 1 || copies[0].AppliedSeq != 10 {
			t.Fatalf("a stale report moved applied_seq: %+v %v", copies, err)
		}
		if err := reg.ReportApplied(ctx, c, 20); err != nil {
			t.Fatal(err)
		}
		if copies, err = reg.Copies(ctx, "mono"); err != nil || copies[0].AppliedSeq != 20 {
			t.Fatalf("a newer report did not advance applied_seq: %+v %v", copies, err)
		}

		bad := c
		bad.Shard.Index = ""
		if err := reg.ReportApplied(ctx, bad, 1); !errors.Is(err, ErrInvalid) {
			t.Fatalf("report applied with an invalid shard: %v", err)
		}
		if err := reg.SetCopyState(ctx, bad, CopyServing); !errors.Is(err, ErrInvalid) {
			t.Fatalf("set copy state with an invalid shard: %v", err)
		}
		bad = c
		bad.NodeID = ""
		if err := reg.ReportApplied(ctx, bad, 1); !errors.Is(err, ErrInvalid) {
			t.Fatalf("report applied with an invalid node: %v", err)
		}
		if err := reg.SetCopyState(ctx, bad, CopyServing); !errors.Is(err, ErrInvalid) {
			t.Fatalf("set copy state with an invalid node: %v", err)
		}
	})
}

// TestCopyExpired checks Expired() agrees with every dialect's SQL steal
// boundary (lease_until < now): a lease with no time left at all is not yet
// stealable, only one that has gone negative.
func TestCopyExpired(t *testing.T) {
	for leaseLeft, want := range map[time.Duration]bool{
		time.Second:  false,
		0:            false,
		-1:           true,
		-time.Second: true,
	} {
		c := Copy{LeaseLeft: leaseLeft}
		if got := c.Expired(); got != want {
			t.Fatalf("LeaseLeft %s: Expired() = %v, want %v", leaseLeft, got, want)
		}
	}
}

// TestRegistryConcurrentSameNodeClaim has one node claim the same shard
// copy from many goroutines at once. Exactly one slot must result, and every
// caller that wins must agree on its final epoch, on every dialect: a race
// between two self-claims must never mint two different fencing tokens for
// what is really one incarnation.
func TestRegistryConcurrentSameNodeClaim(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "same")
		shard := ShardID{Index: "same", Shard: 0}
		const n = 8
		results := make([]Copy, n)
		oks := make([]bool, n)
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			reg := h.open(t).Registry()
			wg.Go(func() {
				results[i], oks[i], errs[i] = reg.ClaimCopy(ctx, shard, "solo", 1, time.Minute)
			})
		}
		wg.Wait()
		for i := range n {
			if errs[i] != nil || !oks[i] {
				t.Fatalf("claim %d: ok=%v err=%v", i, oks[i], errs[i])
			}
			if results[i].Slot != 0 || results[i].NodeID != "solo" {
				t.Fatalf("claim %d: %+v", i, results[i])
			}
		}
		epoch := results[0].Epoch
		for i := 1; i < n; i++ {
			if results[i].Epoch != epoch {
				t.Fatalf("claim %d got epoch %d, claim 0 got %d: one incarnation must agree", i, results[i].Epoch, epoch)
			}
		}
		copies, err := h.open(t).Registry().Copies(ctx, "same")
		if err != nil || len(copies) != 1 || copies[0].Epoch != epoch {
			t.Fatalf("final state %+v %v", copies, err)
		}
	})
}

// TestIndexIncarnation checks a dropped-and-recreated index gets a fresh
// UID, and that Apply and ScanShard stamp every Change and Record with the
// incarnation they belong to, so a tailer can tell the two apart without an
// extra query per batch.
func TestIndexIncarnation(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		ix := st.Indexes()
		m1, err := ix.Create(ctx, IndexMeta{Name: "inc"})
		if err != nil || m1.UID == "" {
			t.Fatalf("create: %+v %v", m1, err)
		}
		shard := ShardID{Index: "inc"}
		mustApply(t, st, upsert("inc", 0, "a", `{}`))
		changes := allChanges(t, st, shard)
		if len(changes) != 1 || changes[0].IndexUID != m1.UID {
			t.Fatalf("changes before recreate: %+v", changes)
		}
		recs, _ := scanAll(t, st, shard)
		if r := recs["d:a"]; r.IndexUID != m1.UID {
			t.Fatalf("scan before recreate: %+v", r)
		}

		if err := ix.Drop(ctx, "inc"); err != nil {
			t.Fatal(err)
		}
		m2, err := ix.Create(ctx, IndexMeta{Name: "inc"})
		if err != nil || m2.UID == "" || m2.UID == m1.UID {
			t.Fatalf("recreate: %+v %v (first uid %q)", m2, err, m1.UID)
		}
		mustApply(t, st, upsert("inc", 0, "b", `{}`))
		changes = allChanges(t, st, shard)
		if len(changes) != 1 || changes[0].ID != "b" || changes[0].IndexUID != m2.UID {
			t.Fatalf("changes after recreate: %+v", changes)
		}
		recs, _ = scanAll(t, st, shard)
		if r := recs["d:b"]; len(recs) != 1 || r.IndexUID != m2.UID {
			t.Fatalf("scan after recreate: %+v", recs)
		}
	})
}

func TestBlobs(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		const chunk = 64 << 10
		st := h.open(t, WithBlobChunkSize(chunk))
		bs := st.Blobs()

		data := make([]byte, 5*chunk+1234)
		r := rand.New(rand.NewPCG(1, 2))
		for i := range data {
			data[i] = byte(r.Uint32())
		}
		info, err := bs.Put(ctx, "segments/idx/0/bundle-1", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if info.Size != int64(len(data)) || info.Chunks != 6 || info.SHA256 != fmt.Sprintf("%x", sum) {
			t.Fatalf("info %+v", info)
		}
		rc, _, err := bs.Get(ctx, "segments/idx/0/bundle-1")
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("read back %d bytes, err %v", len(got), err)
		}

		// Replace with smaller content; the old upload's chunks go away.
		if _, err := bs.Put(ctx, "segments/idx/0/bundle-1", strings.NewReader("small")); err != nil {
			t.Fatal(err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks"); n != 1 {
			t.Fatalf("%d chunks after replacing, want 1", n)
		}
		rc, _, _ = bs.Get(ctx, "segments/idx/0/bundle-1")
		got, _ = io.ReadAll(rc)
		if string(got) != "small" {
			t.Fatalf("replaced blob reads %q", got)
		}
		if _, err := bs.Put(ctx, "segments/idx/1/empty", bytes.NewReader(nil)); err != nil {
			t.Fatal(err)
		}
		if _, err := bs.Put(ctx, "snapshots/x", strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		list, err := bs.List(ctx, "segments/")
		if err != nil || len(list) != 2 || list[0].Name != "segments/idx/0/bundle-1" || list[1].Size != 0 {
			t.Fatalf("list %+v %v", list, err)
		}
		rc, info, err = bs.Get(ctx, "segments/idx/1/empty")
		if err != nil || info.Chunks != 0 {
			t.Fatal(info, err)
		}
		if got, err := io.ReadAll(rc); err != nil || len(got) != 0 {
			t.Fatalf("empty blob %q %v", got, err)
		}

		// Corruption is caught at the end of the stream.
		s := engine(st)
		if _, err := s.w.ExecContext(ctx, s.bind("UPDATE sl_blob_chunks SET data = ? WHERE chunk = 0 AND upload_id = (SELECT upload_id FROM sl_blobs WHERE name = ?)"),
			[]byte("y"), "snapshots/x"); err != nil {
			t.Fatal(err)
		}
		rc, _, _ = bs.Get(ctx, "snapshots/x")
		if _, err := io.ReadAll(rc); !errors.Is(err, ErrChecksum) {
			t.Fatalf("corrupt blob: %v", err)
		}

		if err := bs.Delete(ctx, "snapshots/x"); err != nil {
			t.Fatal(err)
		}
		if err := bs.Delete(ctx, "snapshots/x"); err != nil {
			t.Fatalf("deleting twice: %v", err)
		}
		if _, _, err := bs.Get(ctx, "snapshots/x"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get deleted: %v", err)
		}
		// A failing reader leaves no chunks behind.
		before := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks")
		if _, err := bs.Put(ctx, "broken", io.MultiReader(bytes.NewReader(data[:2*chunk]), errReader{})); err == nil {
			t.Fatal("put from a failing reader succeeded")
		}
		if after := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks"); after != before {
			t.Fatalf("failed put left %d chunks", after-before)
		}
		if _, err := bs.Stat(ctx, "broken"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("failed put is visible: %v", err)
		}

		// Concurrent Puts of one new name from several nodes: one wins and
		// no upload is orphaned.
		var wg sync.WaitGroup
		for n := range 4 {
			nbs := h.open(t, WithBlobChunkSize(chunk)).Blobs()
			wg.Go(func() {
				if _, err := nbs.Put(ctx, "race", bytes.NewReader(data[:(n+1)*chunk])); err != nil {
					t.Errorf("node %d put: %v", n, err)
				}
			})
		}
		wg.Wait()
		info, err = bs.Stat(ctx, "race")
		if err != nil {
			t.Fatal(err)
		}
		chunks := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks WHERE upload_id = (SELECT upload_id FROM sl_blobs WHERE name = ?)", "race")
		if total := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks"); chunks != info.Chunks || total != before+info.Chunks {
			t.Fatalf("after racing puts: %d chunks in total, %d for the winner (%d expected)", total-before, chunks, info.Chunks)
		}
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("disk on fire") }

func TestIndexes(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		ix := st.Indexes()
		m, err := ix.Create(ctx, IndexMeta{Name: "a", Mapping: []byte(`{"name":"text"}`), Settings: []byte(`{"shards":2}`)})
		if err != nil || m.Version != 1 || string(m.Mapping) != `{"name":"text"}` || m.CreatedAt.IsZero() {
			t.Fatalf("create %+v %v", m, err)
		}
		if _, err := ix.Create(ctx, IndexMeta{Name: "a"}); !errors.Is(err, ErrExists) {
			t.Fatalf("create twice: %v", err)
		}
		if _, err := ix.Create(ctx, IndexMeta{Name: "b", Mapping: []byte(`{`)}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("bad mapping: %v", err)
		}
		mustCreateIndex(t, st, "b")
		list, err := ix.List(ctx)
		if err != nil || len(list) != 2 || list[0].Name != "a" || string(list[1].Mapping) != "{}" {
			t.Fatalf("list %+v %v", list, err)
		}
		m.Mapping = []byte(`{"name":"text","n":"number"}`)
		m2, err := ix.Update(ctx, m)
		if err != nil || m2.Version != 2 || !bytes.Equal(m2.Mapping, m.Mapping) {
			t.Fatalf("update %+v %v", m2, err)
		}
		if _, err := ix.Update(ctx, m); !errors.Is(err, ErrConflict) {
			t.Fatalf("stale update: %v", err)
		}
		if _, err := ix.Update(ctx, IndexMeta{Name: "zz", Version: 1}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("update missing: %v", err)
		}
		if _, err := ix.Get(ctx, "zz"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("get missing: %v", err)
		}

		// Dropping removes everything the index owns.
		mustApply(t, st, upsert("a", 0, "d", `{}`), upsert("a", 1, "e", `{}`), upsert("b", 0, "f", `{}`))
		reg := st.Registry()
		if _, ok, err := reg.ClaimCopy(ctx, ShardID{Index: "a"}, "n1", 1, time.Minute); err != nil || !ok {
			t.Fatal(ok, err)
		}
		if err := st.Prune(ctx, ShardID{Index: "a"}, 2); err != nil {
			t.Fatal(err)
		}
		if err := ix.Drop(ctx, "a"); err != nil {
			t.Fatal(err)
		}
		if err := ix.Drop(ctx, "a"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("drop twice: %v", err)
		}
		for _, table := range []string{"sl_documents", "sl_changes", "sl_shard_copies", "sl_pruned"} {
			if n := countRows(t, st, "SELECT COUNT(*) FROM "+table+" WHERE index_name = ?", "a"); n != 0 {
				t.Fatalf("%s keeps %d rows of the dropped index", table, n)
			}
		}
		if n := len(allChanges(t, st, ShardID{Index: "b"})); n != 1 {
			t.Fatalf("index b lost changes: %d", n)
		}
		if _, _, err := st.Apply(ctx, []Change{upsert("a", 0, "d", `{}`)}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("write to a dropped index: %v", err)
		}
	})
}

// TestWatch covers LISTEN/NOTIFY, which only Postgres has: once ready is
// called, the very next commit is announced.
func TestWatch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect != "postgres" {
			t.Skip("no notifications on " + h.dialect)
		}
		st := h.open(t)
		w, ok := st.(Watcher)
		if !ok {
			t.Fatal("postgres store is not a Watcher")
		}
		mustCreateIndex(t, st, "w")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		got := make(chan Notification, 16)
		ready := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- w.Watch(ctx, func() { close(ready) }, func(n Notification) { got <- n })
		}()
		select {
		case <-ready:
		case err := <-done:
			t.Fatalf("watch ended before it was ready: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("watch never became ready")
		}
		_, last := mustApply(t, st, upsert("w", 2, "a", `{}`), upsert("w", 2, "b", `{}`))
		for {
			select {
			case n := <-got:
				if n.Shard != (ShardID{Index: "w", Shard: 2}) || n.Seq != last {
					continue // another test's schema shares the channel
				}
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("watch ended with %v", err)
				}
				return
			case <-time.After(10 * time.Second):
				t.Fatalf("no notification of seq %d after ready", last)
			}
		}
	})
}

// TestHeadSeq: the head is the newest committed seq across shards, and a
// tailer reading it before ChangesAfter never advances past a change of its
// shard that it has not seen.
func TestHeadSeq(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		if head, now, err := st.HeadSeq(ctx); err != nil || head != 0 || time.Since(now).Abs() > time.Minute {
			t.Fatalf("empty store head = %d at %s, %v", head, now, err)
		}
		mustCreateIndex(t, st, "h")
		mustApply(t, st, upsert("h", 0, "a", `{}`), upsert("h", 1, "b", `{}`))
		_, last := mustApply(t, st, upsert("h", 1, "c", `{}`))
		head, _, err := st.HeadSeq(ctx)
		if err != nil || head != last {
			t.Fatalf("head = %d, %v; want %d", head, err, last)
		}
		page, err := st.ChangesAfter(ctx, ShardID{Index: "h", Shard: 0}, 1, 10)
		if err != nil || len(page) != 0 {
			t.Fatalf("shard 0 after 1: %v, %v", page, err)
		}
		_ = st.Close()
		if _, _, err := st.HeadSeq(ctx); !errors.Is(err, ErrClosed) {
			t.Fatalf("head after close: %v", err)
		}
	})
}

// TestGroupCommitIsolation is the reviewer's probe on every dialect: in one
// group-commit batch, a stale IfSeq must not make a later IfAbsent on the
// same new key fail, a genuine conflict still fails, and a missing index or
// invalid change fails only its own request.
func TestGroupCommitIsolation(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "gc")
		collected := make(chan struct{}, 8)
		g := NewGroupCommitter(st, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 5, received: func() { collected <- struct{}{} }})
		defer g.Close()

		bad := upsert("gc", 0, "zz", "{")
		if _, _, err := g.Apply(ctx, []Change{upsert("gc", 0, "ok", `{}`), bad}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid request: %v", err)
		}

		a := upsert("gc", 0, "k", `{"by":"a"}`)
		a.IfSeq = 99 // stale: k does not exist
		b := upsert("gc", 0, "k", `{"by":"b"}`)
		b.IfSeq = IfAbsent
		c := upsert("gc", 0, "m", `{"by":"c"}`)
		c.IfSeq = IfAbsent
		d := upsert("gc", 0, "m", `{"by":"d"}`)
		d.IfSeq = IfAbsent // genuinely conflicts: c creates m first
		missing := upsert("nope", 0, "x", `{}`)
		res := gather(t, g, collected, nil, [][]Change{{a}, {b}, {c}, {d}, {missing}})

		var ce *ConflictError
		if !errors.As(res[0].err, &ce) || ce.Current[0] != 0 {
			t.Fatalf("A (stale if_seq) got %v", res[0].err)
		}
		if res[1].err != nil {
			t.Fatalf("B (if_absent on the same new key) got %v", res[1].err)
		}
		if res[2].err != nil {
			t.Fatalf("C got %v", res[2].err)
		}
		if !errors.As(res[3].err, &ce) || ce.Current[0] != res[2].first {
			t.Fatalf("D got %v, want a conflict with C's seq %d", res[3].err, res[2].first)
		}
		var nf *IndexNotFoundError
		if !errors.As(res[4].err, &nf) || fmt.Sprint(nf.Positions) != "[0]" {
			t.Fatalf("missing index got %v", res[4].err)
		}
		if res[2].first != res[1].first+1 {
			t.Fatalf("B and C got seqs %d and %d", res[1].first, res[2].first)
		}
		recs, _ := scanAll(t, st, ShardID{Index: "gc"})
		if string(recs["d:k"].Body) != `{"by":"b"}` || string(recs["d:m"].Body) != `{"by":"c"}` || len(recs) != 2 {
			t.Fatalf("records %v", recs)
		}
	})
}

// TestDatabaseClock checks the database clock the registry judges leases by
// is Unix milliseconds in UTC, and that MySQL sessions run in UTC so it is
// never ambiguous across a DST change.
func TestDatabaseClock(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		s := engine(h.open(t))
		var dbNow int64
		if err := s.r.QueryRowContext(ctx, "SELECT "+s.d.Now).Scan(&dbNow); err != nil {
			t.Fatal(err)
		}
		if skew := time.Since(time.UnixMilli(dbNow)); skew.Abs() > 5*time.Second {
			t.Fatalf("database clock is %s off the local clock", skew)
		}
		if h.dialect == "mysql" {
			var tz string
			if err := s.r.QueryRowContext(ctx, "SELECT @@session.time_zone").Scan(&tz); err != nil || tz != "+00:00" {
				t.Fatalf("session time zone %q %v", tz, err)
			}
		}
	})
}

func blobChunkCount(t *testing.T, st Store) int {
	return countRows(t, st, "SELECT COUNT(*) FROM sl_blob_chunks")
}

// TestBlobPutSurvivesPostCommitFailure checks that when something goes
// wrong after Put's pointer-switch transaction has committed — a cancelled
// context racing the end of Put, a transient error — Put reports that
// failure but leaves the blob it just committed alone: its chunks must not
// be swept out from under it by Put's own cleanup, which only ever owned
// its failed, pre-commit attempt.
func TestBlobPutSurvivesPostCommitFailure(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		bs := st.Blobs()
		hooks := engine(st).blobs
		data := []byte("hello, world, this is a committed blob")

		injected := errors.New("injected post-commit failure")
		hooks.failAfterCommit = func() error { return injected }
		if _, err := bs.Put(ctx, "p", bytes.NewReader(data)); !errors.Is(err, injected) {
			t.Fatalf("put: %v", err)
		}
		hooks.failAfterCommit = nil

		if got := blobChunkCount(t, st); got == 0 {
			t.Fatal("the committed upload's chunks were swept after a post-commit failure")
		}
		rc, info, err := bs.Get(ctx, "p")
		if err != nil {
			t.Fatalf("get after a post-commit failure: %v", err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("blob content after a post-commit failure: %q %v, want %q", got, err, data)
		}
		if info.Size != int64(len(data)) || info.Chunks == 0 {
			t.Fatalf("blob info after a post-commit failure: %+v", info)
		}
	})
}

// TestBlobPutAmbiguousCommit checks that when switchTo's pointer-switch
// transaction genuinely commits but Put never learns that (a cancelled
// context or a network blip racing the server's own decision), Put reports
// ErrAmbiguousCommit without touching the now-live upload, the blob stays
// intact, and a later Sweep leaves it alone too.
func TestBlobPutAmbiguousCommit(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		bs := st.Blobs()
		hooks := engine(st).blobs
		data := []byte("hello, this blob's commit outcome goes missing")

		hooks.crash = func(point string) bool { return point == "commit" }
		_, err := bs.Put(ctx, "amb", bytes.NewReader(data))
		if !errors.Is(err, ErrAmbiguousCommit) {
			t.Fatalf("put: %v", err)
		}
		hooks.crash = nil

		// The commit landed: the blob is there, intact.
		if got := blobChunkCount(t, st); got == 0 {
			t.Fatal("the live upload's chunks are gone after an ambiguous commit")
		}
		rc, info, err := bs.Get(ctx, "amb")
		if err != nil {
			t.Fatalf("get after an ambiguous commit: %v", err)
		}
		got, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("blob content after an ambiguous commit: %q %v, want %q", got, err, data)
		}
		if info.Size != int64(len(data)) {
			t.Fatalf("blob info after an ambiguous commit: %+v", info)
		}

		// A later Sweep leaves the live upload alone.
		if removed, err := bs.Sweep(ctx, 0); err != nil || removed != 0 {
			t.Fatalf("sweep after an ambiguous commit: %d %v", removed, err)
		}
		if got := blobChunkCount(t, st); got == 0 {
			t.Fatal("sweep removed the live upload's chunks")
		}
		rc2, _, err := bs.Get(ctx, "amb")
		if err != nil {
			t.Fatalf("get after sweep: %v", err)
		}
		_ = rc2.Close()
	})
}

// TestBlobSweep covers what crashes leave behind: a Put that dies mid-write,
// one that dies after switching the pointer but before removing the old
// upload, and the interplay of Sweep with Puts still writing.
func TestBlobSweep(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		const chunk = 1024
		st := h.open(t, WithBlobChunkSize(chunk))
		bs := st.Blobs()
		hooks := engine(st).blobs
		data := bytes.Repeat([]byte("0123456789abcdef"), 4*chunk/16) // 4 chunks

		// A Put crashes after two chunks: nothing is visible, and the
		// leftovers survive a sweep while fresh but not once stale.
		n := 0
		hooks.crash = func(point string) bool { n++; return point == "chunk" && n == 2 }
		if _, err := bs.Put(ctx, "crashed", bytes.NewReader(data)); !errors.Is(err, errSimulatedCrash) {
			t.Fatalf("crashing put: %v", err)
		}
		hooks.crash = nil
		if _, err := bs.Stat(ctx, "crashed"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("crashed put is visible: %v", err)
		}
		if got := blobChunkCount(t, st); got != 2 {
			t.Fatalf("%d chunks after the crash, want 2", got)
		}
		if removed, err := bs.Sweep(ctx, time.Hour); err != nil || removed != 0 {
			t.Fatalf("sweep of a fresh upload: %d %v", removed, err)
		}
		time.Sleep(50 * time.Millisecond)
		if removed, err := bs.Sweep(ctx, 10*time.Millisecond); err != nil || removed != 1 {
			t.Fatalf("sweep of a stale upload: %d %v", removed, err)
		}
		if got := blobChunkCount(t, st); got != 0 {
			t.Fatalf("%d chunks after the sweep", got)
		}
		if got := countRows(t, st, "SELECT COUNT(*) FROM sl_blob_uploads"); got != 0 {
			t.Fatalf("%d upload registrations after the sweep", got)
		}

		// A replacing Put crashes after its commit, before removing the old
		// upload: the new blob is intact, and the old upload's removal was
		// recorded in the same transaction, so any sweep takes it.
		if _, err := bs.Put(ctx, "x", bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		hooks.crash = func(point string) bool { return point == "cleanup" }
		if _, err := bs.Put(ctx, "x", bytes.NewReader(data[:chunk])); !errors.Is(err, errSimulatedCrash) {
			t.Fatalf("crashing replace: %v", err)
		}
		hooks.crash = nil
		if got := blobChunkCount(t, st); got != 5 {
			t.Fatalf("%d chunks after the crashed replace, want 5", got)
		}
		if removed, err := bs.Sweep(ctx, time.Hour); err != nil || removed != 1 {
			t.Fatalf("sweep of a replaced upload: %d %v", removed, err)
		}
		if got := blobChunkCount(t, st); got != 1 {
			t.Fatalf("%d chunks after sweeping the replaced upload, want 1", got)
		}
		rc, _, err := bs.Get(ctx, "x")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, data[:chunk]) {
			t.Fatalf("replaced blob: %d bytes, %v", len(got), err)
		}
		_ = rc.Close()

		// Delete registers the removal too, so a later sweep has nothing left.
		if err := bs.Delete(ctx, "x"); err != nil {
			t.Fatal(err)
		}
		if got := blobChunkCount(t, st); got != 0 {
			t.Fatalf("%d chunks after delete", got)
		}

		// A Put still writing survives a sweep for stale uploads...
		pr, pw := io.Pipe()
		put := make(chan error, 1)
		go func() {
			_, err := bs.Put(ctx, "slow", pr)
			put <- err
		}()
		if _, err := pw.Write(data[:chunk]); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return blobChunkCount(t, st) == 1 })
		if removed, err := bs.Sweep(ctx, time.Hour); err != nil || removed != 0 {
			t.Fatalf("sweep during a live put: %d %v", removed, err)
		}
		_, _ = pw.Write(data[chunk : 2*chunk])
		_ = pw.Close()
		if err := <-put; err != nil {
			t.Fatalf("live put: %v", err)
		}
		// ...but one stalled past olderThan is taken, and its Put fails
		// rather than commit a blob missing chunks.
		pr, pw = io.Pipe()
		go func() {
			_, err := bs.Put(ctx, "stalled", pr)
			put <- err
		}()
		if _, err := pw.Write(data[:chunk]); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return blobChunkCount(t, st) == 3 })
		time.Sleep(50 * time.Millisecond)
		if removed, err := bs.Sweep(ctx, 10*time.Millisecond); err != nil || removed != 1 {
			t.Fatalf("sweep of a stalled put: %d %v", removed, err)
		}
		_, _ = pw.Write(data[chunk : 2*chunk])
		_ = pw.Close()
		if err := <-put; !errors.Is(err, ErrConflict) {
			t.Fatalf("stalled put after the sweep: %v", err)
		}
		if _, err := bs.Stat(ctx, "stalled"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("swept put is visible: %v", err)
		}
		if _, err := bs.Sweep(ctx, 0); err != nil {
			t.Fatal(err)
		}
		if got := blobChunkCount(t, st); got != 2 {
			t.Fatalf("%d chunks left, want the 2 of blob slow", got)
		}
		if _, err := bs.Sweep(ctx, -time.Second); !errors.Is(err, ErrInvalid) {
			t.Fatalf("negative age: %v", err)
		}
	})
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBlobListPrefix(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		bs := h.open(t).Blobs()
		names := []string{"a", "seg/", "seg/0", "seg/1/x", "seg0", "segé", "sef", "\U0010FFFF", "z\U0010FFFF\U0010FFFF", "z\U0010FFFFa"}
		for _, n := range names {
			if _, err := bs.Put(ctx, n, strings.NewReader(n)); err != nil {
				t.Fatalf("put %q: %v", n, err)
			}
		}
		for prefix, want := range map[string]string{
			"seg/":        "[seg/ seg/0 seg/1/x]",
			"seg":         "[seg/ seg/0 seg/1/x seg0 segé]",
			"":            fmt.Sprint(sortedCopy(names)),
			"z\U0010FFFF": fmt.Sprint([]string{"z\U0010FFFFa", "z\U0010FFFF\U0010FFFF"}),
			"q":           "[]",
		} {
			list, err := bs.List(ctx, prefix)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, b := range list {
				got = append(got, b.Name)
			}
			if fmt.Sprint(got) != want && (want != "[]" || got != nil) {
				t.Fatalf("list %q: %q, want %s", prefix, got, want)
			}
		}
	})
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func TestPrefixEnd(t *testing.T) {
	for prefix, want := range map[string]string{
		"abc": "abd", "a\U0010FFFF": "b", "\uD7FF": "\uE000", "é": "ê",
	} {
		if got, ok := prefixEnd(prefix); !ok || got != want {
			t.Fatalf("prefixEnd(%q) = %q, %v", prefix, got, ok)
		}
	}
	for _, p := range []string{"", "\U0010FFFF\U0010FFFF"} {
		if _, ok := prefixEnd(p); ok {
			t.Fatalf("prefixEnd(%q) has an end", p)
		}
	}
}

// TestBlobGetSpan checks Get's span lasts until the stream is closed.
func TestBlobGetSpan(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	st := sqliteHarness(t).open(t, WithTracer(tp.Tracer("test")))
	ctx := context.Background()
	if _, err := st.Blobs().Put(ctx, "b", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	ended := func() bool {
		for _, s := range rec.Ended() {
			if s.Name() == "store.blob_get" {
				return true
			}
		}
		return false
	}
	rc, _, err := st.Blobs().Get(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	if ended() {
		t.Fatal("blob_get span ended before the stream was closed")
	}
	_ = rc.Close()
	if !ended() {
		t.Fatal("blob_get span still open after Close")
	}
}

// TestMappingChanges: an Update that changes the mapping moves the mapping version
// and logs a mapping change to every shard, in its transaction and with contiguous
// seqs; every later change carries the new version; ScanShard yields the mapping as
// of its snapshot first; and only Update writes mapping changes.
func TestMappingChanges(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		ix := st.Indexes()
		m, err := ix.Create(ctx, IndexMeta{Name: "mc", Mapping: []byte(`{"fields":{}}`), Settings: []byte(`{"shards":3}`)})
		if err != nil || m.MappingVersion != 1 {
			t.Fatalf("create: %+v %v", m, err)
		}
		mustApply(t, st, upsert("mc", 1, "a", `{}`))
		// A settings-only update logs nothing.
		m.Settings = []byte(`{"shards":3,"refresh_interval":"2s"}`)
		if m, err = ix.Update(ctx, m); err != nil || m.MappingVersion != 1 || counterValue(t, st) != 1 {
			t.Fatalf("settings update: %+v %v (counter %d)", m, err, counterValue(t, st))
		}
		m.Mapping = []byte(`{"fields":{"brand":"keyword"}}`)
		if m, err = ix.Update(ctx, m); err != nil || m.MappingVersion != 2 {
			t.Fatalf("mapping update: %+v %v", m, err)
		}
		if counterValue(t, st) != 4 {
			t.Fatalf("counter %d after a mapping change to 3 shards", counterValue(t, st))
		}
		for sh := range 3 {
			changes := allChanges(t, st, ShardID{Index: "mc", Shard: sh})
			last := changes[len(changes)-1]
			if last.Kind != KindMapping || last.ID != MappingChangeID || last.Seq != int64(2+sh) ||
				last.MappingVersion != 2 || !bytes.Equal(last.Payload, m.Mapping) || last.IndexUID != m.UID {
				t.Fatalf("shard %d: %+v", sh, last)
			}
		}
		_, after := mustApply(t, st, upsert("mc", 1, "b", `{"brand":"x"}`))
		changes := allChanges(t, st, ShardID{Index: "mc", Shard: 1})
		if c := changes[len(changes)-1]; c.Seq != after || c.MappingVersion != 2 || changes[0].MappingVersion != 1 {
			t.Fatalf("changes %+v", changes)
		}
		var mapping []Record
		_, err = st.ScanShard(ctx, ShardID{Index: "mc", Shard: 1}, func(r Record) error {
			if r.Kind == RecordMapping {
				mapping = append(mapping, r)
			} else if r.MappingVersion != 2 {
				t.Errorf("record %+v", r)
			}
			return nil
		})
		if err != nil || len(mapping) != 1 || !bytes.Equal(mapping[0].Body, m.Mapping) || mapping[0].MappingVersion != 2 {
			t.Fatalf("scan mapping %+v %v", mapping, err)
		}
		if _, _, err := st.Apply(ctx, []Change{{Index: "mc", Kind: KindMapping, ID: MappingChangeID, Payload: []byte(`{}`)}}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Apply of a mapping change: %v", err)
		}
		m.Settings = []byte(`{"shards":0}`)
		if _, err := ix.Update(ctx, m); !errors.Is(err, ErrInvalid) {
			t.Fatalf("shards 0: %v", err)
		}
		if _, err := ix.Create(ctx, IndexMeta{Name: "bad", Settings: []byte(`{"shards":"x"}`)}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("shards x: %v", err)
		}
	})
}

// TestMappingChangesOrderWithApply: under concurrent writers and mapping updates,
// every change of a shard follows the mapping change of the version it carries.
func TestMappingChangesOrderWithApply(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "mo", Mapping: []byte(`{"fields":{}}`)}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for w := range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 25 {
					if _, _, err := st.Apply(ctx, []Change{upsert("mo", 0, fmt.Sprintf("w%d-%d", w, i), `{}`)}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 10 {
				for {
					m, err := st.Indexes().Get(ctx, "mo")
					if err != nil {
						t.Error(err)
						return
					}
					m.Mapping = []byte(fmt.Sprintf(`{"fields":{"f%d":"keyword"}}`, i))
					if _, err = st.Indexes().Update(ctx, m); err == nil {
						break
					} else if !errors.Is(err, ErrConflict) {
						t.Error(err)
						return
					}
				}
			}
		}()
		wg.Wait()
		version := int64(1)
		for _, c := range allChanges(t, st, ShardID{Index: "mo"}) {
			if c.Kind == KindMapping {
				if c.MappingVersion != version+1 {
					t.Fatalf("mapping change %d after version %d", c.MappingVersion, version)
				}
				version = c.MappingVersion
				continue
			}
			if c.MappingVersion != version {
				t.Fatalf("seq %d carries mapping version %d, the log is at %d", c.Seq, c.MappingVersion, version)
			}
		}
		if version != 11 {
			t.Fatalf("ended at mapping version %d", version)
		}
	})
}

// TestShardCountImmutable: an index's shard count is fixed at Create; Update refuses
// to change it, whether it is spelled out or left to its default.
func TestShardCountImmutable(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		ix := st.Indexes()
		m, err := ix.Create(ctx, IndexMeta{Name: "sc", Settings: []byte(`{"shards":3}`)})
		if err != nil {
			t.Fatal(err)
		}
		for _, settings := range []string{`{"shards":2}`, `{"shards":4}`, `{}`} {
			bad := m
			bad.Settings = []byte(settings)
			if _, err := ix.Update(ctx, bad); !errors.Is(err, ErrInvalid) {
				t.Fatalf("update to %s: %v", settings, err)
			}
		}
		m.Settings = []byte(`{"shards":3,"refresh_interval":"5s"}`)
		if _, err := ix.Update(ctx, m); err != nil {
			t.Fatalf("update keeping the shard count: %v", err)
		}
		one, err := ix.Create(ctx, IndexMeta{Name: "sc1"})
		if err != nil {
			t.Fatal(err)
		}
		one.Settings = []byte(`{"shards":1}`)
		if _, err := ix.Update(ctx, one); err != nil {
			t.Fatalf("spelling out the default: %v", err)
		}
	})
}
