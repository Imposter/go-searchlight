package node_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// readLoop searches an index until stopped, counting failed and stale reads.
type readLoop struct {
	wg              sync.WaitGroup
	stop            chan struct{}
	reads, failures atomic.Int64
	stale           atomic.Int64
	firstErr        atomic.Pointer[error]
}

func startReads(n *node.Single, index string, readers int) *readLoop {
	r := &readLoop{stop: make(chan struct{})}
	for range readers {
		r.wg.Go(func() {
			for {
				select {
				case <-r.stop:
					return
				default:
				}
				res, err := n.Search(context.Background(), index, &search.Request{Query: &query.All{}, Size: 1}, api.ReadOptions{})
				r.reads.Add(1)
				if err != nil {
					r.failures.Add(1)
					r.firstErr.CompareAndSwap(nil, &err)
					continue
				}
				if res.Stale {
					r.stale.Add(1)
				}
			}
		})
	}
	return r
}

func (r *readLoop) end() {
	close(r.stop)
	r.wg.Wait()
}

func (r *readLoop) err() error {
	if p := r.firstErr.Load(); p != nil {
		return *p
	}
	return nil
}

// bulkLoad writes count documents made by body, in bulks, returning the last seq.
func bulkLoad(t *testing.T, n *node.Single, index string, count int, body func(i int) string) int64 {
	t.Helper()
	var seq int64
	for start := 0; start < count; start += 2000 {
		var ops []api.WriteOp
		for i := start; i < min(count, start+2000); i++ {
			ops = append(ops, upsert(fmt.Sprintf("d%05d", i), body(i)))
		}
		seq = mustWrite(t, n, index, ops...).Seq
	}
	return seq
}

// rebuiltAside reports whether any copy of the index was rebuilt aside: its root
// names a current copy directory.
func rebuiltAside(t *testing.T, dataDir, uid string) bool {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dataDir, "indexes", uid, "*", "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	return len(matches) > 0
}

func mustParse(t *testing.T, q string) query.Node {
	t.Helper()
	n, problems := query.Parse([]byte(q))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	return n
}

// Typing a field that live documents hold only as [] (or {}) changes no document's
// analysis: the copy adopts the mapping in place, with no rebuild, no failed or stale
// read, and a refresh=wait_for write that does not time out.
func TestEmptyListTypingNeedsNoRebuild(t *testing.T) {
	cfg := testConfig(t)
	cfg.RemapDebounce = 2 * time.Second // as in production: a rebuild would show
	n := open(t, cfg, openStore(t, cfg), nil)
	info, err := n.CreateIndex(ctx(t), "e", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}})
	if err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	seq := bulkLoad(t, n, "e", 10_000, func(i int) string { return fmt.Sprintf(`{"title": "item %d", "images": [], "meta": {}}`, i) })
	if got := count(t, n, "e", &query.All{}, seq); got != 10_000 {
		t.Fatalf("count = %d", got)
	}
	reads := startReads(n, "e", 4)
	res, err := n.Write(ctx(t), "e", []api.WriteOp{upsert("typed", `{"title": "x", "images": ["a.jpg"]}`)}, api.WriteOptions{Refresh: api.RefreshWaitFor})
	reads.end()
	if err != nil || res.Items[0].Err != nil || res.TimedOut {
		t.Fatalf("the typing write: %+v %v", res, err)
	}
	if reads.failures.Load() != 0 || reads.stale.Load() != 0 {
		t.Errorf("%d of %d reads failed (first: %v), %d were stale", reads.failures.Load(), reads.reads.Load(), reads.err(), reads.stale.Load())
	}
	if rebuiltAside(t, cfg.DataDir, info.UID) {
		t.Error("a copy was rebuilt for typing a field only [] held")
	}
	if got, _ := n.GetIndex(ctx(t), "e"); got.Mapping.Fields["images"] != schema.KeywordList {
		t.Fatalf("mapping = %v", got.Mapping.Fields)
	}
	if got := count(t, n, "e", mustParse(t, `{"field": "images", "op": "has", "value": "a.jpg"}`), 0); got != 1 {
		t.Errorf("has a.jpg = %d", got)
	}
	if got := count(t, n, "e", mustParse(t, `{"field": "images", "op": "empty"}`), 0); got != 10_000 {
		t.Errorf("empty images = %d, want the 10,000 [] holders", got)
	}
}

// A mapping change that re-analyzes live documents rebuilds the copy aside: the old
// copy keeps serving (stale) with no failed read, and once the new one is swapped in
// the documents are found under the new field, there and after a restart.
func TestRetypeRebuildsAside(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	info, err := n.CreateIndex(ctx(t), "r", api.IndexSpec{
		Mapping:  &schema.Mapping{Dynamic: schema.DynamicFalse, Fields: map[string]schema.FieldType{"title": schema.Text}},
		Settings: api.IndexSettings{Shards: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	colour := []string{"red", "blue"}
	seq := bulkLoad(t, n, "r", 10_000, func(i int) string { return fmt.Sprintf(`{"title": "item %d", "colour": %q}`, i, colour[i%2]) })
	if got := count(t, n, "r", &query.All{}, seq); got != 10_000 {
		t.Fatalf("count = %d", got)
	}
	reads := startReads(n, "r", 4)
	if _, err := n.PatchMapping(ctx(t), "r", map[string]schema.FieldType{"colour": schema.Keyword}); err != nil {
		t.Fatal(err)
	}
	red := mustParse(t, `{"field": "colour", "op": "eq", "value": "red"}`)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		res, err := n.Search(ctx(t), "r", &search.Request{Query: red, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			t.Fatalf("a read during the rebuild: %v", err)
		}
		if res.Total == 5_000 && !res.Stale {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the rebuild never finished: %d red, stale %v", res.Total, res.Stale)
		}
		time.Sleep(20 * time.Millisecond)
	}
	reads.end()
	if reads.failures.Load() != 0 {
		t.Errorf("%d of %d reads failed during the rebuild: %v", reads.failures.Load(), reads.reads.Load(), reads.err())
	}
	if reads.stale.Load() == 0 {
		t.Error("no read was marked stale while the copy was rebuilt")
	}
	if !rebuiltAside(t, cfg.DataDir, info.UID) {
		t.Error("the copy was not rebuilt aside")
	}
	if got := count(t, n, "r", &query.All{}, 0); got != 10_000 {
		t.Errorf("after the swap the index holds %d documents", got)
	}
	// The swapped-in copy is the one a restart opens.
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	n2 := open(t, cfg, st, nil)
	waitReady(t, n2)
	if got := count(t, n2, "r", red, 0); got != 5_000 {
		t.Errorf("after a restart red = %d", got)
	}
}
