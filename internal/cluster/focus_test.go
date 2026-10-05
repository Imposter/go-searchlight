package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// truth reads a shard's documents from the store: id to body.
func truth(t testing.TB, st store.Store, id store.ShardID) map[string]string {
	t.Helper()
	out := map[string]string{}
	if _, err := st.ScanShard(context.Background(), id, func(r store.Record) error {
		if r.Kind == store.RecordDocument {
			out[r.ID] = string(r.Body)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// localDocs reads a node's own copy of a shard, once it has seq searchable: id to body.
// A copy still recovering is an error to wait out.
func localDocs(n *Node, id store.ShardID, seq int64) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tg, err := n.LocalTarget(ctx, id.Index, id.Shard, seq)
	if err != nil {
		return nil, fmt.Errorf("node %s, %s: %w", n.id, id, err)
	}
	defer tg.Release()
	out := map[string]string{}
	var total int64
	var after []any
	for {
		res, err := tg.Search(ctx, &search.Request{Query: &query.All{}, Size: search.MaxSize, TrackTotal: search.TrackTotalAll, Index: id.Index, SearchAfter: after})
		if err != nil {
			return nil, err
		}
		total = res.Total
		for _, h := range res.Hits {
			out[h.ID] = string(h.Body)
		}
		if len(res.Hits) < search.MaxSize {
			break
		}
		after = res.Hits[len(res.Hits)-1].Sort
	}
	if int64(len(out)) != total {
		return nil, fmt.Errorf("node %s, %s: %d hits of %d", n.id, id, len(out), total)
	}
	return out, nil
}

// sameDocs compares two shard contents, as JSON values.
func sameDocs(a, b map[string]string) error {
	if len(a) != len(b) {
		return fmt.Errorf("%d documents, want %d", len(a), len(b))
	}
	for id, body := range b {
		got, ok := a[id]
		if !ok {
			return fmt.Errorf("%q is missing", id)
		}
		if !jsonEqual(got, body) {
			return fmt.Errorf("%q is %s, want %s", id, got, body)
		}
	}
	return nil
}

func jsonEqual(a, b string) bool {
	var x, y any
	if json.Unmarshal([]byte(a), &x) != nil || json.Unmarshal([]byte(b), &y) != nil {
		return a == b
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return bytes.Equal(xa, ya)
}

// allChanges reads every change of every shard of the store's indexes.
func allChanges(t testing.TB, st store.Store, n *Node) []store.Change {
	t.Helper()
	var out []store.Change
	for _, iv := range n.Indexes() {
		for s := range iv.Shards {
			var from int64
			for {
				page, err := st.ChangesAfter(context.Background(), store.ShardID{Index: iv.Name, Shard: s}, from, 1000)
				if err != nil {
					t.Fatal(err)
				}
				out = append(out, page...)
				if len(page) < 1000 {
					break
				}
				from = page[len(page)-1].Seq
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// converge waits until every live node's copy of every shard of index holds exactly
// the store's documents.
func converge(t testing.TB, c *cluster, st store.Store, index string, shards int) {
	t.Helper()
	head, _, err := st.HeadSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for s := range shards {
		id := store.ShardID{Index: index, Shard: s}
		want := truth(t, st, id)
		for _, tn := range c.live() {
			if _, ok := tn.n.Hosted(id); !ok {
				continue
			}
			eventually(t, time.Minute, fmt.Sprintf("node %d's copy of %s converges", tn.i, id), func() error {
				got, err := localDocs(tn.n, id, head)
				if err != nil {
					return err
				}
				return sameDocs(got, want)
			})
		}
	}
}

// ackLog records acknowledged writes: each committed item's seq, shard and id.
type ackLog struct {
	mu   sync.Mutex
	acks map[int64]string // seq to "shard/id"
}

func (a *ackLog) note(shards int, ops []api.WriteOp, res *api.WriteResult) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.acks == nil {
		a.acks = map[int64]string{}
	}
	for i, it := range res.Items {
		if it.Err == nil && it.Seq > 0 {
			a.acks[it.Seq] = fmt.Sprintf("%d/%s", node.ShardFor(ops[i].ID, shards), ops[i].ID)
		}
	}
}

// checkAcks verifies every acknowledged write is in the changelog at its seq, and that
// the changelog's seqs are contiguous from 1.
func (a *ackLog) checkAcks(t testing.TB, changes []store.Change) {
	t.Helper()
	bySeq := map[int64]store.Change{}
	for i := range changes {
		c := &changes[i]
		if i > 0 && c.Seq != changes[i-1].Seq+1 {
			t.Fatalf("the changelog skips from seq %d to %d", changes[i-1].Seq, c.Seq)
		}
		if _, dup := bySeq[c.Seq]; dup {
			t.Fatalf("seq %d appears twice", c.Seq)
		}
		bySeq[c.Seq] = *c
	}
	if len(changes) > 0 && changes[0].Seq != 1 {
		t.Fatalf("the changelog starts at seq %d", changes[0].Seq)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for seq, want := range a.acks {
		c, ok := bySeq[seq]
		if !ok {
			t.Fatalf("acknowledged seq %d (%s) is not in the changelog", seq, want)
		}
		if got := fmt.Sprintf("%d/%s", c.Shard, c.ID); got != want {
			t.Fatalf("seq %d is %s, acknowledged as %s", seq, got, want)
		}
	}
}

// TestConcurrentWritersOnEveryNode (Review Focus 3): many concurrent bulks on all three
// nodes, upserting, re-upserting and deleting overlapping ids across three shards:
// the seqs are contiguous and in order, every acknowledged change is in the changelog
// at its seq, and every copy on every node converges to exactly the store's documents.
func TestConcurrentWritersOnEveryNode(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, noPrune)
		a := c.start(0)
		createIndex(t, a.n, "cw", 3, 0)
		c.start(1)
		c.start(2)
		waitCopies(t, a.st, "cw", 3, 3, time.Minute)
		var acks ackLog
		var wg sync.WaitGroup
		var failed atomic.Pointer[error]
		for _, tn := range c.live() {
			for w := range 4 {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(tn.i), uint64(w)))
					for b := range 12 {
						ops := make([]api.WriteOp, 0, 25)
						for k := range 25 {
							id := fmt.Sprintf("id-%03d", rng.IntN(150))
							if rng.IntN(5) == 0 {
								ops = append(ops, api.WriteOp{Kind: api.OpDelete, ID: id})
								continue
							}
							ops = append(ops, upsertOp(id, tn.i*100000+w*1000+b*25+k))
						}
						ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
						res, err := tn.n.Write(ctx, "cw", ops, api.WriteOptions{})
						cancel()
						if err != nil {
							failed.CompareAndSwap(nil, &err)
							return
						}
						acks.note(3, ops, res)
					}
				})
			}
		}
		wg.Wait()
		if e := failed.Load(); e != nil {
			t.Fatalf("a bulk failed: %v", *e)
		}
		acks.checkAcks(t, allChanges(t, a.st, a.n))
		converge(t, c, a.st, "cw", 3)
	})
}

// TestNodeLossMidBulk (Review Focus 4): a node is killed while bulks run through it and
// through the others. No acknowledged write is lost, and the surviving copies converge
// to the store.
func TestNodeLossMidBulk(t *testing.T) {
	testtier.Heavy(t)
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, noPrune)
		a := c.start(0)
		createIndex(t, a.n, "mb", 2, 2)
		c.start(1)
		c.start(2)
		waitCopies(t, a.st, "mb", 2, 2, time.Minute)
		var acks ackLog
		var wg sync.WaitGroup
		stop := make(chan struct{})
		for _, tn := range c.live() {
			wg.Go(func() {
				for b := 0; ; b++ {
					select {
					case <-stop:
						return
					default:
					}
					ops := make([]api.WriteOp, 0, 20)
					for k := range 20 {
						ops = append(ops, upsertOp(fmt.Sprintf("n%d-b%d-%d", tn.i, b, k), b))
					}
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					res, err := tn.n.Write(ctx, "mb", ops, api.WriteOptions{})
					cancel()
					if err != nil {
						if tn.i == 1 {
							return // the killed node's writes fail: none of them was acknowledged
						}
						t.Errorf("node %d: %v", tn.i, err)
						return
					}
					acks.note(2, ops, res)
				}
			})
		}
		time.Sleep(700 * time.Millisecond)
		c.node(1).kill()
		time.Sleep(300 * time.Millisecond)
		close(stop)
		wg.Wait()
		changes := allChanges(t, a.st, a.n)
		acks.checkAcks(t, changes)
		waitCopies(t, a.st, "mb", 2, 2, time.Minute)
		converge(t, c, a.st, "mb", 2)
		// Every acknowledged upsert is searchable on the survivors.
		head, _, _ := a.st.HeadSeq(context.Background())
		want := int64(0)
		for s := range 2 {
			want += int64(len(truth(t, a.st, store.ShardID{Index: "mb", Shard: s})))
		}
		for _, tn := range c.live() {
			waitCount(t, tn.n, "mb", head, want)
		}
	})
}

// TestNodeLossMidRecovery (Review Focus 4): the peer a new copy recovers from is killed
// mid-transfer. The recovery falls back (to another peer, or the store) and the copy
// converges.
func TestNodeLossMidRecovery(t *testing.T) {
	d := sqliteDB(t)
	slow := make(chan struct{})
	var entered atomic.Bool
	c := newCluster(t, d, func(i int, o *Options) {
		if i == 0 {
			o.hooks.peerFile = func(_ string, w httpResponseWriter) httpResponseWriter {
				if entered.CompareAndSwap(false, true) {
					<-slow
				}
				return w
			}
		}
	})
	a := c.start(0)
	createIndex(t, a.n, "mr", 1, 0)
	last := bulkLoad(t, a.n, "mr", 0, 4000, 2000)
	waitCount(t, a.n, "mr", last, int64(4000))
	b := c.start(1)
	eventually(t, time.Minute, "node-1's recovery reaches node-0", func() error {
		if !entered.Load() {
			return fmt.Errorf("not yet")
		}
		return nil
	})
	c.node(0).kill()
	close(slow)
	eventually(t, time.Minute, "node-1 serves its recovered copy", func() error {
		got, err := count(context.Background(), b.n, "mr", last)
		if err != nil {
			return err
		}
		if got != 4000 {
			return fmt.Errorf("counts %d", got)
		}
		return nil
	})
	converge(t, c, b.st, "mr", 1)
}

// TestNodeLossMidMerge (Review Focus 4): a node is killed while its copies merge the
// many small segments a burst of refreshed writes left. Restarted, it reopens its
// copies (or rebuilds them) and converges; nothing acknowledged is lost.
func TestNodeLossMidMerge(t *testing.T) {
	testtier.Heavy(t)
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, noPrune)
		a := c.start(0)
		createIndex(t, a.n, "mm", 1, 0)
		b := c.start(1)
		waitCopies(t, a.st, "mm", 1, 2, time.Minute)
		var acks ackLog
		for k := range 40 {
			ops := []api.WriteOp{upsertOp(fmt.Sprintf("m%d", k), k), upsertOp(fmt.Sprintf("m%d", k/2), k)}
			res, err := b.n.Write(tctx(t), "mm", ops, api.WriteOptions{Refresh: api.RefreshTrue})
			if err != nil {
				t.Fatal(err)
			}
			acks.note(1, ops, res)
		}
		b.kill() // merges of its 40 small segments are pending or under way
		acks.checkAcks(t, allChanges(t, a.st, a.n))
		c.start(1)
		waitCopies(t, a.st, "mm", 1, 2, time.Minute)
		converge(t, c, a.st, "mm", 1)
	})
}

// TestMultiShardSearchAcrossNodes: an index whose shards live on different nodes is
// searched through a node holding none of some: the query phase runs on the peers'
// copies, the fetch phase on the very generations they pinned, and the hits carry no
// internal refs. A search sorted and paged across shards matches one over the store's
// truth; percolation and stored-document percolation reach the peers' saved queries.
func TestMultiShardSearchAcrossNodes(t *testing.T) {
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		c.start(1)
		c.start(2)
		createIndex(t, a.n, "ms", 6, 1)
		waitCopies(t, a.st, "ms", 6, 1, time.Minute)
		var last int64
		for k := range 60 {
			last = mustWrite(t, c.node(k%3).n, "ms", upsertOp(fmt.Sprintf("d%02d", k), k))
		}
		q := api.WriteOp{Kind: api.OpQueryUpsert, ID: "cheap", Query: json.RawMessage(`{"field":"price","op":"lt","value":10}`)}
		last = max(last, mustWrite(t, a.n, "ms", q))
		for _, tn := range c.live() {
			res, err := tn.n.Search(tctx(t), "ms", &search.Request{
				Query: &query.All{}, Sort: []search.SortField{{Field: "price", Desc: true}}, Size: 7, TrackTotal: search.TrackTotalAll,
			}, api.ReadOptions{WaitForSeq: last})
			if err != nil {
				t.Fatalf("node %d: %v", tn.i, err)
			}
			if res.Total != 60 || len(res.Hits) != 7 {
				t.Fatalf("node %d: total %d, %d hits", tn.i, res.Total, len(res.Hits))
			}
			for i, h := range res.Hits {
				if len(h.Body) == 0 {
					t.Fatalf("node %d: hit %d has no body", tn.i, i)
				}
				var body struct{ Price float64 }
				_ = json.Unmarshal(h.Body, &body)
				if want := float64(59 - i); body.Price != want {
					t.Fatalf("node %d: hit %d has price %v, want %v", tn.i, i, body.Price, want)
				}
			}
			// Percolate a given document and a stored one.
			pres, err := tn.n.Percolate(tctx(t), "ms", &api.PercolateRequest{
				Docs: []json.RawMessage{json.RawMessage(`{"price":3}`)}, IDs: []string{"d05", "d50"},
			}, api.ReadOptions{WaitForSeq: last})
			if err != nil {
				t.Fatalf("node %d percolate: %v", tn.i, err)
			}
			if !slices.Equal(pres.Results[0].Queries, []string{"cheap"}) || !slices.Equal(pres.Results[1].Queries, []string{"cheap"}) ||
				len(pres.Results[2].Queries) != 0 || !pres.Results[2].Found {
				t.Fatalf("node %d percolated %+v", tn.i, pres.Results)
			}
			info, err := tn.n.GetIndex(tctx(t), "ms")
			if err != nil || info.Docs != 60 || info.Queries != 1 {
				t.Fatalf("node %d describes %+v (%v)", tn.i, info, err)
			}
			cat, err := tn.n.Fields(tctx(t), "ms", 3, api.ReadOptions{WaitForSeq: last})
			if err != nil || len(cat.Fields) == 0 {
				t.Fatalf("node %d fields %+v (%v)", tn.i, cat, err)
			}
		}
	})
}

// TestFetchAfterPinLostRetries: when a peer loses the generation a search pinned (the
// pin expires before the fetch), the shard's query phase runs again and the search
// still answers; when every round loses its pins, the client gets a 503 with
// Retry-After, not the peer's 410.
func TestFetchAfterPinLostRetries(t *testing.T) {
	d := sqliteDB(t)
	var (
		c       *cluster
		dropAll atomic.Bool
	)
	c = newCluster(t, d, func(i int, o *Options) {
		o.PinTTL = time.Millisecond
		o.hooks.pinned = func(_ store.ShardID, pin string) {
			if dropAll.Load() {
				c.node(i).n.pins.remove(pin)
			}
		}
	})
	a := c.start(0)
	c.start(1)
	createIndex(t, a.n, "pin", 4, 1)
	waitCopies(t, a.st, "pin", 4, 1, time.Minute)
	var last int64
	for k := range 20 {
		last = mustWrite(t, a.n, "pin", upsertOp(fmt.Sprintf("p%d", k), k))
	}
	// Expire every pin the moment it is made, as the janitor would after a slow
	// query phase.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, tn := range c.live() {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				tn.n.pins.sweep()
				time.Sleep(100 * time.Microsecond)
			}
		})
	}
	for _, tn := range c.live() {
		res, err := tn.n.Search(tctx(t), "pin", &search.Request{Query: &query.All{}, Size: 20, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: last})
		if err != nil {
			var ae *api.Error
			if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
				t.Fatalf("node %d: a search whose every round lost its pins failed with %v, want a 503", tn.i, err)
			}
			t.Logf("node %d: %v", tn.i, err)
			continue
		}
		if res.Total != 20 {
			t.Fatalf("node %d: total %d", tn.i, res.Total)
		}
	}
	close(stop)
	wg.Wait()

	dropAll.Store(true)
	checked := 0
	for _, tn := range c.live() {
		reads := false
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "pin", Shard: s}); !ok {
				reads = true
			}
		}
		if !reads {
			continue
		}
		checked++
		url := fmt.Sprintf("http://%s/indexes/pin/_search?wait_for_seq=%d", tn.addr, last)
		req, err := http.NewRequestWithContext(tctx(t), http.MethodPost, url, strings.NewReader(`{"query": {"all": []}, "size": 20}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
			t.Fatalf("node %d: a search whose every round lost its pins answered %d (Retry-After %q): %s, want a 503 with Retry-After",
				tn.i, resp.StatusCode, resp.Header.Get("Retry-After"), b)
		}
	}
	if checked == 0 {
		t.Fatal("every node hosts every shard: no search reads a peer")
	}
}

// TestDynamicFieldFromAnotherNode: a field one node adds to the mapping (dynamically)
// is known to another at once: a query naming it is no 400, and a strict index takes a
// document with it.
func TestDynamicFieldFromAnotherNode(t *testing.T) {
	d := sqliteDB(t)
	c := newCluster(t, d, func(_ int, o *Options) { o.CatalogInterval = time.Hour })
	a := c.start(0)
	b := c.start(1)
	createIndex(t, a.n, "dyn", 1, 0)
	// b learns of the index (before the new field), then takes a copy; its catalogue
	// is not synced again.
	if err := b.n.SyncCatalog(tctx(t)); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "dyn", 1, 2, time.Minute)
	last := mustWrite(t, a.n, "dyn", api.WriteOp{Kind: api.OpUpsert, ID: "x", Body: json.RawMessage(`{"color":"red"}`)})
	q, problems := query.Parse([]byte(`{"field":"color","op":"eq","value":"red"}`))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	res, err := b.n.Search(tctx(t), "dyn", &search.Request{Query: q, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: last})
	if err != nil || res.Total != 1 {
		t.Fatalf("node-1 searching a field node-0 added: %v (%v)", res, err)
	}
}

// noPrune keeps the whole changelog, for tests that read it back.
func noPrune(_ int, o *Options) { o.PruneInterval = time.Hour }

// httpResponseWriter is http.ResponseWriter, named for the hook's signature.
type httpResponseWriter = http.ResponseWriter
