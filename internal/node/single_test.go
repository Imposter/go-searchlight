package node_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/node/nodetest"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
)

var quiet = slog.New(slog.DiscardHandler)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoreURL = storetest.SQLiteURL(storetest.Migrated(t, filepath.Join(dir, "sl.db")))
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.RefreshInterval = 20 * time.Millisecond
	cfg.ChangelogPollInterval = 20 * time.Millisecond
	cfg.RemapDebounce = 0
	cfg.NodeID = "n1"
	cfg.MergeThreads = 1
	return cfg
}

func openStore(t *testing.T, cfg config.Config) store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), cfg.StoreURL, store.WithLogger(quiet))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func open(t *testing.T, cfg config.Config, st store.Store, mod func(*node.Options)) *node.Single {
	t.Helper()
	o := node.Options{Store: st, Config: cfg, Logger: quiet} // the replica tailer
	if mod != nil {
		mod(&o)
	}
	n, err := node.NewSingle(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	return n
}

// waitServing waits until every copy of n serves: CreateIndex waits for a new index's
// copies only up to max_lag, and a busy disk can stretch their first flush past it.
func waitServing(t *testing.T, n *node.Single) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		h, err := n.Health(ctx(t))
		if err == nil && h.Unassigned == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copies never served: %+v %v", h, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitReady waits until n is ready: after a restart its copies recover (resume
// from their segments) before they serve.
func waitReady(t *testing.T, n *node.Single) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for n.Ready(ctx(t)) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("never ready: %v", n.Ready(ctx(t)))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

func mustWrite(t *testing.T, n *node.Single, index string, ops ...api.WriteOp) *api.WriteResult {
	t.Helper()
	res, err := n.Write(ctx(t), index, ops, api.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for i, it := range res.Items {
		if it.Err != nil {
			t.Fatalf("op %d: %v", i, it.Err)
		}
	}
	return res
}

func upsert(id, body string) api.WriteOp {
	return api.WriteOp{Kind: api.OpUpsert, ID: id, Body: json.RawMessage(body)}
}

func count(t *testing.T, n *node.Single, index string, q query.Node, wait int64) int64 {
	t.Helper()
	resp, err := n.Search(ctx(t), index, &search.Request{Query: q, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: wait})
	if err != nil {
		t.Fatal(err)
	}
	return resp.Total
}

func TestShardForIsStable(t *testing.T) {
	// Every node routes alike, forever: these values must never change.
	got := []int{node.ShardFor("a", 4), node.ShardFor("doc-17", 4), node.ShardFor("x", 7), node.ShardFor("", 3), node.ShardFor("a", 1)}
	if fmt.Sprint(got) != "[0 0 4 1 0]" {
		t.Errorf("ShardFor = %v", got)
	}
}

func TestRestartKeepsData(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "r", api.IndexSpec{Settings: api.IndexSettings{Shards: 3}}); err != nil {
		t.Fatal(err)
	}
	var ops []api.WriteOp
	for i := range 50 {
		ops = append(ops, upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"n": %d}`, i)))
	}
	res := mustWrite(t, n, "r", ops...)
	if got := count(t, n, "r", &query.All{}, res.Seq); got != 50 {
		t.Fatalf("count = %d", got)
	}
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	n2 := open(t, cfg, st, nil)
	// The copies reopen their segments and resume; once ready, all is searchable.
	waitReady(t, n2)
	if got := count(t, n2, "r", &query.All{}, 0); got != 50 {
		t.Fatalf("after a restart count = %d", got)
	}
	res = mustWrite(t, n2, "r", api.WriteOp{Kind: api.OpDelete, ID: "d0"})
	if got := count(t, n2, "r", &query.All{}, res.Seq); got != 49 {
		t.Fatalf("after a delete count = %d", got)
	}
	info, err := n2.GetIndex(ctx(t), "r")
	if err != nil || info.Docs != 49 || info.Settings.Shards != 3 {
		t.Errorf("index = %+v %v", info, err)
	}
}

// TestSearchMatchesBruteForce checks the query-then-fetch over several shards: hits,
// order, bodies and totals equal query.Match over every document.
func TestSearchMatchesBruteForce(t *testing.T) {
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), nil)
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword, "price": schema.Number, "tags": schema.KeywordList}}
	if _, err := n.CreateIndex(ctx(t), "bf", api.IndexSpec{Mapping: m, Settings: api.IndexSettings{Shards: 4}}); err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(7, 9))
	brands := []string{"a", "b", "c", "d"}
	bodies := map[string]string{}
	var ops []api.WriteOp
	for i := range 300 {
		id := fmt.Sprintf("doc%03d", i)
		body := fmt.Sprintf(`{"brand": %q, "price": %d, "tags": [%q]}`, brands[rng.IntN(4)], rng.IntN(1000), brands[rng.IntN(4)])
		bodies[id] = body
		ops = append(ops, upsert(id, body))
	}
	mustWrite(t, n, "bf", ops...)
	// Updates and deletes across shards.
	var later []api.WriteOp
	for i := range 40 {
		id := fmt.Sprintf("doc%03d", rng.IntN(300))
		if i%2 == 0 {
			later = append(later, api.WriteOp{Kind: api.OpDelete, ID: id})
			delete(bodies, id)
		} else {
			body := fmt.Sprintf(`{"brand": "a", "price": %d}`, rng.IntN(1000))
			later = append(later, upsert(id, body))
			bodies[id] = body
		}
	}
	res := mustWrite(t, n, "bf", later...)
	for trial := range 30 {
		lo := rng.IntN(800)
		q := fmt.Sprintf(`{"any": [{"field": "brand", "op": "eq", "value": %q}, {"field": "price", "op": "between", "value": [%d, %d]}]}`, brands[rng.IntN(4)], lo, lo+150)
		qn, ps := query.Parse([]byte(q))
		if len(ps) > 0 {
			t.Fatal(ps)
		}
		var want []string
		prices := map[string]float64{}
		for id, body := range bodies {
			doc, _, err := schema.Analyze(m, id, []byte(body))
			if err != nil {
				t.Fatal(err)
			}
			if query.Match(qn, &doc) {
				want = append(want, id)
				prices[id] = *doc.Fields["price"].Number
			}
		}
		slices.SortFunc(want, func(a, b string) int {
			if prices[a] != prices[b] {
				if prices[a] > prices[b] {
					return -1
				}
				return 1
			}
			if a < b {
				return -1
			}
			return 1
		})
		size := 1 + rng.IntN(40)
		resp, err := n.Search(ctx(t), "bf", &search.Request{Query: qn, Sort: []search.SortField{{Field: "price", Desc: true}}, Size: size, TrackTotal: search.TrackTotalAll},
			api.ReadOptions{WaitForSeq: res.Seq})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Total != int64(len(want)) {
			t.Fatalf("trial %d %s: total %d, want %d", trial, q, resp.Total, len(want))
		}
		got := make([]string, len(resp.Hits))
		for i, h := range resp.Hits {
			got[i] = h.ID
			if string(h.Body) != bodies[h.ID] {
				t.Fatalf("hit %s has body %s, want %s", h.ID, h.Body, bodies[h.ID])
			}
		}
		if exp := want[:min(size, len(want))]; fmt.Sprint(got) != fmt.Sprint(exp) {
			t.Fatalf("trial %d %s:\n got %v\nwant %v", trial, q, got, exp)
		}
	}
}

func TestDynamicMappingUnderConcurrentWriters(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "dyn", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	const writers, rounds = 32, 25
	var wg sync.WaitGroup
	var last, slowest atomic.Int64
	var failures atomic.Int64
	began := time.Now()
	for w := range writers {
		wg.Go(func() {
			for r := range rounds {
				// Each bulk adds fields of its own, and all race to type "shared": the
				// first commit types it, the others' values then do not fit it.
				value := `"text"`
				if w%2 == 0 {
					value = "12"
				}
				var ops []api.WriteOp
				for k := range 3 {
					ops = append(ops, upsert(fmt.Sprintf("w%d-%d-%d", w, r, k), fmt.Sprintf(`{"own%d_%d": %d, "shared": %s}`, w, r, k, value)))
				}
				start := time.Now()
				res, err := n.Write(context.Background(), "dyn", ops, api.WriteOptions{})
				if err != nil {
					failures.Add(1)
					t.Errorf("writer %d round %d: %v", w, r, err)
					return
				}
				for _, it := range res.Items {
					if it.Err != nil {
						failures.Add(1)
						t.Errorf("writer %d round %d: %v", w, r, it.Err)
					}
				}
				slowest.Store(max(slowest.Load(), int64(time.Since(start))))
				for {
					cur := last.Load()
					if res.Seq <= cur || last.CompareAndSwap(cur, res.Seq) {
						break
					}
				}
			}
		})
	}
	wg.Wait()
	t.Logf("%d writers x %d bulks, each adding a field: %v in all, the slowest write %v",
		writers, rounds, time.Since(began).Round(time.Millisecond), time.Duration(slowest.Load()).Round(time.Millisecond))
	if failures.Load() != 0 {
		t.Fatalf("%d writes failed", failures.Load())
	}
	want := writers*rounds + 1
	info, err := n.GetIndex(ctx(t), "dyn")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Mapping.Fields) != want {
		t.Errorf("mapping holds %d fields, want %d", len(info.Mapping.Fields), want)
	}
	meta, err := st.Indexes().Get(ctx(t), "dyn")
	if err != nil {
		t.Fatal(err)
	}
	var stored schema.Mapping
	if err := json.Unmarshal(meta.Mapping, &stored); err != nil || len(stored.Fields) != want {
		t.Errorf("stored mapping holds %d fields: %v", len(stored.Fields), err)
	}
	if got := count(t, n, "dyn", &query.All{}, last.Load()); got != writers*rounds*3 {
		t.Errorf("count = %d", got)
	}
}

func TestPatchMappingOnEveryDynamicMode(t *testing.T) {
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), nil)
	for name, mode := range map[string]schema.DynamicMode{"loose": schema.DynamicFalse, "strict": schema.DynamicStrict, "dyn": schema.DynamicTrue} {
		if _, err := n.CreateIndex(ctx(t), name, api.IndexSpec{Mapping: &schema.Mapping{Dynamic: mode}, Settings: api.IndexSettings{Shards: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	// A document under dynamic false keeps its unmapped field unindexed.
	mustWrite(t, n, "loose", upsert("1", `{"colour": "red"}`))
	if info, _ := n.GetIndex(ctx(t), "loose"); len(info.Mapping.Fields) != 0 {
		t.Errorf("dynamic false grew the mapping: %v", info.Mapping.Fields)
	}
	// Adding fields works on every mode (the replica rebuilds a copy whose
	// documents hold a field a mapping change maps).
	add := map[string]schema.FieldType{"price": schema.Number}
	for _, name := range []string{"loose", "strict", "dyn"} {
		info, err := n.PatchMapping(ctx(t), name, add)
		if err != nil || info.Mapping.Fields["price"] != schema.Number {
			t.Errorf("%s: %+v %v", name, info, err)
		}
	}
}

func TestDropIndexRemovesCopies(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	// A copy of an incarnation the catalogue no longer has is removed at start.
	stale := filepath.Join(cfg.DataDir, "indexes", "0123456789abcdef")
	if err := os.MkdirAll(filepath.Join(stale, "0"), 0o750); err != nil {
		t.Fatal(err)
	}
	n := open(t, cfg, st, nil)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("a stale copy survived the start: %v", err)
	}
	info, err := n.CreateIndex(ctx(t), "gone", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}})
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, n, "gone", upsert("a", `{"x": 1}`))
	dir := filepath.Join(cfg.DataDir, "indexes", info.UID)
	if _, err := os.Stat(dir); err != nil {
		t.Fatal(err)
	}
	if err := n.DeleteIndex(ctx(t), "gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the dropped index's copies remain: %v", err)
	}
	if _, err := n.GetIndex(ctx(t), "gone"); err == nil {
		t.Error("a dropped index is still described")
	}
	again, err := n.CreateIndex(ctx(t), "gone", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}})
	if err != nil || again.UID == info.UID || again.Docs != 0 {
		t.Errorf("recreated = %+v %v", again, err)
	}
	if h, _ := n.Health(ctx(t)); h.Status != api.StatusGreen {
		t.Errorf("health after a drop = %+v", h)
	}
}

// haltingTailer stops at once, as a copy that cannot apply a change does.
type haltingTailer struct {
	node.Tailer
}

func (t haltingTailer) Run(context.Context) error { return errors.New("cannot apply seq 3") }

// recoveringTailer reports that its copy is being rebuilt.
type recoveringTailer struct {
	node.Tailer
}

func (recoveringTailer) StateName() string { return node.StateRecovering }

func TestCopyStatesInHealth(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	var wrap atomic.Value
	wrap.Store("")
	n := open(t, cfg, st, func(o *node.Options) {
		o.NewTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			base := nodetest.NewTailer(st, sh, id, env)
			switch wrap.Load() {
			case "halt":
				return haltingTailer{base}
			case "recover":
				return recoveringTailer{base}
			}
			return base
		}
	})
	if err := n.Ready(ctx(t)); err != nil {
		t.Fatalf("an empty node is not ready: %v", err)
	}
	if _, err := n.CreateIndex(ctx(t), "ok", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	wrap.Store("recover")
	if _, err := n.CreateIndex(ctx(t), "rec", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	h, _ := n.Health(ctx(t))
	// One node: a recovering copy leaves its shard with no serving copy (red).
	if h.Status != api.StatusRed || h.Unassigned != 1 {
		t.Errorf("a recovering copy: health %+v", h)
	}
	if err := n.Ready(ctx(t)); err == nil {
		t.Error("ready with a recovering copy")
	}
	// A recovering copy is neither read nor written, at once (a 503), not after a
	// deadline: the shard it exposes may be empty or half loaded.
	var rae *api.Error
	start := time.Now()
	if _, err := n.Search(ctx(t), "rec", &search.Request{Query: &query.All{}}, api.ReadOptions{}); !errors.As(err, &rae) || rae.Status != 503 {
		t.Errorf("a search of a recovering copy: %v", err)
	}
	if _, err := n.Write(ctx(t), "rec", []api.WriteOp{upsert("1", `{}`)}, api.WriteOptions{}); !errors.As(err, &rae) || rae.Status != 503 {
		t.Errorf("a write to a recovering copy: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("failing fast took %v", time.Since(start))
	}
	wrap.Store("halt")
	if _, err := n.CreateIndex(ctx(t), "bad", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	// Wait for the copy itself to halt: health is already red, for "rec".
	deadline := time.Now().Add(10 * time.Second)
	for {
		shards, _ := n.Shards(ctx(t))
		if halted := slices.ContainsFunc(shards, func(s api.ShardInfo) bool { return s.Index == "bad" && s.State == api.ShardHalted }); halted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the halting copy never halted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if h, _ = n.Health(ctx(t)); h.Status != api.StatusRed || h.Unassigned != 2 {
		t.Fatalf("a halted and a recovering copy: health %+v", h)
	}
	shards, _ := n.Shards(ctx(t))
	var states []string
	for _, s := range shards {
		states = append(states, s.Index+":"+s.State)
	}
	if fmt.Sprint(states) != "[bad:halted ok:serving rec:recovering]" {
		t.Errorf("shards = %v", states)
	}
	// A halted copy takes no writes: its changes would never be applied.
	_, err := n.Write(ctx(t), "bad", []api.WriteOp{upsert("1", `{}`)}, api.WriteOptions{})
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != 503 {
		t.Errorf("a write to a halted copy: %v", err)
	}
}

func TestBackgroundLoopsRunUntilClose(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	started, stopped := make(chan struct{}), make(chan struct{})
	n := open(t, cfg, st, func(o *node.Options) {
		o.Background = []func(context.Context) error{func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil
		}}
	})
	<-started
	select {
	case <-stopped:
		t.Fatal("the loop stopped before Close")
	default:
	}
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not stop the loop")
	}
}

func TestWriteValidation(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxDocBytes = 64
	n := open(t, cfg, openStore(t, cfg), nil)
	m := &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{"t": schema.Text}}
	if _, err := n.CreateIndex(ctx(t), "v", api.IndexSpec{Mapping: m, Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	res, err := n.Write(ctx(t), "v", []api.WriteOp{
		upsert("ok", `{"t": "fine"}`),
		upsert("", `{"t": "x"}`),
		upsert("big", fmt.Sprintf(`{"t": %q}`, string(make([]byte, 80)))),
		upsert("strict", `{"u": 1}`),
		{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "t", "op": "lt", "value": 3}`)},
		{Kind: api.OpQueryUpsert, ID: "q2", Query: json.RawMessage(`{"field": "t", "op": "contains", "value": "fin"}`), Meta: json.RawMessage(`{"a": 1}`)},
		{Kind: api.OpQueryUpsert, ID: "q3", Query: json.RawMessage(`{"all": []}`), Meta: json.RawMessage(`[1]`)},
	}, api.WriteOptions{Percolate: true})
	if err != nil {
		t.Fatal(err)
	}
	var statuses []int
	for _, it := range res.Items {
		if it.Err == nil {
			statuses = append(statuses, 200)
		} else {
			statuses = append(statuses, api.ProblemFor(it.Err).Status)
		}
	}
	if fmt.Sprint(statuses) != "[200 400 413 400 400 200 400]" {
		t.Errorf("statuses = %v", statuses)
	}
	if res.Items[0].Seq == 0 || res.Items[5].Seq != res.Seq || res.Items[0].Seq >= res.Items[5].Seq {
		t.Errorf("seqs: %+v, last %d", res.Items, res.Seq)
	}
	if _, err := n.Write(ctx(t), "nope", []api.WriteOp{upsert("a", `{}`)}, api.WriteOptions{}); err == nil {
		t.Error("a write to a missing index succeeded")
	}
}

// blockingDrops is a store whose index drops wait for release, as a big index's do.
type blockingDrops struct {
	store.Store
	store.RecordReader
	indexes *blockingIndexes
}

func (s blockingDrops) Indexes() store.IndexStore { return s.indexes }

type blockingIndexes struct {
	store.IndexStore
	entered, release chan struct{}
}

func (x *blockingIndexes) Drop(ctx context.Context, name string) error {
	x.entered <- struct{}{}
	<-x.release
	return x.IndexStore.Drop(ctx, name)
}

// I2: dropping an index runs outside the node lock: while the store deletes a big
// index, the node serves and creates other indexes.
func TestDropDoesNotBlockOtherIndexes(t *testing.T) {
	cfg := testConfig(t)
	base := openStore(t, cfg)
	bi := &blockingIndexes{IndexStore: base.Indexes(), entered: make(chan struct{}), release: make(chan struct{})}
	st := blockingDrops{Store: base, RecordReader: base.(store.RecordReader), indexes: bi} //nolint:forcetypeassert,errcheck // every store reads records
	n := open(t, cfg, st, nil)
	for _, name := range []string{"big", "other"} {
		if _, err := n.CreateIndex(ctx(t), name, api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	res := mustWrite(t, n, "other", upsert("a", `{"x": 1}`))
	done := make(chan error, 1)
	go func() { done <- n.DeleteIndex(context.Background(), "big") }()
	<-bi.entered
	// The drop is under way: other indexes answer, a new one is created, and the
	// dropped name is taken by no one.
	if got := count(t, n, "other", &query.All{}, res.Seq); got != 1 {
		t.Errorf("count = %d", got)
	}
	if _, err := n.CreateIndex(ctx(t), "third", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Errorf("a create during a drop: %v", err)
	}
	var ae *api.Error
	if _, err := n.CreateIndex(ctx(t), "big", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); !errors.As(err, &ae) || ae.Status != 409 {
		t.Errorf("a create of the name being dropped: %v", err)
	}
	if _, err := n.GetIndex(ctx(t), "big"); !errors.As(err, &ae) || ae.Status != 404 {
		t.Errorf("an index being dropped: %v", err)
	}
	close(bi.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if list, _ := n.ListIndexes(ctx(t)); len(list) != 2 {
		t.Errorf("indexes after the drop: %d", len(list))
	}
}

// flakyStore is a store whose database can be taken away: Ping and record reads fail
// while down, and HeadSeq while headDown.
type flakyStore struct {
	store.Store
	rr   store.RecordReader
	down *atomic.Bool
	// headDown, when set, fails HeadSeq too.
	headDown *atomic.Bool
	// hung, when set, makes Ping and record reads wait for their deadline: a
	// database that hangs rather than fails.
	hung   *atomic.Bool
	pinged chan error
}

func (s flakyStore) hang(ctx context.Context) error {
	if s.hung != nil && s.hung.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

var errUnreachable = errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")

func (s flakyStore) Ping(ctx context.Context) error {
	err := s.ping(ctx)
	if s.pinged != nil {
		select {
		case s.pinged <- err:
		case <-ctx.Done():
		}
	}
	return err
}

func (s flakyStore) ping(ctx context.Context) error {
	if err := s.hang(ctx); err != nil {
		return err
	}
	if s.down.Load() {
		return errUnreachable
	}
	return s.Store.Ping(ctx)
}

func (s flakyStore) GetRecord(ctx context.Context, kind store.RecordKind, id store.ShardID, key string) (store.Record, error) {
	if err := s.hang(ctx); err != nil {
		return store.Record{}, err
	}
	if s.down.Load() {
		return store.Record{}, errUnreachable
	}
	return s.rr.GetRecord(ctx, kind, id, key)
}

func (s flakyStore) HeadSeq(ctx context.Context) (int64, time.Time, error) {
	if s.headDown != nil && s.headDown.Load() {
		return 0, time.Time{}, errUnreachable
	}
	return s.Store.HeadSeq(ctx)
}

func (s flakyStore) ListQueries(ctx context.Context, index, after string, limit int) ([]store.Record, error) {
	if s.down.Load() {
		return nil, errUnreachable
	}
	return s.rr.ListQueries(ctx, index, after, limit)
}

// I5 (spec section 10): with the database unreachable, reads keep serving from the
// copies marked stale, GET falls back to the copy, and readiness turns false after
// max_lag; it all recovers with the database.
func TestDatabaseUnreachable(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxLag = 300 * time.Millisecond
	base := openStore(t, cfg)
	down := &atomic.Bool{}
	headDown := &atomic.Bool{}
	st := flakyStore{Store: base, rr: base.(store.RecordReader), down: down, headDown: headDown} //nolint:forcetypeassert,errcheck // every store reads records
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "s", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	res := mustWrite(t, n, "s", upsert("a", `{"x": 1}`),
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "x", "op": "eq", "value": 1}`), Meta: json.RawMessage(`{"m": 1}`)})
	sr, err := n.Search(ctx(t), "s", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: res.Seq})
	if err != nil || sr.Stale || sr.Total != 1 {
		t.Fatalf("a search with the database up: %+v %v", sr, err)
	}
	if err := n.Ready(ctx(t)); err != nil {
		t.Fatalf("not ready with the database up: %v", err)
	}

	down.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for n.Ready(ctx(t)) == nil {
		if time.Now().After(deadline) {
			t.Fatal("still ready long after the database went away")
		}
		time.Sleep(20 * time.Millisecond)
	}
	sr, err = n.Search(ctx(t), "s", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
	if err != nil || !sr.Stale || sr.Total != 1 {
		t.Errorf("a search with the database down: %+v %v", sr, err)
	}
	doc, err := n.GetDocument(ctx(t), "s", "a")
	if err != nil || !doc.Stale || string(doc.Body) != `{"x": 1}` {
		t.Errorf("GET with the database down: %+v %v", doc, err)
	}
	q, err := n.GetQuery(ctx(t), "s", "q")
	if err != nil || !q.Stale || string(q.Meta) != `{"m":1}` || !strings.Contains(string(q.Query), `"op":"eq"`) {
		t.Errorf("GET query with the database down: %+v %s %v", q, q.Query, err)
	}
	var ae *api.Error
	if _, err := n.GetDocument(ctx(t), "s", "missing"); !errors.As(err, &ae) || ae.Status != 404 || ae.Extra["stale"] != true {
		t.Errorf("a missing document with the database down: %v", err)
	}
	// A seq another node may have committed cannot be checked: retryable, not invalid.
	headDown.Store(true)
	if _, err := n.Search(ctx(t), "s", &search.Request{Query: &query.All{}}, api.ReadOptions{WaitForSeq: res.Seq + 10}); !errors.As(err, &ae) || ae.Status != 503 {
		t.Errorf("a wait_for_seq past the node's head with the database down: %v, want a 503", err)
	}
	headDown.Store(false)
	if h, _ := n.Health(ctx(t)); h.Status != api.StatusGreen {
		t.Errorf("the copies still serve: health %+v", h)
	}

	down.Store(false)
	deadline = time.Now().Add(5 * time.Second)
	for n.Ready(ctx(t)) != nil {
		if time.Now().After(deadline) {
			t.Fatal("not ready after the database came back")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sr, _ := n.Search(ctx(t), "s", &search.Request{Query: &query.All{}}, api.ReadOptions{}); sr.Stale {
		t.Error("still stale with the database back")
	}
}

// Readiness and staleness follow the database by the node's clock: unready once the
// database has not answered for more than max_lag, stale once a ping fails, and both
// cleared once a ping answers. The clock is fake: each ping is one advance of the ping
// interval, and the test waits for the node to record each answer before it advances
// again, so the answer is stamped at the time of its ping.
func TestReadinessFollowsTheDatabaseByTheClock(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxLag = time.Second
	const pingEvery = 250 * time.Millisecond // max_lag/4
	base := openStore(t, cfg)
	down := &atomic.Bool{}
	pinged := make(chan error)
	st := flakyStore{Store: base, rr: base.(store.RecordReader), down: down, pinged: pinged} //nolint:forcetypeassert,errcheck // every store reads records
	clk := clock.NewFake(time.Now())
	n := open(t, cfg, st, func(o *node.Options) { o.Clock = clk })
	ping := func(wantUp bool) {
		t.Helper()
		if err := clk.BlockUntilArmed(ctx(t), pingEvery); err != nil {
			t.Fatalf("the ping loop is not waiting for its next ping: %v", err)
		}
		clk.Advance(pingEvery)
		select {
		case err := <-pinged:
			if (err == nil) != wantUp {
				t.Fatalf("a ping answered %v with the database up %v", err, wantUp)
			}
		case <-ctx(t).Done():
			t.Fatal("no ping")
		}
		recorded := func() bool { return n.DBStale() }
		if wantUp {
			recorded = func() bool { return n.DBAnsweredWithin(0) && !n.DBStale() }
		}
		for wait := ctx(t); !recorded(); runtime.Gosched() {
			if wait.Err() != nil {
				t.Fatal("the node did not record the ping's answer")
			}
		}
	}
	if err := n.Ready(ctx(t)); err != nil || n.DBStale() {
		t.Fatalf("a fresh node: ready %v, stale %v", err, n.DBStale())
	}
	ping(true)
	answered := clk.Now()

	down.Store(true)
	ping(false)
	for clk.Since(answered)+pingEvery <= cfg.MaxLag {
		ping(false)
		if !n.DBStale() {
			t.Fatal("reads are not stale while the pings fail")
		}
		if err := n.Ready(ctx(t)); err != nil {
			t.Fatalf("unready %s after the database last answered, within max_lag: %v", clk.Since(answered), err)
		}
	}
	ping(false)
	if err := n.Ready(ctx(t)); err == nil {
		t.Fatalf("still ready %s after the database last answered", clk.Since(answered))
	}
	if n.DBAnsweredWithin(cfg.MaxLag) {
		t.Fatal("the database counts as answering within max_lag")
	}

	down.Store(false)
	ping(true)
	ping(true)
	if err := n.Ready(ctx(t)); err != nil || n.DBStale() {
		t.Fatalf("the database answers again: ready %v, stale %v", err, n.DBStale())
	}
}

// I1(d): an index holds at most max_index_fields fields; a document or mapping change
// past it is refused at the field.
func TestFieldLimit(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxIndexFields = 4
	n := open(t, cfg, openStore(t, cfg), nil)
	if _, err := n.CreateIndex(ctx(t), "lim", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	res, err := n.Write(ctx(t), "lim", []api.WriteOp{
		upsert("1", `{"a": 1, "b": 2}`),
		upsert("2", `{"c": 1, "d": 2, "e": 3}`),
		upsert("3", `{"a": 5, "c": 6}`),
	}, api.WriteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var ae *api.Error
	if res.Items[0].Err != nil || res.Items[2].Err != nil {
		t.Errorf("writes under the limit: %+v", res.Items)
	}
	if !errors.As(res.Items[1].Err, &ae) || ae.Status != 400 || ae.Problems[0].Loc != "body.c" {
		t.Errorf("a document past the limit: %v", res.Items[1].Err)
	}
	info, _ := n.GetIndex(ctx(t), "lim")
	if len(info.Mapping.Fields) != 3 {
		t.Errorf("mapping = %v", info.Mapping.Fields)
	}
	if _, err := n.PatchMapping(ctx(t), "lim", map[string]schema.FieldType{"x": schema.Number, "y": schema.Number}); !errors.As(err, &ae) || ae.Status != 400 || ae.Problems[0].Loc != "fields.x" {
		t.Errorf("a mapping change past the limit: %v", err)
	}
	if _, err := n.PatchMapping(ctx(t), "lim", map[string]schema.FieldType{"x": schema.Number}); err != nil {
		t.Errorf("a mapping change to the limit: %v", err)
	}
}

// I5: a database that hangs rather than fails: GET falls back to the copy within
// max_lag instead of waiting out the request, the pings time out and mark reads
// stale, and once marked, GET goes straight to the copy.
func TestDatabaseHangs(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxLag = 400 * time.Millisecond
	base := openStore(t, cfg)
	down, hung := &atomic.Bool{}, &atomic.Bool{}
	st := flakyStore{Store: base, rr: base.(store.RecordReader), down: down, hung: hung} //nolint:forcetypeassert,errcheck // every store reads records
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "h", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	res := mustWrite(t, n, "h", upsert("a", `{"x": 1}`))
	if got := count(t, n, "h", &query.All{}, res.Seq); got != 1 {
		t.Fatalf("count = %d", got)
	}
	hung.Store(true)
	start := time.Now()
	doc, err := n.GetDocument(ctx(t), "h", "a")
	if err != nil || !doc.Stale || time.Since(start) > 3*time.Second {
		t.Fatalf("GET from a hung database: %+v %v after %v", doc, err, time.Since(start))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		sr, err := n.Search(ctx(t), "h", &search.Request{Query: &query.All{}}, api.ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if sr.Stale {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reads never marked stale with the database hung")
		}
		time.Sleep(20 * time.Millisecond)
	}
	start = time.Now()
	if doc, err := n.GetDocument(ctx(t), "h", "a"); err != nil || !doc.Stale || time.Since(start) > 200*time.Millisecond {
		t.Errorf("GET once stale: %+v %v after %v", doc, err, time.Since(start))
	}
	hung.Store(false)
	deadline = time.Now().Add(5 * time.Second)
	for {
		sr, _ := n.Search(ctx(t), "h", &search.Request{Query: &query.All{}}, api.ReadOptions{})
		if !sr.Stale {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("still stale after the database recovered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if doc, err := n.GetDocument(ctx(t), "h", "a"); err != nil || doc.Stale || doc.Seq == 0 {
		t.Errorf("GET after recovery: %+v %v", doc, err)
	}
}

// A settings-only patch reuses the mapping's stored bytes, so the store logs no
// mapping change (which would cost every copy a remap); a mapping patch logs one per
// shard and the node adopts the new mapping version.
func TestSettingsPatchLogsNoMappingChange(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "s", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	head := func() int64 {
		h, _, err := st.HeadSeq(ctx(t))
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	before := head()
	d := 250 * time.Millisecond
	if _, err := n.PatchSettings(ctx(t), "s", api.SettingsPatch{RefreshInterval: &d}); err != nil {
		t.Fatal(err)
	}
	if after := head(); after != before {
		t.Errorf("a settings patch logged %d changes", after-before)
	}
	meta, _ := st.Indexes().Get(ctx(t), "s")
	if _, err := n.PatchMapping(ctx(t), "s", map[string]schema.FieldType{"x": schema.Number}); err != nil {
		t.Fatal(err)
	}
	if after := head(); after != before+2 {
		t.Errorf("a mapping patch on 2 shards logged %d changes", after-before)
	}
	meta2, _ := st.Indexes().Get(ctx(t), "s")
	if meta2.MappingVersion != meta.MappingVersion+1 {
		t.Errorf("mapping version %d -> %d", meta.MappingVersion, meta2.MappingVersion)
	}
}

// A saved query committed before a restart but not yet applied by the copy is seen by
// the first percolation after it: the copies' saved-query wait starts at the head.
func TestPercolateAfterRestartSeesQueries(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	var fake *nodetest.Tailer
	n := open(t, cfg, st, func(o *node.Options) {
		o.NewTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			tl := nodetest.NewTailer(st, sh, id, env)
			fake = tl.(*nodetest.Tailer) //nolint:forcetypeassert,errcheck // NewTailer returns a *Tailer
			return tl
		}
	})
	if _, err := n.CreateIndex(ctx(t), "p", api.IndexSpec{Mapping: &schema.Mapping{Fields: map[string]schema.FieldType{"x": schema.Number}}, Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	fake.Pause() // the query commits but the copy never applies it before the restart
	mustWrite(t, n, "p", api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "x", "op": "eq", "value": 1}`)})
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	n2 := open(t, cfg, st, nil)
	waitReady(t, n2)
	res, err := n2.Percolate(ctx(t), "p", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"x": 1}`)}}, api.ReadOptions{})
	if err != nil || string(res.Results[0].Queries) != `["q"]` {
		t.Errorf("percolation after a restart: %+v %v", res, err)
	}
}

// laggingTailer reports a copy an hour behind.
type laggingTailer struct {
	node.Tailer
}

func (laggingTailer) Lag() (int64, time.Duration) { return 10, time.Hour }

// A copy that trails the changelog by more than max_lag marks reads stale and health
// yellow, but leaves the node ready: one busy copy must not take it out of rotation.
func TestLaggingCopyIsStaleNotUnready(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, func(o *node.Options) {
		o.NewTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			return laggingTailer{nodetest.NewTailer(st, sh, id, env)}
		}
	})
	if _, err := n.CreateIndex(ctx(t), "lag", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := n.Ready(ctx(t)); err != nil {
		t.Errorf("a lagging copy made the node unready: %v", err)
	}
	res, err := n.Search(ctx(t), "lag", &search.Request{Query: &query.All{}}, api.ReadOptions{})
	if err != nil || !res.Stale {
		t.Errorf("a read of a lagging copy: %+v %v", res, err)
	}
	h, _ := n.Health(ctx(t))
	shards, _ := n.Shards(ctx(t))
	if h.Status != api.StatusYellow || len(shards) != 1 || !shards[0].Stale || shards[0].State != api.ShardServing {
		t.Errorf("health %+v, shards %+v", h, shards)
	}
}

// flippingTailer reports tailing for the next ok calls of StateName, then recovering:
// a copy the tailer marks recovering between a reader's check and its Acquire.
type flippingTailer struct {
	node.Tailer
	ok *atomic.Int32
}

func (f flippingTailer) StateName() string {
	if f.ok.Add(-1) >= 0 {
		return node.StateTailing
	}
	return node.StateRecovering
}

// A reader that checked the copy serves, then acquired its shard, checks again after:
// a copy marked recovering in between (its tailer about to swap in an empty shard) is
// not read.
func TestAcquireRechecksTheCopy(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	ok := &atomic.Int32{}
	ok.Store(1 << 20)
	n := open(t, cfg, st, func(o *node.Options) {
		o.NewTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			return flippingTailer{Tailer: nodetest.NewTailer(st, sh, id, env), ok: ok}
		}
	})
	if _, err := n.CreateIndex(ctx(t), "fl", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Search(ctx(t), "fl", &search.Request{Query: &query.All{}}, api.ReadOptions{}); err != nil {
		t.Fatalf("a read of a serving copy: %v", err)
	}
	ok.Store(1) // serving at the check, recovering by the recheck
	_, err := n.Search(ctx(t), "fl", &search.Request{Query: &query.All{}}, api.ReadOptions{})
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != 503 {
		t.Errorf("a read racing the copy going recovering: %v", err)
	}
}

// closedCopy writes 40 documents to a one-shard index on a node, closes the node and
// returns the copy's directory and the last write's seq.
func closedCopy(t *testing.T, cfg config.Config, st store.Store) (string, int64) {
	t.Helper()
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "c", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	var ops []api.WriteOp
	for i := range 40 {
		ops = append(ops, upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"n": %d, "title": "item %d"}`, i, i)))
	}
	res := mustWrite(t, n, "c", ops...)
	if got := count(t, n, "c", &query.All{}, res.Seq); got != 40 {
		t.Fatalf("count = %d", got)
	}
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}
	dirs, err := filepath.Glob(filepath.Join(cfg.DataDir, "indexes", "*", "0"))
	if err != nil || len(dirs) != 1 {
		t.Fatalf("the copy's directory under %s: %v %v", cfg.DataDir, dirs, err)
	}
	return dirs[0], res.Seq
}

// segmentsOf lists the segment files in dir.
func segmentsOf(t *testing.T, dir string) []string {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*.seg"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segment in %s: %v", dir, err)
	}
	return segs
}

// TestCorruptSegmentIsRebuilt: a copy whose segment fails its checksum when the node
// opens it is never served: the node wipes it and the copy is rebuilt from the store,
// whole.
func TestCorruptSegmentIsRebuilt(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	dir, seq := closedCopy(t, cfg, st)
	seg := segmentsOf(t, dir)[0]
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(seg, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	n2 := open(t, cfg, st, nil)
	waitReady(t, n2)
	if got := count(t, n2, "c", &query.All{}, seq); got != 40 {
		t.Fatalf("after the corrupt copy was rebuilt count = %d, want 40", got)
	}
	if _, err := os.Stat(seg); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the corrupt segment %s is still there: %v", seg, err)
	}
}

// TestNewerFormatSegmentIsRefused: a copy holding a segment of a newer format than this
// build reads is refused, and its files are left as they are: a binary rolled back
// must not destroy its successor's copy.
func TestNewerFormatSegmentIsRefused(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	dir, _ := closedCopy(t, cfg, st)
	seg := segmentsOf(t, dir)[0]
	raw, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint16(raw[8:], segment.FormatMajor+1)
	if err := os.WriteFile(seg, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	_, err = node.NewSingle(context.Background(), node.Options{Store: st, Config: cfg, Logger: quiet})
	if !errors.Is(err, segment.ErrNewerFormat) {
		t.Fatalf("opening a copy of a newer format: %v, want segment.ErrNewerFormat", err)
	}
	if got, err := os.ReadFile(seg); err != nil || !bytes.Equal(got, raw) {
		t.Fatalf("the newer-format segment was not left as it was: %v", err)
	}
}

// TestUnreadableCopyIsNotWiped: a copy whose files cannot be read for a reason other
// than their content (here its manifest is a directory: an I/O error, as a permission
// or a resource error would be) is not wiped: the error is returned, and the files stay.
func TestUnreadableCopyIsNotWiped(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	dir, _ := closedCopy(t, cfg, st)
	segs := segmentsOf(t, dir)
	manifest := filepath.Join(dir, "manifest")
	if err := os.Rename(manifest, manifest+".aside"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(manifest, "in-the-way"), 0o750); err != nil {
		t.Fatal(err)
	}

	_, err := node.NewSingle(context.Background(), node.Options{Store: st, Config: cfg, Logger: quiet})
	if err == nil || segment.Rebuildable(err) {
		t.Fatalf("opening a copy whose manifest cannot be read: %v, want an error that is not corruption", err)
	}
	for _, seg := range segs {
		if _, err := os.Stat(seg); err != nil {
			t.Errorf("the copy was wiped over an I/O error: %v", err)
		}
	}
}
