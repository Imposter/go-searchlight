package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/node/nodetest"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

var quiet = slog.New(slog.DiscardHandler)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoreURL = "sqlite:///" + filepath.ToSlash(filepath.Join(dir, "sl.db"))
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.RefreshInterval = 20 * time.Millisecond
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
	o := node.Options{Store: st, Config: cfg, NewTailer: nodetest.NewTailer, Logger: quiet}
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
	// The copies reopen their segments: everything is searchable at once.
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

func TestPatchMappingRefusesDynamicFalse(t *testing.T) {
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), nil)
	for name, mode := range map[string]schema.DynamicMode{"loose": schema.DynamicFalse, "strict": schema.DynamicStrict, "dyn": schema.DynamicTrue} {
		if _, err := n.CreateIndex(ctx(t), name, api.IndexSpec{Mapping: &schema.Mapping{Dynamic: mode}, Settings: api.IndexSettings{Shards: 1}}); err != nil {
			t.Fatal(err)
		}
	}
	add := map[string]schema.FieldType{"price": schema.Number}
	_, err := n.PatchMapping(ctx(t), "loose", add)
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != 409 {
		t.Errorf("dynamic false: err = %v, want a 409", err)
	}
	for _, name := range []string{"strict", "dyn"} {
		info, err := n.PatchMapping(ctx(t), name, add)
		if err != nil || info.Mapping.Fields["price"] != schema.Number {
			t.Errorf("%s: %+v %v", name, info, err)
		}
	}
	// A document under dynamic false keeps its unmapped field unindexed.
	res := mustWrite(t, n, "loose", upsert("1", `{"colour": "red"}`))
	if info, _ := n.GetIndex(ctx(t), "loose"); len(info.Mapping.Fields) != 0 {
		t.Errorf("dynamic false grew the mapping: %v", info.Mapping.Fields)
	}
	if got := count(t, n, "loose", &query.All{}, res.Seq); got != 1 {
		t.Errorf("count = %d", got)
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

// flakyStore is a store whose database can be taken away: Ping and record reads
// fail while down.
type flakyStore struct {
	store.Store
	rr   store.RecordReader
	down *atomic.Bool
	// hung, when set, makes Ping and record reads wait for their deadline: a
	// database that hangs rather than fails.
	hung *atomic.Bool
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
	st := flakyStore{Store: base, rr: base.(store.RecordReader), down: down} //nolint:forcetypeassert,errcheck // every store reads records
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "s", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
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
