package node_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/search"
)

func aggSearch(t *testing.T, raw string) *search.Request {
	t.Helper()
	r, ps := search.ParseRequest([]byte(raw))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	return r
}

// TestRequestCacheOnANode: a node's size-0 searches are answered from its request
// cache until a write changes the shards, and its copies share their results through
// Redis, which a restarted node reads.
func TestRequestCacheOnANode(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := testConfig(t)
	cfg.RequestCacheRedisURL = "redis://" + mr.Addr()
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "rc", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	var ops []api.WriteOp
	for i := range 40 {
		ops = append(ops, upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand": "b%d", "n": %d}`, i%4, i)))
	}
	res := mustWrite(t, n, "rc", ops...)
	const raw = `{"size": 0, "aggs": {"brands": {"terms": {"field": "brand"}}, "n": {"stats": {"field": "n"}}}}`
	first, err := n.Search(ctx(t), "rc", aggSearch(t, raw), api.ReadOptions{WaitForSeq: res.Seq})
	if err != nil {
		t.Fatal(err)
	}
	second, err := n.Search(ctx(t), "rc", aggSearch(t, raw), api.ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Total != 40 || !reflect.DeepEqual(first.Aggs, second.Aggs) {
		t.Fatalf("totals %d, %d; aggregations equal %v", first.Total, second.Total, reflect.DeepEqual(first.Aggs, second.Aggs))
	}
	if s := n.RequestCacheStats(); s.Hits < 2 {
		t.Fatalf("stats %v: want both shards' second search answered from memory", s)
	}
	// A copy's contents are named for Redis once a poll has checked its own mark;
	// a memory hit after that writes the result there.
	waitKeys := func(want int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for len(mr.Keys()) < want {
			if time.Now().After(deadline) {
				t.Fatalf("Redis holds %v: want %d results", mr.Keys(), want)
			}
			if _, err := n.Search(ctx(t), "rc", aggSearch(t, raw), api.ReadOptions{}); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitKeys(2)

	res = mustWrite(t, n, "rc", upsert("late", `{"brand": "b9", "n": 100}`))
	after, err := n.Search(ctx(t), "rc", aggSearch(t, raw), api.ReadOptions{WaitForSeq: res.Seq})
	if err != nil {
		t.Fatal(err)
	}
	if after.Total != 41 {
		t.Fatalf("total %d after a write, want 41", after.Total)
	}
	// The write changed one shard: its new result is a third key, the other's stands.
	waitKeys(3)
	if keys := len(mr.Keys()); keys != 3 {
		t.Fatalf("%d Redis keys, want 3", keys)
	}
	if err := n.Close(ctx(t)); err != nil {
		t.Fatal(err)
	}

	n2 := open(t, cfg, st, nil)
	waitReady(t, n2)
	again, err := n2.Search(ctx(t), "rc", aggSearch(t, raw), api.ReadOptions{WaitForSeq: res.Seq})
	if err != nil {
		t.Fatal(err)
	}
	if again.Total != 41 || !reflect.DeepEqual(again.Aggs, after.Aggs) {
		t.Fatalf("after a restart: total %d, aggregations equal %v", again.Total, reflect.DeepEqual(again.Aggs, after.Aggs))
	}
	if s := n2.RequestCacheStats(); s.StoreHits != 2 {
		t.Fatalf("restarted node stats %v: want both shards read from Redis", s)
	}
}
