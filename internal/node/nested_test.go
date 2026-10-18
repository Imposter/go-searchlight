package node_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// On an index with dynamic false, a sub-field its documents' objects hold unmapped is
// indexed for presence alone; mapping it rebuilds the copy, after which nested
// queries on it find those objects, each correlated with its own other sub-fields.
func TestNestedSubFieldMappedLater(t *testing.T) {
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), nil)
	info, err := n.CreateIndex(ctx(t), "v", api.IndexSpec{
		Mapping: &schema.Mapping{Dynamic: schema.DynamicFalse, Fields: map[string]schema.FieldType{
			"_v": schema.Nested, "_v.size": schema.Keyword,
		}},
		Settings: api.IndexSettings{Shards: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	seq := bulkLoad(t, n, "v", 200, func(i int) string {
		return fmt.Sprintf(`{"_v": [{"size": "L", "colour": %q}, {"size": "M", "colour": "red"}]}`, []string{"red", "blue"}[i%2])
	})
	present := mustParse(t, `{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "eq", "value": "L"}}}`)
	if got := count(t, n, "v", present, seq); got != 200 {
		t.Fatalf("size L: %d", got)
	}
	if _, err := n.PatchMapping(ctx(t), "v", map[string]schema.FieldType{"_v.colour": schema.Keyword}); err != nil {
		t.Fatal(err)
	}
	redL := mustParse(t, `{"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "L"}, {"field": "_v.colour", "op": "eq", "value": "red"}]}}}`)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		res, err := n.Search(ctx(t), "v", &search.Request{Query: redL, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			t.Fatalf("a read during the rebuild: %v", err)
		}
		if res.Total == 100 && !res.Stale {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the rebuild never finished: %d red L, stale %v", res.Total, res.Stale)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rebuiltAside(t, cfg.DataDir, info.UID) {
		t.Error("the copy was not rebuilt for mapping a sub-field its objects hold")
	}
}

// A field documents hold as one object, which dynamic typing does not type, is
// indexed for presence alone until it is mapped nested: mapping it rebuilds the copy,
// whose nested queries then find each object.
func TestNestedFieldMappedLater(t *testing.T) {
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), nil)
	info, err := n.CreateIndex(ctx(t), "o", api.IndexSpec{
		Mapping:  &schema.Mapping{Dynamic: schema.DynamicFalse, Fields: map[string]schema.FieldType{"title": schema.Text}},
		Settings: api.IndexSettings{Shards: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	seq := bulkLoad(t, n, "o", 100, func(i int) string {
		return fmt.Sprintf(`{"title": "t%d", "_v": {"size": %q}}`, i, []string{"L", "M"}[i%2])
	})
	if got := count(t, n, "o", mustParse(t, `{"field": "title", "op": "exists"}`), seq); got != 100 {
		t.Fatalf("count %d", got)
	}
	if _, err := n.PatchMapping(ctx(t), "o", map[string]schema.FieldType{"_v": schema.Nested, "_v.size": schema.Keyword}); err != nil {
		t.Fatal(err)
	}
	large := mustParse(t, `{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "eq", "value": "L"}}}`)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		res, err := n.Search(ctx(t), "o", &search.Request{Query: large, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			t.Fatalf("a read during the rebuild: %v", err)
		}
		if res.Total == 50 && !res.Stale {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the rebuild never finished: %d L, stale %v", res.Total, res.Stale)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !rebuiltAside(t, cfg.DataDir, info.UID) {
		t.Error("the copy was not rebuilt for mapping nested a field its documents hold as objects")
	}
}
