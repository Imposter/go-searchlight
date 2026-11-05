package cluster

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestNoRequestCacheOnTheWire: request_cache: false reaches a peer, and needs no wire
// level of its own.
func TestNoRequestCacheOnTheWire(t *testing.T) {
	for _, off := range []bool{false, true} {
		wr, err := encodeRequest(&search.Request{Query: &query.All{}, NoRequestCache: off})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(wr)
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Contains(string(raw), "no_request_cache"); got != off {
			t.Fatalf("NoRequestCache %v: wire form %s", off, raw)
		}
		var back wireRequest
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		r, err := back.decode()
		if err != nil {
			t.Fatal(err)
		}
		if r.NoRequestCache != off || back.Needs != 0 {
			t.Fatalf("NoRequestCache %v: decoded %v, needs level %d", off, r.NoRequestCache, back.Needs)
		}
	}
	// A search of the newest wire level carries it too.
	wr, err := encodeRequest(&search.Request{
		Query: &query.All{}, NoRequestCache: true,
		Aggs: map[string]search.Agg{"lo": {Type: search.AggMin, Field: "price"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(wr)
	if err != nil {
		t.Fatal(err)
	}
	var back wireRequest
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	r, err := back.decode()
	if err != nil {
		t.Fatal(err)
	}
	if !r.NoRequestCache || back.Needs != levelMetrics {
		t.Fatalf("a min search: NoRequestCache %v, needs level %d (want %d)", r.NoRequestCache, back.Needs, levelMetrics)
	}
}
