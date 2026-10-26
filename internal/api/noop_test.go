package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// bulkItems reads a _bulk response's items.
func bulkItems(t testing.TB, m map[string]any) []map[string]any {
	t.Helper()
	raw, ok := m["items"].([]any)
	if !ok {
		t.Fatalf("no items in %v", m)
	}
	out := make([]map[string]any, len(raw))
	for i, it := range raw {
		out[i], _ = it.(map[string]any)
	}
	return out
}

// An upsert of the stored document's bytes is a noop at the stored seq: no new seq, the
// changelog's head stays, and a read at the response's seq sees the document. A body
// that differs by a byte writes, as does every upsert with detect_noop=false, a
// conditional one, and one whose id another op of the bulk names.
func TestUnchangedUpsertIsANoop(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("n", `{"mapping": {"fields": {"brand": "keyword"}}, "settings": {"shards": 2}}`)
	first := seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/n/docs/a", `{"brand": "x"}`))

	r := e.must(http.StatusOK, "PUT", "/indexes/n/docs/a", `{"brand": "x"}`)
	if r["result"] != "noop" || r["seq"] != float64(first) {
		t.Fatalf("an unchanged PUT = %v, want a noop at seq %d", r, first)
	}
	r = e.must(http.StatusOK, "POST", "/indexes/n/_bulk", ndjson(`{"upsert": {"id": "a"}}`, `{"brand": "x"}`))
	if it := bulkItems(t, r)[0]; it["result"] != "noop" || it["seq"] != float64(first) || r["seq"] != float64(first) || it["status"] != 200.0 {
		t.Fatalf("an unchanged bulk upsert = %v", r)
	}
	if c := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/n/_count?wait_for_seq=%d", first), `{}`); c["count"] != 1.0 {
		t.Fatalf("count at the noop's seq = %v", c)
	}

	r = e.must(http.StatusOK, "PUT", "/indexes/n/docs/a", `{"brand":  "x"}`)
	second := seqOf(t, r)
	if r["result"] != nil || second <= first {
		t.Fatalf("a changed PUT = %v", r)
	}
	if r = e.must(http.StatusOK, "PUT", "/indexes/n/docs/a?detect_noop=false", `{"brand":  "x"}`); r["result"] != nil || seqOf(t, r) <= second {
		t.Fatalf("detect_noop=false = %v", r)
	}
	if r = e.must(http.StatusOK, "PUT", "/indexes/n/docs/a?version=2", `{"brand":  "x"}`); r["result"] != nil {
		t.Fatalf("a versioned unchanged PUT = %v, want a write", r)
	}
	r = e.must(http.StatusOK, "POST", "/indexes/n/_bulk", ndjson(
		`{"upsert": {"id": "a"}}`, `{"brand":  "x"}`,
		`{"upsert": {"id": "a"}}`, `{"brand":  "x"}`))
	for _, it := range bulkItems(t, r) {
		if it["result"] != nil {
			t.Fatalf("an id twice in a bulk = %v, want writes", r)
		}
	}
	e.problem(e.do("PUT", "/indexes/n/docs/a?detect_noop=maybe", `{}`), 400, "invalid_request")
}

// With percolate=true an unchanged upsert reports its document's matches (and their
// details) as a write of the same body does, against the saved queries as they are
// now: a saved query added after the document was stored matches it too.
func TestUnchangedUpsertPercolates(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("np", `{"mapping": {"fields": {"brand": "keyword", "_v": {"type": "nested", "fields": {"size": "keyword"}}}}}`)
	e.must(http.StatusOK, "PUT", "/indexes/np/queries/acme", `{"query": {"field": "brand", "op": "eq", "value": "acme"}}`)
	doc := `{"brand": "acme", "_v": [{"size": "s"}, {"size": "l"}]}`
	path := "/indexes/np/_bulk?percolate=true&percolate_matches=true&percolate_inner_hits=true"
	r := e.must(http.StatusOK, "POST", path, ndjson(`{"upsert": {"id": "d"}}`, doc))
	written := bulkItems(t, r)[0]
	large := seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/np/queries/large", `{"query": {"nested": {"path": "_v", "query": {"field": "_v.size", "op": "eq", "value": "l"}, "inner_hits": {}}}}`))
	r = e.must(http.StatusOK, "POST", path+"&refresh=wait_for", ndjson(`{"upsert": {"id": "d"}}`, doc))
	it := bulkItems(t, r)[0]
	if it["result"] != "noop" || it["seq"] != written["seq"] || r["percolated"] != true {
		t.Fatalf("an unchanged percolated upsert = %v", r)
	}
	if ps, _ := r["percolated_seq"].(float64); int64(ps) < large || r["seq"] != written["seq"] {
		t.Fatalf("percolated_seq %v, seq %v: want percolated_seq at least the saved query's %d, seq the noop's", r["percolated_seq"], r["seq"], large)
	}
	if got, _ := json.Marshal(it["queries"]); string(got) != `["acme","large"]` {
		t.Fatalf("queries = %s, want acme and large", got)
	}
	ms, _ := it["matches"].([]any)
	if len(ms) != 2 {
		t.Fatalf("matches = %v", it["matches"])
	}
	second, _ := ms[1].(map[string]any)
	if hits, _ := json.Marshal(second["inner_hits"]); !strings.Contains(string(hits), `"offsets":[1]`) {
		t.Fatalf("inner hits = %s, want offset 1", hits)
	}
}

// The property: for random bulks of upserts (from a few bodies), deletes and versioned
// upserts over a few ids, each item is a noop exactly when the model says it is (an
// unconditional upsert of an id named once in its bulk, whose stored body is the same
// bytes), the response's seq covers every item, and the stored documents are the
// model's after every bulk, as if every upsert had written.
func TestUnchangedNoopProperty(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("np", `{"mapping": {"fields": {"k": "keyword", "n": "number"}}, "settings": {"shards": 3}}`)
	r := rand.New(rand.NewPCG(7, 11))
	bodies := []string{`{"k": "a"}`, `{"k": "b"}`, `{"k":"a"}`, `{"n": 1, "k": "a"}`, `{"k": "a", "n": 1}`}
	ids := []string{"x", "y", "z", "w"}
	stored := map[string]string{}
	versions := map[string]int64{}
	var version int64
	rounds := testtier.Pick(40, 200)
	for round := range rounds {
		var lines []string
		type op struct {
			id, body  string
			del       bool
			versioned bool
		}
		var ops []op
		named := map[string]int{}
		for range 1 + r.IntN(6) {
			o := op{id: ids[r.IntN(len(ids))]}
			switch r.IntN(10) {
			case 0:
				o.del = true
			case 1:
				o.versioned = true
				o.body = bodies[r.IntN(len(bodies))]
			default:
				o.body = bodies[r.IntN(len(bodies))]
			}
			ops = append(ops, o)
			named[o.id]++
		}
		want := make([]bool, len(ops))
		for i, o := range ops {
			cur, exists := stored[o.id]
			switch {
			case o.del:
				lines = append(lines, fmt.Sprintf(`{"delete": {"id": %q}}`, o.id))
				delete(stored, o.id)
			case o.versioned:
				version++
				lines = append(lines, fmt.Sprintf(`{"upsert": {"id": %q, "version": %d}}`, o.id, version), o.body)
				stored[o.id] = o.body
				versions[o.id] = version
			default:
				lines = append(lines, fmt.Sprintf(`{"upsert": {"id": %q}}`, o.id), o.body)
				want[i] = named[o.id] == 1 && exists && cur == o.body
				stored[o.id] = o.body
			}
		}
		resp := e.do("POST", "/indexes/np/_bulk", ndjson(lines...))
		if resp.status != http.StatusOK {
			t.Fatalf("round %d: %d %s", round, resp.status, resp.body)
		}
		m := resp.json(t)
		top, _ := m["seq"].(float64)
		for i, it := range bulkItems(t, m) {
			if it["status"] != 200.0 && !(ops[i].del && it["status"] == 404.0) {
				t.Fatalf("round %d item %d: %v", round, i, it)
			}
			noop := it["result"] == "noop"
			if noop != want[i] {
				t.Fatalf("round %d item %d (%+v): noop %v, want %v: %s", round, i, ops[i], noop, want[i], resp.body)
			}
			if seq, _ := it["seq"].(float64); seq > top {
				t.Fatalf("round %d: item seq %v above the response's %v", round, seq, top)
			}
		}
		for _, id := range ids {
			got := e.do("GET", "/indexes/np/docs/"+id, "")
			body, ok := stored[id]
			if !ok {
				if got.status != http.StatusNotFound {
					t.Fatalf("round %d: %s is %d %s, want absent", round, id, got.status, got.body)
				}
				continue
			}
			var d struct {
				Body json.RawMessage `json:"body"`
			}
			var compact bytes.Buffer
			_ = json.Compact(&compact, []byte(body))
			if err := json.Unmarshal(got.body, &d); err != nil || string(d.Body) != compact.String() {
				t.Fatalf("round %d: %s is %s, want %s", round, id, got.body, body)
			}
		}
	}
}
