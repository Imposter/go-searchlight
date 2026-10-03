package api_test

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

func TestIndexLifecycle(t *testing.T) {
	e := newEnv(t, envOpts{})
	created := e.must(http.StatusCreated, "PUT", "/indexes/items",
		`{"mapping": {"dynamic": "strict", "fields": {"title": "text", "price": "number"}}, "settings": {"shards": 2, "refresh_interval": "50ms"}}`)
	if created["name"] != "items" || created["uid"] == "" {
		t.Errorf("created = %v", created)
	}
	settings := created["settings"].(map[string]any) //nolint:forcetypeassert,errcheck // the test checks the shape
	if settings["shards"] != 2.0 || settings["refresh_interval"] != "50ms" {
		t.Errorf("settings = %v", settings)
	}
	e.problem(e.do("PUT", "/indexes/items", `{}`), http.StatusConflict, "index_exists")
	e.must(http.StatusCreated, "PUT", "/indexes/other", "")

	list := e.must(http.StatusOK, "GET", "/indexes", "")
	var names []string
	for _, ix := range list["indexes"].([]any) { //nolint:forcetypeassert,errcheck // the test checks the shape
		names = append(names, ix.(map[string]any)["name"].(string)) //nolint:forcetypeassert,errcheck // the shape
	}
	if fmt.Sprint(names) != "[items other]" {
		t.Errorf("indexes = %v", names)
	}

	got := e.must(http.StatusOK, "GET", "/indexes/items", "")
	if got["version"] != 1.0 {
		t.Errorf("version = %v", got["version"])
	}
	patched := e.must(http.StatusOK, "PATCH", "/indexes/items/mapping", `{"fields": {"tags": "keyword_list"}}`)
	fields := patched["mapping"].(map[string]any)["fields"].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	if fields["tags"] != "keyword_list" || fields["title"] != "text" {
		t.Errorf("patched mapping = %v", fields)
	}
	p := e.problem(e.do("PATCH", "/indexes/items/mapping", `{"fields": {"price": "keyword"}}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "fields.price") {
		t.Errorf("a type change: %v", p)
	}
	e.problem(e.do("PATCH", "/indexes/items/mapping", `{"fields": {"x": "blob"}}`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PATCH", "/indexes/items/mapping", `{"fieldz": {}}`), http.StatusBadRequest, "invalid_request")

	s := e.must(http.StatusOK, "PATCH", "/indexes/items/settings", `{"refresh_interval": "1s", "replicas_per_shard": 2}`)
	settings = s["settings"].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	if settings["refresh_interval"] != "1s" || settings["replicas_per_shard"] != 2.0 {
		t.Errorf("patched settings = %v", settings)
	}
	e.problem(e.do("PATCH", "/indexes/items/settings", `{"shards": 3}`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PATCH", "/indexes/items/settings", `{"refresh_interval": "soon"}`), http.StatusBadRequest, "invalid_request")

	for _, bad := range []string{"Items", "_x", "a b", strings.Repeat("a", 256)} {
		e.problem(e.do("PUT", "/indexes/"+url.PathEscape(bad), ""), http.StatusBadRequest, "invalid_request")
	}
	p = e.problem(e.do("PUT", "/indexes/x", `{"settings": {"shards": 0}}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "settings.shards") {
		t.Errorf("shards 0: %v", p)
	}
	p = e.problem(e.do("PUT", "/indexes/x", `{"mapping": {"fields": {"_id": "keyword"}}}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "mapping.fields._id") {
		t.Errorf("_id field: %v", p)
	}
	e.problem(e.do("PUT", "/indexes/x", `{"mapping": {}, "extra": 1}`), http.StatusBadRequest, "invalid_request")

	e.must(http.StatusOK, "DELETE", "/indexes/items", "")
	e.problem(e.do("GET", "/indexes/items", ""), http.StatusNotFound, "index_not_found")
	e.problem(e.do("DELETE", "/indexes/items", ""), http.StatusNotFound, "index_not_found")
	e.problem(e.do("POST", "/indexes/items/_search", `{}`), http.StatusNotFound, "index_not_found")
	// A recreated index starts empty.
	e.must(http.StatusCreated, "PUT", "/indexes/items", "")
	r := e.must(http.StatusOK, "POST", "/indexes/items/_count", "")
	if r["count"] != 0.0 {
		t.Errorf("recreated index count = %v", r)
	}
}

func TestDocuments(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.must(http.StatusCreated, "PUT", "/indexes/shop", `{"mapping": {"dynamic": "strict", "fields": {"title": "text", "price": "number"}}}`)

	w := e.must(http.StatusOK, "PUT", "/indexes/shop/docs/a%2Fb", `{"title": "Red Chair", "price": 10}`)
	seq := seqOf(t, w)
	if w["id"] != "a/b" {
		t.Errorf("an encoded id: %v", w)
	}
	got := e.must(http.StatusOK, "GET", "/indexes/shop/docs/a%2Fb", "")
	if int64(got["seq"].(float64)) != seq || got["body"].(map[string]any)["title"] != "Red Chair" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("GET = %v", got)
	}

	p := e.problem(e.do("PUT", fmt.Sprintf("/indexes/shop/docs/a%%2Fb?if_seq=%d", seq+100), `{"title": "x"}`), http.StatusConflict, "conflict")
	if int64(p["current_seq"].(float64)) != seq { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("conflict = %v, want current_seq %d", p, seq)
	}
	w = e.must(http.StatusOK, "PUT", fmt.Sprintf("/indexes/shop/docs/a%%2Fb?if_seq=%d", seq), `{"title": "Blue Chair", "price": 12}`)
	seq2 := seqOf(t, w)
	e.problem(e.do("PUT", "/indexes/shop/docs/a%2Fb?op_type=create", `{"title": "x"}`), http.StatusConflict, "conflict")
	e.must(http.StatusOK, "PUT", "/indexes/shop/docs/new?op_type=create", `{"title": "New"}`)
	e.problem(e.do("PUT", "/indexes/shop/docs/new?op_type=create&if_seq=3", `{}`), http.StatusBadRequest, "invalid_request")

	p = e.problem(e.do("PUT", "/indexes/shop/docs/s", `{"title": "x", "colour": "red"}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "body.colour") {
		t.Errorf("strict mapping: %v", p)
	}
	e.problem(e.do("PUT", "/indexes/shop/docs/s", `[1, 2]`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PUT", "/indexes/shop/docs/s", `{"title": `), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PUT", "/indexes/shop/docs/"+strings.Repeat("x", 513), `{}`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PUT", "/indexes/nope/docs/a", `{}`), http.StatusNotFound, "index_not_found")
	e.problem(e.do("GET", "/indexes/nope/docs/a", ""), http.StatusNotFound, "index_not_found")
	e.problem(e.do("PUT", "/indexes/shop/docs/a?refresh=sometimes", `{}`), http.StatusBadRequest, "invalid_request")

	d := e.must(http.StatusOK, "DELETE", fmt.Sprintf("/indexes/shop/docs/a%%2Fb?if_seq=%d", seq2), "")
	if seqOf(t, d) <= seq2 {
		t.Errorf("delete seq %v", d)
	}
	e.problem(e.do("GET", "/indexes/shop/docs/a%2Fb", ""), http.StatusNotFound, "document_not_found")
	// Deleting it again finds nothing: a 404, and no change is written.
	p = e.problem(e.do("DELETE", "/indexes/shop/docs/a%2Fb", ""), http.StatusNotFound, "document_not_found")
	if p["result"] != "not_found" {
		t.Errorf("a delete of nothing = %v", p)
	}
	e.problem(e.do("DELETE", "/indexes/shop/queries/none", ""), http.StatusNotFound, "query_not_found")
	e.problem(e.do("DELETE", "/indexes/shop/docs/a%2Fb?op_type=create", ""), http.StatusBadRequest, "invalid_request")

	// Dynamic mapping types new fields from their first value, before the write
	// commits, so the field is searchable at once.
	e.must(http.StatusCreated, "PUT", "/indexes/dyn", "")
	w = e.must(http.StatusOK, "PUT", "/indexes/dyn/docs/1?refresh=wait_for", `{"brand": "Acme", "price": 3, "tags": ["a", "b"], "ok": true}`)
	if w["timed_out"] != nil {
		t.Errorf("refresh=wait_for timed out: %v", w)
	}
	info := e.must(http.StatusOK, "GET", "/indexes/dyn", "")
	fields := info["mapping"].(map[string]any)["fields"].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	want := map[string]any{"brand": "text", "price": "number", "tags": "keyword_list", "ok": "bool"}
	if fmt.Sprint(fields) != fmt.Sprint(want) {
		t.Errorf("dynamic mapping = %v, want %v", fields, want)
	}
	r := e.must(http.StatusOK, "POST", "/indexes/dyn/_search", `{"query": {"field": "tags", "op": "has", "value": "b"}}`)
	if fmt.Sprint(ids(r)) != "[1]" {
		t.Errorf("search on a dynamic field = %v", r)
	}
	if info["docs"] != 1.0 {
		t.Errorf("index docs = %v", info["docs"])
	}
}

func TestReadYourWrites(t *testing.T) {
	// The background refresh is off: only wait_for_seq and refresh make writes
	// searchable, so the test sees each mechanism alone.
	e := newEnv(t, envOpts{})
	e.must(http.StatusCreated, "PUT", "/indexes/ryw", `{"settings": {"shards": 3, "refresh_interval": -1}}`)
	search := func(q string) map[string]any {
		return e.must(http.StatusOK, "POST", "/indexes/ryw/_search"+q, `{"query": {"all": []}, "size": 100}`)
	}
	e.must(http.StatusOK, "PUT", "/indexes/ryw/docs/a?refresh=true", `{"n": 1}`)
	if r := search(""); totalOf(r) != 1 {
		t.Fatalf("refresh=true: search sees %v", r)
	}

	// Without a refresh the write is committed but not yet searchable ...
	w := e.must(http.StatusOK, "PUT", "/indexes/ryw/docs/b", `{"n": 2}`)
	seq := seqOf(t, w)
	if r := search(""); totalOf(r) != 1 {
		t.Fatalf("no refresh, refresh disabled: search sees %v", r)
	}
	// ... and GET is realtime.
	e.must(http.StatusOK, "GET", "/indexes/ryw/docs/b", "")

	// Turn the refresh on: wait_for_seq waits for the write, on every shard.
	e.must(http.StatusOK, "PATCH", "/indexes/ryw/settings", `{"refresh_interval": "10ms"}`)
	for i := range 5 {
		w = e.must(http.StatusOK, "PUT", fmt.Sprintf("/indexes/ryw/docs/c%d", i), `{"n": 3}`)
		seq = seqOf(t, w)
		r := search(fmt.Sprintf("?wait_for_seq=%d", seq))
		if totalOf(r) != 3+i || !slices.Contains(ids(r), fmt.Sprintf("c%d", i)) {
			t.Fatalf("wait_for_seq=%d: search sees %v", seq, ids(r))
		}
	}
	c := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/ryw/_count?wait_for_seq=%d", seq), `{"query": {"field": "n", "op": "eq", "value": 3}}`)
	if c["count"] != 5.0 {
		t.Errorf("count = %v", c)
	}
	e.must(http.StatusOK, "DELETE", "/indexes/ryw/docs/a?refresh=wait_for", "")
	if r := search(""); slices.Contains(ids(r), "a") {
		t.Errorf("refresh=wait_for on a delete: search still sees a")
	}
}
