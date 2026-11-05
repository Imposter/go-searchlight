package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
)

// TestItemPositions: every bulk item names the shard its id routes to (ShardFor) and
// its seq there: the one it committed at, or for a noop (an unchanged upsert, a
// versioned write not above the stored one) the stored target's. A client forms a
// token of one item's position (the response token's uid, its shard and seq) and
// reads that item back with it alone.
func TestItemPositions(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("ip", `{"settings": {"shards": 4}}`)
	ids := idsOnShards(4)
	var body strings.Builder
	for i, id := range ids {
		fmt.Fprintf(&body, "{\"upsert\": {\"id\": %q, \"version\": 2}}\n{\"n\": %d}\n", id, i)
	}
	r := e.must(http.StatusOK, "POST", "/indexes/ip/_bulk", body.String())
	tok, err := api.ParseToken(tokenOf(t, r))
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range bulkItems(t, r) {
		id, _ := it["id"].(string)
		if it["shard"] != float64(node.ShardFor(id, 4)) || it["seq"] == nil {
			t.Fatalf("item %v: want shard %d and a seq", it, node.ShardFor(id, 4))
		}
	}
	first := bulkItems(t, r)[2]
	own := &api.Token{UID: tok.UID}
	own.Note(int(first["shard"].(float64)), int64(first["seq"].(float64))) //nolint:forcetypeassert,errcheck // checked above
	if d := e.must(http.StatusOK, "GET", "/indexes/ip/docs/"+ids[2]+"?wait_for="+own.String(), ""); d["id"] != ids[2] {
		t.Fatalf("a read waiting for one item's own token = %v", d)
	}
	if c := e.must(http.StatusOK, "POST", "/indexes/ip/_count?wait_for="+own.String(), `{"query": {"field": "n", "op": "eq", "value": 2}}`); c["count"] != 1.0 {
		t.Fatalf("a count waiting for one item's own token = %v, want its document", c)
	}

	body.Reset()
	fmt.Fprintf(&body, "{\"upsert\": {\"id\": %q, \"version\": 1}}\n{\"n\": 9}\n", ids[0])
	fmt.Fprintf(&body, "{\"upsert\": {\"id\": %q}}\n{\"n\": 1}\n", ids[1])
	again := bulkItems(t, e.must(http.StatusOK, "POST", "/indexes/ip/_bulk", body.String()))
	before := bulkItems(t, r)
	for i, it := range again {
		if it["result"] != "noop" || it["shard"] != before[i]["shard"] || it["seq"] != before[i]["seq"] {
			t.Errorf("noop item %d = %v, want the stored target's shard and seq %v/%v", i, it, before[i]["shard"], before[i]["seq"])
		}
	}
}

// TestCommittedToken: a bulk's committed token names only what it committed, while its
// token also names where its noops found their targets; a bulk that commits nothing has
// no committed token. A later commit to a shard is above every seq an earlier one took
// there.
func TestCommittedToken(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("ct", `{"settings": {"shards": 2}}`)
	ids := idsOnShards(2)
	first := e.must(http.StatusOK, "POST", "/indexes/ct/_bulk", fmt.Sprintf("{\"upsert\": {\"id\": %q}}\n{\"n\": 0}\n", ids[0]))
	committed0 := first["committed"].(string) //nolint:forcetypeassert,errcheck // checked below
	if committed0 != first["token"] {
		t.Fatalf("a bulk that committed everything: token %v, committed %v", first["token"], first["committed"])
	}
	r := e.must(http.StatusOK, "POST", "/indexes/ct/_bulk", fmt.Sprintf("{\"upsert\": {\"id\": %q}}\n{\"n\": 0}\n{\"upsert\": {\"id\": %q}}\n{\"n\": 1}\n", ids[0], ids[1]))
	tok, err := api.ParseToken(tokenOf(t, r))
	if err != nil {
		t.Fatal(err)
	}
	com, err := api.ParseToken(r["committed"].(string)) //nolint:forcetypeassert,errcheck // ParseToken refuses ""
	if err != nil {
		t.Fatal(err)
	}
	if com.Seq(0) != 0 || com.Seq(1) == 0 || tok.Seq(0) != tokenSeq(t, committed0, 0) || tok.Seq(1) != com.Seq(1) {
		t.Fatalf("token %v, committed %v: want the unchanged upsert's stored seq in token alone", tok.Seqs, com.Seqs)
	}
	noop := e.must(http.StatusOK, "POST", "/indexes/ct/_bulk", fmt.Sprintf("{\"upsert\": {\"id\": %q}}\n{\"n\": 0}\n", ids[0]))
	if noop["committed"] != nil || noop["token"] == nil {
		t.Fatalf("a bulk of an unchanged upsert: token %v, committed %v", noop["token"], noop["committed"])
	}
	later := e.must(http.StatusOK, "PUT", "/indexes/ct/docs/"+ids[0], `{"n": 5}`)
	if got := tokenSeq(t, later["committed"].(string), 0); got <= tokenSeq(t, committed0, 0) { //nolint:forcetypeassert,errcheck // the shape
		t.Fatalf("a later commit to shard 0 took seq %d, not above the earlier %d", got, tokenSeq(t, committed0, 0))
	}
}

// TestGetReturnsItsPosition: a GET of a document or a saved query names its shard and a
// token of it at its seq, which reads it back.
func TestGetReturnsItsPosition(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("gp", `{"settings": {"shards": 3}}`)
	w := e.must(http.StatusOK, "PUT", "/indexes/gp/docs/x", `{"n": 1}`)
	d := e.must(http.StatusOK, "GET", "/indexes/gp/docs/x", "")
	if d["shard"] != float64(node.ShardFor("x", 3)) || d["token"] != w["token"] || d["seq"] != w["seq"] {
		t.Fatalf("GET = %v after the write %v: want its shard, seq and token", d, w)
	}
	q := e.must(http.StatusOK, "PUT", "/indexes/gp/queries/q1", `{"query": {"all": []}}`)
	g := e.must(http.StatusOK, "GET", "/indexes/gp/queries/q1", "")
	if g["shard"] != float64(node.ShardFor("q1", 3)) || g["token"] != q["token"] {
		t.Fatalf("GET of a saved query = %v after the write %v", g, q)
	}
	if c := e.must(http.StatusOK, "POST", "/indexes/gp/_count?wait_for="+d["token"].(string), `{}`); c["count"] != 1.0 { //nolint:forcetypeassert,errcheck // checked above
		t.Fatalf("a count waiting for the GET's token = %v", c)
	}
}
