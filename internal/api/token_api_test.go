package api_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
)

// TestWaitForWaitsOnlyOnTheTokensShards: a read with wait_for waits for each shard the
// token names to have its seq searchable, and for no other: a token of a write to one
// shard is answered while another shard's copy stands still, and a token naming that
// shard times out (504). Merged tokens wait for both.
func TestWaitForWaitsOnlyOnTheTokensShards(t *testing.T) {
	e := newEnv(t, envOpts{fakeTailers: true, cfg: func(c *config.Config) { c.RequestTimeout = 2 * time.Second }})
	e.createIndex("tk", `{"settings": {"shards": 2}}`)
	on := idsOnShards(2)
	tok0 := tokenOf(t, e.must(http.StatusOK, "PUT", "/indexes/tk/docs/"+on[0], `{"n": 0}`))
	e.tailer("tk", 1).Pause()
	tok1 := tokenOf(t, e.must(http.StatusOK, "PUT", "/indexes/tk/docs/"+on[1], `{"n": 1}`))
	if r := e.must(http.StatusOK, "POST", "/indexes/tk/_count?wait_for="+tok0, `{}`); r["count"] != 1.0 {
		t.Fatalf("a count waiting for shard 0's write = %v, want 1 (shard 1 stands still)", r)
	}
	e.problem(e.do("POST", "/indexes/tk/_count?wait_for="+tok1, `{}`), http.StatusGatewayTimeout, "timeout")
	tl := e.tailer("tk", 1)
	tl.Resume()
	for deadline := time.Now().Add(30 * time.Second); tl.Shard().RefreshedSeq() < tokenSeq(t, tok1, 1); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("shard 1 never caught up after Resume (at %d)", tl.Shard().RefreshedSeq())
		}
	}
	if r := e.must(http.StatusOK, "POST", "/indexes/tk/_count?wait_for="+mergeTokens(t, tok0, tok1), `{}`); r["count"] != 2.0 {
		t.Fatalf("a count waiting for both writes = %v", r)
	}
	if got := e.must(http.StatusOK, "GET", "/indexes/tk/docs/"+on[1]+"?wait_for="+tok1, ""); got["id"] != on[1] {
		t.Fatalf("GET waiting for its shard's write = %v", got)
	}
}

// TestWaitForRefusals: a malformed token, a token past its shard's newest committed
// seq, one of another incarnation of the index, and wait_for_seq (removed in 2.0) are
// each a 400 that says which parameter.
func TestWaitForRefusals(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("rf", `{"settings": {"shards": 2}}`)
	tok := tokenOf(t, e.must(http.StatusOK, "PUT", "/indexes/rf/docs/a", `{"n": 1}`))
	for name, path := range map[string]string{
		"malformed":   "/indexes/rf/_search?wait_for=not-a-token",
		"truncated":   "/indexes/rf/_search?wait_for=" + tok[:len(tok)-3],
		"past":        "/indexes/rf/_search?wait_for=" + tokenPast(t, tok, 1000),
		"seq removed": "/indexes/rf/_search?wait_for_seq=1",
	} {
		p := e.problem(e.do("POST", path, `{}`), http.StatusBadRequest, "invalid_request")
		loc := "params.wait_for"
		if name == "seq removed" {
			loc = "params.wait_for_seq"
		}
		if !hasLoc(p, loc) {
			t.Errorf("%s: %v, want a problem at %s", name, p, loc)
		}
	}
	e.must(http.StatusOK, "DELETE", "/indexes/rf", "")
	e.createIndex("rf", `{"settings": {"shards": 2}}`)
	p := e.problem(e.do("POST", "/indexes/rf/_search?wait_for="+tok, `{}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "params.wait_for") {
		t.Errorf("a token of the dropped index: %v", p)
	}
}

// TestTokenNamesTheShardsWritten: a bulk's token names each shard it committed to, at the
// newest seq it took there, and no other shard.
func TestTokenNamesTheShardsWritten(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("tn", `{"settings": {"shards": 4}}`)
	on := idsOnShards(4)
	r := e.must(http.StatusOK, "POST", "/indexes/tn/_bulk", ndjson(
		fmt.Sprintf(`{"upsert": {"id": %q}}`, on[1]), `{"n": 1}`,
		fmt.Sprintf(`{"upsert": {"id": %q}}`, on[3]), `{"n": 3}`,
		fmt.Sprintf(`{"upsert": {"id": "%s-b"}}`, on[3]), `{"n": 4}`,
	))
	tok := tokenOf(t, r)
	want := map[int]int64{}
	for _, it := range bulkItems(t, r) {
		id := it["id"].(string) //nolint:forcetypeassert,errcheck // the shape
		s := node.ShardFor(id, 4)
		want[s] = max(want[s], int64(it["seq"].(float64))) //nolint:forcetypeassert,errcheck // the shape
	}
	for s := range 4 {
		if got := tokenSeq(t, tok, s); got != want[s] {
			t.Errorf("token %s names shard %d at %d, want %d", tok, s, got, want[s])
		}
	}
}

// idsOnShards returns a document id on each of n shards, by the node's routing.
func idsOnShards(n int) []string {
	out := make([]string, n)
	found := 0
	for i := 0; found < n; i++ {
		id := fmt.Sprint("d", i)
		if s := node.ShardFor(id, n); out[s] == "" {
			out[s] = id
			found++
		}
	}
	return out
}
