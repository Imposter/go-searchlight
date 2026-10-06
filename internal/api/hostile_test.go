package api_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// TestBodyLimitBelowSegmentLimit pins the binding decision that no accepted body can
// carry a document a segment would refuse.
func TestBodyLimitBelowSegmentLimit(t *testing.T) {
	if config.MaxBodyLimit+schema.MaxIDBytes > segment.MaxStoredBytes {
		t.Fatalf("max_body_bytes may reach %d bytes, and with an id more than a segment stores (%d)", config.MaxBodyLimit, segment.MaxStoredBytes)
	}
}

// TestHostileRequests is Review Focus 5: oversized bodies, too many bulk operations,
// deep and huge queries, malformed JSON and slow clients get a 4xx, never a crash.
func TestHostileRequests(t *testing.T) {
	e := newEnv(t, envOpts{cfg: func(c *config.Config) {
		c.MaxBodyBytes = 256 << 10
		c.MaxDocBytes = 8 << 10
	}})
	e.must(http.StatusCreated, "PUT", "/indexes/h", `{"mapping": {"fields": {"title": "text", "price": "number"}}}`)

	t.Run("oversized body with a length", func(t *testing.T) {
		e := e.with(t)
		got := e.do("POST", "/indexes/h/_search", `{"query": {"all": []}, "pad": "`+strings.Repeat("x", 300<<10)+`"}`)
		e.problem(got, http.StatusRequestEntityTooLarge, "too_large")
	})
	t.Run("oversized body without a length", func(t *testing.T) {
		e := e.with(t)
		// A reader with no length is sent chunked: the limit applies as it is read.
		// The server answers 413 at the limit, then drains the rest (up to 1 MiB
		// here) before it closes, so the client, still sending, reads the answer
		// rather than a reset.
		body := io.MultiReader(strings.NewReader(`{"pad": "`), io.LimitReader(neverEnding('x'), 1<<20))
		got := e.doReader("POST", "/indexes/h/_search", body)
		e.problem(got, http.StatusRequestEntityTooLarge, "too_large")
	})
	t.Run("far oversized body without a length", func(t *testing.T) {
		// Past what the server drains, it closes on the client: the client sees the
		// 413, or a reset while it is still writing. Either way the server refused.
		before := api.TooLargeAnswered(e.srv)
		body := io.MultiReader(strings.NewReader(`{"pad": "`), io.LimitReader(neverEnding('x'), 8<<20))
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, e.url+"/indexes/h/_search", body)
		if err != nil {
			t.Fatal(err)
		}
		res, err := e.client.Do(req)
		if err == nil {
			_ = res.Body.Close()
			if res.StatusCode != http.StatusRequestEntityTooLarge {
				t.Fatalf("an 8 MiB body: HTTP %d", res.StatusCode)
			}
		}
		if got := api.TooLargeAnswered(e.srv); got != before+1 {
			t.Fatalf("the server answered %d 413s for an 8 MiB body (client error: %v)", got-before, err)
		}
	})
	t.Run("oversized document", func(t *testing.T) {
		e := e.with(t)
		e.problem(e.do("PUT", "/indexes/h/docs/big", `{"title": "`+strings.Repeat("y", 9<<10)+`"}`), http.StatusRequestEntityTooLarge, "too_large")
		e.problem(e.do("POST", "/indexes/h/_bulk", ndjson(`{"upsert": {"id": "big"}}`, `{"title": "`+strings.Repeat("y", 9<<10)+`"}`)), http.StatusRequestEntityTooLarge, "too_large")
	})
	t.Run("10001 bulk operations", func(t *testing.T) {
		e := e.with(t)
		var b strings.Builder
		for i := range 10_001 {
			fmt.Fprintf(&b, "{\"delete\":{\"id\":\"%d\"}}\n", i)
		}
		e.problem(e.do("POST", "/indexes/h/_bulk", b.String()), http.StatusRequestEntityTooLarge, "too_large")
		if c := e.must(http.StatusOK, "POST", "/indexes/h/_count", ""); c["count"] != 0.0 {
			t.Errorf("a refused bulk wrote: %v", c)
		}
	})
	t.Run("deep query", func(t *testing.T) {
		e := e.with(t)
		q := `{"field": "price", "op": "gt", "value": 1}`
		for range 6 {
			q = `{"all": [` + q + `, {"field": "price", "op": "lt", "value": 9}]}`
		}
		p := e.problem(e.do("POST", "/indexes/h/_search", `{"query": `+q+`}`), http.StatusBadRequest, "invalid_request")
		if !hasLoc(p, "query") {
			t.Errorf("deep query: %v", p)
		}
		e.problem(e.do("PUT", "/indexes/h/queries/deep", `{"query": `+q+`}`), http.StatusBadRequest, "invalid_request")
	})
	t.Run("deeply nested JSON", func(t *testing.T) {
		e := e.with(t)
		nested := `{"query": ` + strings.Repeat("[", 100_000) + strings.Repeat("]", 100_000) + `}`
		e.problem(e.do("POST", "/indexes/h/_search", nested), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("POST", "/indexes/h/_percolate", `{"docs": [`+strings.Repeat("[", 50_000)+`]}`), http.StatusBadRequest, "invalid_request")
	})
	t.Run("too many conditions", func(t *testing.T) {
		e := e.with(t)
		var leaves []string
		for i := range 200 {
			leaves = append(leaves, fmt.Sprintf(`{"field": "price", "op": "eq", "value": %d}`, i))
		}
		e.problem(e.do("POST", "/indexes/h/_search", `{"query": {"any": [`+strings.Join(leaves, ",")+`]}}`), http.StatusBadRequest, "invalid_request")
	})
	t.Run("huge size and from", func(t *testing.T) {
		e := e.with(t)
		p := e.problem(e.do("POST", "/indexes/h/_search", `{"size": 1000000000}`), http.StatusBadRequest, "invalid_request")
		if !hasLoc(p, "size") {
			t.Errorf("size: %v", p)
		}
		e.problem(e.do("POST", "/indexes/h/_search", `{"size": 1e300}`), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("POST", "/indexes/h/_search", `{"from": 100000}`), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("GET", "/indexes/h/queries?size=99999999999999999999", ""), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("GET", "/indexes/h/_fields?entries=-1", ""), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("POST", "/indexes/h/_search?wait_for_seq=abc", `{}`), http.StatusBadRequest, "invalid_request")
	})
	t.Run("malformed JSON", func(t *testing.T) {
		e := e.with(t)
		for _, body := range []string{`{"query": {`, `[]`, `"x"`, `{"query": {"all": []}} trailing`, "\xff\xfe"} {
			e.problem(e.do("POST", "/indexes/h/_search", body), http.StatusBadRequest, "invalid_request")
		}
		e.problem(e.do("PUT", "/indexes/h/queries/q", `{"query": {"all": [}`), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("PATCH", "/indexes/h/mapping", `{`), http.StatusBadRequest, "invalid_request")
		e.problem(e.do("POST", "/indexes/h/_percolate", `{"docs": `), http.StatusBadRequest, "invalid_request")
	})
	t.Run("too many documents to percolate", func(t *testing.T) {
		e := e.with(t)
		e.problem(e.do("POST", "/indexes/h/_percolate", `{"docs": [`+strings.Repeat(`{},`, 10_000)+`{}]}`), http.StatusRequestEntityTooLarge, "too_large")
	})
	// The node still serves after all of that.
	e.must(http.StatusOK, "PUT", "/indexes/h/docs/ok?refresh=true", `{"title": "fine"}`)
	if c := e.must(http.StatusOK, "POST", "/indexes/h/_count", ""); c["count"] != 1.0 {
		t.Errorf("count after the hostile requests = %v", c)
	}
}

// TestSlowClients is the slowloris half of Review Focus 5, on a node whose
// read_timeout is short: a client that stalls its headers or body is cut off.
func TestSlowClients(t *testing.T) {
	e := newEnv(t, envOpts{cfg: func(c *config.Config) { c.ReadTimeout = 400 * time.Millisecond }})
	e.must(http.StatusCreated, "PUT", "/indexes/h", "")
	t.Run("a slow body", func(t *testing.T) {
		e := e.with(t)
		conn := dial(t, e.url)
		fmt.Fprintf(conn, "POST /indexes/h/_search HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{")
		start := time.Now()
		status := readStatus(t, conn, 5*time.Second)
		if time.Since(start) > 3*time.Second {
			t.Errorf("a stalled body held the connection %v", time.Since(start))
		}
		if status != http.StatusRequestTimeout {
			t.Errorf("a stalled body answered %d, want 408", status)
		}
	})
	t.Run("slow headers", func(t *testing.T) {
		e := e.with(t)
		conn := dial(t, e.url)
		fmt.Fprintf(conn, "GET /healthz HTTP/1.1\r\nHost: x\r\n")
		start := time.Now()
		status := readStatus(t, conn, 5*time.Second)
		if time.Since(start) > 3*time.Second || (status != 0 && status != http.StatusRequestTimeout) {
			t.Errorf("stalled headers: status %d after %v", status, time.Since(start))
		}
	})
	e.must(http.StatusOK, "GET", "/healthz", "")
}

// neverEnding is an endless reader of one byte.
type neverEnding byte

func (b neverEnding) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(b)
	}
	return len(p), nil
}

func dial(t *testing.T, url string) net.Conn {
	t.Helper()
	var d net.Dialer
	conn, err := d.DialContext(context.Background(), "tcp", strings.TrimPrefix(url, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readStatus reads a response's status from conn, 0 when the server closed the
// connection without one.
func readStatus(t *testing.T, conn net.Conn, within time.Duration) int {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(within))
	res, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatalf("the server kept a stalled connection open for %v", within)
		}
		return 0
	}
	_ = res.Body.Close()
	return res.StatusCode
}

func TestUnknownParametersAreRefused(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.must(http.StatusCreated, "PUT", "/indexes/u", "")
	p := e.problem(e.do("POST", "/indexes/u/_search?wait_for_sq=3", `{}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "params.wait_for_sq") {
		t.Errorf("a misspelled parameter: %v", p)
	}
	e.problem(e.do("POST", "/indexes/u/_search?wait_for_seq=1&wait_for_seq=2", `{}`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("GET", "/healthz?x=1", ""), http.StatusBadRequest, "invalid_request")
}

func writeTokens(t *testing.T, lines ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAuth(t *testing.T) {
	const rw, ro = "rw-token-0123456789abcdef", "ro-token-0123456789abcdef"
	tokens := writeTokens(t, "# api tokens", "", rw, ro+" read", "other-token-0123456789 write")
	e := newEnv(t, envOpts{cfg: func(c *config.Config) { c.TokensFile, c.InsecureNoAuth = tokens, false }})

	got := e.do("PUT", "/indexes/a", "")
	e.problem(got, http.StatusUnauthorized, "unauthorized")
	if got.header.Get("WWW-Authenticate") == "" {
		t.Error("a 401 without WWW-Authenticate")
	}
	e.problem(e.do("GET", "/indexes", "", "Authorization", "Bearer wrong-token-0123456789"), http.StatusUnauthorized, "unauthorized")
	e.problem(e.do("GET", "/indexes", "", "Authorization", "Basic "+rw), http.StatusUnauthorized, "unauthorized")
	e.problem(e.do("GET", "/indexes", "", "Authorization", "Bearer "), http.StatusUnauthorized, "unauthorized")

	e.token = ro
	e.problem(e.do("PUT", "/indexes/a", ""), http.StatusForbidden, "forbidden")
	e.token = rw
	e.must(http.StatusCreated, "PUT", "/indexes/a", "")
	e.must(http.StatusOK, "PUT", "/indexes/a/docs/1", `{"x": 1}`)
	e.token = ro
	e.must(http.StatusOK, "POST", "/indexes/a/_search", `{}`)
	e.must(http.StatusOK, "GET", "/indexes/a/docs/1", "")
	e.must(http.StatusOK, "GET", "/_cluster/health", "")
	e.problem(e.do("DELETE", "/indexes/a/docs/1", ""), http.StatusForbidden, "forbidden")
	e.problem(e.do("POST", "/indexes/a/_bulk", ndjson(`{"delete": {"id": "1"}}`)), http.StatusForbidden, "forbidden")
	e.token = "other-token-0123456789"
	e.must(http.StatusOK, "DELETE", "/indexes/a/docs/1", "")

	// Probes need no token.
	e.token = ""
	e.must(http.StatusOK, "GET", "/healthz", "")
	e.must(http.StatusOK, "GET", "/readyz", "")
	e.problem(e.do("GET", "/_cluster/health", ""), http.StatusUnauthorized, "unauthorized")
	// An unauthenticated request is refused before its body is read.
	e.problem(e.do("POST", "/indexes/a/_search", strings.Repeat("x", 64<<10)), http.StatusUnauthorized, "unauthorized")
}

func TestAuthConfiguration(t *testing.T) {
	cfg := testConfig(t)
	cfg.InsecureNoAuth = false
	if _, err := api.NewServer(nil, nil, cfg); err == nil || !strings.Contains(err.Error(), "insecure_no_auth") {
		t.Errorf("no tokens file and auth not explicitly off: err = %v", err)
	}
	for _, lines := range [][]string{{"short"}, {"# only a comment"}, {"a-long-enough-token-0123 admin"}, {"tok-0123456789abcdef read extra"}} {
		cfg.TokensFile = writeTokens(t, lines...)
		if _, err := api.NewServer(nil, nil, cfg); err == nil {
			t.Errorf("tokens file %q: want an error", lines)
		}
	}
	cfg.TokensFile = filepath.Join(t.TempDir(), "missing")
	if _, err := api.NewServer(nil, nil, cfg); err == nil {
		t.Error("a missing tokens file: want an error")
	}
	// Auth off is served without a token.
	e := newEnv(t, envOpts{})
	e.must(http.StatusCreated, "PUT", "/indexes/open", "")
}

func TestBackpressure(t *testing.T) {
	e := newEnv(t, envOpts{fakeTailers: true, node: func(o *node.Options) { o.MaxApplyLag = 2 }})
	e.must(http.StatusCreated, "PUT", "/indexes/bp", "")
	tl := e.tailer("bp", 0)
	tl.Pause()
	var got resp
	for i := range 10 {
		got = e.do("PUT", fmt.Sprintf("/indexes/bp/docs/%d", i), `{"n": 1}`)
		if got.status != http.StatusOK {
			break
		}
	}
	e.problem(got, http.StatusTooManyRequests, "too_many_requests")
	if got.header.Get("Retry-After") == "" {
		t.Error("a 429 without Retry-After")
	}
	tl.Resume()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if r := e.do("PUT", "/indexes/bp/docs/after", `{"n": 2}`); r.status == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writes stay refused after the copy caught up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// stub is a Coordinator whose methods tests override; the rest panic (a 500).
type stub struct {
	api.Coordinator
	search func(ctx context.Context) (*search.Response, error)
	write  func(ctx context.Context) (*api.WriteResult, error)
}

func (s *stub) Search(ctx context.Context, _ string, _ *search.Request, _ api.ReadOptions) (*api.SearchResult, error) {
	resp, err := s.search(ctx)
	if err != nil {
		return nil, err
	}
	return &api.SearchResult{Response: resp}, nil
}

func (s *stub) Write(ctx context.Context, _ string, _ []api.WriteOp, _ api.WriteOptions) (*api.WriteResult, error) {
	return s.write(ctx)
}

func stubServer(t *testing.T, c api.Coordinator, mod func(*config.Config)) string {
	t.Helper()
	cfg := testConfig(t)
	if mod != nil {
		mod(&cfg)
	}
	srv, err := api.NewServer(c, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return hs.URL
}

func TestShardBackpressureAndSearchQueueAre429(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	c := &stub{
		write: func(context.Context) (*api.WriteResult, error) {
			return nil, fmt.Errorf("apply: %w", shard.ErrBackpressure)
		},
		search: func(ctx context.Context) (*search.Response, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return &search.Response{TotalRelation: search.RelationEq}, nil
		},
	}
	url := stubServer(t, c, func(cfg *config.Config) { cfg.SearchQueue = 1 })
	e := &env{t: t, url: url, client: http.DefaultClient}
	got := e.do("PUT", "/indexes/x/docs/1", `{}`)
	e.problem(got, http.StatusTooManyRequests, "too_many_requests")
	if got.header.Get("Retry-After") != "1" {
		t.Errorf("Retry-After = %q", got.header.Get("Retry-After"))
	}

	done := make(chan resp, 1)
	go func() { done <- e.do("POST", "/indexes/x/_search", `{}`) }()
	<-entered
	got = e.do("POST", "/indexes/x/_count", `{}`)
	e.problem(got, http.StatusTooManyRequests, "too_many_requests")
	close(release)
	if r := <-done; r.status != http.StatusOK {
		t.Errorf("the queued search: %d %s", r.status, r.body)
	}
	// A slot is free again.
	if r := e.do("POST", "/indexes/x/_count", `{}`); r.status != http.StatusOK {
		t.Errorf("after the queue drained: %d %s", r.status, r.body)
	}
}

func TestRequestDeadline(t *testing.T) {
	c := &stub{search: func(ctx context.Context) (*search.Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	url := stubServer(t, c, func(cfg *config.Config) { cfg.RequestTimeout = 100 * time.Millisecond })
	e := &env{t: t, url: url, client: http.DefaultClient}
	start := time.Now()
	e.problem(e.do("POST", "/indexes/x/_search", `{}`), http.StatusGatewayTimeout, "timeout")
	if time.Since(start) > 5*time.Second {
		t.Errorf("the deadline took %v", time.Since(start))
	}
	// A coordinator panic is a 500, not a crash.
	e.problem(e.do("GET", "/indexes", ""), http.StatusInternalServerError, "internal")
}

func TestWaitForSeqTimesOut(t *testing.T) {
	// 8 s, not 2: under go test ./... -count=2's full parallel suite (many heavy
	// packages sharing this machine's cores), the fake tailer's catch-up after Resume
	// can be scheduled out past 2 s; a real tailer never takes this long, but the
	// deadline below still has to actually elapse once, by design.
	e := newEnv(t, envOpts{fakeTailers: true, cfg: func(c *config.Config) { c.RequestTimeout = 8 * time.Second }})
	e.must(http.StatusCreated, "PUT", "/indexes/w", "")
	// A seq past the newest committed one is refused at once, before a bulk writes.
	e.problem(e.do("POST", "/indexes/w/_search?wait_for_seq=999999", `{}`), http.StatusBadRequest, "invalid_request")
	p := e.problem(e.do("POST", "/indexes/w/_bulk?percolate=true&wait_for_seq=999999", ndjson(`{"upsert": {"id": "a"}}`, `{"x": 1}`)), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "params.wait_for_seq") {
		t.Errorf("bulk = %v", p)
	}
	e.problem(e.do("GET", "/indexes/w/docs/a", ""), http.StatusNotFound, "document_not_found")

	// A copy that does not catch up within the deadline: a read waiting for it is a
	// 504; a bulk whose percolation must wait for a saved query on it is still
	// committed, and says so.
	e.tailer("w", 0).Pause()
	q := seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/w/queries/q", `{"query": {"all": []}}`))
	e.problem(e.do("POST", fmt.Sprintf("/indexes/w/_search?wait_for_seq=%d", q), `{}`), http.StatusGatewayTimeout, "timeout")
	r := e.must(http.StatusOK, "POST", "/indexes/w/_bulk?percolate=true", ndjson(`{"upsert": {"id": "a"}}`, `{"x": 1}`))
	if r["timed_out"] != true || r["percolated"] != false || seqOf(t, r) == 0 {
		t.Errorf("bulk = %v", r)
	}
	if item := r["items"].([]any)[0].(map[string]any); item["queries"] != nil || item["status"] != 200.0 { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("item = %v", item)
	}
	e.must(http.StatusOK, "GET", "/indexes/w/docs/a", "")
	// Caught up, the same bulk sees the saved query it was waiting for.
	e.tailer("w", 0).Resume()
	r = e.must(http.StatusOK, "POST", "/indexes/w/_bulk?percolate=true", ndjson(`{"upsert": {"id": "b"}}`, `{"x": 2}`))
	if item := r["items"].([]any)[0].(map[string]any); fmt.Sprint(item["queries"]) != "[q]" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("item after catching up = %v", item)
	}
}

// TestInvalidUTF8CannotPoisonTheChangelog is C1: a body under max_doc_bytes whose
// invalid UTF-8 expands (each byte to a 3-byte U+FFFD) past what a segment stores
// must be refused before it commits, or the copy halts on it for good.
func TestInvalidUTF8CannotPoisonTheChangelog(t *testing.T) {
	e := newEnv(t, envOpts{cfg: func(c *config.Config) {
		c.MaxBodyBytes = 16 << 20
		c.MaxDocBytes = 12 << 20
	}})
	e.must(http.StatusCreated, "PUT", "/indexes/p", "")
	body := `{"t": "` + strings.Repeat("\xff", 11<<20) + `"}`
	e.problem(e.do("PUT", "/indexes/p/docs/bad", body), http.StatusRequestEntityTooLarge, "too_large")
	r := e.must(http.StatusOK, "POST", "/indexes/p/_bulk", ndjson(`{"upsert": {"id": "bad2"}}`, body[:len(body)-2]+`"}`))
	if items := r["items"].([]any); items[0].(map[string]any)["status"] != 413.0 { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("bulk item = %v", items[0])
	}
	// The copy is unharmed: it takes and serves writes.
	w := e.must(http.StatusOK, "PUT", "/indexes/p/docs/ok?refresh=wait_for", `{"t": "fine"}`)
	if w["timed_out"] != nil {
		t.Fatalf("the copy did not apply a write after the refusal: %v", w)
	}
	e.must(http.StatusOK, "GET", "/readyz", "")
	if h := e.must(http.StatusOK, "GET", "/_cluster/health", ""); h["status"] != "green" {
		t.Errorf("health = %v", h)
	}
}

// Over TLS the API speaks HTTP/2 (ServeTLS enables it). A body refused at the limit
// is answered 413 without a drain: HTTP/2 resets the stream alone, and the client
// reads the answer on a connection that survives.
func TestOversizedBodyOverHTTP2(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxBodyBytes = 64 << 10
	cfg.MaxDocBytes = 64 << 10
	srv, err := api.NewServer(&stub{}, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewUnstartedServer(srv)
	hs.EnableHTTP2 = true
	var conns atomic.Int64
	hs.Config.ConnState = func(_ net.Conn, st http.ConnState) {
		if st == http.StateNew {
			conns.Add(1)
		}
	}
	hs.StartTLS()
	t.Cleanup(hs.Close)
	for i := range 20 {
		body := io.MultiReader(strings.NewReader(`{"pad": "`), io.LimitReader(neverEnding('x'), 4<<20))
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, hs.URL+"/indexes/h/_search", body)
		if err != nil {
			t.Fatal(err)
		}
		res, err := hs.Client().Do(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		_ = res.Body.Close()
		if res.ProtoMajor != 2 || res.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("request %d: %s %d, want HTTP/2 413", i, res.Proto, res.StatusCode)
		}
	}
	// The connection survived the refusals: a request after them succeeds on it.
	res, err := hs.Client().Get(hs.URL + "/healthz")
	if err != nil || res.StatusCode != http.StatusOK {
		t.Fatalf("healthz after the refusals: %v %v", res, err)
	}
	_ = res.Body.Close()
	if n := conns.Load(); n != 1 {
		t.Fatalf("%d connections for 21 requests, want 1: a 413 must not end an HTTP/2 connection", n)
	}
}
