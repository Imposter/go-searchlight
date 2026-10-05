package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	legacyrouter "github.com/getkin/kin-openapi/routers/legacy"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/cluster"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/node/nodetest"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
)

// clusterSuite is the test whose subtests newEnv serves over a one-node cluster.Node.
const clusterSuite = "TestSuitesOnSingleNodeCluster"

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.DiscardHandler))
	os.Exit(m.Run())
}

// env is an API served over a single node on a fresh SQLite store (or, under
// clusterSuite, over a cluster of one: cluster.Node must serve the API exactly as
// node.Single does).
type env struct {
	t      testing.TB
	url    string
	srv    *api.Server
	node   api.Coordinator
	st     store.Store
	cfg    config.Config
	token  string
	client *http.Client
	// validate checks every response against api/openapi.yaml.
	validate bool

	mu      sync.Mutex
	tailers map[store.ShardID]*nodetest.Tailer
}

// with returns e reporting to t (a subtest's).
func (e *env) with(t *testing.T) *env {
	return &env{t: t, url: e.url, srv: e.srv, node: e.node, st: e.st, cfg: e.cfg, token: e.token, client: e.client, validate: e.validate, tailers: e.tailers}
}

// tailer returns a shard copy's tailer (with fakeTailers).
func (e *env) tailer(index string, s int) *nodetest.Tailer {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tailers[store.ShardID{Index: index, Shard: s}]
}

// envOpts adjust newEnv.
type envOpts struct {
	cfg  func(*config.Config)
	node func(*node.Options)
	// fakeTailers uses nodetest.Tailer, which tests can pause, instead of the
	// replica tailer.
	fakeTailers bool
}

// testConfig is a node's configuration for tests: SQLite in a temp dir, auth off,
// a quick refresh.
func testConfig(t testing.TB) config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StoreURL = storetest.SQLiteURL(storetest.Migrated(t, filepath.Join(dir, "searchlight.db")))
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.InsecureNoAuth = true
	cfg.RefreshInterval = 20 * time.Millisecond
	cfg.ChangelogPollInterval = 20 * time.Millisecond
	cfg.RemapDebounce = 0
	cfg.RequestTimeout = 10 * time.Second
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.NodeID = "test-node"
	cfg.AdvertiseAddress = "127.0.0.1:8780"
	cfg.MergeThreads = 1
	cfg.SearchThreads = 2
	return cfg
}

func openStore(t testing.TB, cfg config.Config) store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), cfg.StoreURL, store.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func newEnv(t testing.TB, o envOpts) *env {
	t.Helper()
	cfg := testConfig(t)
	if o.cfg != nil {
		o.cfg(&cfg)
	}
	st := openStore(t, cfg)
	e := &env{t: t, st: st, cfg: cfg, tailers: map[store.ShardID]*nodetest.Tailer{}}
	var newTailer node.NewTailerFunc // nil: the replica tailer
	if o.fakeTailers {
		newTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			tl := nodetest.NewTailer(st, sh, id, env)
			e.mu.Lock()
			e.tailers[id] = tl.(*nodetest.Tailer) //nolint:forcetypeassert,errcheck // NewTailer returns a *Tailer
			e.mu.Unlock()
			return tl
		}
	}
	nopts := node.Options{
		Store: st, Config: cfg, NewTailer: newTailer, Version: "test",
		Logger: slog.New(slog.DiscardHandler),
		ShardOptions: func(o *shard.Options) {
			o.DisableMerges = true
			o.Logger = slog.New(slog.DiscardHandler)
		},
	}
	if o.node != nil {
		o.node(&nopts)
	}
	var n api.Coordinator
	if strings.HasPrefix(t.Name(), clusterSuite+"/") {
		cn, err := cluster.New(context.Background(), cluster.Options{
			Store: st, Config: cfg, Version: "test", Logger: slog.New(slog.DiscardHandler),
			Engine: func(eo *node.Options) {
				cl := eo.Cluster
				*eo = nopts
				eo.Cluster = cl
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := cn.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		n = cn
	} else {
		sn, err := node.NewSingle(context.Background(), nopts)
		if err != nil {
			t.Fatal(err)
		}
		n = sn
	}
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	srv, err := api.NewServer(n, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewUnstartedServer(srv)
	tmpl := srv.HTTPServer()
	hs.Config.ReadTimeout, hs.Config.ReadHeaderTimeout, hs.Config.WriteTimeout = tmpl.ReadTimeout, tmpl.ReadHeaderTimeout, tmpl.WriteTimeout
	hs.Start()
	t.Cleanup(hs.Close)
	e.url, e.srv, e.node, e.client, e.validate = hs.URL, srv, n, hs.Client(), true
	return e
}

// resp is a response: its status, headers and body.
type resp struct {
	status int
	header http.Header
	body   []byte
}

// json decodes the body into a generic value.
func (r resp) json(t testing.TB) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("response %d is not a JSON object: %v: %s", r.status, err, r.body)
	}
	return m
}

func (e *env) do(method, path, body string, header ...string) resp {
	e.t.Helper()
	return e.doReader(method, path, strings.NewReader(body), header...)
}

func (e *env) doReader(method, path string, body io.Reader, header ...string) resp {
	e.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, e.url+path, body)
	if err != nil {
		e.t.Fatal(err)
	}
	if e.token != "" {
		req.Header.Set("Authorization", "Bearer "+e.token)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	res, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		e.t.Fatalf("%s %s: reading the body: %v", method, path, err)
	}
	if e.validate {
		if err := validateResponse(req, res, b); err != nil {
			e.t.Errorf("%s %s: the response does not match api/openapi.yaml: %v: %s", method, path, err, b)
		}
	}
	return resp{status: res.StatusCode, header: res.Header, body: b}
}

// specRouter routes requests to api/openapi.yaml's operations, by path alone.
var specRouter = sync.OnceValues(func() (routers.Router, error) {
	openapi3filter.RegisterBodyDecoder(api.ProblemContentType, openapi3filter.JSONBodyDecoder)
	doc, err := openapi3.NewLoader().LoadFromFile(filepath.Join("..", "..", "api", "openapi.yaml"))
	if err != nil {
		return nil, err
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, err
	}
	doc.Servers = nil
	return legacyrouter.NewRouter(doc)
})

// validateResponse checks a response against api/openapi.yaml: its status is one the
// operation lists, and its body matches that response's schema. A request no
// operation serves (an unmatched path) is skipped.
func validateResponse(req *http.Request, res *http.Response, body []byte) error {
	router, err := specRouter()
	if err != nil {
		return err
	}
	routed := req.Clone(context.Background())
	routed.URL.Path = req.URL.EscapedPath() // an id with %2F is one segment
	route, params, err := router.FindRoute(routed)
	if err != nil {
		return nil //nolint:nilerr // not an operation of the spec: nothing to check
	}
	// Every status must be listed for its operation, not fall through to default.
	if route.Operation.Responses.Value(strconv.Itoa(res.StatusCode)) == nil {
		return fmt.Errorf("status %d is not listed for %s %s", res.StatusCode, route.Method, route.Path)
	}
	opts := &openapi3filter.Options{IncludeResponseStatus: true}
	return openapi3filter.ValidateResponse(context.Background(), &openapi3filter.ResponseValidationInput{
		RequestValidationInput: &openapi3filter.RequestValidationInput{Request: routed, PathParams: params, Route: route, Options: opts},
		Status:                 res.StatusCode,
		Header:                 res.Header,
		Body:                   io.NopCloser(bytes.NewReader(body)),
		Options:                opts,
	})
}

// must runs a request and requires status.
func (e *env) must(status int, method, path, body string) map[string]any {
	e.t.Helper()
	r := e.do(method, path, body)
	if r.status != status {
		e.t.Fatalf("%s %s: HTTP %d, want %d: %s", method, path, r.status, status, r.body)
	}
	if len(bytes.TrimSpace(r.body)) == 0 {
		return nil
	}
	return r.json(e.t)
}

// problem requires a problem response with status and code, returning it.
func (e *env) problem(r resp, status int, code string) map[string]any {
	e.t.Helper()
	if r.status != status {
		e.t.Fatalf("HTTP %d, want %d: %s", r.status, status, r.body)
	}
	if ct := r.header.Get("Content-Type"); ct != api.ProblemContentType {
		e.t.Errorf("Content-Type %q, want %q", ct, api.ProblemContentType)
	}
	m := r.json(e.t)
	if m["code"] != code || m["type"] != api.ProblemType+code || int(m["status"].(float64)) != status { //nolint:forcetypeassert,errcheck // a test of the shape
		e.t.Errorf("problem %v, want code %s", m, code)
	}
	return m
}

// locs lists a problem's locs.
func locs(m map[string]any) []string {
	var out []string
	ps, _ := m["problems"].([]any)
	for _, p := range ps {
		if pm, ok := p.(map[string]any); ok {
			loc, _ := pm["loc"].(string)
			out = append(out, loc)
		}
	}
	return out
}

func hasLoc(m map[string]any, prefix string) bool {
	for _, l := range locs(m) {
		if strings.HasPrefix(l, prefix) {
			return true
		}
	}
	return false
}

// seqOf reads a write response's seq.
func seqOf(t testing.TB, m map[string]any) int64 {
	t.Helper()
	f, ok := m["seq"].(float64)
	if !ok || f <= 0 {
		t.Fatalf("no seq in %v", m)
	}
	return int64(f)
}

// ids lists a search response's hit ids.
func ids(m map[string]any) []string {
	var out []string
	hits, _ := m["hits"].([]any)
	for _, h := range hits {
		if hm, ok := h.(map[string]any); ok {
			id, _ := hm["id"].(string)
			out = append(out, id)
		}
	}
	return out
}

// totalOf reads a search response's total.
func totalOf(m map[string]any) int {
	tot, _ := m["total"].(map[string]any)
	v, _ := tot["value"].(float64)
	return int(v)
}

// ndjson joins lines with newlines, ending with one.
func ndjson(lines ...string) string { return strings.Join(lines, "\n") + "\n" }
