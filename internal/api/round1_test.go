package api_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
)

// I3: the request bytes in flight are bounded; past the budget a request gets 429,
// whether its length is known up front or learned as it is read.
func TestInflightByteBudget(t *testing.T) {
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	c := &stub{write: func(context.Context) (*api.WriteResult, error) {
		entered <- struct{}{}
		<-release
		return &api.WriteResult{Items: []api.ItemResult{{Seq: 1}}}, nil
	}}
	url := stubServer(t, c, func(cfg *config.Config) {
		cfg.MaxBodyBytes = 64 << 10
		cfg.MaxDocBytes = 64 << 10
		cfg.InflightAmplification = 2 // a 40 KiB bulk weighs 80 KiB
		cfg.MaxInflightWriteBytes = 128 << 10
	})
	e := &env{t: t, url: url, client: http.DefaultClient}
	bulk := ndjson(`{"upsert": {"id": "a"}}`, `{"pad": "`+strings.Repeat("x", 40<<10)+`"}`)
	var wg sync.WaitGroup
	wg.Go(func() {
		if r := e.do("POST", "/indexes/x/_bulk", bulk); r.status != http.StatusOK {
			t.Errorf("the first bulk: %d %s", r.status, r.body)
		}
	})
	<-entered
	got := e.do("POST", "/indexes/x/_bulk", bulk)
	e.problem(got, http.StatusTooManyRequests, "too_many_requests")
	if got.header.Get("Retry-After") == "" {
		t.Error("no Retry-After")
	}
	// A body of unknown length is weighed as it is read.
	e.problem(e.doReader("POST", "/indexes/x/_bulk", io.MultiReader(strings.NewReader(bulk))), http.StatusTooManyRequests, "too_many_requests")
	// Reads have a budget of their own.
	close(release)
	wg.Wait()
	if r := e.do("POST", "/indexes/x/_bulk", bulk); r.status != http.StatusOK {
		t.Errorf("after the budget freed: %d %s", r.status, r.body)
	}
}

func TestRequestShapeChecks(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.must(http.StatusCreated, "PUT", "/indexes/r", "")
	// I8: a malformed query string is refused, not partly read.
	p := e.problem(e.do("POST", "/indexes/r/_search?wait_for_seq=1&x=%zz", `{}`), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "params") {
		t.Errorf("a malformed query string: %v", p)
	}
	// Trailing brackets after a JSON body are refused.
	e.problem(e.do("PUT", "/indexes/r2", `{"mapping": {}}}`), http.StatusBadRequest, "invalid_request")
	e.problem(e.do("PATCH", "/indexes/r/settings", `{"refresh_interval": "1s"}]`), http.StatusBadRequest, "invalid_request")
	// A bulk id that is not valid UTF-8 is refused, not rewritten.
	p = e.problem(e.do("POST", "/indexes/r/_bulk", ndjson("{\"upsert\": {\"id\": \"a\xffb\"}}", `{}`)), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "line.1.upsert.id") {
		t.Errorf("an invalid UTF-8 id: %v", p)
	}
	// A lone surrogate escape would decode to U+FFFD: refused, while U+FFFD
	// spelled out is an id like any other.
	p = e.problem(e.do("POST", "/indexes/r/_bulk", ndjson(`{"upsert": {"id": "a\ud800"}}`, `{}`)), http.StatusBadRequest, "invalid_request")
	if !hasLoc(p, "line.1.upsert.id") {
		t.Errorf("a lone surrogate id: %v", p)
	}
	e.must(http.StatusOK, "POST", "/indexes/r/_bulk", ndjson(`{"upsert": {"id": "a\ufffd"}}`, `{}`))
	// "." and ".." cannot be addressed by a path, so no document may have them.
	r := e.must(http.StatusOK, "POST", "/indexes/r/_bulk", ndjson(`{"upsert": {"id": "."}}`, `{}`, `{"upsert": {"id": ".."}}`, `{}`, `{"upsert": {"id": "..."}}`, `{}`))
	items := r["items"].([]any) //nolint:forcetypeassert,errcheck // the shape
	for i, want := range []float64{400, 400, 200} {
		if got := items[i].(map[string]any)["status"]; got != want { //nolint:forcetypeassert,errcheck // the shape
			t.Errorf("item %d: status %v, want %v", i, got, want)
		}
	}
	// I4: a document over max_doc_bytes is refused from its length alone.
	doc := `{"pad": "` + strings.Repeat("x", int(e.cfg.MaxDocBytes)) + `"}`
	e.problem(e.do("PUT", "/indexes/r/docs/big", doc), http.StatusRequestEntityTooLarge, "too_large")
}

// selfSigned writes a certificate and key for 127.0.0.1.
func selfSigned(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "searchlight-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

// closer is a Coordinator that only closes.
type closer struct {
	api.Coordinator
	closed chan struct{}
}

func (c *closer) Ready(context.Context) error { return nil }

func (c *closer) Close(context.Context) error {
	close(c.closed)
	return nil
}

// TestRunTLSAndShutdownGrace serves over TLS, then shuts down: readiness turns false
// while the API still serves for shutdown_grace, then the coordinator closes.
func TestRunTLSAndShutdownGrace(t *testing.T) {
	cfg := testConfig(t)
	cfg.TLSCert, cfg.TLSKey = selfSigned(t)
	cfg.ShutdownGrace = 500 * time.Millisecond
	c := &closer{closed: make(chan struct{})}
	srv, err := api.NewServer(c, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, ln, srv) }()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
	e := &env{t: t, url: "https://" + ln.Addr().String(), client: client}
	e.must(http.StatusOK, "GET", "/readyz", "")
	cancel()
	deadline := time.Now().Add(400 * time.Millisecond)
	sawDraining := false
	for time.Now().Before(deadline) && !sawDraining {
		sawDraining = e.do("GET", "/readyz", "").status == http.StatusServiceUnavailable
	}
	if !sawDraining {
		t.Error("readiness did not turn false during the shutdown grace")
	}
	e.must(http.StatusOK, "GET", "/healthz", "") // still serving
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.closed:
	default:
		t.Error("the coordinator was not closed")
	}
}
