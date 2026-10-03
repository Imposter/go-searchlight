package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestGracefulShutdownUnderLoad stops the API while writers and readers are busy:
// requests in flight finish, new connections are refused, the coordinator closes its
// copies, and every write that was acknowledged is in the store.
func TestGracefulShutdownUnderLoad(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n, err := node.NewSingle(context.Background(), node.Options{
		Store: st, Config: cfg, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := api.NewServer(n, nil, cfg)
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
	go func() { done <- srv.Run(ctx, ln) }()
	base := "http://" + ln.Addr().String()
	client := &http.Client{Timeout: 30 * time.Second}
	call := func(method, path, body string) (int, []byte, error) {
		req, err := http.NewRequestWithContext(context.Background(), method, base+path, strings.NewReader(body))
		if err != nil {
			return 0, nil, err
		}
		res, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		return res.StatusCode, b, err
	}
	if code, b, err := call("PUT", "/indexes/load", `{"settings": {"shards": 2}}`); err != nil || code != http.StatusCreated {
		t.Fatalf("create: %d %s %v", code, b, err)
	}

	var mu sync.Mutex
	acked := map[string]int64{}
	var bad []string
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := range 6 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if w%3 == 2 {
					code, b, err := call("POST", "/indexes/load/_search", `{"size": 5}`)
					if err != nil {
						return // the listener is closed
					}
					if code != http.StatusOK && code != http.StatusServiceUnavailable {
						mu.Lock()
						bad = append(bad, fmt.Sprintf("search: %d %s", code, b))
						mu.Unlock()
					}
					continue
				}
				var lines []string
				var ids []string
				for k := range 20 {
					id := fmt.Sprintf("w%d-%d-%d", w, i, k)
					ids = append(ids, id)
					lines = append(lines, fmt.Sprintf(`{"upsert": {"id": %q}}`, id), `{"n": 1}`)
				}
				code, b, err := call("POST", "/indexes/load/_bulk", ndjson(lines...))
				if err != nil {
					return
				}
				switch code {
				case http.StatusOK:
					var r struct {
						Errors bool `json:"errors"`
						Items  []struct {
							Seq int64 `json:"seq"`
						} `json:"items"`
					}
					if err := json.Unmarshal(b, &r); err != nil || r.Errors {
						mu.Lock()
						bad = append(bad, fmt.Sprintf("bulk: %s %v", b, err))
						mu.Unlock()
						continue
					}
					mu.Lock()
					for k, id := range ids {
						acked[id] = r.Items[k].Seq
					}
					mu.Unlock()
				case http.StatusServiceUnavailable:
				default:
					mu.Lock()
					bad = append(bad, fmt.Sprintf("bulk: %d %s", code, b))
					mu.Unlock()
				}
			}
		})
	}
	time.Sleep(500 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}
	close(stop)
	wg.Wait()
	for _, b := range bad {
		t.Error(b)
	}
	if len(acked) == 0 {
		t.Fatal("no write was acknowledged before the shutdown")
	}
	// New connections are refused.
	if _, _, err := call("GET", "/healthz", ""); err == nil {
		t.Error("the listener still accepts after shutdown")
	}
	// The coordinator is closed: its copies no longer serve.
	if _, err := n.Search(context.Background(), "load", &search.Request{}, api.ReadOptions{}); err == nil {
		t.Error("the node still searches after shutdown")
	}
	// Every acknowledged write is durable in the store, at its seq.
	rr := st.(store.RecordReader) //nolint:forcetypeassert,errcheck // every store reads records
	for id, seq := range acked {
		rec, err := rr.GetRecord(context.Background(), store.RecordDocument, store.ShardID{Index: "load", Shard: node.ShardFor(id, 2)}, id)
		if err != nil || rec.Seq != seq || !bytes.Equal(rec.Body, []byte(`{"n": 1}`)) {
			t.Fatalf("acknowledged %s at seq %d: %+v %v", id, seq, rec, err)
		}
	}
	t.Logf("%d writes acknowledged before the shutdown", len(acked))

	// A restarted node serves every acknowledged write.
	n2, err := node.NewSingle(context.Background(), node.Options{
		Store: st, Config: cfg, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n2.Close(context.Background()) }()
	var maxSeq int64
	for _, seq := range acked {
		maxSeq = max(maxSeq, seq)
	}
	wctx, wcancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer wcancel()
	// The copies recover before they serve; readiness says when, as a load
	// balancer would ask.
	for n2.Ready(wctx) != nil {
		if wctx.Err() != nil {
			t.Fatal("the restarted node never became ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp, err := n2.Search(wctx, "load", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: maxSeq})
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("the restarted node did not catch up")
	}
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total < int64(len(acked)) {
		t.Errorf("after a restart the index holds %d documents, %d were acknowledged", resp.Total, len(acked))
	}
}
