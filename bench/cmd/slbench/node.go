package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/Imposter/go-searchlight/bench/workloads"
	"github.com/Imposter/go-searchlight/internal/slproc"
)

// nodeStopTimeout bounds a node's graceful stop before it is killed.
const nodeStopTimeout = 2 * time.Minute

// nodeSpec is a searchlight node slbench runs.
type nodeSpec struct {
	bin, dir, nodeID string
	// store is the node's store_url; empty means SQLite in dir.
	store string
	// token is the API token; empty generates one. clusterToken opens the peer API.
	token, clusterToken string
	settings            []string
}

// runNode starts a node, its log appended to dir/searchlight.log, and waits until it
// is ready. stop stops it gracefully (killing it past nodeStopTimeout) and reports a
// failure to w.
func runNode(ctx context.Context, sp nodeSpec) (*slproc.Node, func(w io.Writer), error) {
	if err := os.MkdirAll(sp.dir, 0o750); err != nil {
		return nil, nil, err
	}
	logFile, err := os.OpenFile(filepath.Join(sp.dir, "searchlight.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	node, err := slproc.Launch(ctx, slproc.Options{
		Bin: sp.bin, Dir: sp.dir, StoreURL: sp.store, NodeID: sp.nodeID, Token: sp.token,
		ClusterToken: sp.clusterToken, Settings: sp.settings, Log: logFile,
	})
	if err == nil {
		err = node.WaitReady(ctx)
	}
	if err != nil {
		if node != nil {
			_ = node.Kill()
		}
		_ = logFile.Close()
		return nil, nil, fmt.Errorf("starting searchlight (%s): %w", sp.nodeID, err)
	}
	stop := func(w io.Writer) {
		if err := node.Stop(nodeStopTimeout); err != nil {
			fmt.Fprintln(w, "slbench: stopping searchlight:", err)
		}
		_ = logFile.Close()
	}
	return node, stop, nil
}

// recoverer is Searchlight's [workloads.Recoverer]: a cluster of its own on a shared
// store (Postgres or MySQL), whose first node holds the dataset, and to which each
// recovery adds a node with an empty data directory.
type recoverer struct {
	spec       nodeSpec
	source     *slproc.Node
	stopSource func(io.Writer)
	engine     *workloads.Searchlight
	replicas   int
	client     *http.Client
}

// newRecoverer starts the recovery cluster's source node.
func newRecoverer(ctx context.Context, sp nodeSpec, log io.Writer) (*recoverer, error) {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	sp.clusterToken, sp.token = hex.EncodeToString(b), slproc.NewToken()
	src := sp
	src.dir, src.nodeID = filepath.Join(sp.dir, "source"), "recovery-source"
	if err := os.RemoveAll(src.dir); err != nil {
		return nil, err
	}
	node, stop, err := runNode(ctx, src)
	if err != nil {
		return nil, err
	}
	return &recoverer{
		spec: sp, source: node, stopSource: stop, client: &http.Client{},
		engine: workloads.NewSearchlight(workloads.SearchlightOptions{URL: node.URL, Token: node.Token, Log: log}),
	}, nil
}

// Source implements workloads.Recoverer.
func (r *recoverer) Source() workloads.Engine { return r.engine }

// Recover implements workloads.Recoverer: it starts a node with an empty data
// directory and times it until its own copies of index serve want documents. A node
// that fails keeps its directory (and its searchlight.log) for diagnosis.
func (r *recoverer) Recover(ctx context.Context, index string, want int64) (took time.Duration, err error) {
	r.replicas++
	sp := r.spec
	sp.nodeID = fmt.Sprintf("recovery-replica-%d", r.replicas)
	sp.dir = filepath.Join(r.spec.dir, sp.nodeID)
	if err := os.RemoveAll(sp.dir); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(sp.dir, 0o750); err != nil {
		return 0, err
	}
	logFile, err := os.OpenFile(filepath.Join(sp.dir, "searchlight.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	defer logFile.Close()
	start := time.Now()
	node, err := slproc.Launch(ctx, slproc.Options{
		Bin: sp.bin, Dir: sp.dir, StoreURL: sp.store, NodeID: sp.nodeID, Token: sp.token,
		ClusterToken: sp.clusterToken, Settings: sp.settings, Log: logFile,
	})
	if err != nil {
		return 0, err
	}
	defer func() {
		if serr := node.Stop(nodeStopTimeout); serr != nil {
			_ = node.Kill()
		}
		if err == nil {
			_ = os.RemoveAll(sp.dir)
		}
	}()
	rctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	for {
		served, err := r.servedBy(rctx, node, index)
		if err == nil && served == want {
			return time.Since(start), nil
		}
		select {
		case <-node.Exited():
			return 0, fmt.Errorf("the recovering node exited: %w\n%s", node.ExitErr(), node.Logs().Tail(40))
		case <-rctx.Done():
			return 0, fmt.Errorf("the recovering node served %d of %d documents: %w", served, want, errors.Join(rctx.Err(), err))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// servedBy is the documents of index node's own copies serve, once every one of them
// serves (0 until then).
func (r *recoverer) servedBy(ctx context.Context, node *slproc.Node, index string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, node.URL+"/_cluster/shards", http.NoBody)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer "+node.Token)
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("/_cluster/shards: HTTP %d", resp.StatusCode)
	}
	var body struct {
		Shards []struct {
			Index string `json:"index"`
			Node  string `json:"node"`
			State string `json:"state"`
			Docs  int64  `json:"docs"`
		} `json:"shards"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	var docs int64
	copies := 0
	for _, c := range body.Shards {
		if c.Index != index || c.Node != node.NodeID() {
			continue
		}
		if c.State != "serving" {
			return 0, nil
		}
		copies++
		docs += c.Docs
	}
	if copies == 0 {
		return 0, nil
	}
	return docs, nil
}

// close stops the source node.
func (r *recoverer) close(w io.Writer) { r.stopSource(w) }
