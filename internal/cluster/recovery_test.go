package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// bulkLoad writes n documents to index through node in batches, returning the last
// seq.
func bulkLoad(t testing.TB, n *Node, index string, from, count, batch int) int64 {
	t.Helper()
	var last int64
	for lo := from; lo < from+count; lo += batch {
		ops := make([]api.WriteOp, 0, batch)
		for i := lo; i < min(lo+batch, from+count); i++ {
			ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("doc-%07d", i), Body: json.RawMessage(fmt.Sprintf(
				`{"title":"product %d of the catalogue, a long enough title to weigh","brand":"brand %d","price":%d,"tags":["t%d","u%d"],"sku":"SKU-%07d"}`,
				i, i%113, i%997, i%17, i%31, i))})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		res, err := n.Write(ctx, index, ops, api.WriteOptions{})
		cancel()
		if err != nil {
			t.Fatalf("bulk at %d: %v", lo, err)
		}
		for i, it := range res.Items {
			if it.Err != nil {
				t.Fatalf("bulk op %d at %d: %v", i, lo, it.Err)
			}
		}
		last = res.Seq
	}
	return last
}

// cutOnce makes the first stream of a file named match stop dead after limit bytes:
// the connection drops mid-transfer.
type cutOnce struct {
	mu    sync.Mutex
	match func(name string) bool
	limit int64
	fired atomic.Bool
	// corrupt flips one byte of the first stream instead of cutting it.
	corrupt bool
}

// arm sets which file is cut, and where.
func (c *cutOnce) arm(match func(name string) bool, limit int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.match, c.limit = match, limit
}

func (c *cutOnce) wrap(name string, w http.ResponseWriter) http.ResponseWriter {
	c.mu.Lock()
	match, limit := c.match, c.limit
	c.mu.Unlock()
	if match == nil || !match(name) || c.fired.Load() {
		return w
	}
	if !c.fired.CompareAndSwap(false, true) {
		return w
	}
	return &cutWriter{ResponseWriter: w, left: limit, corrupt: c.corrupt}
}

type cutWriter struct {
	http.ResponseWriter
	left    int64
	corrupt bool
	done    bool
}

func (w *cutWriter) Write(p []byte) (int, error) {
	if w.corrupt {
		if !w.done && int64(len(p)) > w.left {
			q := append([]byte(nil), p...)
			q[w.left] ^= 0xff
			w.done = true
			return w.ResponseWriter.Write(q)
		}
		w.left -= int64(len(p))
		return w.ResponseWriter.Write(p)
	}
	if w.left <= 0 {
		panic(http.ErrAbortHandler) // the connection drops
	}
	if int64(len(p)) > w.left {
		_, _ = w.ResponseWriter.Write(p[:w.left])
		w.left = 0
		if f, ok := w.ResponseWriter.(http.Flusher); ok {
			f.Flush()
		}
		panic(http.ErrAbortHandler)
	}
	w.left -= int64(len(p))
	return w.ResponseWriter.Write(p)
}

// biggestSegment names the largest segment file of node's copy of id.
func biggestSegment(t testing.TB, tn *tnode, id store.ShardID) (string, int64) {
	t.Helper()
	sn, err := tn.n.Snapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Release()
	var name string
	var size int64
	for _, f := range sn.Files() {
		if strings.HasSuffix(f.Name, ".seg") && f.Size > size {
			name, size = f.Name, f.Size
		}
	}
	return name, size
}

func snapshotBytes(t testing.TB, tn *tnode, id store.ShardID) int64 {
	t.Helper()
	sn, err := tn.n.Snapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Release()
	var total int64
	for _, f := range sn.Files() {
		total += f.Size
	}
	return total
}

// TestPeerRecovery100k: a new node recovers a 100,000-document shard from a peer.
// The transfer of the biggest segment file is cut mid-stream and resumes from where
// it stopped (the bytes fetched add up to the copy's size, once), and another file
// arrives corrupted once and is fetched again after it fails its checksum. The
// recovered copy holds every document and then tails the changelog.
func TestPeerRecovery100k(t *testing.T) {
	docs := 100_000
	if testing.Short() {
		docs = 20_000
	}
	d := sqliteDB(t)
	cut := &cutOnce{}
	bad := &cutOnce{corrupt: true}
	c := newCluster(t, d, func(i int, o *Options) {
		if i == 0 {
			o.hooks = &testHooks{peerFile: func(name string, w http.ResponseWriter) http.ResponseWriter {
				return bad.wrap(name, cut.wrap(name, w))
			}}
		}
	})
	a := c.start(0)
	createIndex(t, a.n, "big", 1, 0)
	began := time.Now()
	last := bulkLoad(t, a.n, "big", 0, docs, 5000)
	t.Logf("loaded %d documents in %s", docs, time.Since(began))
	id := store.ShardID{Index: "big", Shard: 0}
	if got, err := count(tctx(t), a.n, "big", last); err != nil || got != int64(docs) {
		t.Fatalf("node-0 counts %d (%v)", got, err)
	}
	if err := a.n.LocalCopies()[0].Info.State; err != api.ShardServing {
		t.Fatalf("node-0's copy is %s", err)
	}
	name, size := biggestSegment(t, a, id)
	cut.arm(func(n string) bool { return n == name }, size/3)
	bad.arm(func(n string) bool { return n != name && n != "manifest" && strings.HasSuffix(n, ".seg") }, 100)
	total := snapshotBytes(t, a, id)
	t.Logf("the copy is %d bytes; cutting %s (%d bytes) after %d", total, name, size, size/3)

	began = time.Now()
	b := c.start(1)
	waitCopies(t, a.st, "big", 1, 2, 5*time.Minute)
	took := time.Since(began)
	fetched := b.n.fetch.bytes.Load()
	t.Logf("recovered in %s: %d bytes fetched (%.1f MB/s), %d resumes", took, fetched,
		float64(fetched)/(1<<20)/took.Seconds(), b.n.fetch.resumes.Load())
	if !cut.fired.Load() || !bad.fired.Load() {
		t.Fatalf("the transfer was cut: %v, a file corrupted: %v", cut.fired.Load(), bad.fired.Load())
	}
	if b.n.fetch.resumes.Load() < 1 {
		t.Fatal("the cut transfer did not resume")
	}
	extra := int64(0)
	if bad.fired.Load() {
		extra = 64 << 20 // a corrupted file is fetched again in full
	}
	if fetched < total || fetched > total+extra+(1<<20) {
		t.Fatalf("fetched %d bytes for a %d-byte copy", fetched, total)
	}
	if got, err := count(tctx(t), b.n, "big", last); err != nil || got != int64(docs) {
		t.Fatalf("node-1 counts %d (%v), want %d", got, err, docs)
	}
	// The recovered copy tails on.
	last = mustWrite(t, a.n, "big", upsertOp("after", 1))
	if got, err := count(tctx(t), b.n, "big", last); err != nil || got != int64(docs)+1 {
		t.Fatalf("node-1 counts %d (%v) after a new write", got, err)
	}
	// The staging directory is gone once the copy is in.
	if _, err := os.Stat(filepath.Join(b.cfg.DataDir, "recovery")); err == nil {
		entries, _ := os.ReadDir(filepath.Join(b.cfg.DataDir, "recovery"))
		if len(entries) > 0 {
			t.Errorf("staging left behind: %v", entries)
		}
	}
}

// TestRecoveryResumesAcrossAttempts: a recovery whose attempt fails after staging some
// files reuses them (checked against their sums) on the next attempt.
func TestRecoveryResumesAcrossAttempts(t *testing.T) {
	d := sqliteDB(t)
	var mu sync.Mutex
	failManifest := true
	c := newCluster(t, d, func(i int, o *Options) {
		if i == 0 {
			o.hooks = &testHooks{peerFile: func(name string, w http.ResponseWriter) http.ResponseWriter {
				mu.Lock()
				defer mu.Unlock()
				if name == "manifest" && failManifest {
					failManifest = false
					return &cutWriter{ResponseWriter: w, left: 0}
				}
				return w
			}}
		}
	})
	a := c.start(0)
	createIndex(t, a.n, "res", 1, 0)
	last := bulkLoad(t, a.n, "res", 0, 3000, 1000)
	if got, err := count(tctx(t), a.n, "res", last); err != nil || got != 3000 {
		t.Fatalf("node-0 counts %d (%v)", got, err)
	}
	id := store.ShardID{Index: "res", Shard: 0}
	total := snapshotBytes(t, a, id)
	b := c.start(1)
	waitCopies(t, a.st, "res", 1, 2, time.Minute)
	if got, err := count(tctx(t), b.n, "res", last); err != nil || got != 3000 {
		t.Fatalf("node-1 counts %d (%v)", got, err)
	}
	mu.Lock()
	cutDone := !failManifest
	mu.Unlock()
	if !cutDone {
		t.Fatal("the manifest transfer was never cut")
	}
	// Every file but the manifest was staged by the first attempt and kept.
	if fetched := b.n.fetch.bytes.Load(); fetched < total || fetched > total+total/2 {
		t.Fatalf("fetched %d bytes for a %d-byte copy: the staged files were fetched again", fetched, total)
	}
}
