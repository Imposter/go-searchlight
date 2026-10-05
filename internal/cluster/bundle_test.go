package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// publishUntil uploads bundles from tn until the newest bundle of id is at or past
// seq, and returns the shard's bundles.
func publishUntil(t *testing.T, tn *tnode, id store.ShardID, seq int64) []bundleRef {
	t.Helper()
	var list []bundleRef
	eventually(t, time.Minute, fmt.Sprintf("a bundle of %s at seq %d", id, seq), func() error {
		tn.n.publishBundles(context.Background())
		var err error
		list, err = tn.n.listBundles(context.Background(), shardBundlePrefix(id, tn.n.indexUIDs()[id.Index]))
		if err != nil {
			return err
		}
		if len(list) == 0 || list[len(list)-1].seq < seq {
			return fmt.Errorf("bundles %v", list)
		}
		return nil
	})
	return list
}

// writeDocs writes docs k..k+n-1 of index and returns the last seq.
func writeDocs(t *testing.T, n *Node, index string, from, count int) int64 {
	t.Helper()
	var last int64
	for k := from; k < from+count; k++ {
		last = mustWrite(t, n, index, upsertOp(fmt.Sprintf("d%03d", k), k))
	}
	return last
}

// wipeAndRestart stops node i, removes its data directory and starts it again: its
// copies must be rebuilt, and no peer can serve them.
func wipeAndRestart(t *testing.T, c *cluster, i int, before func()) *tnode {
	t.Helper()
	tn := c.node(i)
	tn.stop()
	if err := os.RemoveAll(tn.cfg.DataDir); err != nil {
		t.Fatal(err)
	}
	if before != nil {
		before()
	}
	return c.start(i)
}

// TestBundleRecoveryWithoutPeer: a copy that must be rebuilt while no peer serves it
// restores its shard's recovery bundle, then replays the changelog after it.
func TestBundleRecoveryWithoutPeer(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	createIndex(t, a.n, "bun", 1, 0)
	id := store.ShardID{Index: "bun", Shard: 0}
	seq := writeDocs(t, a.n, "bun", 0, 40)
	waitCount(t, a.n, "bun", seq, 40)
	bundles := publishUntil(t, a, id, seq)
	last := writeDocs(t, a.n, "bun", 40, 15)

	a = wipeAndRestart(t, c, 0, nil)
	waitCount(t, a.n, "bun", last, 55)
	if got := a.n.fetch.restored.Load(); got != 1 {
		t.Fatalf("%d bundles restored, want 1 (the copy was rebuilt from elsewhere)", got)
	}
	if got := a.n.fetch.rejected.Load(); got != 0 {
		t.Fatalf("%d bundles rejected", got)
	}
	if b := bundles[len(bundles)-1]; b.seq >= last {
		t.Fatalf("the bundle at seq %d already held every write (last %d): nothing was replayed after it", b.seq, last)
	}
}

// TestCorruptBundleFallsBackToScanShard: a bundle whose bytes were damaged in the
// database is refused whole, never installed, and the copy is rebuilt from the
// store's snapshot instead.
func TestCorruptBundleFallsBackToScanShard(t *testing.T) {
	d := sqliteDB(t)
	c := newCluster(t, d, nil)
	a := c.start(0)
	createIndex(t, a.n, "bad", 1, 0)
	id := store.ShardID{Index: "bad", Shard: 0}
	seq := writeDocs(t, a.n, "bad", 0, 40)
	waitCount(t, a.n, "bad", seq, 40)
	bundles := publishUntil(t, a, id, seq)
	last := writeDocs(t, a.n, "bad", 40, 5)

	a = wipeAndRestart(t, c, 0, func() { corruptBlob(t, d, bundles[len(bundles)-1].name) })
	waitCount(t, a.n, "bad", last, 45)
	if got := a.n.fetch.restored.Load(); got != 0 {
		t.Fatalf("%d bundles restored from a corrupt one", got)
	}
	if got := a.n.fetch.rejected.Load(); got != 1 {
		t.Fatalf("%d bundles rejected, want 1", got)
	}
}

// corruptBlob flips bytes in the middle of the first chunk of a blob stored in the
// SQLite database d.
func corruptBlob(t *testing.T, d *db, name string) {
	t.Helper()
	path := strings.TrimPrefix(d.url, "sqlite://")
	path, _, _ = strings.Cut(path, "?")
	if len(path) > 2 && path[0] == '/' && path[2] == ':' {
		path = path[1:]
	}
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	ctx := context.Background()
	var upload string
	var data []byte
	err = raw.QueryRowContext(ctx, `SELECT c.upload_id, c.data FROM sl_blob_chunks c JOIN sl_blobs b ON b.upload_id = c.upload_id
		WHERE b.name = ? AND c.chunk = 0`, name).Scan(&upload, &data)
	if err != nil {
		t.Fatalf("reading the blob %s: %v", name, err)
	}
	for i := len(data) / 2; i < len(data)/2+8 && i < len(data); i++ {
		data[i] ^= 0xff
	}
	if _, err := raw.ExecContext(ctx, `UPDATE sl_blob_chunks SET data = ? WHERE upload_id = ? AND chunk = 0`, data, upload); err != nil {
		t.Fatal(err)
	}
}

// TestPruningKeepsBundlePoints: the changelog after the oldest retained bundle is kept
// although every copy is past it; once that bundle is gone pruning moves on to the
// copies. Uploads keep bundle_retention bundles, and a dropped index's bundles are
// deleted.
func TestPruningKeepsBundlePoints(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	createIndex(t, a.n, "keep", 1, 0)
	id := store.ShardID{Index: "keep", Shard: 0}
	ctx := context.Background()

	var list []bundleRef
	var seqs []int64
	for k := range 3 {
		seq := writeDocs(t, a.n, "keep", 10*k, 10)
		waitCount(t, a.n, "keep", seq, int64(10*(k+1)))
		list = publishUntil(t, a, id, seq)
		seqs = append(seqs, seq)
	}
	second := seqs[1]
	if len(list) != a.n.opts.BundleRetention || list[0].seq != second {
		t.Fatalf("bundles %v: want the newest %d, from seq %d", list, a.n.opts.BundleRetention, second)
	}
	last := writeDocs(t, a.n, "keep", 30, 10)
	waitCount(t, a.n, "keep", last, 40)

	eventually(t, time.Minute, "the changelog is pruned up to the oldest bundle", func() error {
		if _, err := a.st.ChangesAfter(ctx, id, 0, 1); !errors.Is(err, store.ErrPruned) {
			return fmt.Errorf("nothing pruned yet: %w", err)
		}
		return nil
	})
	for range 5 {
		if _, err := a.st.ChangesAfter(ctx, id, second, 1); err != nil {
			t.Fatalf("the changelog was pruned past the oldest bundle (seq %d): %v", second, err)
		}
		time.Sleep(a.n.opts.PruneInterval)
	}

	for _, b := range list {
		if err := a.st.Blobs().Delete(ctx, b.name); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, time.Minute, "pruning moves on to the copy once the bundles are gone", func() error {
		if _, err := a.st.ChangesAfter(ctx, id, second, 1); !errors.Is(err, store.ErrPruned) {
			return fmt.Errorf("still kept: %w", err)
		}
		return nil
	})

	publishUntil(t, a, id, last)
	if err := a.n.DeleteIndex(tctx(t), "keep"); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Minute, "the dropped index's bundles are deleted", func() error {
		left, err := a.n.listBundles(ctx, bundlePrefix)
		if err != nil || len(left) > 0 {
			return fmt.Errorf("bundles left %v: %w", left, err)
		}
		return nil
	})
}

// TestCorruptNewestBundleFallsBackToAnOlderOne: of two bundles, a damaged newest one is
// refused and the older one restored; the changelog after it replays the rest.
func TestCorruptNewestBundleFallsBackToAnOlderOne(t *testing.T) {
	d := sqliteDB(t)
	c := newCluster(t, d, nil)
	a := c.start(0)
	createIndex(t, a.n, "two", 1, 0)
	id := store.ShardID{Index: "two", Shard: 0}
	first := writeDocs(t, a.n, "two", 0, 30)
	waitCount(t, a.n, "two", first, 30)
	publishUntil(t, a, id, first)
	second := writeDocs(t, a.n, "two", 30, 20)
	waitCount(t, a.n, "two", second, 50)
	bundles := publishUntil(t, a, id, second)
	if len(bundles) != 2 {
		t.Fatalf("bundles %v, want two", bundles)
	}
	last := writeDocs(t, a.n, "two", 50, 5)

	a = wipeAndRestart(t, c, 0, func() { corruptBlob(t, d, bundles[1].name) })
	waitCount(t, a.n, "two", last, 55)
	if got := a.n.fetch.rejected.Load(); got != 1 {
		t.Fatalf("%d bundles rejected, want the newest", got)
	}
	if got := a.n.fetch.restored.Load(); got != 1 {
		t.Fatalf("%d bundles restored, want the older one", got)
	}
}
