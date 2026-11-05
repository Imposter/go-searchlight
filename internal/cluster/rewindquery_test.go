package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestRewindLowersAQuerySeqWithNoCopy (from the M2 review): a node that hosts no copy
// of the shard writes saved queries that a lossy failover then loses. No tailer of its
// hears of the rewind, so its querySeq still names the lost seq, which the peer holding
// the copy refuses as past the head. The node then reads the shard's head, lowers its
// querySeq and percolates again: the caller, who asked for no wait, gets an answer.
func TestRewindLowersAQuerySeqWithNoCopy(t *testing.T) {
	d := sqliteDB(t)
	c := newCluster(t, d, nil)
	a := c.start(0)
	b := c.start(1)
	if _, err := a.n.CreateIndex(tctx(t), "rq", api.IndexSpec{Settings: api.IndexSettings{Shards: 1, ReplicasPerShard: 1}}); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "rq", 1, 1, time.Minute)
	id := store.ShardID{Index: "rq", Shard: 0}
	_, holders := serving(t, a.st, id)
	writer, holder := b, a
	if holders[0] == b.cfg.NodeID {
		writer, holder = a, b
	}
	kept := mustWrite(t, writer.n, "rq", upsertOp("kept", 1))
	var ops []api.WriteOp
	for _, q := range []string{"q1", "q2", "q3", "q4"} {
		ops = append(ops, api.WriteOp{Kind: api.OpQueryUpsert, ID: q, Query: json.RawMessage(`{"field": "price", "op": "exists"}`)})
	}
	lost := mustWrite(t, writer.n, "rq", ops...)
	req := &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"price": 1}`)}}
	if _, err := writer.n.Percolate(tctx(t), "rq", req, api.ReadOptions{WaitFor: lost}); err != nil {
		t.Fatal(err)
	}
	rewindSQLiteFile(t, d.url, kept.Seq(0))
	fresh := mustWriteEventually(t, holder.n, "rq", upsertOp("fresh", 5))
	waitCount(t, holder.n, "rq", fresh, 2) // the copy rebuilt: kept and fresh
	t.Logf("kept %d, lost queries at %d, fresh write at %d; copy on %v, writer %s", kept.Seq(0), lost.Seq(0), fresh.Seq(0), holders, writer.cfg.NodeID)
	// The copy rebuilt on the holder serves again once the writer's routing view shows
	// it: until then a read through the writer is a retryable 503 (no serving copy),
	// as a client would see, and retries.
	eventually(t, 30*time.Second, "a percolation through the writer after the rewind", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := writer.n.Percolate(ctx, "rq", req, api.ReadOptions{})
		if err != nil && !errors.Is(err, ErrNoServingCopy) {
			t.Fatalf("a percolation through the writer after the rewind: %v", err)
		}
		return err
	})
}

// mustWriteEventually retries a write a rewound store refuses (503) for a while.
func mustWriteEventually(t *testing.T, n *Node, index string, ops ...api.WriteOp) *api.Token {
	t.Helper()
	var tok *api.Token
	eventually(t, 30*time.Second, "a write after the rewind", func() error {
		res, err := n.Write(tctx(t), index, ops, api.WriteOptions{})
		if err != nil {
			return err
		}
		for _, it := range res.Items {
			if it.Err != nil {
				return it.Err
			}
		}
		tok = res.Token
		return nil
	})
	return tok
}

// rewindSQLiteFile loses every commit after seq k from the SQLite store at storeURL,
// as a failover that lost them would, all at once.
func rewindSQLiteFile(t *testing.T, storeURL string, k int64) {
	t.Helper()
	u, err := url.Parse(storeURL)
	if err != nil {
		t.Fatal(err)
	}
	path := strings.TrimPrefix(u.Path, "/")
	if _, err := os.Stat(path); err != nil {
		path = u.Path
	}
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	tx, err := raw.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		"DELETE FROM sl_changes WHERE seq > ?",
		"DELETE FROM sl_documents WHERE seq > ?",
		"DELETE FROM sl_queries WHERE seq > ?",
		"UPDATE sl_shard_seq SET value = ? WHERE value > ?",
		"UPDATE sl_counter SET value = ? WHERE id = 1 AND value > ?",
	} {
		args := []any{k}
		if strings.Count(q, "?") == 2 {
			args = append(args, k)
		}
		if _, err := tx.ExecContext(context.Background(), q, args...); err != nil {
			t.Fatalf("rewind %q: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("rewind: %v", err)
	}
}
