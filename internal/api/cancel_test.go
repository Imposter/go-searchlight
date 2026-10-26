package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// slowStore delays every Apply by up to its delay, as a busy database would, and gives
// up when its context ends first (as a cancelled transaction rolls back).
type slowStore struct {
	store.Store
	store.RecordReader
	delay atomic.Int64 // the longest delay, in nanoseconds
}

func (s *slowStore) Apply(ctx context.Context, batch []store.Change) (int64, int64, error) {
	if d := s.delay.Load(); d > 0 {
		t := time.NewTimer(time.Duration(rand.Int64N(d)))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return 0, 0, ctx.Err()
		}
	}
	return s.Store.Apply(ctx, batch)
}

// The property: a _bulk whose client goes away at a random point (while the request is
// read, analyzed, waiting for its group commit, committing, or percolating) leaves
// either every one of its writes committed or none, and the client's retry of the same
// bulk leaves every document written, with each item a noop exactly when the abandoned
// attempt committed. So a client that times out and sends the bulk again gets the
// state it asked for, whatever the server had done.
func TestAbandonedBulkIsAllOrNothing(t *testing.T) {
	slow := &slowStore{}
	e := newEnv(t, envOpts{node: func(o *node.Options) {
		rr, ok := o.Store.(store.RecordReader)
		if !ok {
			t.Fatal("the store reads no records")
		}
		slow.Store, slow.RecordReader = o.Store, rr
		o.Store = slow
	}})
	e.createIndex("c", `{"mapping": {"fields": {"title": "text", "round": "number", "i": "number"}}, "settings": {"shards": 2}}`)
	e.must(http.StatusOK, "PUT", "/indexes/c/queries/q", `{"query": {"field": "title", "op": "words_any", "value": ["chair"]}}`)
	slow.delay.Store(int64(20 * time.Millisecond))
	r := rand.New(rand.NewPCG(3, 5))
	const docs = 120
	ids := make([]string, docs)
	for i := range ids {
		ids[i] = fmt.Sprintf("d%d", i)
	}
	rounds := testtier.Pick(30, 120)
	committedRounds := 0
	for round := 1; round <= rounds; round++ {
		var b strings.Builder
		for i, id := range ids {
			fmt.Fprintf(&b, "{\"upsert\": {\"id\": %q}}\n{\"title\": \"oak chair %d of round %d\", \"round\": %d, \"i\": %d}\n", id, i, round, round, i)
		}
		bulk := b.String()
		path := "/indexes/c/_bulk?percolate=true"
		if r.IntN(2) == 0 {
			path += "&refresh=wait_for"
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int64N(int64(40*time.Millisecond*time.Duration(testtier.RaceSlowdown)))))
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+path, strings.NewReader(bulk))
		if err != nil {
			t.Fatal(err)
		}
		if res, err := e.client.Do(req); err == nil {
			_ = res.Body.Close()
		}
		cancel()
		deadline := time.Now().Add(10 * time.Second * time.Duration(testtier.RaceSlowdown))
		for api.InflightWriteBytes(e.srv) != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: the abandoned bulk is still in progress", round)
			}
			time.Sleep(time.Millisecond)
		}

		rounds := map[float64]int{}
		for s := range 2 {
			if err := scanShard(slow.Store, "c", s, func(body []byte) {
				var d struct{ Round float64 }
				_ = json.Unmarshal(body, &d)
				rounds[d.Round]++
			}); err != nil {
				t.Fatal(err)
			}
		}
		committed := rounds[float64(round)]
		if committed != 0 && committed != docs {
			t.Fatalf("round %d: %d of %d documents of the abandoned bulk committed: %v", round, committed, docs, rounds)
		}
		if committed == docs {
			committedRounds++
		}

		m := e.must(http.StatusOK, "POST", "/indexes/c/_bulk?percolate=true", bulk)
		noops := 0
		for _, it := range bulkItems(t, m) {
			if it["result"] == "noop" {
				noops++
			}
			if q, _ := json.Marshal(it["queries"]); string(q) != `["q"]` {
				t.Fatalf("round %d: item %v percolated to %s", round, it, q)
			}
		}
		if want := map[bool]int{true: docs, false: 0}[committed == docs]; noops != want {
			t.Fatalf("round %d: the retry had %d noops, want %d (the abandoned bulk committed %d)", round, noops, want, committed)
		}
	}
	t.Logf("%d of %d abandoned bulks committed", committedRounds, rounds)
}

// scanShard reads every stored document body of shard s of index from st.
func scanShard(st store.Store, index string, s int, fn func(body []byte)) error {
	_, err := st.ScanShard(context.Background(), store.ShardID{Index: index, Shard: s}, func(r store.Record) error {
		if r.Kind == store.RecordDocument {
			fn(r.Body)
		}
		return nil
	})
	return err
}
