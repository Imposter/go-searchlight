package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// TestSplitWrites drives the multi-statement write paths on a real server:
// with the dialect's limits shrunk, a mixed batch over several shards
// (document and query upserts, rewrites, deletes, an upsert then delete of
// one key and the reverse) and a mapping change are written in many
// statements of one transaction. On Postgres that is writeSQL in parts
// (deletes in the first, notifications in the last, the counter in every
// one); on MySQL, VALUES and IN lists cut by placeholders and bytes. SQLite
// writes one row a statement whatever the limits.
func TestSplitWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "sp", Settings: []byte(`{"shards":3}`)}); err != nil {
			t.Fatal(err)
		}
		qp := func(i int) []byte {
			p, err := EncodeQueryPayload(json.RawMessage(fmt.Sprintf(`{"eq":{"n":%d}}`, i)), json.RawMessage(`{"pad":"`+strings.Repeat("m", 40)+`"}`))
			if err != nil {
				t.Fatal(err)
			}
			return p
		}
		body := func(i int) string { return fmt.Sprintf(`{"v":%d,"pad":%q}`, i, strings.Repeat("x", 50)) }
		model := make(map[string]string) // "d:id" or "q:id" -> body (documents) or query DSL
		apply := func(batch []Change) (int64, int64) {
			t.Helper()
			first, last := mustApply(t, st, batch...)
			for i, c := range batch {
				if c.Seq != first+int64(i) {
					t.Fatalf("change %d has seq %d, want %d", i, c.Seq, first+int64(i))
				}
				switch c.Kind {
				case KindUpsert:
					model[fmt.Sprintf("%d/d:%s", c.Shard, c.ID)] = string(c.Payload)
				case KindQueryUpsert:
					q, err := DecodeQueryPayload(c.Payload)
					if err != nil {
						t.Fatal(err)
					}
					model[fmt.Sprintf("%d/q:%s", c.Shard, c.ID)] = string(q.Query)
				case KindDelete:
					delete(model, fmt.Sprintf("%d/d:%s", c.Shard, c.ID))
				case KindQueryDelete:
					delete(model, fmt.Sprintf("%d/q:%s", c.Shard, c.ID))
				}
			}
			return first, last
		}

		// Records to delete, written with the normal limits.
		var pre []Change
		for i := range 30 {
			pre = append(pre, upsert("sp", i%3, fmt.Sprint("p", i), body(i)))
		}
		for i := range 10 {
			pre = append(pre, Change{Index: "sp", Shard: i % 3, Kind: KindQueryUpsert, ID: fmt.Sprint("pq", i), Payload: qp(i)})
		}
		apply(pre)

		s := engine(st)
		s.d.Changelog.Limits = dialect.Limits{Params: 40, Bytes: 400}
		write := s.d.Changelog.Write
		statements := 0
		s.d.Changelog.Write = func(w *dialect.Write, l dialect.Limits) []dialect.Stmt {
			out := write(w, l)
			statements = len(out)
			return out
		}

		// On Postgres, watch for the commit's notifications.
		got := make(chan Notification, 64)
		if w, ok := st.(Watcher); ok {
			wctx, cancel := context.WithCancel(ctx)
			defer cancel()
			ready := make(chan struct{})
			go func() { _ = w.Watch(wctx, func() { close(ready) }, func(n Notification) { got <- n }) }()
			select {
			case <-ready:
			case <-time.After(10 * time.Second):
				t.Fatal("watch never became ready")
			}
		}

		var batch []Change
		for i := range 60 {
			batch = append(batch, upsert("sp", i%3, fmt.Sprint("d", i), body(i)))
		}
		for i := range 12 {
			batch = append(batch, Change{Index: "sp", Shard: i % 3, Kind: KindQueryUpsert, ID: fmt.Sprint("q", i), Payload: qp(100 + i)})
		}
		for i := range 20 {
			batch = append(batch, del("sp", i%3, fmt.Sprint("p", i)))
		}
		for i := range 5 {
			batch = append(batch, Change{Index: "sp", Shard: i % 3, Kind: KindQueryDelete, ID: fmt.Sprint("pq", i)})
		}
		batch = append(batch,
			upsert("sp", 1, "d1", body(1001)),                       // a rewrite: the last change wins
			del("sp", 1, "d4"),                                      // upserted above, then deleted
			upsert("sp", 2, "p20", body(2020)),                      // a preloaded document rewritten
			del("sp", 0, "never"),                                   // a delete of nothing
			del("sp", 2, "p23"), upsert("sp", 2, "p23", body(2023)), // deleted, then upserted again
		)
		first, last := apply(batch)

		if h.dialect != "sqlite" && statements < 5 {
			t.Fatalf("the batch was written in %d statements; the limits did not split it", statements)
		}
		if got := counterValue(t, st); got != last {
			t.Fatalf("counter %d after a batch ending at %d", got, last)
		}
		if head, _, err := st.HeadSeq(ctx); err != nil || head != last {
			t.Fatalf("head %d %v, want %d", head, err, last)
		}
		top := make(map[int]int64)
		for i, c := range batch {
			top[c.Shard] = first + int64(i)
		}
		for sh := range 3 {
			shard := ShardID{Index: "sp", Shard: sh}
			recs, asOf := scanAll(t, st, shard)
			if asOf != last {
				t.Fatalf("shard %d scanned as of %d, want %d", sh, asOf, last)
			}
			want := 0
			for k, v := range model {
				if !strings.HasPrefix(k, fmt.Sprintf("%d/", sh)) {
					continue
				}
				want++
				r, ok := recs[strings.TrimPrefix(k, fmt.Sprintf("%d/", sh))]
				if !ok || string(r.Body) != v {
					t.Errorf("shard %d %s = %q (%v), want %q", sh, k, r.Body, ok, v)
				}
			}
			if len(recs) != want {
				t.Errorf("shard %d has %d records, want %d", sh, len(recs), want)
			}
			changes := allChanges(t, st, shard)
			n := 0
			for _, c := range append(append([]Change{}, pre...), batch...) {
				if c.Shard == sh {
					n++
				}
			}
			if len(changes) != n {
				t.Errorf("shard %d has %d changes, want %d", sh, len(changes), n)
			}
		}
		if _, ok := st.(Watcher); ok {
			seen := make(map[int]bool)
			deadline := time.After(10 * time.Second)
			for len(seen) < 3 {
				select {
				case n := <-got:
					if n.Shard.Index == "sp" && n.Seq == top[n.Shard.Shard] {
						seen[n.Shard.Shard] = true
					}
				case <-deadline:
					t.Fatalf("notifications for shards %v of 3, want each shard's top seq %v", seen, top)
				}
			}
		}

		// A mapping change under the same limits logs one change per shard,
		// contiguous, and moves the counter.
		meta, err := st.Indexes().Get(ctx, "sp")
		if err != nil {
			t.Fatal(err)
		}
		meta.Mapping = []byte(`{"properties":{"v":{"type":"long"}}}`)
		if _, err := st.Indexes().Update(ctx, meta); err != nil {
			t.Fatal(err)
		}
		if got := counterValue(t, st); got != last+3 {
			t.Fatalf("counter %d after the mapping change, want %d", got, last+3)
		}
		for sh := range 3 {
			changes := allChanges(t, st, ShardID{Index: "sp", Shard: sh})
			if c := changes[len(changes)-1]; c.Kind != KindMapping || c.Seq != last+1+int64(sh) {
				t.Fatalf("shard %d ends with %+v", sh, c)
			}
		}
		if f, _ := mustApply(t, st, upsert("sp", 0, "after", `{}`)); f != last+4 {
			t.Fatalf("the next change took seq %d, want %d", f, last+4)
		}
	})
}
