package shard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// Crash and reopen: a crash at any kill point of a refresh or a merge leaves a
// directory that reopens to the old or the new commit, whole, with no orphan files,
// and replaying the changelog from CommittedSeq brings the copy back to the latest
// state: nothing acknowledged and refreshed is lost.
func TestCrashAtKillPoints(t *testing.T) {
	type tc struct {
		op    string // "refresh" or "merge"
		point string
		// newCommit is whether the crash leaves the new commit in place.
		newCommit bool
	}
	var cases []tc
	for _, p := range []string{pointRefreshBuilt, pointCommitSidecars, pointManifestWritten} {
		cases = append(cases, tc{"refresh", p, false})
	}
	cases = append(cases, tc{"refresh", pointManifestRenamed, true})
	for _, p := range []string{pointMergeBuilt, pointCommitSidecars, pointManifestWritten} {
		cases = append(cases, tc{"merge", p, false})
	}
	cases = append(cases, tc{"merge", pointManifestRenamed, true})

	for _, c := range cases {
		t.Run(c.op+"/"+c.point, func(t *testing.T) {
			t.Parallel()
			var armed atomic.Bool
			opts := testOptions()
			opts.hooks = &testHooks{at: func(point string) error {
				if point == c.point && armed.CompareAndSwap(true, false) {
					return errSimulatedCrash
				}
				return nil
			}}
			h := newHarness(t, opts)
			// Three segments, with deletes and updates across them, and saved queries.
			for i := range 30 {
				h.upsert(fmt.Sprintf("d%02d", i%17))
				if i%10 == 9 {
					h.del(fmt.Sprintf("d%02d", i%7))
					h.putQuery(fmt.Sprintf("q%d", i%4))
					h.refresh()
				}
			}
			durable := h.s.CommittedSeq()
			if durable != h.seq {
				t.Fatalf("CommittedSeq %d, want %d", durable, h.seq)
			}

			var err error
			want := durable
			switch c.op {
			case "refresh":
				h.upsert("d01", "new")
				h.del("d02")
				h.delQuery("q1")
				h.putQuery("q9")
				if c.newCommit {
					want = h.seq
				}
				armed.Store(true)
				err = h.s.Refresh(context.Background())
			case "merge":
				// The merge drops this delete; the commit's other kill points are
				// reached whatever it writes.
				h.del("d03")
				h.refresh()
				durable, want = h.seq, h.seq
				armed.Store(true)
				err = h.s.ForceMerge(context.Background(), 1)
			}
			if !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("%s = %v, want the simulated crash", c.op, err)
			}
			if !errors.Is(h.s.Err(), ErrFailed) {
				t.Fatalf("Err() = %v after a crash", h.s.Err())
			}
			h.abandon()

			h.opts.hooks = nil
			h.open()
			if got := h.s.CommittedSeq(); got != want {
				t.Fatalf("reopened at CommittedSeq %d, want %d (durable before: %d)", got, want, durable)
			}
			if got, ref := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, ref) {
				t.Fatalf("files after reopen %v, manifest references %v", got, ref)
			}
			g := h.s.Acquire()
			if err := checkGeneration(g, h.snapshots[want]); err != nil {
				g.Release()
				t.Fatal(err)
			}
			if err := checkQueries(g, h.querySnapshots[want]); err != nil {
				g.Release()
				t.Fatal(err)
			}
			g.Release()

			// The tailer replays the changelog from CommittedSeq.
			h.replayFrom(want)
			h.refresh()
			h.check()
			h.checkQueries()
			h.upsert("after")
			h.refresh()
			h.forceMerge(1)
			h.check()
			h.reopen()
			h.check()
			h.checkQueries()
		})
	}
}

// replayFrom re-applies every logged change after seq, as Task 9's tailer does.
func (h *harness) replayFrom(seq int64) {
	h.t.Helper()
	i := slices.IndexFunc(h.log, func(c Change) bool { return c.Seq > seq })
	if i < 0 {
		return
	}
	if err := h.s.Apply(context.Background(), h.log[i:]); err != nil {
		h.t.Fatalf("replay: %v", err)
	}
}

func (h *harness) putQuery(ids ...string) {
	h.t.Helper()
	var changes []Change
	for _, id := range ids {
		h.seq++
		raw := fmt.Sprintf(`{"all":[{"field":"v","op":"gte","value":%d}]}`, h.seq)
		n, problems := query.Parse([]byte(raw))
		if len(problems) > 0 {
			h.t.Fatal(problems)
		}
		meta := fmt.Sprintf(`{"seq":%d}`, h.seq)
		changes = append(changes, Change{Seq: h.seq, Kind: QueryUpsert, QueryID: id, Query: n, Meta: []byte(meta)})
		h.queries()[id] = raw
	}
	h.apply(changes)
}

func (h *harness) delQuery(ids ...string) {
	h.t.Helper()
	var changes []Change
	for _, id := range ids {
		h.seq++
		changes = append(changes, Change{Seq: h.seq, Kind: QueryDelete, QueryID: id})
		delete(h.queries(), id)
	}
	h.apply(changes)
}

func (h *harness) checkQueries() {
	h.t.Helper()
	g := h.s.Acquire()
	defer g.Release()
	if err := checkQueries(g, h.queries()); err != nil {
		h.t.Fatal(err)
	}
}

// checkQueries verifies g holds exactly want (query id to its JSON).
func checkQueries(g *Generation, want map[string]string) error {
	seen := map[string]bool{}
	for _, qv := range g.QuerySegments {
		for ord := range qv.NumQueries {
			if qv.Deletes.Contains(ord) {
				continue
			}
			q, err := qv.Segment.Query(ord)
			if err != nil {
				return err
			}
			if seen[q.ID] {
				return fmt.Errorf("query %q is live twice", q.ID)
			}
			seen[q.ID] = true
			raw, ok := want[q.ID]
			if !ok {
				return fmt.Errorf("query %q is live but deleted", q.ID)
			}
			got, err := encodeQuery(q.Query)
			if err != nil {
				return err
			}
			if string(got) != raw {
				return fmt.Errorf("query %q is %s, want %s", q.ID, got, raw)
			}
			if wantMeta := fmt.Sprintf(`{"seq":%d}`, q.Seq); string(q.Meta) != wantMeta {
				return fmt.Errorf("query %q meta %s, want %s", q.ID, q.Meta, wantMeta)
			}
		}
	}
	if len(seen) != len(want) || g.NumQueries() != uint64(len(want)) {
		return fmt.Errorf("%d live queries (NumQueries %d), want %d", len(seen), g.NumQueries(), len(want))
	}
	for id := range want {
		if _, _, ok := g.LookupQuery(id); !ok {
			return fmt.Errorf("LookupQuery(%q) found nothing", id)
		}
	}
	return nil
}
