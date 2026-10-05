package shard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Crash and reopen: a crash before, during or after a flush, during a refresh, or
// during a merge commit while refreshes have published generations no flush has
// persisted, leaves a directory that reopens to the last durable manifest, whole, with
// no orphan and no missing file; replaying the changelog from CommittedSeq then brings
// the copy back to the latest state. Each case runs with removals that succeed (POSIX,
// and Windows once nothing maps the file) and with every removal refused while the
// shard runs (Windows, where a reader or a scanner holds files open), the leftovers
// being collected at the reopen.
func TestCrashAtKillPoints(t *testing.T) {
	type tc struct {
		name, point string
		// run is the operation the crash interrupts; with no point, the crash comes
		// after it returns.
		run func(h *harness) error
		// published is whether the crash leaves everything published durable.
		published bool
	}
	refresh := func(h *harness) error {
		h.upsert("d04", "late")
		h.del("d05")
		h.putQuery("q7")
		return h.s.Refresh(context.Background())
	}
	flush := func(h *harness) error { return h.s.Flush(context.Background()) }
	merge := func(h *harness) error { return h.s.ForceMerge(context.Background(), 1) }
	nothing := func(*harness) error { return nil }
	cases := []tc{
		{"before a flush", "", nothing, false},
		{"refresh/" + pointRefreshBuilt, pointRefreshBuilt, refresh, false},
		{"flush/" + pointFlushSynced, pointFlushSynced, flush, false},
		{"flush/" + pointManifestWritten, pointManifestWritten, flush, false},
		{"flush/" + pointManifestRenamed, pointManifestRenamed, flush, true},
		{"after a flush", "", flush, true},
		{"merge/" + pointMergeBuilt, pointMergeBuilt, merge, false},
		{"merge/" + pointFlushSynced, pointFlushSynced, merge, false},
		{"merge/" + pointManifestWritten, pointManifestWritten, merge, false},
		{"merge/" + pointManifestRenamed, pointManifestRenamed, merge, true},
	}
	for _, refused := range []bool{false, true} {
		removals := "removals succeed"
		if refused {
			removals = "removals refused"
		}
		for _, c := range cases {
			t.Run(removals+"/"+c.name, func(t *testing.T) {
				t.Parallel()
				var h *harness
				var armed atomic.Bool
				var syncedMu sync.Mutex
				synced := map[string]bool{}
				opts := testOptions()
				opts.hooks = &testHooks{
					synced: func(p string) {
						syncedMu.Lock()
						synced[filepath.Base(p)] = true
						syncedMu.Unlock()
					},
					at: func(point string) error {
						if point == pointManifestWritten {
							syncedMu.Lock()
							checkManifestSynced(t, filepath.Join(h.dir, manifestName+".tmp"), synced)
							syncedMu.Unlock()
						}
						if point == c.point && armed.CompareAndSwap(true, false) {
							return errSimulatedCrash
						}
						return nil
					},
					remove: func(p string) error {
						if refused {
							return errors.New("the file is being used by another process")
						}
						return os.Remove(p)
					},
				}
				h = newHarness(t, opts)
				// Durable: three segments, with deletes and updates across them, and
				// saved queries.
				for i := range 30 {
					h.upsert(fmt.Sprintf("d%02d", i%17))
					if i%10 == 9 {
						h.del(fmt.Sprintf("d%02d", i%7))
						h.putQuery(fmt.Sprintf("q%d", i%4))
						h.refresh()
					}
				}
				h.flush()
				durable := h.s.CommittedSeq()
				if durable != h.seq {
					t.Fatalf("CommittedSeq %d after a flush, want %d", durable, h.seq)
				}
				// Published, not flushed: updates that mask flushed copies (new
				// deletes on flushed segments), a delete, saved query changes.
				h.upsert("d01", "new")
				h.del("d02")
				h.delQuery("q1")
				h.refresh()
				h.upsert("d03", "d16")
				h.putQuery("q9")
				h.refresh()
				if got := h.s.CommittedSeq(); got != durable {
					t.Fatalf("CommittedSeq %d after refreshes, want %d until a flush", got, durable)
				}
				published := h.seq

				armed.Store(c.point != "")
				err := c.run(h)
				if c.point != "" {
					if !errors.Is(err, errSimulatedCrash) {
						t.Fatalf("%s = %v, want the simulated crash", c.name, err)
					}
					if !errors.Is(h.s.Err(), ErrFailed) {
						t.Fatalf("Err() = %v after a crash", h.s.Err())
					}
					if err := h.s.WaitRefreshed(context.Background(), h.seq+1); !errors.Is(err, ErrFailed) {
						t.Fatalf("WaitRefreshed on a failed shard = %v", err)
					}
				} else if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				h.abandon()

				want := durable
				if c.published {
					want = published
				}
				h.opts.hooks = nil
				h.open()
				if got := h.s.CommittedSeq(); got != want {
					t.Fatalf("reopened at CommittedSeq %d, want %d (durable %d, published %d)", got, want, durable, published)
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
				if got, ref := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, ref) {
					t.Fatalf("files after the replay and a reopen %v, manifest references %v", got, ref)
				}
			})
		}
	}
}

// checkManifestSynced fails t unless every file the manifest at path names is in synced.
func checkManifestSynced(t *testing.T, path string, synced map[string]bool) {
	t.Helper()
	man, err := readManifestFile(path, quietLogger)
	if err != nil {
		t.Errorf("reading the manifest being written: %v", err)
		return
	}
	var names []string
	for _, ms := range man.Segments {
		names = append(names, ms.ID+segment.FileExt)
		if ms.DelGen > 0 {
			names = append(names, deletesName(ms.ID, ms.DelGen))
		}
	}
	for _, ms := range man.QuerySegments {
		names = append(names, ms.ID+defaultQueryExt)
		if ms.DelGen > 0 {
			names = append(names, deletesName(ms.ID, ms.DelGen))
		}
	}
	for _, name := range names {
		if !synced[name] {
			t.Errorf("a manifest names %s, which no flush or merge fsynced", name)
		}
	}
}

// A file outlives every durable manifest that lists it, and no longer: a sidecar a
// refresh supersedes stays until a flush's manifest stops listing it, and a segment
// merged away, flushed or only ever published, goes once the merge's flush has dropped
// it and readers have released it.
func TestFilesOutliveTheirLastDurableManifest(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a", "b", "c")
	h.commit()
	h.upsert("a")
	h.commit()
	first := referencedFiles(t, h.dir)
	h.upsert("b")
	h.refresh()
	if got := dirFiles(t, h.dir); !isSubset(first, got) {
		t.Fatalf("a refresh removed files the durable manifest lists: %v, manifest %v", got, first)
	}
	h.upsert("d")
	h.refresh() // a segment only ever published
	reader := h.s.Acquire()
	h.forceMerge(1)
	h.s.jan.drain()
	for _, name := range first {
		if _, err := os.Stat(filepath.Join(h.dir, name)); err != nil && name != manifestName && strings.HasSuffix(name, segment.FileExt) {
			t.Fatalf("segment %s, which a reader holds, was removed: %v", name, err)
		}
	}
	reader.Release()
	h.s.jan.drain()
	if got, ref := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, ref) {
		t.Fatalf("files after the merge's flush %v, manifest references %v", got, ref)
	}
	h.check()
	h.reopen()
	h.check()
}

func isSubset(sub, of []string) bool {
	for _, s := range sub {
		if !slices.Contains(of, s) {
			return false
		}
	}
	return true
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
