package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// stressFor is how long the stress tests write: a second in the short tier.
func stressFor() time.Duration { return testtier.Pick(time.Second, 4*time.Second) }

// TestOrderingUnderMixedLoad: lane committers on three nodes, direct multi-index
// Applies, conditional skips and noops, remaps and prunes, with a tailer per shard
// that checks, at every poll, that the changes after its position start at the next
// seq, are contiguous, cover the head it read just before, and carry the mapping
// version the log holds. At the end every seq a writer was given is in the log exactly
// once. (From the M1 review.)
func TestOrderingUnderMixedLoad(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		const shards = 4
		var nodes []Store
		var gcs []*GroupCommitter
		for range 3 {
			st := h.open(t)
			nodes = append(nodes, st)
			gc := NewGroupCommitter(st, GroupCommitOptions{})
			t.Cleanup(func() { _ = gc.Close() })
			gcs = append(gcs, gc)
		}
		if _, err := nodes[0].Indexes().Create(ctx, IndexMeta{Name: "st", Mapping: []byte(`{"fields":{}}`), Settings: fmt.Appendf(nil, `{"shards":%d}`, shards)}); err != nil {
			t.Fatal(err)
		}
		mustCreateShards(t, nodes[0], "su", 2)
		for sh := range shards {
			v := upsert("st", sh, "v", `{}`)
			v.Version = 100
			mustApply(t, nodes[0], v)
		}

		var mu sync.Mutex
		given := map[ShardID]map[int64]string{}
		note := func(batch []Change) error {
			mu.Lock()
			defer mu.Unlock()
			for _, c := range batch {
				if c.Seq == 0 {
					continue
				}
				m := given[c.ShardID()]
				if m == nil {
					m = map[int64]string{}
					given[c.ShardID()] = m
				}
				if prev, ok := m[c.Seq]; ok {
					return fmt.Errorf("%s seq %d given to %s and %s", c.ShardID(), c.Seq, prev, c.ID)
				}
				m[c.Seq] = c.ID
			}
			return nil
		}
		for sh := range shards {
			for _, c := range allChanges(t, nodes[0], ShardID{Index: "st", Shard: sh}) {
				_ = note([]Change{c})
			}
		}

		stop := make(chan struct{})
		errs := make(chan error, 64)
		fail := func(err error) {
			select {
			case errs <- err:
			default:
			}
		}
		var tailPos [shards]atomic.Int64
		var wg sync.WaitGroup

		// Tailers.
		tailer := h.open(t)
		tailDone := make(chan struct{})
		var tailWG sync.WaitGroup
		seen := make([]map[int64]bool, shards)
		for sh := range shards {
			seen[sh] = map[int64]bool{}
			tailWG.Go(func() {
				id := ShardID{Index: "st", Shard: sh}
				after, version := int64(0), int64(1)
				for {
					select {
					case <-tailDone:
						return
					default:
					}
					head, _, err := tailer.HeadSeq(ctx, id)
					if err != nil {
						fail(fmt.Errorf("tail %s head: %w", id, err))
						return
					}
					page, err := tailer.ChangesAfter(ctx, id, after, 50)
					if err != nil {
						fail(fmt.Errorf("tail %s after %d: %w", id, after, err))
						return
					}
					for _, c := range page {
						if c.Seq != after+1 {
							fail(fmt.Errorf("tail %s: seq %d right after %d", id, c.Seq, after))
							return
						}
						if c.Kind == KindMapping {
							if c.MappingVersion != version+1 {
								fail(fmt.Errorf("tail %s: mapping %d after %d", id, c.MappingVersion, version))
								return
							}
							version = c.MappingVersion
						} else if c.MappingVersion != version {
							fail(fmt.Errorf("tail %s: seq %d carries mapping version %d, log at %d", id, c.Seq, c.MappingVersion, version))
							return
						}
						seen[sh][c.Seq] = true
						after = c.Seq
					}
					if len(page) < 50 && after < head {
						fail(fmt.Errorf("tail %s: head %d read before the changes, which end at %d", id, head, after))
						return
					}
					tailPos[sh].Store(after)
				}
			})
		}

		// Lane committers across every shard, with skips and noops.
		for w := range 9 {
			wg.Go(func() {
				gc := gcs[w%3]
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					var batch []Change
					for sh := range shards {
						if (sh+i+w)%3 == 0 {
							continue
						}
						batch = append(batch, upsert("st", sh, fmt.Sprintf("g%d-%d-%d", w, i, sh), `{}`))
						if i%2 == 0 {
							d := del("st", sh, fmt.Sprintf("missing-%d-%d", w, i))
							d.IfSeq = IfExists
							batch = append(batch, d)
						}
						if i%3 == 0 {
							v := upsert("st", sh, "v", `{}`)
							v.Version = 5
							batch = append(batch, v)
						}
					}
					if len(batch) == 0 {
						continue
					}
					cctx, cancel := context.WithTimeout(ctx, time.Minute)
					err := gc.Apply(cctx, batch)
					cancel()
					if err != nil {
						fail(fmt.Errorf("gc writer %d: %w", w, err))
						return
					}
					if err := note(batch); err != nil {
						fail(err)
						return
					}
				}
			})
		}
		// Direct multi-index Applies, in reverse key order in the batch.
		for w := range 3 {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					batch := []Change{
						upsert("su", i%2, fmt.Sprintf("d%d-%d", w, i), `{}`),
						upsert("st", (i+w)%shards, fmt.Sprintf("d%d-%d", w, i), `{}`),
						upsert("st", (i+w+1)%shards, fmt.Sprintf("e%d-%d", w, i), `{}`),
					}
					cctx, cancel := context.WithTimeout(ctx, time.Minute)
					err := nodes[w].Apply(cctx, batch)
					cancel()
					if err != nil {
						fail(fmt.Errorf("direct writer %d: %w", w, err))
						return
					}
					if err := note(batch); err != nil {
						fail(err)
						return
					}
				}
			})
		}
		// Remaps.
		remaps := 0
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(40 * time.Millisecond):
				}
				m, err := nodes[i%3].Indexes().Get(ctx, "st")
				if err != nil {
					fail(err)
					return
				}
				m.Mapping = fmt.Appendf(nil, `{"fields":{"f%d":"keyword"}}`, i)
				if _, err := nodes[i%3].Indexes().Update(ctx, m); err != nil && !errors.Is(err, ErrConflict) {
					fail(fmt.Errorf("remap: %w", err))
					return
				}
				remaps++
			}
		})
		// Prunes below what every tailer has passed (and settings-only updates).
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(25 * time.Millisecond):
				}
				sh := i % shards
				below := tailPos[sh].Load() - 5
				if below > 1 {
					if err := nodes[1].Prune(ctx, ShardID{Index: "st", Shard: sh}, below); err != nil {
						fail(fmt.Errorf("prune: %w", err))
						return
					}
				}
			}
		})

		time.Sleep(stressFor())
		close(stop)
		wg.Wait()
		// Let the tailers catch up.
		deadline := time.Now().Add(30 * time.Second)
		for sh := 0; sh < shards; sh++ {
			want := headSeq(t, nodes[0], ShardID{Index: "st", Shard: sh})
			for tailPos[sh].Load() < want && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
		}
		close(tailDone)
		tailWG.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if t.Failed() {
			return
		}
		total := 0
		for sh := range shards {
			id := ShardID{Index: "st", Shard: sh}
			head := headSeq(t, nodes[0], id)
			if tailPos[sh].Load() != head {
				t.Errorf("%s: tailer at %d, head %d", id, tailPos[sh].Load(), head)
			}
			for seq := range given[id] {
				if !seen[sh][seq] && seq > 0 {
					// Seeds before the tailer started at 0 are seen too.
					t.Errorf("%s: seq %d given to %s never seen by the tailer", id, seq, given[id][seq])
				}
			}
			total += len(seen[sh])
		}
		t.Logf("%s: %d changes tailed over %d shards, %d remaps", h.dialect, total, shards, remaps)
	})
}

// TestDropRecreateUnderLoad: writers on every shard through lane committers while the
// index is dropped and recreated; a tailer per shard checks that a new incarnation's
// first seq is above every seq it saw of the old one, and never sees a seq twice.
// (From the M1 review.)
func TestDropRecreateUnderLoad(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		const shards = 3
		var nodes []Store
		var gcs []*GroupCommitter
		for range 2 {
			st := h.open(t)
			nodes = append(nodes, st)
			gc := NewGroupCommitter(st, GroupCommitOptions{})
			t.Cleanup(func() { _ = gc.Close() })
			gcs = append(gcs, gc)
		}
		mustCreateShards(t, nodes[0], "dr", shards)
		stop := make(chan struct{})
		errs := make(chan error, 64)
		fail := func(err error) {
			select {
			case errs <- err:
			default:
			}
		}
		var wg, tailWG sync.WaitGroup
		tailer := h.open(t)
		tailDone := make(chan struct{})
		for sh := range shards {
			tailWG.Go(func() {
				id := ShardID{Index: "dr", Shard: sh}
				after := int64(0)
				uid := ""
				for {
					select {
					case <-tailDone:
						return
					default:
					}
					page, err := tailer.ChangesAfter(ctx, id, after, 50)
					if err != nil {
						fail(fmt.Errorf("tail %s: %w", id, err))
						return
					}
					for _, c := range page {
						switch {
						case c.IndexUID != uid:
							if c.Seq <= after {
								fail(fmt.Errorf("tail %s: incarnation %s starts at %d, at or below %d", id, c.IndexUID, c.Seq, after))
								return
							}
							uid = c.IndexUID
						case c.Seq != after+1:
							fail(fmt.Errorf("tail %s: seq %d right after %d within one incarnation", id, c.Seq, after))
							return
						}
						after = c.Seq
					}
				}
			})
		}
		for w := range 6 {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					batch := make([]Change, shards)
					for sh := range shards {
						batch[sh] = upsert("dr", sh, fmt.Sprintf("w%d-%d", w, i), `{}`)
					}
					cctx, cancel := context.WithTimeout(ctx, time.Minute)
					err := gcs[w%2].Apply(cctx, batch)
					cancel()
					var pe *PartialError
					switch {
					case err == nil, errors.Is(err, ErrNotFound):
					case errors.As(err, &pe):
						for _, f := range pe.Failed {
							if !errors.Is(f.Err, ErrNotFound) {
								fail(fmt.Errorf("writer %d: shard %s: %w", w, f.Shard, f.Err))
								return
							}
						}
					default:
						fail(fmt.Errorf("writer %d: %w", w, err))
						return
					}
				}
			})
		}
		for r := range 6 {
			time.Sleep(150 * time.Millisecond)
			if err := nodes[r%2].Indexes().Drop(ctx, "dr"); err != nil {
				t.Fatalf("drop %d: %v", r, err)
			}
			if _, err := nodes[(r+1)%2].Indexes().Create(ctx, IndexMeta{Name: "dr", Settings: fmt.Appendf(nil, `{"shards":%d}`, shards)}); err != nil {
				t.Fatalf("create %d: %v", r, err)
			}
		}
		time.Sleep(150 * time.Millisecond)
		close(stop)
		wg.Wait()
		time.Sleep(500 * time.Millisecond)
		close(tailDone)
		tailWG.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	})
}
