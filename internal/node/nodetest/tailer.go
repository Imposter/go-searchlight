// Package nodetest holds test doubles for package node: a minimal changelog tailer
// that tests can pause, where the replica tailer cannot be.
package nodetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// PollInterval is how often a Tailer polls when nothing wakes it.
const PollInterval = 20 * time.Millisecond

// pageSize is the changes read at a time.
const pageSize = 1000

// ErrHalted is a change the tailer cannot apply: Run returns it, wrapped.
var ErrHalted = errors.New("nodetest: the copy halted")

// Tailer is a minimal changelog tailer: it replays the shard's changes after the
// shard's applied seq, analyzing documents under the stored mapping and parsing saved
// queries, then advances the copy to the node's head. Wake runs a catch-up right
// away; CatchUp runs one synchronously. It never recovers or rebuilds a copy, so it
// suits a single node whose changelog is never pruned.
type Tailer struct {
	st      store.Store
	sh      *shard.Shard
	id      store.ShardID
	head    func() int64
	wake    chan struct{}
	applied atomic.Int64
	paused  atomic.Bool
	mu      sync.Mutex // one catch-up at a time
}

var _ node.Tailer = (*Tailer)(nil)

// NewTailer is a [node.NewTailerFunc].
func NewTailer(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
	t := &Tailer{st: st, sh: sh, id: id, head: env.Head, wake: make(chan struct{}, 1)}
	if t.head == nil {
		t.head = func() int64 { return 0 }
	}
	t.applied.Store(sh.AppliedSeq())
	return t
}

// Shard implements [node.Tailer].
func (t *Tailer) Shard() *shard.Shard { return t.sh }

// Applied implements [node.Tailer].
func (t *Tailer) Applied() int64 { return t.applied.Load() }

// Wake implements [node.Tailer].
func (t *Tailer) Wake() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// Pause stops the tailer applying changes until Resume (tests of a copy that lags). It
// blocks until a catch-up already under way (started before the flag was visible to
// it) finishes, so no write committed after Pause returns can ever be read by one that
// was already looping against the live store: CatchUp itself re-checks the flag, under
// the same lock, before doing anything.
func (t *Tailer) Pause() {
	t.paused.Store(true)
	t.mu.Lock()
	t.mu.Unlock() //nolint:gocritic,staticcheck // an empty critical section on purpose
}

// Resume undoes Pause and wakes the tailer.
func (t *Tailer) Resume() {
	t.paused.Store(false)
	t.Wake()
}

// Run implements [node.Tailer]: it catches up at every wake and poll until ctx ends.
// A change it cannot apply halts it (the error wraps ErrHalted); store errors are
// retried at the next poll.
func (t *Tailer) Run(ctx context.Context) error {
	tick := time.NewTicker(PollInterval)
	defer tick.Stop()
	for {
		if !t.paused.Load() {
			if err := t.CatchUp(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				if errors.Is(err, ErrHalted) {
					return err
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.wake:
		case <-tick.C:
		}
	}
}

// CatchUp applies every committed change of the shard, then advances the copy to the
// head read before it started (every change at or below it was committed then, so
// none is skipped).
func (t *Tailer) CatchUp(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.paused.Load() {
		return nil
	}
	head := t.head()
	for {
		changes, err := t.st.ChangesAfter(ctx, t.id, t.sh.AppliedSeq(), pageSize)
		if err != nil {
			return err
		}
		if len(changes) > 0 {
			if err := t.apply(ctx, changes); err != nil {
				return err
			}
			// Past the page, mapping changes (applied as nothing) included.
			if err := t.sh.Advance(changes[len(changes)-1].Seq); err != nil {
				return err
			}
		}
		if len(changes) < pageSize {
			break
		}
	}
	if head > t.sh.AppliedSeq() {
		if err := t.sh.Advance(head); err != nil {
			return err
		}
	}
	t.applied.Store(t.sh.AppliedSeq())
	return nil
}

// apply converts changes under the stored mapping and applies them, waiting out
// backpressure with a refresh.
func (t *Tailer) apply(ctx context.Context, changes []store.Change) error {
	meta, err := t.st.Indexes().Get(ctx, t.id.Index)
	if err != nil {
		return err
	}
	m := &schema.Mapping{}
	if err := json.Unmarshal(meta.Mapping, m); err != nil {
		return fmt.Errorf("%w: mapping: %w", ErrHalted, err)
	}
	t.sh.SetMapping(m)
	out := make([]shard.Change, 0, len(changes))
	for i := range changes {
		c := &changes[i]
		sc := shard.Change{Seq: c.Seq, IndexUID: c.IndexUID}
		switch c.Kind {
		case store.KindMapping:
			// The catalogue's mapping, read above, already holds it: documents are
			// analyzed under the newest mapping, which this stand-in allows itself
			// (the replica tailer adopts each mapping at its seq).
			continue
		case store.KindUpsert:
			doc, _, err := schema.Analyze(m, c.ID, c.Payload)
			if err != nil {
				return fmt.Errorf("%w: seq %d: %w", ErrHalted, c.Seq, err)
			}
			sc.Kind, sc.Doc = shard.Upsert, &doc
		case store.KindDelete:
			sc.Kind, sc.DocID = shard.Delete, c.ID
		case store.KindQueryUpsert:
			p, err := store.DecodeQueryPayload(c.Payload)
			if err != nil {
				return fmt.Errorf("%w: seq %d: %w", ErrHalted, c.Seq, err)
			}
			n, problems := query.Parse(p.Query)
			if len(problems) > 0 {
				return fmt.Errorf("%w: seq %d: %s", ErrHalted, c.Seq, problems[0])
			}
			meta := []byte(p.Meta)
			if len(meta) == 0 {
				meta = []byte("{}")
			}
			sc.Kind, sc.QueryID, sc.Query, sc.Meta = shard.QueryUpsert, c.ID, n, meta
		case store.KindQueryDelete:
			sc.Kind, sc.QueryID = shard.QueryDelete, c.ID
		default:
			return fmt.Errorf("%w: seq %d: kind %q", ErrHalted, c.Seq, c.Kind)
		}
		out = append(out, sc)
	}
	if len(out) == 0 {
		return nil // only mapping changes: CatchUp advances past them
	}
	for {
		err := t.sh.Apply(ctx, out)
		if !errors.Is(err, shard.ErrBackpressure) {
			if err != nil {
				return fmt.Errorf("%w: %w", ErrHalted, err)
			}
			return nil
		}
		if err := t.sh.Refresh(ctx); err != nil {
			return err
		}
	}
}
