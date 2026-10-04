package cluster

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// Maintenance: the live node with the lowest id (the leader) prunes the changelog and
// sweeps abandoned blob uploads, and every node collects its own unused copy
// directories and recovery staging. Leadership needs no election: every node reads the
// same registry, and the leader's jobs are idempotent and safe when two nodes briefly
// both believe they lead. The store only executes Prune; the policy lives here.
//
// # Pruning (spec section 9)
//
// A shard's changelog is pruned below its floor: the lowest applied seq (the durable
// CommittedSeq each copy reports) over the copies that count:
//
//   - every live copy: held under an unexpired lease by a live node, whatever its
//     state. Recovery points are covered by the same rule: a copy being recovered
//     holds its slot from the claim on, at applied seq 0 (or, rebuilt aside, at the
//     seq its old copy reached, below the snapshot it fetches) until its recovered copy
//     reports its seq; and the snapshot a peer serves is at or past the seq that peer's
//     copy reports.
//   - a cleanly stopped node's copies: Stop leaves their rows retiring at their final
//     applied seqs and lets the leases run out, and such a row counts for
//     RetiringRetention (15 min) after its lease ran out, by the database's clock, so
//     a node restarted within it replays the tail of the changelog rather than
//     rebuilding (spec section 9).
//
// Bounded, so no copy holds the floor forever:
//
//   - Stalls: a copy behind its shard's most advanced one that makes no progress for
//     PruneStallTimeout (15 min) stops counting. Progress is its registry applied seq
//     together with the progress counter its node reports over the peer API (applied,
//     loaded and fetched work), keyed by (shard, node, epoch): halted copies, copies
//     stuck recovering and copies whose node went silent fall under the one rule. A
//     copy that moves again counts again at once; one pruned past rebuilds (its next
//     read says store.ErrPruned), from a peer or the store, neither of which needs the
//     pruned changes.
//   - Age: a change older than ChangelogRetention (24 h) is pruned whatever copy still
//     needs it; that copy rebuilds.
//
// A shard with no copy that counts is pruned by age alone.

// pruneState is the leader's memory of each copy's progress, and of how far it has
// pruned each shard.
type pruneState struct {
	mu       sync.Mutex
	progress map[copyKey]progressMark
	below    map[store.ShardID]int64
}

type copyKey struct {
	shard store.ShardID
	node  string
	epoch int64
}

type progressMark struct {
	applied, progress int64
	since             time.Time
	seen              bool
}

// leader reports whether this node is the live node with the lowest id.
func (n *Node) leader() bool {
	live := n.view.Load().liveNodes()
	return len(live) > 0 && live[0] == n.id
}

// maintenanceLoop prunes every PruneInterval and sweeps every SweepInterval while this
// node leads, and collects unused directories every GCInterval.
func (n *Node) maintenanceLoop(ctx context.Context) {
	prune := time.NewTicker(n.opts.PruneInterval)
	defer prune.Stop()
	sweep := time.NewTicker(n.opts.SweepInterval)
	defer sweep.Stop()
	gc := time.NewTicker(n.opts.GCInterval)
	defer gc.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-prune.C:
			if n.leader() {
				if err := n.pruneAll(ctx); err != nil && ctx.Err() == nil {
					n.log.WarnContext(ctx, "pruning the changelog failed", slog.Any("error", err))
				}
			}
		case <-sweep.C:
			if n.leader() {
				removed, err := n.st.Blobs().Sweep(ctx, n.opts.SweepAge)
				switch {
				case err != nil && ctx.Err() == nil:
					n.log.WarnContext(ctx, "sweeping abandoned blob uploads failed", slog.Any("error", err))
				case removed > 0:
					n.log.InfoContext(ctx, "swept abandoned blob uploads", slog.Int("removed", removed))
				}
			}
		case <-gc.C:
			n.collectUnused(ctx)
		}
	}
}

// pruneAll prunes every shard's changelog below its floor.
func (n *Node) pruneAll(ctx context.Context) error {
	if err := n.refreshView(ctx); err != nil {
		return err
	}
	v := n.view.Load()
	n.releaseDecommissioned(ctx, v)
	progress := n.copyProgress(ctx, v)
	now := time.Now()
	cutoff := now.Add(-n.opts.ChangelogRetention)
	for _, iv := range n.Indexes() {
		for s := range iv.Shards {
			id := store.ShardID{Index: iv.Name, Shard: s}
			floor, ok := n.pruneFloor(v.copies[id], v, progress, now)
			age, err := n.ageFloor(ctx, id, floor, cutoff)
			if err != nil {
				return err
			}
			if !ok || age > floor {
				floor = age
			}
			if floor <= 0 || floor+1 <= n.prune.prunedBelow(id) {
				continue
			}
			if err := n.st.Prune(ctx, id, floor+1); err != nil {
				return err
			}
			n.prune.setBelow(id, floor+1)
		}
	}
	n.prune.forget(v)
	return nil
}

func (p *pruneState) forget(v *view) {
	p.mu.Lock()
	defer p.mu.Unlock()
	present := map[copyKey]bool{}
	for id, list := range v.copies {
		for i := range list {
			present[copyKey{shard: id, node: list[i].NodeID, epoch: list[i].Epoch}] = true
		}
	}
	for k := range p.progress {
		if !present[k] {
			delete(p.progress, k)
		}
	}
	for id := range p.below {
		if _, ok := v.copies[id]; !ok {
			delete(p.below, id)
		}
	}
}

// releaseDecommissioned deletes the retiring rows of nodes gone from sl_nodes (they
// stopped cleanly and never came back) once their leases ran out more than
// RetiringRetention ago, by the database's clock: they no longer hold the prune floor,
// and nothing else needs them.
func (n *Node) releaseDecommissioned(ctx context.Context, v *view) {
	for _, list := range v.copies {
		for i := range list {
			c := &list[i]
			if _, registered := v.nodes[c.NodeID]; registered || c.State != store.CopyRetiring || c.LeaseLeft > -n.opts.RetiringRetention {
				continue
			}
			if err := n.reg.ReleaseCopy(ctx, *c); err != nil && !errors.Is(err, store.ErrLeaseLost) {
				n.log.WarnContext(ctx, "releasing a decommissioned node's copy failed", slog.String("shard", c.Shard.String()),
					slog.String("node", c.NodeID), slog.Any("error", err))
				continue
			}
			n.inst.lease(ctx, "release")
			n.log.InfoContext(ctx, "released a decommissioned node's copy", slog.String("shard", c.Shard.String()), slog.String("node", c.NodeID))
		}
	}
}

// pruneFloor is the seq a shard's changelog may be pruned up to (inclusive) on account
// of its copies: the lowest applied seq of the copies that count (see above). ok is
// false when none counts.
func (n *Node) pruneFloor(copies []store.Copy, v *view, progress map[copyKey]int64, now time.Time) (int64, bool) {
	type counted struct {
		c        *store.Copy
		retiring bool
	}
	var list []counted
	var top int64
	for i := range copies {
		c := &copies[i]
		retired := c.State == store.CopyRetiring && c.LeaseLeft > -n.opts.RetiringRetention
		if !v.usable(c) && !retired {
			continue
		}
		list = append(list, counted{c: c, retiring: retired && !v.usable(c)})
		top = max(top, c.AppliedSeq)
	}
	floor, ok := int64(0), false
	for _, e := range list {
		c := e.c
		k := copyKey{shard: c.Shard, node: c.NodeID, epoch: c.Epoch}
		still := n.prune.observe(k, c.AppliedSeq, progress[k], now)
		if !e.retiring && c.AppliedSeq < top && still >= n.opts.PruneStallTimeout {
			continue // stalled behind the others: pruning goes on without it
		}
		if !ok || c.AppliedSeq < floor {
			floor, ok = c.AppliedSeq, true
		}
	}
	return floor, ok
}

// ageFloor is the newest seq of id's changelog older than cutoff (0 when the change
// after from is not): changes up to it are pruned whatever copy needs them.
func (n *Node) ageFloor(ctx context.Context, id store.ShardID, from int64, cutoff time.Time) (int64, error) {
	lo := max(from, n.prune.prunedBelow(id)-1, 0)
	first, ok, err := n.firstAfter(ctx, id, lo)
	if errors.Is(err, store.ErrPruned) {
		// Pruned past lo by an earlier leader: find where the log starts.
		head, _, herr := n.st.HeadSeq(ctx)
		if herr != nil {
			return 0, herr
		}
		a, b := lo, head
		for a < b { // the smallest x the log still holds the changes after
			mid := a + (b-a)/2
			_, _, err := n.firstAfter(ctx, id, mid)
			switch {
			case errors.Is(err, store.ErrPruned):
				a = mid + 1
			case err != nil:
				return 0, err
			default:
				b = mid
			}
		}
		lo = a
		first, ok, err = n.firstAfter(ctx, id, lo)
	}
	if err != nil || !ok || !first.At.Before(cutoff) {
		return 0, err
	}
	// first (after lo) is older than cutoff: find the largest x whose first change
	// after it is still older; that change, x+1, is the newest older than cutoff
	// (commit order is seq order, so the changes' times rise with their seqs).
	head, _, err := n.st.HeadSeq(ctx)
	if err != nil {
		return 0, err
	}
	a, b := lo, head
	for a < b {
		mid := a + (b-a+1)/2
		c, ok, err := n.firstAfter(ctx, id, mid)
		if err != nil {
			return 0, err
		}
		if ok && c.At.Before(cutoff) {
			a = mid
		} else {
			b = mid - 1
		}
	}
	c, ok, err := n.firstAfter(ctx, id, a)
	if err != nil || !ok {
		return 0, err
	}
	return c.Seq, nil
}

// firstAfter is id's first change after seq.
func (n *Node) firstAfter(ctx context.Context, id store.ShardID, seq int64) (store.Change, bool, error) {
	changes, err := n.st.ChangesAfter(ctx, id, seq, 1)
	if err != nil || len(changes) == 0 {
		return store.Change{}, false, err
	}
	return changes[0], true, nil
}

// copyProgress asks every live node for its copies' progress counters: this node's
// directly, the others over the peer API. A node that does not answer reports none
// (its copies' applied seqs alone then show their progress).
func (n *Node) copyProgress(ctx context.Context, v *view) map[copyKey]int64 {
	out := map[copyKey]int64{}
	add := func(node string, list []peerCopy) {
		for i := range list {
			c := &list[i]
			out[copyKey{shard: store.ShardID{Index: c.Index, Shard: c.Shard}, node: node, epoch: c.Epoch}] = c.Progress
		}
	}
	add(n.id, n.localPeerCopies())
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range v.liveNodes() {
		nd, ok := v.nodes[id]
		if id == n.id || !ok || nd.Address == "" {
			continue
		}
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			var reply copiesReply
			if err := n.call(cctx, id, nd.Address, http.MethodGet, peerPrefix+"copies", nil, &reply); err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			add(id, reply.Copies)
		})
	}
	wg.Wait()
	return out
}

// observe records a copy's applied seq and progress counter and returns how long
// neither has moved.
func (p *pruneState) observe(k copyKey, applied, progress int64, now time.Time) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.progress == nil {
		p.progress = map[copyKey]progressMark{}
	}
	prev := p.progress[k]
	if !prev.seen || prev.applied != applied || prev.progress != progress {
		p.progress[k] = progressMark{applied: applied, progress: progress, since: now, seen: true}
		return 0
	}
	return now.Sub(prev.since)
}

func (p *pruneState) prunedBelow(id store.ShardID) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.below[id]
}

func (p *pruneState) setBelow(id store.ShardID, below int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.below == nil {
		p.below = map[store.ShardID]int64{}
	}
	p.below[id] = max(p.below[id], below)
}

// collectUnused removes, after CopyDirGrace, the copy directories this node holds no
// copy from and the recovery staging no recovery uses, once the shard needs neither:
// it has its copy target (an explicit one: under "every node" this node will claim the
// shard again) served elsewhere, or this node's own copy serves (staging), or its
// index is gone (staging).
func (n *Node) collectUnused(ctx context.Context) {
	if n.draining.Load() {
		return
	}
	v := n.view.Load()
	targets := map[string]int{}
	for _, iv := range n.Indexes() {
		targets[iv.Name] = iv.ReplicasPerShard
	}
	settled := func(id store.ShardID) bool {
		t := targets[id.Index]
		return t > 0 && v.servingElsewhere(id, n.id) >= t
	}
	for _, d := range n.UnhostedCopyDirs() {
		since := d.UnhostedAt
		if since.IsZero() {
			since = n.startedAt
		}
		if n.leaseFor(d.Shard) != nil || !settled(d.Shard) || time.Since(since) < n.opts.CopyDirGrace {
			continue
		}
		if err := n.RemoveCopyDir(ctx, d.Shard); err != nil {
			n.log.WarnContext(ctx, "removing an unused copy directory failed", slog.String("dir", d.Path), slog.Any("error", err))
		}
	}
	root := filepath.Join(n.cfg.DataDir, "recovery")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		path := filepath.Join(root, e.Name())
		id, ok := stagingShard(e.Name())
		since := n.fetch.idleSince(id)
		if since.IsZero() {
			since = n.startedAt
		}
		if !ok || n.fetch.active(id) || time.Since(since) < n.opts.CopyDirGrace {
			continue
		}
		if _, known := targets[id.Index]; known && !settled(id) && !n.peerValid(id) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			n.log.WarnContext(ctx, "removing unused recovery staging failed", slog.String("dir", path), slog.Any("error", err))
		}
	}
}

// stagingShard reads the shard a recovery staging directory is named for.
func stagingShard(name string) (store.ShardID, bool) {
	dot := strings.LastIndexByte(name, '.')
	if dot <= 0 {
		return store.ShardID{}, false
	}
	index, err := url.PathUnescape(name[:dot])
	s, serr := strconv.Atoi(name[dot+1:])
	if err != nil || serr != nil {
		return store.ShardID{}, false
	}
	return store.ShardID{Index: index, Shard: s}, true
}
