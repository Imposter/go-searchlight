package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// localTarget reads this node's own copy, holding one generation of it from the first
// call to Release.
type localTarget struct {
	c     *copyState
	g     *shard.Generation
	stale bool
}

var _ ShardTarget = (*localTarget)(nil)

func (t *localTarget) Search(ctx context.Context, r *search.Request) (*search.ShardResult, error) {
	return search.ExecuteShard(ctx, t.g, r)
}

func (t *localTarget) Fetch(ctx context.Context, hits []search.Hit, fields []string) error {
	return search.FetchShard(ctx, t.g, hits, fields)
}

func (t *localTarget) Percolate(ctx context.Context, _ json.RawMessage, docs []schema.Doc) ([]percolate.IDs, error) {
	if t.g.NumQueries() == 0 {
		return make([]percolate.IDs, len(docs)), nil
	}
	return t.c.perc.Percolate(ctx, t.g, docs)
}

func (t *localTarget) Get(_ context.Context, id string) ([]byte, bool, error) {
	return t.g.Get(id)
}

func (t *localTarget) GetQuery(_ context.Context, id string) (*api.SavedQuery, bool, error) {
	seg, ord, ok := t.g.LookupQuery(id)
	if !ok {
		return nil, false, nil
	}
	sq, err := t.g.QuerySegments[seg].Segment.Query(ord)
	if err != nil {
		return nil, false, err
	}
	q, err := percolate.EncodeQuery(sq.Query)
	if err != nil {
		return nil, false, err
	}
	return &api.SavedQuery{ID: id, Seq: sq.Seq, Query: q, Meta: sq.Meta}, true, nil
}

func (t *localTarget) Stale() bool { return t.stale }

func (t *localTarget) Release() { t.g.Release() }

// fallbackTarget reads a copy elsewhere, and falls back to this node's own copy (stale,
// or recovering: it then says why it cannot serve) when no other copy answers its first
// call.
type fallbackTarget struct {
	primary  ShardTarget
	fallback func() (ShardTarget, error)
	used     ShardTarget // the target the first call settled on
}

var _ ShardTarget = (*fallbackTarget)(nil)

// first runs a read's first call: on the primary, then, when no other copy could
// serve it, on the fallback.
func (f *fallbackTarget) first(call func(ShardTarget) error) error {
	if f.used != nil {
		return call(f.used)
	}
	err := call(f.primary)
	if err == nil || !isUnavailable(err) {
		f.used = f.primary
		return err
	}
	fb, ferr := f.fallback()
	if ferr != nil {
		f.used = f.primary
		return err
	}
	f.primary.Release()
	f.used = fb
	return call(fb)
}

func (f *fallbackTarget) Search(ctx context.Context, r *search.Request) (res *search.ShardResult, err error) {
	err = f.first(func(t ShardTarget) error {
		res, err = t.Search(ctx, r)
		return err
	})
	return res, err
}

func (f *fallbackTarget) Fetch(ctx context.Context, hits []search.Hit, fields []string) error {
	if f.used == nil {
		return fmt.Errorf("%w: fetch before search", search.ErrStaleHit)
	}
	return f.used.Fetch(ctx, hits, fields)
}

func (f *fallbackTarget) Percolate(ctx context.Context, mapping json.RawMessage, docs []schema.Doc) (out []percolate.IDs, err error) {
	err = f.first(func(t ShardTarget) error {
		out, err = t.Percolate(ctx, mapping, docs)
		return err
	})
	return out, err
}

func (f *fallbackTarget) Get(ctx context.Context, id string) (body []byte, found bool, err error) {
	err = f.first(func(t ShardTarget) error {
		body, found, err = t.Get(ctx, id)
		return err
	})
	return body, found, err
}

func (f *fallbackTarget) GetQuery(ctx context.Context, id string) (q *api.SavedQuery, found bool, err error) {
	err = f.first(func(t ShardTarget) error {
		q, found, err = t.GetQuery(ctx, id)
		return err
	})
	return q, found, err
}

func (f *fallbackTarget) Stale() bool {
	if f.used == nil {
		return f.primary.Stale()
	}
	return f.used.Stale()
}

func (f *fallbackTarget) Release() {
	if f.used != nil {
		f.used.Release()
		return
	}
	f.primary.Release()
}

// isUnavailable reports whether err says a copy could not serve (a 503), as opposed
// to a failure of the request itself.
func isUnavailable(err error) bool {
	var ae *api.Error
	return errors.As(err, &ae) && ae.Status == http.StatusServiceUnavailable
}

// targets are the read targets of one read, one per shard.
type targets []ShardTarget

func (ts targets) release() {
	for _, t := range ts {
		if t != nil {
			t.Release()
		}
	}
}

// stale reports whether any target's reads are stale.
func (ts targets) stale() bool {
	for _, t := range ts {
		if t != nil && t.Stale() {
			return true
		}
	}
	return false
}

// acquireTargets returns a read target for every shard of idx, each with every change
// up to waitSeq searchable and, forQueries, every saved query committed through this
// node to its shard. Release them when done.
func (n *Single) acquireTargets(ctx context.Context, idx *index, waitSeq int64, forQueries bool) (targets, error) {
	if waitSeq > 0 {
		if err := n.checkHead(ctx, waitSeq); err != nil {
			return nil, err
		}
		// Wake every copy first, so idle ones advance to the seq together.
		for _, c := range idx.copies() {
			c.tailer.Wake()
		}
	}
	out := make(targets, len(idx.shards))
	for s := range idx.shards {
		t, err := n.acquireShard(ctx, idx, s, waitSeq, forQueries)
		if err != nil {
			out.release()
			if idx.dropped.Load() {
				return nil, indexNotFound(idx.name)
			}
			return nil, err
		}
		out[s] = t
	}
	return out, nil
}

// acquireShard returns a read target for shard s of idx: this node's copy when it is
// current, else a serving copy elsewhere (a cluster node), falling back to this node's
// copy when no other answers.
func (n *Single) acquireShard(ctx context.Context, idx *index, s int, waitSeq int64, forQueries bool) (ShardTarget, error) {
	sl := idx.shards[s]
	queryWait := int64(0)
	if forQueries {
		queryWait = sl.querySeq.Load()
	}
	c := sl.local.Load()
	if c != nil && (n.cl == nil || (c.notServing() == nil && !n.copyStale(c))) {
		return n.localTarget(ctx, c, waitSeq, queryWait)
	}
	if n.cl != nil {
		rt, err := n.cl.Remote(ctx, sl.id, max(waitSeq, queryWait))
		switch {
		case err == nil && c != nil:
			return &fallbackTarget{primary: rt, fallback: func() (ShardTarget, error) { return n.localTarget(ctx, c, waitSeq, queryWait) }}, nil
		case err == nil:
			return rt, nil
		case c != nil:
			return n.localTarget(ctx, c, waitSeq, queryWait)
		}
		return nil, err
	}
	return nil, api.Unavailable(shard.ErrClosed, "shard %d of index %q has no copy on this node", s, idx.name)
}

// LocalTarget returns a read target on this node's own copy of shard s of index, once
// it has every change up to waitSeq searchable (under ctx). It fails (503) when this
// node hosts no serving copy of the shard, or holds its lease not surely (lapsed): the
// peer API serves remote reads with it.
func (n *Single) LocalTarget(ctx context.Context, index string, s int, waitSeq int64) (ShardTarget, error) {
	idx, err := n.lookup(ctx, index)
	if err != nil {
		return nil, err
	}
	if s < 0 || s >= len(idx.shards) {
		return nil, api.NotFound(api.CodeNotFound, "index %q has no shard %d", index, s)
	}
	c := idx.shards[s].local.Load()
	if c == nil {
		return nil, api.Unavailable(shard.ErrClosed, "this node holds no copy of shard %d of index %q", s, index)
	}
	if err := c.peerServing(); err != nil {
		return nil, err
	}
	if waitSeq > 0 {
		if err := n.checkHead(ctx, waitSeq); err != nil {
			return nil, err
		}
	}
	t, err := n.localTarget(ctx, c, waitSeq, 0)
	if err == nil && c.lapsed() {
		// The lease lapsed while the read waited: no peer may use the copy now.
		t.Release()
		return nil, c.peerServing()
	}
	return t, err
}

// localTarget holds a generation of copy c once it has waitSeq searchable (a 504 at the
// deadline) and queryWait refreshed.
func (n *Single) localTarget(ctx context.Context, c *copyState, waitSeq, queryWait int64) (ShardTarget, error) {
	if waitSeq > 0 {
		if err := c.notServing(); err != nil {
			return nil, err
		}
		if err := n.waitCopy(ctx, c, waitSeq); err != nil {
			return nil, err
		}
	}
	if queryWait > 0 {
		if sh := c.shard(); sh != nil && sh.RefreshedSeq() < queryWait {
			c.tailer.Wake()
			if err := c.waitRefreshed(ctx, queryWait); err != nil {
				return nil, err
			}
		}
	}
	g, err := c.acquire()
	if err != nil {
		return nil, err
	}
	return &localTarget{c: c, g: g, stale: n.copyStale(c)}, nil
}

// waitCopy waits, under ctx's deadline, until copy c has seq searchable: a 504 when the
// deadline passes first, a 503 when the copy halts.
func (n *Single) waitCopy(ctx context.Context, c *copyState, seq int64) error {
	if sh := c.shard(); sh != nil && sh.RefreshedSeq() >= seq {
		return nil
	}
	c.tailer.Wake()
	err := c.waitRefreshed(ctx, seq)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		var at int64
		if sh := c.shard(); sh != nil {
			at = sh.RefreshedSeq()
		}
		return &api.Error{
			Status: http.StatusGatewayTimeout, Code: api.CodeTimeout, Err: err,
			Detail: fmt.Sprintf("seq %d was not searchable on shard %d before the deadline (it is at %d)", seq, c.id.Shard, at),
		}
	}
	if h := c.halted.Load(); h != nil {
		return api.Unavailable(*h, "shard %d of index %q has halted", c.id.Shard, c.id.Index)
	}
	return err
}
