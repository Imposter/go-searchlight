package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// candidate is a serving copy of a shard on a peer, which a read may use.
type candidate struct {
	node, addr string
	shard      store.ShardID
}

// key names the copy for adaptive replica selection's staleness memory.
func (c candidate) key() string { return c.node + "|" + c.shard.String() }

// candidates lists the serving copies of id on live peers, best first (adaptive
// replica selection).
func (n *Node) candidates(id store.ShardID) []candidate {
	v := n.view.Load()
	var out []candidate
	for i := range v.copies[id] {
		c := &v.copies[id][i]
		if c.NodeID == n.id || c.State != store.CopyServing || !v.usable(c) {
			continue
		}
		nd, ok := v.nodes[c.NodeID]
		if !ok || nd.Address == "" {
			continue
		}
		out = append(out, candidate{node: c.NodeID, addr: nd.Address, shard: id})
	}
	n.ars.order(out, len(v.live))
	return out
}

// peerError is a transport failure reaching a peer: the peer is gone, or did not
// answer in time.
type peerError struct {
	node string
	err  error
}

func (e *peerError) Error() string { return fmt.Sprintf("cluster: peer %s: %v", e.node, e.err) }
func (e *peerError) Unwrap() error { return e.err }

// retryable reports whether a failed peer call is worth another copy: the peer could
// not be reached or answered in time, could not serve (503, 502, 500), or is busy
// (429). A request the peer refused as such (a 400, a 404) is not.
func retryable(err error) bool {
	var pe *peerError
	if errors.As(err, &pe) {
		return true
	}
	var ae *api.Error
	if errors.As(err, &ae) {
		switch ae.Status {
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusInternalServerError,
			http.StatusTooManyRequests, http.StatusGatewayTimeout:
			return true
		}
		return false
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// call sends a peer API request to the peer at addr and decodes its JSON reply into
// out (when not nil). An error reply comes back as an *api.Error; a transport failure
// as a *peerError.
func (n *Node) call(ctx context.Context, peer, addr, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	// addr is a peer's advertise address from the registry: peers are trusted.
	req, err := http.NewRequestWithContext(ctx, method, n.scheme+"://"+addr+path, body) //nolint:gosec // see above
	if err != nil {
		return err
	}
	n.authorize(req)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	done := n.ars.start(peer)
	start := time.Now()
	resp, err := n.client.Do(req) //nolint:gosec // a peer's registered address
	if err != nil {
		done(true, -1, -1)
		n.inst.peer(ctx, path, peer, 0, time.Since(start))
		return &peerError{node: peer, err: err}
	}
	defer resp.Body.Close()
	n.inst.peer(ctx, path, peer, resp.StatusCode, time.Since(start))
	service, queue := reported(resp.Header)
	if resp.StatusCode >= 400 {
		failed := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
		done(failed, service, queue)
		return decodeError(resp)
	}
	if out == nil {
		done(false, service, queue)
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		done(true, -1, -1)
		return &peerError{node: peer, err: fmt.Errorf("decoding the reply: %w", err)}
	}
	done(false, service, queue)
	return nil
}

// authorize adds the cluster token and the trace context to a peer request, and the
// caller's deadline.
func (n *Node) authorize(req *http.Request) {
	if n.cfg.ClusterToken != "" {
		req.Header.Set("Authorization", "Bearer "+n.cfg.ClusterToken)
	}
	if dl, ok := req.Context().Deadline(); ok {
		req.Header.Set(headerDeadline, strconv.FormatInt(max(1, time.Until(dl).Milliseconds()), 10))
	}
	otel.GetTextMapPropagator().Inject(req.Context(), propagation.HeaderCarrier(req.Header))
}

// reported reads the service time and queue a peer reported (-1 when it did not).
func reported(h http.Header) (service, queue float64) {
	service, queue = -1, -1
	if v, err := strconv.ParseFloat(h.Get(headerServiceTime), 64); err == nil {
		service = v
	}
	if v, err := strconv.ParseFloat(h.Get(headerQueue), 64); err == nil {
		queue = v
	}
	return service, queue
}

// decodeError turns a peer's error reply into an *api.Error.
func decodeError(resp *http.Response) error {
	var er errorReply
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if json.Unmarshal(b, &er) != nil || er.Status == 0 {
		return &api.Error{Status: resp.StatusCode, Code: api.CodeUnavailable, Detail: "the peer answered " + resp.Status, Err: errors.New(string(b))}
	}
	return &api.Error{
		Status: er.Status, Code: er.Code, Detail: er.Detail, Problems: er.Problems,
		RetryAfter: time.Duration(er.RetryAfterMs) * time.Millisecond, Err: fmt.Errorf("peer: %s", er.Detail),
	}
}

// remoteTarget reads a shard from a serving copy on a peer: the first call tries the
// candidates in order until one answers (a failed or timed-out one is retried on the
// next), and the rest go to the copy that did.
type remoteTarget struct {
	n       *Node
	id      store.ShardID
	waitSeq int64
	cands   []candidate
	chosen  *candidate
	pin     string
	stale   bool
}

var _ node.ShardTarget = (*remoteTarget)(nil)

// attempt runs fn on the chosen copy, or on each candidate in turn until one answers.
func (t *remoteTarget) attempt(ctx context.Context, op string, fn func(ctx context.Context, c candidate) error) error {
	if t.chosen != nil {
		return fn(ctx, *t.chosen)
	}
	var last error
	for i, c := range t.cands {
		actx, cancel := context.WithTimeout(ctx, t.n.opts.PeerTimeout)
		err := fn(actx, c)
		cancel()
		if err == nil {
			t.chosen = &t.cands[i]
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !retryable(err) {
			return err
		}
		last = err
		t.n.inst.retry(ctx, op)
		t.n.log.DebugContext(ctx, "a peer read failed; trying another copy", slog.String("op", op), slog.String("shard", t.id.String()),
			slog.String("peer", c.node), slog.Any("error", err))
	}
	return api.Unavailable(last, "no copy of shard %d of index %q answered", t.id.Shard, t.id.Index)
}

func (t *remoteTarget) ref() shardRef {
	return shardRef{Index: t.id.Index, Shard: t.id.Shard, WaitSeq: t.waitSeq}
}

func (t *remoteTarget) noteStale(c candidate, stale bool) {
	t.stale = stale
	t.n.ars.noteStale(c.key(), stale)
}

func (t *remoteTarget) Search(ctx context.Context, r *search.Request) (*search.ShardResult, error) {
	wr, err := encodeRequest(r)
	if err != nil {
		return nil, err
	}
	msg := searchMsg{shardRef: t.ref(), Request: wr, Pin: r.NoBodies}
	var reply searchReply
	err = t.attempt(ctx, "search", func(ctx context.Context, c candidate) error {
		reply = searchReply{}
		if err := t.n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"search", msg, &reply); err != nil {
			return err
		}
		if reply.Result == nil {
			return &peerError{node: c.node, err: errors.New("no result")}
		}
		t.noteStale(c, reply.Stale)
		return nil
	})
	if err != nil {
		return nil, err
	}
	t.pin = reply.Pin
	return reply.Result, nil
}

func (t *remoteTarget) Fetch(ctx context.Context, hits []search.Hit, fields []string) error {
	if t.chosen == nil || t.pin == "" {
		return fmt.Errorf("%w: no pinned generation to fetch from", search.ErrStaleHit)
	}
	var reply fetchReply
	err := t.n.call(ctx, t.chosen.node, t.chosen.addr, http.MethodPost, peerPrefix+"fetch", fetchMsg{Pin: t.pin, Hits: hits, Fields: fields}, &reply)
	if err != nil {
		var ae *api.Error
		switch {
		case errors.As(err, &ae) && ae.Status == http.StatusGone:
			return fmt.Errorf("%w: %w", search.ErrStaleHit, err)
		case retryable(err) && ctx.Err() == nil:
			return fmt.Errorf("%w: %w", node.ErrTargetLost, err)
		}
		return err
	}
	if len(reply.Bodies) != len(hits) {
		return fmt.Errorf("%w: the peer returned %d bodies for %d hits", node.ErrTargetLost, len(reply.Bodies), len(hits))
	}
	for i := range hits {
		hits[i].Body = reply.Bodies[i]
	}
	return nil
}

func (t *remoteTarget) Percolate(ctx context.Context, mapping json.RawMessage, docs []schema.Doc) ([][]string, error) {
	msg := percolateMsg{shardRef: t.ref(), Mapping: mapping, Docs: make([]wireDoc, len(docs))}
	for i := range docs {
		msg.Docs[i] = wireDoc{ID: docs[i].ID, Body: docs[i].Body}
	}
	var reply percolateReply
	err := t.attempt(ctx, "percolate", func(ctx context.Context, c candidate) error {
		reply = percolateReply{}
		if err := t.n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"percolate", msg, &reply); err != nil {
			return err
		}
		t.noteStale(c, reply.Stale)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(reply.Matches) != len(docs) {
		return nil, fmt.Errorf("cluster: the peer percolated %d documents of %d", len(reply.Matches), len(docs))
	}
	return reply.Matches, nil
}

func (t *remoteTarget) get(ctx context.Context, id string, saved bool) (*getReply, error) {
	var reply getReply
	err := t.attempt(ctx, "get", func(ctx context.Context, c candidate) error {
		reply = getReply{}
		if err := t.n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"get", getMsg{shardRef: t.ref(), ID: id, Query: saved}, &reply); err != nil {
			return err
		}
		t.noteStale(c, reply.Stale)
		return nil
	})
	return &reply, err
}

func (t *remoteTarget) Get(ctx context.Context, id string) ([]byte, bool, error) {
	reply, err := t.get(ctx, id, false)
	if err != nil {
		return nil, false, err
	}
	return reply.Body, reply.Found, nil
}

func (t *remoteTarget) GetQuery(ctx context.Context, id string) (*api.SavedQuery, bool, error) {
	reply, err := t.get(ctx, id, true)
	if err != nil {
		return nil, false, err
	}
	return reply.Saved, reply.Found && reply.Saved != nil, nil
}

func (t *remoteTarget) Stale() bool { return t.stale }

// Release unpins the generation the fetch phase held, in the background: the pin
// expires on its own if the call is lost.
func (t *remoteTarget) Release() {
	if t.pin == "" || t.chosen == nil {
		return
	}
	c, pin := *t.chosen, t.pin
	t.pin = ""
	t.n.background(func(ctx context.Context) {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_ = t.n.call(ctx, c.node, c.addr, http.MethodDelete, peerPrefix+"pins/"+pin, nil, nil)
	})
}

// background runs fn in a goroutine bounded by the node's life; Stop waits for it.
func (n *Node) background(fn func(ctx context.Context)) {
	n.bgMu.Lock()
	defer n.bgMu.Unlock()
	if n.bgClosed {
		return
	}
	n.bg.Go(func() { fn(n.loopCtx) })
}

// closeBackground stops background work and waits for it.
func (n *Node) closeBackground() {
	n.bgMu.Lock()
	n.bgClosed = true
	n.bgMu.Unlock()
	n.bg.Wait()
}

// --- the engine's hooks (node.Cluster) ----------------------------------------------

// Remote implements node.Cluster.
func (h *clusterHooks) Remote(_ context.Context, id store.ShardID, waitSeq int64) (node.ShardTarget, error) {
	cands := h.n.candidates(id)
	if len(cands) == 0 {
		return nil, api.Unavailable(store.ErrClosed, "no other node holds a serving copy of shard %d of index %q", id.Shard, id.Index)
	}
	return &remoteTarget{n: h.n, id: id, waitSeq: waitSeq, cands: cands}, nil
}

// Committed implements node.Cluster: a push hint to every peer holding a copy of a
// written shard. Hints are best effort; polling (and LISTEN on Postgres) is the
// guarantee.
func (h *clusterHooks) Committed(index string, shards map[int]int64) {
	n := h.n
	v := n.view.Load()
	for s, seq := range shards {
		id := store.ShardID{Index: index, Shard: s}
		for i := range v.copies[id] {
			c := &v.copies[id][i]
			if c.NodeID == n.id || !v.usable(c) {
				continue
			}
			if nd, ok := v.nodes[c.NodeID]; ok && nd.Address != "" {
				n.hints.send(c.NodeID, nd.Address, hint{Index: index, Shard: s, Seq: seq})
			}
		}
	}
}

// WaitRefreshed implements node.Cluster: it waits on every serving copy of id on a
// live peer, together. A copy whose peer cannot be reached, or that does not serve
// now, is skipped: no read is routed to it either.
func (h *clusterHooks) WaitRefreshed(ctx context.Context, id store.ShardID, seq int64, mode api.RefreshMode) error {
	n := h.n
	cands := n.candidates(id)
	if len(cands) == 0 {
		return nil
	}
	errs := make([]error, len(cands))
	var wg sync.WaitGroup
	for i, c := range cands {
		wg.Go(func() {
			msg := waitMsg{shardRef: shardRef{Index: id.Index, Shard: id.Shard}, Seq: seq, Refresh: mode == api.RefreshTrue}
			err := n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"wait", msg, nil)
			var pe *peerError
			var ae *api.Error
			switch {
			case err == nil, errors.As(err, &pe) && ctx.Err() == nil:
			case errors.As(err, &ae) && ae.Status == http.StatusServiceUnavailable:
			default:
				errs[i] = err
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// Allocate implements node.Cluster: this node claims its copies of a new index now.
func (h *clusterHooks) Allocate(ctx context.Context, index string) error {
	if err := h.n.refreshView(ctx); err != nil {
		return err
	}
	return h.n.allocatePass(ctx, index, false, true)
}

// CopyStopped implements node.Cluster: a copy whose tailer stopped on its own (its
// lease was lost at a registry write, or it could not go on) is unhosted; the
// allocator claims it again if the node may still hold it.
func (h *clusterHooks) CopyStopped(c store.Copy, err error) {
	n := h.n
	n.log.Warn("a shard copy's tailer stopped; unhosting it", slog.String("shard", c.Shard.String()), slog.Any("error", err))
	if errors.Is(err, store.ErrLeaseLost) {
		n.inst.lease(context.Background(), "lost")
	}
	n.background(func(ctx context.Context) {
		if l := n.leaseFor(c.Shard); l != nil && l.copy.Epoch == c.Epoch {
			n.loseCopy(ctx, l)
		}
	})
}

// Counts implements node.Cluster.
func (h *clusterHooks) Counts(ctx context.Context, id store.ShardID) (uint64, uint64, error) {
	cands := h.n.candidates(id)
	if len(cands) == 0 {
		return 0, 0, api.Unavailable(store.ErrClosed, "no serving copy of %s", id)
	}
	t := &remoteTarget{n: h.n, id: id, cands: cands}
	var reply countsReply
	err := t.attempt(ctx, "counts", func(ctx context.Context, c candidate) error {
		return h.n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"counts", shardRef{Index: id.Index, Shard: id.Shard}, &reply)
	})
	return reply.Docs, reply.Queries, err
}

// --- push hints ---------------------------------------------------------------------

// hinter sends push hints, one queue per peer, coalescing each shard's hints to the
// newest seq while a send is under way.
type hinter struct {
	n      *Node
	mu     sync.Mutex
	queues map[string]*hintQueue
	closed bool
	wg     sync.WaitGroup
}

type hintQueue struct {
	addr    string
	pending map[store.ShardID]int64
	wake    chan struct{}
	stop    chan struct{}
}

func newHinter(n *Node) *hinter { return &hinter{n: n, queues: map[string]*hintQueue{}} }

// hintTimeout bounds one hint send.
const hintTimeout = time.Second

func (h *hinter) send(peer, addr string, x hint) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	q := h.queues[peer]
	if q == nil {
		q = &hintQueue{addr: addr, pending: map[store.ShardID]int64{}, wake: make(chan struct{}, 1), stop: make(chan struct{})}
		h.queues[peer] = q
		h.wg.Go(func() { h.run(peer, q) })
	}
	q.addr = addr
	id := store.ShardID{Index: x.Index, Shard: x.Shard}
	q.pending[id] = max(q.pending[id], x.Seq)
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (h *hinter) run(peer string, q *hintQueue) {
	for {
		select {
		case <-q.stop:
			return
		case <-q.wake:
		}
		h.mu.Lock()
		batch := q.pending
		q.pending = map[store.ShardID]int64{}
		addr := q.addr
		h.mu.Unlock()
		msg := hintMsg{Hints: make([]hint, 0, len(batch))}
		for id, seq := range batch {
			msg.Hints = append(msg.Hints, hint{Index: id.Index, Shard: id.Shard, Seq: seq})
		}
		ctx, cancel := context.WithTimeout(context.Background(), hintTimeout)
		if err := h.n.call(ctx, peer, addr, http.MethodPost, peerPrefix+"hint", msg, nil); err != nil {
			h.n.log.Debug("a push hint was not delivered; the peer's poll picks the change up", slog.String("peer", peer), slog.Any("error", err))
		}
		cancel()
	}
}

func (h *hinter) close() {
	h.mu.Lock()
	h.closed = true
	for _, q := range h.queues {
		close(q.stop)
	}
	h.mu.Unlock()
	h.wg.Wait()
}
