package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// The internal peer API, under /_internal/ on the node's listener. Every request
// carries the cluster token (Authorization: Bearer <cluster_token>); with none
// configured the API refuses every peer, unless insecure_no_auth is set.
//
//	POST   /_internal/hint                      push hints: wake the named copies' tailers
//	POST   /_internal/search                    a shard's query (or whole search) on this node's copy
//	POST   /_internal/fetch                     bodies of hits from a pinned generation
//	DELETE /_internal/pins/{id}                 unpin a generation
//	POST   /_internal/percolate                 percolate documents against a copy's saved queries
//	POST   /_internal/get                       a stored document or saved query from a copy
//	POST   /_internal/wait                      wait until a copy has a seq searchable
//	POST   /_internal/counts                    a copy's live documents and saved queries
//	GET    /_internal/copies                    this node's copies and their progress
//	POST   /_internal/snapshots                 take a snapshot of a copy for a recovery
//	GET    /_internal/snapshots/{id}/files/{name}  stream one snapshot file (Range resumes it)
//	DELETE /_internal/snapshots/{id}            release a snapshot
type peerAPI struct {
	n        *Node
	inflight atomic.Int64
}

// maxPeerBody bounds a peer request's body.
const maxPeerBody = 256 << 20

func (p *peerAPI) handler() http.Handler {
	mux := http.NewServeMux()
	handle := func(pattern string, fn func(r *http.Request) (any, error)) {
		mux.Handle(pattern, p.wrap(pattern, fn))
	}
	handle("POST /_internal/hint", p.hint)
	handle("POST /_internal/search", p.search)
	handle("POST /_internal/fetch", p.fetch)
	handle("DELETE /_internal/pins/{id}", p.unpin)
	handle("POST /_internal/percolate", p.percolate)
	handle("POST /_internal/get", p.get)
	handle("POST /_internal/wait", p.wait)
	handle("POST /_internal/counts", p.counts)
	handle("GET /_internal/copies", p.copies)
	mux.Handle("POST /_internal/snapshots", p.wrapStream("POST /_internal/snapshots", p.snapshot))
	handle("DELETE /_internal/snapshots/{id}", p.release)
	mux.Handle("GET /_internal/snapshots/{id}/files/{name}", p.wrapStream("GET /_internal/snapshots/{id}/files/{name}", p.file))
	mux.Handle("/", p.wrap("unmatched", func(*http.Request) (any, error) {
		return nil, api.NotFound(api.CodeNotFound, "no peer endpoint here")
	}))
	return mux
}

// authorized checks a peer request's cluster token.
func (p *peerAPI) authorized(r *http.Request) *api.Error {
	token := p.n.cfg.ClusterToken
	if token == "" {
		if p.n.cfg.InsecureNoAuth {
			return nil
		}
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "cluster_token is not set on this node: the peer API is closed"}
	}
	scheme, presented, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "the cluster token is required"}
	}
	want := sha256.Sum256([]byte(token))
	got := sha256.Sum256([]byte(strings.TrimSpace(presented)))
	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		return &api.Error{Status: http.StatusUnauthorized, Code: api.CodeUnauthorized, Detail: "the cluster token is not valid"}
	}
	return nil
}

// begin starts a peer request: auth, the caller's trace and deadline, a span. It
// returns the request under the context the work runs in.
func (p *peerAPI) begin(w http.ResponseWriter, r *http.Request, route string) (*http.Request, trace.Span, func(), *api.Error) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := p.n.tr.Start(ctx, "peer "+route, trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(attribute.String("peer.route", route)))
	cancel := func() {}
	if ms, err := strconv.ParseInt(r.Header.Get(headerDeadline), 10, 64); err == nil && ms > 0 {
		ctx, cancel = clock.WithTimeout(ctx, p.n.clock, time.Duration(ms)*time.Millisecond)
	}
	r = r.WithContext(ctx)
	if e := p.authorized(r); e != nil {
		return r, span, cancel, e
	}
	q := p.inflight.Add(1)
	w.Header().Set(headerQueue, strconv.FormatInt(q-1, 10))
	end := cancel
	cancel = func() {
		p.inflight.Add(-1)
		end()
	}
	return r, span, cancel, nil
}

func (p *peerAPI) wrap(route string, fn func(r *http.Request) (any, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := p.n.clock.Now()
		r, span, done, e := p.begin(w, r, route)
		defer span.End()
		defer done()
		if e != nil {
			writeError(w, e)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPeerBody)
		out, err := fn(r)
		w.Header().Set(headerServiceTime, strconv.FormatFloat(p.n.clock.Since(start).Seconds(), 'f', -1, 64))
		if err != nil {
			e := api.ProblemFor(err)
			if e.Status >= 500 {
				span.RecordError(err)
				span.SetStatus(codes.Error, e.Detail)
				p.n.log.DebugContext(r.Context(), "peer request failed", slog.String("route", route), slog.Any("error", err)) //nolint:contextcheck // r carries the request context
			}
			writeError(w, e)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if out == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_ = json.NewEncoder(w).Encode(out)
	})
}

func (p *peerAPI) wrapStream(route string, fn func(w http.ResponseWriter, r *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r, span, done, e := p.begin(w, r, route)
		defer span.End()
		defer done()
		if e != nil {
			writeError(w, e)
			return
		}
		if err := fn(w, r); err != nil {
			writeError(w, api.ProblemFor(err))
		}
	})
}

// writeError writes an error reply.
func writeError(w http.ResponseWriter, e *api.Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(errorReply{Status: e.Status, Code: e.Code, Detail: e.Detail, Problems: e.Problems, RetryAfterMs: e.RetryAfter.Milliseconds()})
}

func decode(r *http.Request, v any) error {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		return api.InvalidAt("body", "the peer request is not valid JSON: %v", err)
	}
	return nil
}

func (p *peerAPI) hint(r *http.Request) (any, error) {
	var msg hintMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	for _, h := range msg.Hints {
		p.n.Wake(store.ShardID{Index: h.Index, Shard: h.Shard}, h.Seq)
	}
	return nil, nil
}

// errStale is a read refused because this node's copy is stale and the caller wants a
// current one (it has other copies to try).
func errStale(ref shardRef) error {
	return &api.Error{
		Status: http.StatusServiceUnavailable, Code: codeStale,
		Detail: "this node's copy of shard " + strconv.Itoa(ref.Shard) + " of index " + strconv.Quote(ref.Index) + " is stale",
	}
}

// target returns a read target on this node's copy for ref: one that serves peers, and
// is current unless the caller allows stale.
func (p *peerAPI) target(r *http.Request, ref shardRef) (node.ShardTarget, error) {
	h := p.n.opts.hooks
	var began time.Duration
	var wall time.Time
	if h != nil && h.served != nil {
		began, wall = p.n.lc.Now(), p.n.lc.Wall()
	}
	t, err := p.n.LocalTarget(r.Context(), ref.Index, ref.Shard, ref.WaitSeq)
	if err != nil {
		return nil, err
	}
	if !ref.AllowStale && t.Stale() {
		t.Release()
		return nil, errStale(ref)
	}
	if h != nil && h.served != nil {
		h.served(store.ShardID{Index: ref.Index, Shard: ref.Shard}, began, wall)
	}
	return t, nil
}

func (p *peerAPI) search(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg searchMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	if msg.Request == nil {
		return nil, api.InvalidAt("request", "no search request")
	}
	req, err := msg.Request.decode()
	if err != nil {
		return nil, err
	}
	t, err := p.target(r, msg.shardRef)
	if err != nil {
		return nil, err
	}
	res, err := t.Search(ctx, req)
	if err != nil {
		t.Release()
		return nil, err
	}
	reply := &searchReply{Result: res, Stale: t.Stale()}
	if msg.Pin {
		reply.Pin = p.n.pins.add(t)
		if h := p.n.opts.hooks; h != nil && h.pinned != nil {
			h.pinned(store.ShardID{Index: msg.Index, Shard: msg.Shard}, reply.Pin)
		}
	} else {
		t.Release()
	}
	return reply, nil
}

func (p *peerAPI) fetch(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg fetchMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	var reply fetchReply
	err := p.n.pins.use(msg.Pin, func(t node.ShardTarget) error {
		if err := t.Fetch(ctx, msg.Hits, msg.Fields); err != nil {
			return err
		}
		reply.Bodies = make([]json.RawMessage, len(msg.Hits))
		for i := range msg.Hits {
			reply.Bodies[i] = msg.Hits[i].Body
		}
		return nil
	})
	if errors.Is(err, errPinGone) {
		return nil, &api.Error{Status: http.StatusGone, Code: api.CodeNotFound, Detail: "the pinned generation expired or was released"}
	}
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

func (p *peerAPI) unpin(r *http.Request) (any, error) {
	p.n.pins.remove(r.PathValue("id"))
	return nil, nil
}

func (p *peerAPI) percolate(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg percolateMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	m := &schema.Mapping{Fields: map[string]schema.FieldType{}}
	if len(msg.Mapping) > 0 {
		if err := json.Unmarshal(msg.Mapping, m); err != nil {
			return nil, api.InvalidAt("mapping", "%v", err)
		}
	}
	docs := make([]schema.Doc, len(msg.Docs))
	for i, d := range msg.Docs {
		doc, _, err := schema.AnalyzeForMatch(m, d.ID, d.Body)
		if err != nil {
			return nil, api.InvalidAt("docs."+strconv.Itoa(i), "%v", err)
		}
		docs[i] = doc
	}
	t, err := p.target(r, msg.shardRef)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	matches, err := t.Percolate(ctx, msg.Mapping, docs)
	if err != nil {
		return nil, err
	}
	out := make([]json.RawMessage, len(matches))
	for i, m := range matches {
		out[i] = json.RawMessage(m)
		if len(m) == 0 {
			out[i] = json.RawMessage("[]")
		}
	}
	return &percolateReply{Matches: out, Stale: t.Stale()}, nil
}

func (p *peerAPI) get(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg getMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	t, err := p.target(r, msg.shardRef)
	if err != nil {
		return nil, err
	}
	defer t.Release()
	reply := &getReply{Stale: t.Stale()}
	if msg.Query {
		reply.Saved, reply.Found, err = t.GetQuery(ctx, msg.ID)
	} else {
		reply.Body, reply.Found, err = t.Get(ctx, msg.ID)
	}
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (p *peerAPI) wait(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg waitMsg
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	mode := api.RefreshWaitFor
	if msg.Refresh {
		mode = api.RefreshTrue
	}
	return nil, p.n.WaitLocal(ctx, msg.Index, msg.Shard, msg.Seq, mode)
}

func (p *peerAPI) counts(r *http.Request) (any, error) {
	ctx := r.Context()
	var msg shardRef
	if err := decode(r, &msg); err != nil {
		return nil, err
	}
	docs, queries, err := p.n.Counts(ctx, store.ShardID{Index: msg.Index, Shard: msg.Shard})
	if err != nil {
		return nil, err
	}
	return &countsReply{Docs: docs, Queries: queries}, nil
}

func (p *peerAPI) copies(*http.Request) (any, error) {
	return &copiesReply{Copies: p.n.localPeerCopies()}, nil
}

// localPeerCopies describes this node's copies for the peer API: with their epochs and
// progress counters. A copy's counter is the high-water mark of its applied seq, its
// documents and the bytes its current recovery attempt fetched: it moves only when the
// copy gets further than it ever got, so a recovery that fails and redoes the same work
// over and over shows no progress.
func (n *Node) localPeerCopies() []peerCopy {
	local := n.LocalCopies()
	out := make([]peerCopy, 0, len(local))
	held := make(map[copyKey]bool, len(local))
	defer n.forgetProgress(held)
	for i := range local {
		lc := &local[i]
		id := store.ShardID{Index: lc.Info.Index, Shard: lc.Info.Shard}
		now := lc.Info.AppliedSeq + int64(lc.Info.Docs) + n.fetch.progressOf(id) //nolint:gosec // a document count
		k := copyKey{shard: id, node: n.id, epoch: lc.Copy.Epoch}
		held[k] = true
		out = append(out, peerCopy{
			ShardInfo: lc.Info, Epoch: lc.Copy.Epoch, Paused: lc.Paused,
			Progress: n.progressMark(k, now),
		})
	}
	return out
}

func (n *Node) forgetProgress(held map[copyKey]bool) {
	n.progressMu.Lock()
	defer n.progressMu.Unlock()
	for k := range n.progressHW {
		if !held[k] {
			delete(n.progressHW, k)
		}
	}
}

// progressMark raises copy k's progress high-water mark to now and returns it.
func (n *Node) progressMark(k copyKey, now int64) int64 {
	n.progressMu.Lock()
	defer n.progressMu.Unlock()
	if n.progressHW == nil {
		n.progressHW = map[copyKey]int64{}
	}
	hw := max(n.progressHW[k], now)
	n.progressHW[k] = hw
	return hw
}

// snapshot takes a snapshot of a copy for a peer's recovery and lists its files with
// their sums. Hashing a big copy the first time can outlast the listener's write
// timeout: the write deadline moves on before each file, bounding each file's hashing
// instead of the whole listing.
func (p *peerAPI) snapshot(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	var msg shardRef
	if err := decode(r, &msg); err != nil {
		return err
	}
	id := store.ShardID{Index: msg.Index, Shard: msg.Shard}
	oldest, newest, err := p.n.SegmentMajors(ctx, id)
	if err != nil {
		return err
	}
	if err := p.servableMajors(msg, oldest, newest); err != nil {
		return err
	}
	sn, err := p.n.Snapshot(ctx, id)
	if err != nil {
		return err
	}
	oldest, newest = sn.FormatMajors()
	if err := p.servableMajors(msg, oldest, newest); err != nil {
		sn.Release()
		return err
	}
	rc := http.NewResponseController(w)
	reply := &snapshotReply{Seq: sn.Seq(), IndexUID: sn.IndexUID(), MappingVersion: sn.MappingVersion(), FormatMajor: oldest}
	if h := p.n.opts.hooks; h != nil && h.snapshotMajor != nil {
		reply.FormatMajor = h.snapshotMajor(reply.FormatMajor)
	}
	for _, f := range sn.Files() {
		_ = rc.SetWriteDeadline(time.Now().Add(snapshotHashBound)) //nolint:forbidigo // a connection deadline is by the OS clock
		sum, err := p.n.sums.sum(id, sn, f)
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			sn.Release()
			return err
		}
		reply.Files = append(reply.Files, wireFile{Name: f.Name, Size: f.Size, SHA256: sum})
	}
	reply.ID = p.n.snaps.add(id, sn)
	p.n.log.InfoContext(ctx, "serving a recovery snapshot", slog.String("shard", id.String()),
		slog.Int64("seq", reply.Seq), slog.Int("files", len(reply.Files)))
	_ = rc.SetWriteDeadline(time.Now().Add(snapshotHashBound)) //nolint:forbidigo // a connection deadline is by the OS clock
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(reply)
}

// servableMajors refuses a snapshot whose segments the requester could not open
// (newer than it reads) or did not want (older than its MinMajor).
func (p *peerAPI) servableMajors(msg shardRef, oldest, newest int) error {
	if h := p.n.opts.hooks; h != nil && h.snapshotMajor != nil {
		oldest = h.snapshotMajor(oldest)
	}
	reads := msg.ReadsMajor
	if reads == 0 {
		reads = legacyReadsMajor
	}
	switch {
	case newest > reads:
		return api.Conflict(codeNewerSegments, "this copy's segments are in format %d, newer than the %d the requester reads", newest, reads)
	case oldest < msg.MinMajor:
		return api.Conflict(codeOlderSegments, "this copy's segments are as old as format %d, the requester asked for %d", oldest, msg.MinMajor)
	}
	return nil
}

// snapshotHashBound bounds hashing one snapshot file (and writing a snapshot file's
// next chunk).
const snapshotHashBound = 2 * time.Minute

func (p *peerAPI) release(r *http.Request) (any, error) {
	p.n.snaps.remove(r.PathValue("id"))
	return nil, nil
}

// file streams one snapshot file; a Range header resumes it from an offset. The
// stream is cut once the copy no longer serves peers (its lease lapsed, it was
// unhosted): the recovering node resumes from another source.
func (p *peerAPI) file(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("name")
	return p.n.snaps.use(r.PathValue("id"), p.n.peerValid, func(id store.ShardID, sn *shard.Snapshot) error {
		f, err := sn.Open(name)
		if errors.Is(err, shard.ErrNoSuchFile) {
			return api.NotFound(api.CodeNotFound, "the snapshot has no file %q", name)
		}
		if err != nil {
			return err
		}
		defer f.Close()
		w = &leaseWriter{ResponseWriter: w, rc: http.NewResponseController(w), valid: func() bool { return p.n.peerValid(id) }}
		if p.n.opts.hooks != nil && p.n.opts.hooks.peerFile != nil {
			w = p.n.opts.hooks.peerFile(name, w)
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, name, time.Time{}, f)
		return nil
	})
}

// leaseWriter streams a snapshot file: before each write it moves the write deadline
// on (a big file outlasts the listener's write timeout, a stalled reader does not
// outlast the bound), and it aborts the stream once the copy no longer serves peers.
type leaseWriter struct {
	http.ResponseWriter
	rc    *http.ResponseController
	valid func() bool
}

func (w *leaseWriter) Write(b []byte) (int, error) {
	if !w.valid() {
		panic(http.ErrAbortHandler) // the copy's lease lapsed: drop the connection
	}
	_ = w.rc.SetWriteDeadline(time.Now().Add(peerWriteBound)) //nolint:forbidigo // a connection deadline is by the OS clock
	return w.ResponseWriter.Write(b)
}

// peerWriteBound is how long one write of a snapshot stream may block.
const peerWriteBound = 30 * time.Second

// peerValid reports whether this node's copy of id serves peers now: its lease surely
// holds, it is not quarantined, and it is not paused.
func (n *Node) peerValid(id store.ShardID) bool {
	l := n.leaseFor(id)
	return l != nil && l.valid() && !l.quarantined() && !n.Paused(id)
}

// --- pins and snapshots ---------------------------------------------------------------

// errPinGone is a pin that expired or was released.
var errPinGone = errors.New("cluster: the pin is gone")

// pinTable holds the generations searches pinned on this node for their fetch phase.
//
// A pin outlives its copy's serving: the query phase was admitted while the copy served
// under its lease, and the fetch reads only the generation that phase searched, which
// the pin holds open whatever becomes of the copy. So a copy that pauses, retires, is
// released or closes between a search's phases still answers its fetch. A pin lasts
// until the search releases it, PinTTL passes unused, or the node stops: a stopping
// node retires its copies first and keeps serving for shutdown_grace, so the fetches
// of searches under way drain before Stop drops the pins.
type pinTable struct {
	ttl   time.Duration
	clock clock.Clock
	mu    sync.Mutex
	m     map[string]*pinEntry
}

type pinEntry struct {
	mu      sync.Mutex // held while a fetch uses the target
	t       node.ShardTarget
	expires time.Time
	gone    bool
}

func newPinTable(ttl time.Duration, clk clock.Clock) *pinTable {
	return &pinTable{ttl: ttl, clock: clk, m: map[string]*pinEntry{}}
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (p *pinTable) add(t node.ShardTarget) string {
	pin := newID()
	p.mu.Lock()
	p.m[pin] = &pinEntry{t: t, expires: p.clock.Now().Add(p.ttl)}
	p.mu.Unlock()
	return pin
}

// use runs fn on a pinned target, keeping it pinned meanwhile.
func (p *pinTable) use(pin string, fn func(node.ShardTarget) error) error {
	p.mu.Lock()
	e := p.m[pin]
	if e != nil {
		e.expires = p.clock.Now().Add(p.ttl)
	}
	p.mu.Unlock()
	if e == nil {
		return errPinGone
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.gone {
		return errPinGone
	}
	return fn(e.t)
}

func (p *pinTable) remove(pin string) {
	p.mu.Lock()
	e := p.m[pin]
	delete(p.m, pin)
	p.mu.Unlock()
	if e != nil {
		e.drop()
	}
}

func (e *pinEntry) drop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.gone {
		e.gone = true
		e.t.Release()
	}
}

// sweep releases the pins whose time is up.
func (p *pinTable) sweep() {
	now := p.clock.Now()
	var expired []*pinEntry
	p.mu.Lock()
	for pin, e := range p.m {
		if now.After(e.expires) {
			expired = append(expired, e)
			delete(p.m, pin)
		}
	}
	p.mu.Unlock()
	for _, e := range expired {
		e.drop()
	}
}

func (p *pinTable) closeAll() {
	p.mu.Lock()
	list := make([]*pinEntry, 0, len(p.m))
	for pin, e := range p.m {
		list = append(list, e)
		delete(p.m, pin)
	}
	p.mu.Unlock()
	for _, e := range list {
		e.drop()
	}
}

// snapTable holds the snapshots peers are recovering from. An idle snapshot expires
// after SnapshotTTL, and every snapshot after SnapshotMaxAge however busy (it holds its
// generation's segments on disk): a recovery resumes on a fresh one.
type snapTable struct {
	ttl, maxAge time.Duration
	clock       clock.Clock
	mu          sync.Mutex
	m           map[string]*snapEntry
}

type snapEntry struct {
	id      store.ShardID
	sn      *shard.Snapshot
	created time.Time
	expires time.Time
	users   int
	gone    bool
}

func newSnapTable(ttl, maxAge time.Duration, clk clock.Clock) *snapTable {
	return &snapTable{ttl: ttl, maxAge: maxAge, clock: clk, m: map[string]*snapEntry{}}
}

func (s *snapTable) add(id store.ShardID, sn *shard.Snapshot) string {
	key := newID()
	now := s.clock.Now()
	s.mu.Lock()
	s.m[key] = &snapEntry{id: id, sn: sn, created: now, expires: now.Add(s.ttl)}
	s.mu.Unlock()
	return key
}

// use runs fn on a snapshot, which is kept meanwhile and for SnapshotTTL after, while
// it is younger than SnapshotMaxAge (410 after) and its copy still serves peers (503
// otherwise).
func (s *snapTable) use(key string, valid func(store.ShardID) bool, fn func(store.ShardID, *shard.Snapshot) error) error {
	s.mu.Lock()
	e := s.m[key]
	if e == nil || s.clock.Since(e.created) > s.maxAge {
		s.mu.Unlock()
		return &api.Error{Status: http.StatusGone, Code: api.CodeNotFound, Detail: "the snapshot expired; take another"}
	}
	if !valid(e.id) {
		s.mu.Unlock()
		return api.Unavailable(store.ErrLeaseLost, "this node's copy of %s no longer serves peers", e.id)
	}
	e.users++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		e.users--
		e.expires = s.clock.Now().Add(s.ttl)
		release := e.gone && e.users == 0
		s.mu.Unlock()
		if release {
			e.sn.Release()
		}
	}()
	return fn(e.id, e.sn)
}

func (s *snapTable) remove(key string) {
	s.mu.Lock()
	e := s.m[key]
	delete(s.m, key)
	release := false
	if e != nil {
		e.gone = true
		release = e.users == 0
	}
	s.mu.Unlock()
	if release {
		e.sn.Release()
	}
}

func (s *snapTable) sweep() {
	now := s.clock.Now()
	var release []*shard.Snapshot
	s.mu.Lock()
	for key, e := range s.m {
		if e.users == 0 && (now.After(e.expires) || now.Sub(e.created) > s.maxAge) {
			delete(s.m, key)
			e.gone = true
			release = append(release, e.sn)
		}
	}
	s.mu.Unlock()
	for _, sn := range release {
		sn.Release()
	}
}

func (s *snapTable) closeAll() {
	s.mu.Lock()
	var release []*shard.Snapshot
	for key, e := range s.m {
		delete(s.m, key)
		e.gone = true
		if e.users == 0 {
			release = append(release, e.sn)
		}
	}
	s.mu.Unlock()
	for _, sn := range release {
		sn.Release()
	}
}

// janitorLoop expires pins and snapshots.
func (n *Node) janitorLoop(ctx context.Context) {
	t := n.clock.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		n.pins.sweep()
		n.snaps.sweep()
	}
}

// sumCache remembers the SHA-256 of snapshot files on disk, keyed by their path, size
// and modification time: segment and query segment files never change once written
// (their names are unique), so a file is hashed once whatever the number of
// recoveries. The files a snapshot encodes itself (sidecars, the manifest) are small
// and hashed each time. A copy's entries are dropped when it is unhosted.
type sumCache struct {
	mu sync.Mutex
	m  map[sumKey]sumEntry
}

type sumKey struct {
	path  string
	size  int64
	mtime int64
}

type sumEntry struct {
	shard store.ShardID
	sum   string
}

// maxSums bounds the cache.
const maxSums = 1 << 16

func newSumCache() *sumCache { return &sumCache{m: map[sumKey]sumEntry{}} }

func (c *sumCache) sum(id store.ShardID, sn *shard.Snapshot, f shard.SnapshotFile) (string, error) {
	var key sumKey
	path, onDisk := sn.Path(f.Name)
	if onDisk {
		info, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		key = sumKey{path: path, size: info.Size(), mtime: info.ModTime().UnixNano()}
		c.mu.Lock()
		e, ok := c.m[key]
		c.mu.Unlock()
		if ok && key.size == f.Size {
			return e.sum, nil
		}
	}
	r, err := sn.Open(f.Name)
	if err != nil {
		return "", err
	}
	defer r.Close()
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if onDisk {
		c.mu.Lock()
		if len(c.m) >= maxSums {
			clear(c.m)
		}
		c.m[key] = sumEntry{shard: id, sum: sum}
		c.mu.Unlock()
	}
	return sum, nil
}

// drop forgets the sums of a shard's files.
func (c *sumCache) drop(id store.ShardID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.m {
		if e.shard == id {
			delete(c.m, k)
		}
	}
}
