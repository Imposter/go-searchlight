package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// applyAttempts bounds Apply's retries of transient failures.
const applyAttempts = 3

// sqlStore is the dialect-independent store engine: the transaction shapes
// and protocols, over the statements of its dialect.
type sqlStore struct {
	d      *dialect.Dialect
	pools  dialect.Pools
	w      *gatedDB
	r      *sql.DB
	log    *slog.Logger
	tracer trace.Tracer
	dur    metric.Float64Histogram
	errs   metric.Int64Counter
	attr   attribute.KeyValue
	chunk  int
	closed atomic.Bool

	// beforeCommit, when set (tests only), runs inside Apply's transaction
	// after the changes are written and before COMMIT.
	beforeCommit func(ctx context.Context, first, last int64)

	gateKey     string
	dbPath      string
	stopBg      context.CancelFunc
	bg          sync.WaitGroup
	walGauge    metric.Float64Gauge
	checkpoints atomic.Int64
	truncates   atomic.Int64
	logWarn     atomic.Int64

	reg   *registry
	blobs *blobStore
	idx   *indexStore
}

func newSQLStore(d *dialect.Dialect, pools dialect.Pools, o *options) (*sqlStore, error) {
	in := telemetry.NewInstruments(o.meter)
	s := &sqlStore{
		d: d, pools: pools, w: &gatedDB{DB: pools.Write}, r: pools.Read,
		log:      o.logger.With(slog.String("component", "store"), slog.String("dialect", d.Name)),
		tracer:   o.tracer,
		dur:      in.Histogram(telemetry.MetricStoreOperationDuration),
		errs:     in.Counter(telemetry.MetricStoreErrors),
		attr:     attribute.String("dialect", d.Name),
		chunk:    o.blobChunk,
		dbPath:   o.dbPath,
		walGauge: in.Gauge(telemetry.MetricStoreWALSize),
	}
	if err := in.Err(); err != nil {
		return nil, fmt.Errorf("store instruments: %w", err)
	}
	if pools.Write.Stats().MaxOpenConnections == 1 {
		s.w.g, s.gateKey = acquireGate(o.gateKey), o.gateKey
	}
	s.logWarn.Store(d.LogWarnBytes)
	if d.Checkpoint != "" && d.CheckpointEvery > 0 && d.PendingLog != nil && s.dbPath != "" {
		ctx, cancel := context.WithCancel(context.Background())
		s.stopBg = cancel
		s.bg.Go(func() { s.checkpointLoop(ctx) })
	}
	s.reg = &registry{s: s}
	s.blobs = &blobStore{s: s}
	s.idx = &indexStore{s: s}
	return s, nil
}

// queryer is what statements run on: a pool, a transaction or a connection.
type queryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// returning runs a write the store reads back: one statement taking
// writeArgs where the dialect returns rows from the write, and otherwise the
// write with writeArgs and then the read with readArgs.
func returning(ctx context.Context, q queryer, r dialect.Returning, writeArgs, readArgs []any) (*sql.Rows, error) {
	if r.Write == "" {
		return q.QueryContext(ctx, r.Read, writeArgs...)
	}
	if _, err := q.ExecContext(ctx, r.Write, writeArgs...); err != nil {
		return nil, err
	}
	return q.QueryContext(ctx, r.Read, readArgs...)
}

// returningRow is returning for a write that reads back at most one row.
func returningRow(ctx context.Context, q queryer, r dialect.Returning, writeArgs, readArgs []any) (*sql.Row, error) {
	if r.Write == "" {
		return q.QueryRowContext(ctx, r.Read, writeArgs...), nil
	}
	if _, err := q.ExecContext(ctx, r.Write, writeArgs...); err != nil {
		return nil, err
	}
	return q.QueryRowContext(ctx, r.Read, readArgs...), nil
}

// start opens a span for one public operation and returns a function that
// records its duration and outcome.
func (s *sqlStore) start(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, func(*error)) {
	ctx, span := s.tracer.Start(ctx, "store."+op,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(append(attrs, attribute.String("db.system.name", s.d.Name))...))
	t0 := time.Now()
	return ctx, func(errp *error) {
		set := metric.WithAttributeSet(attribute.NewSet(attribute.String("operation", op), s.attr))
		s.dur.Record(ctx, time.Since(t0).Seconds(), set)
		if err := *errp; err != nil {
			span.RecordError(err)
			if IsTransient(err) {
				span.SetStatus(codes.Error, err.Error())
				s.errs.Add(ctx, 1, set)
			}
		}
		span.End()
	}
}

func (s *sqlStore) retryable(err error) bool {
	return s.d.Retryable != nil && s.d.Retryable(err)
}

func (s *sqlStore) Dialect() string { return s.d.Name }

func (s *sqlStore) Registry() RegistryStore { return s.reg }

func (s *sqlStore) Blobs() BlobStore { return s.blobs }

func (s *sqlStore) Indexes() IndexStore { return s.idx }

func (s *sqlStore) Ping(ctx context.Context) (err error) {
	ctx, end := s.start(ctx, "ping")
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	if s.w.g != nil {
		// One write connection (SQLite): a long commit holds it, which says nothing
		// about whether the database answers. A read connection does.
		return s.r.PingContext(ctx)
	}
	if err := s.w.PingContext(ctx); err != nil {
		return err
	}
	if s.r != s.w.DB {
		return s.r.PingContext(ctx)
	}
	return nil
}

func (s *sqlStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	if s.stopBg != nil {
		s.stopBg()
		s.bg.Wait()
	}
	if s.w.g != nil {
		releaseGate(s.gateKey)
	}
	return s.pools.Close()
}

// checkpointLoop checkpoints the write-ahead log off the write path until ctx ends.
// Every CheckpointEvery it records the log's size and how much of it is pending, and
// runs Checkpoint once CheckpointMinLog is pending. A log copied back in full is
// restarted by the next write, and journal_size_limit cuts its file down then; under
// a steady stream of writes that moment never comes, so once the file has grown past
// TruncateAbove the loop also empties it (truncate). A checkpoint a long reader keeps
// from copying every frame, with the log past LogWarnBytes, is warned about at most
// once a minute.
func (s *sqlStore) checkpointLoop(ctx context.Context) {
	t := time.NewTicker(s.d.CheckpointEvery)
	defer t.Stop()
	var warned time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		size := s.walSize()
		pending, err := s.d.PendingLog(s.dbPath)
		if err != nil {
			pending = s.d.CheckpointMinLog
		}
		s.walGauge.Record(ctx, float64(size), metric.WithAttributes(s.attr, attribute.Bool("pending", false)))
		s.walGauge.Record(ctx, float64(pending), metric.WithAttributes(s.attr, attribute.Bool("pending", true)))
		oversized := pending > 0 && s.d.TruncateCheckpoint != "" && size > s.d.TruncateAbove
		if pending < s.d.CheckpointMinLog && !oversized {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, time.Minute)
		frames, copied, err := s.checkpoint(cctx)
		if err == nil && oversized && copied == frames {
			err = s.truncate(cctx)
		}
		cancel()
		switch {
		case err != nil:
			if ctx.Err() == nil {
				s.log.DebugContext(ctx, "checkpoint failed; retried at the next tick", slog.Any("error", err))
			}
		case copied < frames && size >= s.logWarn.Load() && time.Since(warned) >= time.Minute:
			warned = time.Now()
			s.log.WarnContext(ctx, "the write-ahead log keeps growing: a long read pins it, so checkpoints cannot copy it back",
				slog.Int64("wal_bytes", size), slog.Int64("frames", frames), slog.Int64("checkpointed", copied))
		}
	}
}

// checkpoint runs Checkpoint on a read connection, beside the writers, and returns
// the log's frames and those copied back.
func (s *sqlStore) checkpoint(ctx context.Context) (frames, copied int64, err error) {
	s.checkpoints.Add(1)
	var busy int64
	err = s.r.QueryRowContext(ctx, s.d.Checkpoint).Scan(&busy, &frames, &copied)
	return frames, copied, err
}

// truncateBusyMS bounds how long truncate waits for readers while it holds the write
// connection.
const truncateBusyMS = 50

// truncate empties the log file with TruncateCheckpoint, right after a passive one so
// that little is left to copy. It blocks every writer while it runs, so it takes the
// write connection in the high lane, as soon as the commit in flight ends, leaving
// only that commit's frames to copy (more than CheckpointMinLog: it leaves the log to
// a later tick), and gives up after truncateBusyMS on readers.
func (s *sqlStore) truncate(ctx context.Context) error {
	conn, err := s.w.Conn(withHighLane(ctx))
	if err != nil {
		return err
	}
	defer conn.Close()
	if pending, err := s.d.PendingLog(s.dbPath); err != nil || pending > s.d.CheckpointMinLog {
		return err
	}
	var was int64
	if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&was); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", truncateBusyMS)); err != nil {
		return err
	}
	defer func() {
		if _, err := conn.ExecContext(context.WithoutCancel(ctx), fmt.Sprintf("PRAGMA busy_timeout = %d", was)); err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	s.truncates.Add(1)
	var busy, frames, copied int64
	return conn.QueryRowContext(ctx, s.d.TruncateCheckpoint).Scan(&busy, &frames, &copied)
}

// walSize is the write-ahead log file's size, 0 when there is none.
func (s *sqlStore) walSize() int64 {
	info, err := os.Stat(s.dbPath + "-wal")
	if err != nil {
		return 0
	}
	return info.Size()
}

// rollback ends a transaction that did not commit.
func rollback(tx *sql.Tx) { _ = tx.Rollback() }

// --- Apply ---------------------------------------------------------------

// prepared is a validated batch with its stored payload text. The prepared
// forms of several requests concatenate (GroupCommitter).
type prepared struct {
	payload     []string
	query       []QueryPayload // KindQueryUpsert only
	conditional bool
}

func (p *prepared) append(q *prepared) {
	p.payload = append(p.payload, q.payload...)
	p.query = append(p.query, q.query...)
	p.conditional = p.conditional || q.conditional
}

// prepare validates a batch and reports the first bad change as a
// *ChangeError with its position in batch.
func prepare(batch []Change) (*prepared, error) {
	p := &prepared{payload: make([]string, len(batch)), query: make([]QueryPayload, len(batch))}
	for i := range batch {
		if err := p.prepareOne(i, &batch[i]); err != nil {
			return nil, &ChangeError{Position: i, Err: err}
		}
	}
	return p, nil
}

func (p *prepared) prepareOne(i int, c *Change) error {
	if err := validShard(c.ShardID()); err != nil {
		return err
	}
	if err := validKey("id", c.ID, MaxID); err != nil {
		return err
	}
	if !c.Kind.valid() {
		return invalidf("kind %q", c.Kind)
	}
	if c.IfSeq < IfExists {
		return invalidf("if_seq %d", c.IfSeq)
	}
	if c.IfSeq != 0 {
		p.conditional = true
	}
	switch c.Kind {
	case KindUpsert:
		if size := len(c.ID) + len(c.Payload); size > MaxPayloadBytes {
			return invalidf("the document is %d bytes with its id, more than %d", size, MaxPayloadBytes)
		}
		if err := validJSON("payload", c.Payload); err != nil {
			return err
		}
		p.payload[i] = string(c.Payload)
	case KindQueryUpsert:
		if err := validJSON("payload", c.Payload); err != nil {
			return err
		}
		q, err := DecodeQueryPayload(c.Payload)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrInvalid, err)
		}
		if len(q.Meta) == 0 || string(q.Meta) == "null" {
			q.Meta = json.RawMessage("{}")
		}
		p.query[i] = q
		p.payload[i] = string(c.Payload)
	case KindDelete, KindQueryDelete:
		if len(c.Payload) != 0 {
			return invalidf("%s carries a payload", c.Kind)
		}
	}
	return nil
}

func (s *sqlStore) Apply(ctx context.Context, batch []Change) (first, last int64, err error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	p, err := prepare(batch)
	if err != nil {
		return 0, 0, err
	}
	return s.applyPrepared(ctx, batch, p)
}

// applyPrepared commits a batch prepare has accepted.
func (s *sqlStore) applyPrepared(ctx context.Context, batch []Change, p *prepared) (first, last int64, err error) {
	ctx, end := s.start(ctx, "apply", attribute.Int("changes", len(batch)))
	defer end(&err)
	if s.closed.Load() {
		return 0, 0, ErrClosed
	}
	for attempt := 1; ; attempt++ {
		first, last, err = s.applyOnce(ctx, batch, p)
		if err == nil || attempt == applyAttempts || !s.retryable(err) || ctx.Err() != nil {
			return first, last, err
		}
		s.log.DebugContext(ctx, "retrying apply after a transient failure", slog.Int("attempt", attempt), slog.Any("error", err))
	}
}

// recKey identifies a document or saved query.
type recKey struct {
	query bool
	index string
	shard int
	id    string
}

func keyOf(c *Change) recKey {
	return recKey{query: c.Kind.isQuery(), index: c.Index, shard: c.Shard, id: c.ID}
}

func (s *sqlStore) applyOnce(ctx context.Context, batch []Change, p *prepared) (first, last int64, err error) {
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return 0, 0, err
	}
	committed := false
	defer func() {
		if !committed {
			rollback(tx)
		}
	}()

	// The counter row lock serializes every Apply, so seqs commit in order.
	var counter, nowMs int64
	if err := tx.QueryRowContext(ctx, s.d.Changelog.LockCounter).Scan(&counter, &nowMs); err != nil {
		return 0, 0, fmt.Errorf("lock counter: %w", err)
	}
	for i := range batch {
		batch[i].Seq = 0
	}
	all := batch
	var keptAt []int // the kept changes' positions in all; nil when all are kept

	idx, err := s.checkIndexes(ctx, tx, batch)
	if err != nil {
		return 0, 0, err
	}
	if p.conditional {
		skip, err := s.checkConditions(ctx, tx, batch, counter+1)
		if err != nil {
			return 0, 0, err
		}
		if skip != nil {
			// IfExists misses are dropped here, inside the transaction: the
			// rest takes contiguous seqs.
			kept := make([]Change, 0, len(batch))
			kp := &prepared{conditional: true}
			keptAt = make([]int, 0, len(batch))
			for i := range batch {
				if !skip[i] {
					keptAt = append(keptAt, i)
					kept = append(kept, batch[i])
					kp.payload = append(kp.payload, p.payload[i])
					kp.query = append(kp.query, p.query[i])
				}
			}
			batch, p = kept, kp
		}
	}
	first, last = counter+1, counter+int64(len(batch))
	if len(batch) == 0 {
		return first, last, nil // nothing to write: every change was skipped
	}

	// The changelog. Each row carries the uid of the incarnation of its
	// index as of this Apply, so a tailer can tell a drop-and-recreate apart
	// from a continuing index without an extra query per batch.
	w := &dialect.Write{Changes: make([]dialect.ChangeRow, len(batch)), Counter: last}
	for i := range batch {
		c := &batch[i]
		st := idx[c.Index]
		w.Changes[i] = dialect.ChangeRow{
			Seq: first + int64(i), Index: c.Index, Shard: c.Shard, Kind: string(c.Kind), ID: c.ID, Payload: p.payload[i],
			At: nowMs, IndexUID: st.uid, MappingVersion: st.mappingVersion,
		}
	}
	netState(w, batch, p, first)
	if w.Notify, err = s.notifications(batch, first); err != nil {
		return 0, 0, err
	}
	if err := s.write(ctx, tx, w); err != nil {
		return 0, 0, err
	}
	if s.beforeCommit != nil {
		s.beforeCommit(ctx, first, last)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	committed = true
	for k := range batch {
		i := k
		if keptAt != nil {
			i = keptAt[k]
		}
		all[i].Seq = first + int64(k)
	}
	return first, last, nil
}

// write runs the dialect's statements for w inside tx, which holds the
// counter lock: the changelog rows, the net state, the counter's new value
// and the notifications. A statement text that runs several times in a row
// is prepared once for the run.
func (s *sqlStore) write(ctx context.Context, tx *sql.Tx, w *dialect.Write) error {
	stmts := s.d.Changelog.Write(w, s.d.Changelog.Limits)
	var run *sql.Stmt // prepared for stmts[i].SQL while it repeats
	defer func() {
		if run != nil {
			run.Close()
		}
	}()
	for i, st := range stmts {
		repeats := i+1 < len(stmts) && stmts[i+1].SQL == st.SQL
		var err error
		switch {
		case run != nil:
			_, err = run.ExecContext(ctx, st.Args...)
		case repeats:
			if run, err = tx.PrepareContext(ctx, st.SQL); err == nil {
				_, err = run.ExecContext(ctx, st.Args...)
			}
		default:
			_, err = tx.ExecContext(ctx, st.SQL, st.Args...)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", st.What, err)
		}
		if run != nil && !repeats {
			if err := run.Close(); err != nil {
				return fmt.Errorf("%s: %w", st.What, err)
			}
			run = nil
		}
	}
	return nil
}

// indexState is an index's incarnation and mapping version as of a
// transaction, which every changelog row carries.
type indexState struct {
	uid            string
	mappingVersion int64
}

// checkIndexes fails with an *IndexNotFoundError naming every change whose
// index does not exist, and otherwise returns each named index's current
// incarnation and mapping version, so the changelog row can carry them.
func (s *sqlStore) checkIndexes(ctx context.Context, tx *sql.Tx, batch []Change) (map[string]indexState, error) {
	states := make(map[string]indexState, 1)
	var missing *IndexNotFoundError
	for i := range batch {
		name := batch[i].Index
		st, seen := states[name]
		uid := st.uid
		if !seen {
			var mapping string
			err := tx.QueryRowContext(ctx, s.d.Changelog.IndexState, name).Scan(&st.uid, &st.mappingVersion, &mapping)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				st = indexState{}
			case err != nil:
				return nil, err
			}
			states[name] = st
			uid = st.uid
			if uid == "" {
				if missing == nil {
					missing = &IndexNotFoundError{}
				}
				missing.Indexes = append(missing.Indexes, name)
			}
		}
		if want := batch[i].IndexUID; uid != "" && want != "" && want != uid {
			// The caller meant an incarnation that was dropped: for it, the
			// index is missing.
			if missing == nil {
				missing = &IndexNotFoundError{}
			}
			if !slices.Contains(missing.Indexes, name) {
				missing.Indexes = append(missing.Indexes, name)
			}
			missing.Positions = append(missing.Positions, i)
			continue
		}
		if uid == "" {
			missing.Positions = append(missing.Positions, i)
		}
	}
	if missing != nil {
		return nil, missing
	}
	return states, nil
}

// checkConditions evaluates IfSeq conditions in batch order, as if each
// change before that passed its condition applied, and fails with a
// *ConflictError listing every failure. An IfExists change whose target is
// missing is not a failure: it is reported in skip, and treated as not
// applied by the changes after it.
func (s *sqlStore) checkConditions(ctx context.Context, tx *sql.Tx, batch []Change, first int64) (skip []bool, err error) {
	cur := make(map[recKey]int64)
	var conflict *ConflictError
	for i := range batch {
		c := &batch[i]
		k := keyOf(c)
		if c.IfSeq != 0 {
			seq, ok := cur[k]
			if !ok {
				q := s.d.Changelog.DocumentSeq
				if k.query {
					q = s.d.Changelog.QuerySeq
				}
				err := tx.QueryRowContext(ctx, q, c.Index, c.Shard, c.ID).Scan(&seq)
				switch {
				case errors.Is(err, sql.ErrNoRows):
					seq = 0
				case err != nil:
					return nil, fmt.Errorf("check if_seq: %w", err)
				}
			}
			if c.IfSeq == IfExists && seq == 0 {
				if skip == nil {
					skip = make([]bool, len(batch))
				}
				skip[i] = true
				continue
			}
			if (c.IfSeq == IfAbsent && seq != 0) || (c.IfSeq > 0 && seq != c.IfSeq) {
				if conflict == nil {
					conflict = &ConflictError{}
				}
				conflict.Positions = append(conflict.Positions, i)
				conflict.Current = append(conflict.Current, seq)
				continue // a failed change does not apply
			}
		}
		if c.Kind.isUpsert() {
			cur[k] = first + int64(i)
		} else {
			cur[k] = 0
		}
	}
	if conflict != nil {
		return nil, conflict
	}
	return skip, nil
}

// netState adds the batch's net effect on sl_documents and sl_queries to w:
// the last change to each key wins, so each key appears once.
func netState(w *dialect.Write, batch []Change, p *prepared, first int64) {
	lastPos := make(map[recKey]int, len(batch))
	for i := range batch {
		lastPos[keyOf(&batch[i])] = i
	}
	type group struct {
		query bool
		index string
		shard int
	}
	deletes := make(map[group]int) // the group's position in its delete list
	for i := range batch {
		c := &batch[i]
		k := keyOf(c)
		if lastPos[k] != i {
			continue
		}
		seq := first + int64(i)
		switch c.Kind {
		case KindUpsert:
			w.Documents = append(w.Documents, dialect.DocumentRow{Index: c.Index, Shard: c.Shard, ID: c.ID, Body: p.payload[i], Seq: seq})
		case KindQueryUpsert:
			w.Queries = append(w.Queries, dialect.QueryRow{
				Index: c.Index, Shard: c.Shard, ID: c.ID, Query: string(p.query[i].Query), Meta: string(p.query[i].Meta), Seq: seq,
			})
		case KindDelete, KindQueryDelete:
			groups := &w.DocumentDeletes
			if k.query {
				groups = &w.QueryDeletes
			}
			g := group{query: k.query, index: c.Index, shard: c.Shard}
			at, ok := deletes[g]
			if !ok {
				at = len(*groups)
				deletes[g] = at
				*groups = append(*groups, dialect.DeleteGroup{Index: c.Index, Shard: c.Shard})
			}
			(*groups)[at].IDs = append((*groups)[at].IDs, c.ID)
		}
	}
}

// notifications are the payloads announcing each shard's highest new seq.
// They are sent inside the transaction, so they are delivered exactly when
// it commits. There are none where the dialect cannot listen.
func (s *sqlStore) notifications(batch []Change, first int64) ([]string, error) {
	if s.d.Listen == nil {
		return nil, nil
	}
	top := make(map[ShardID]int64)
	var order []ShardID
	for i := range batch {
		id := batch[i].ShardID()
		if _, ok := top[id]; !ok {
			order = append(order, id)
		}
		top[id] = first + int64(i)
	}
	out := make([]string, 0, len(order))
	for _, id := range order {
		payload, err := json.Marshal(notification{Index: id.Index, Shard: id.Shard, Seq: top[id]})
		if err != nil {
			return nil, err
		}
		out = append(out, string(payload))
	}
	return out, nil
}

type notification struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
	Seq   int64  `json:"seq"`
}

// --- Reads ----------------------------------------------------------------

func (s *sqlStore) ChangesAfter(ctx context.Context, shard ShardID, seq int64, limit int) (out []Change, err error) {
	ctx, end := s.start(ctx, "changes_after", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard))
	defer end(&err)
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := validShard(shard); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = DefaultChangesLimit
	}
	rows, err := s.r.QueryContext(ctx, s.d.Changelog.ChangesAfter, shard.Index, shard.Shard, seq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		c := Change{Index: shard.Index, Shard: shard.Shard}
		var kind string
		var payload []byte
		var at int64
		if err := rows.Scan(&c.Seq, &kind, &c.ID, &payload, &at, &c.IndexUID, &c.MappingVersion); err != nil {
			return nil, err
		}
		c.Kind = Kind(kind)
		if len(payload) > 0 {
			c.Payload = payload
		}
		c.At = millis(at)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Read the prune horizon after the changes: a prune that committed
	// before the read above is visible here, and one that commits later did
	// not remove anything that read returned.
	var below int64
	err = s.r.QueryRowContext(ctx, s.d.Changelog.Horizon, shard.Index, shard.Shard).Scan(&below)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return nil, err
	case seq < below-1:
		return nil, fmt.Errorf("%s after seq %d (pruned below %d): %w", shard, seq, below, ErrPruned)
	}
	return out, nil
}

func (s *sqlStore) ScanShard(ctx context.Context, shard ShardID, fn func(Record) error) (asOf int64, err error) {
	ctx, end := s.start(ctx, "scan_shard", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard))
	defer end(&err)
	if s.closed.Load() {
		return 0, ErrClosed
	}
	if err := validShard(shard); err != nil {
		return 0, err
	}
	tx, err := s.r.BeginTx(ctx, s.d.SnapshotTx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	// The counter is read first, in the same snapshot as the rows.
	if err := tx.QueryRowContext(ctx, s.d.Changelog.ReadCounter).Scan(&asOf); err != nil {
		return 0, err
	}
	// The index's current incarnation and mapping, read once in this scan's
	// snapshot: the mapping is the first record, and the incarnation and
	// mapping version are stamped on every record.
	var uid string
	var mv int64
	var mapping []byte
	switch err := tx.QueryRowContext(ctx, s.d.Changelog.IndexState, shard.Index).Scan(&uid, &mv, &mapping); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return 0, err
	default:
		r := Record{Kind: RecordMapping, Index: shard.Index, Shard: shard.Shard, Body: mapping, IndexUID: uid, MappingVersion: mv}
		if err := fn(r); err != nil {
			return 0, err
		}
	}
	if err := scanRows(ctx, tx, s.d.Changelog.ScanDocuments, shard, func(rows *sql.Rows) (Record, error) {
		r := Record{Kind: RecordDocument, Index: shard.Index, Shard: shard.Shard, IndexUID: uid, MappingVersion: mv}
		return r, rows.Scan(&r.ID, &r.Body, &r.Seq)
	}, fn); err != nil {
		return 0, err
	}
	if err := scanRows(ctx, tx, s.d.Changelog.ScanQueries, shard, func(rows *sql.Rows) (Record, error) {
		r := Record{Kind: RecordQuery, Index: shard.Index, Shard: shard.Shard, IndexUID: uid, MappingVersion: mv}
		return r, rows.Scan(&r.ID, &r.Body, &r.Meta, &r.Seq)
	}, fn); err != nil {
		return 0, err
	}
	return asOf, tx.Commit()
}

func scanRows(ctx context.Context, tx *sql.Tx, q string, shard ShardID, scan func(*sql.Rows) (Record, error), fn func(Record) error) error {
	rows, err := tx.QueryContext(ctx, q, shard.Index, shard.Shard)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		r, err := scan(rows)
		if err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *sqlStore) HeadSeq(ctx context.Context) (seq int64, now time.Time, err error) {
	ctx, end := s.start(ctx, "head_seq")
	defer end(&err)
	if s.closed.Load() {
		return 0, time.Time{}, ErrClosed
	}
	var nowMs int64
	err = s.r.QueryRowContext(ctx, s.d.Changelog.ReadHead).Scan(&seq, &nowMs)
	return seq, millis(nowMs), err
}

// --- Pruning ---------------------------------------------------------------

// pruneStep bounds the seq range one prune DELETE covers, so no single
// statement (or SQLite write lock) runs long.
const pruneStep = 5000

func (s *sqlStore) Prune(ctx context.Context, shard ShardID, belowSeq int64) (err error) {
	ctx, end := s.start(ctx, "prune", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard))
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	if err := validShard(shard); err != nil {
		return err
	}
	if belowSeq <= 1 {
		return nil
	}
	below, err := s.raiseHorizon(ctx, shard, belowSeq)
	if err != nil {
		return err
	}
	// The horizon is published, so ChangesAfter already refuses the range;
	// delete it in short statements.
	m := &s.d.Maintenance
	for {
		var lo sql.NullInt64
		if err := s.r.QueryRowContext(ctx, m.MinSeq, shard.Index, shard.Shard, below).Scan(&lo); err != nil {
			return err
		}
		if !lo.Valid {
			return nil
		}
		hi := min(lo.Int64+pruneStep, below)
		if _, err := s.w.ExecContext(ctx, m.DeleteChanges, shard.Index, shard.Shard, lo.Int64, hi); err != nil {
			return err
		}
	}
}

// raiseHorizon records belowSeq (clamped to the next seq) as the shard's
// prune horizon unless it is already higher, and returns the horizon. It
// takes the counter lock so it is serialized with Apply and other prunes.
func (s *sqlStore) raiseHorizon(ctx context.Context, shard ShardID, belowSeq int64) (int64, error) {
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)
	var counter, nowMs int64
	if err := tx.QueryRowContext(ctx, s.d.Changelog.LockCounter).Scan(&counter, &nowMs); err != nil {
		return 0, err
	}
	belowSeq = min(belowSeq, counter+1)
	row, err := returningRow(ctx, tx, s.d.Maintenance.RaiseHorizon,
		[]any{shard.Index, shard.Shard, belowSeq}, []any{shard.Index, shard.Shard})
	if err != nil {
		return 0, err
	}
	if err := row.Scan(&belowSeq); err != nil {
		return 0, err
	}
	return belowSeq, tx.Commit()
}

// --- Notifications -----------------------------------------------------------

// watchingStore adds Watch on dialects with notifications.
type watchingStore struct{ *sqlStore }

func (s *watchingStore) Watch(ctx context.Context, ready func(), fn func(Notification)) error {
	if s.closed.Load() {
		return ErrClosed
	}
	conn, err := s.r.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return s.d.Listen(ctx, conn, ready, func(payload string) {
		var n notification
		if err := json.Unmarshal([]byte(payload), &n); err != nil {
			s.log.WarnContext(ctx, "ignoring a malformed change notification", slog.String("payload", payload))
			return
		}
		fn(Notification{Shard: ShardID{Index: n.Index, Shard: n.Shard}, Seq: n.Seq})
	})
}
