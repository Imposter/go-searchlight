package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// notifyChannel is the Postgres NOTIFY channel Apply announces commits on.
const notifyChannel = "searchlight_changes"

// maxRowsPerInsert caps the rows of one multi-row INSERT.
const maxRowsPerInsert = 500

// applyAttempts bounds Apply's retries of transient failures.
const applyAttempts = 3

// sqlStore is the dialect-independent store engine.
type sqlStore struct {
	d      *dialect.Dialect
	pools  dialect.Pools
	w, r   *sql.DB
	q      queries
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

	reg   *registry
	blobs *blobStore
	idx   *indexStore
}

// queries are the fixed statements, bound to the dialect's placeholders.
type queries struct {
	lockCounter, updateCounter, readCounter string
	readHead                                string
	readEpoch                               string
	changesAfter, horizon                   string
	docSeq, querySeq                        string
	scanDocs, scanQueries                   string
	indexUID                                string
	indexState                              string
}

func newSQLStore(d *dialect.Dialect, pools dialect.Pools, o *options) (*sqlStore, error) {
	in := telemetry.NewInstruments(o.meter)
	s := &sqlStore{
		d: d, pools: pools, w: pools.Write, r: pools.Read,
		log:    o.logger.With(slog.String("component", "store"), slog.String("dialect", d.Name)),
		tracer: o.tracer,
		dur:    in.Histogram(telemetry.MetricStoreOperationDuration),
		errs:   in.Counter(telemetry.MetricStoreErrors),
		attr:   attribute.String("dialect", d.Name),
		chunk:  o.blobChunk,
	}
	if err := in.Err(); err != nil {
		return nil, fmt.Errorf("store instruments: %w", err)
	}
	s.q = queries{
		lockCounter:   s.bind("SELECT value, " + d.Now + " FROM sl_counter WHERE id = 1" + d.ForUpdate),
		updateCounter: s.bind("UPDATE sl_counter SET value = ? WHERE id = 1"),
		readCounter:   s.bind("SELECT value FROM sl_counter WHERE id = 1"),
		readHead:      s.bind("SELECT value, " + d.Now + " FROM sl_counter WHERE id = 1"),
		readEpoch:     "SELECT value FROM sl_counter WHERE id = 2",
		changesAfter: s.bind(`SELECT seq, kind, id, payload, at, index_uid, mapping_version FROM sl_changes
WHERE index_name = ? AND shard = ? AND seq > ? ORDER BY seq LIMIT ?`),
		horizon:     s.bind("SELECT below_seq FROM sl_pruned WHERE index_name = ? AND shard = ?"),
		docSeq:      s.bind("SELECT seq FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?"),
		querySeq:    s.bind("SELECT seq FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?"),
		scanDocs:    s.bind("SELECT id, body, seq FROM sl_documents WHERE index_name = ? AND shard = ? ORDER BY id"),
		scanQueries: s.bind("SELECT id, query, meta, seq FROM sl_queries WHERE index_name = ? AND shard = ? ORDER BY id"),
		indexUID:    s.bind("SELECT uid FROM sl_indexes WHERE name = ?"),
		indexState:  s.bind("SELECT uid, mapping_version, mapping FROM sl_indexes WHERE name = ?"),
	}
	s.reg = &registry{s: s}
	s.blobs = &blobStore{s: s}
	s.idx = &indexStore{s: s}
	return s, nil
}

// bind rewrites ? placeholders for dialects that number them. Statements
// never contain a literal question mark.
func (s *sqlStore) bind(q string) string {
	if !s.d.Dollar || !strings.Contains(q, "?") {
		return q
	}
	var b strings.Builder
	b.Grow(len(q) + 16)
	n := 0
	for i := 0; i < len(q); i++ {
		if q[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(q[i])
	}
	return b.String()
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
	if err := s.w.PingContext(ctx); err != nil {
		return err
	}
	if s.r != s.w {
		return s.r.PingContext(ctx)
	}
	return nil
}

func (s *sqlStore) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	return s.pools.Close()
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
	if c.IfSeq < IfAbsent {
		return invalidf("if_seq %d", c.IfSeq)
	}
	if c.IfSeq != 0 {
		p.conditional = true
	}
	switch c.Kind {
	case KindUpsert:
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
	if err := tx.QueryRowContext(ctx, s.q.lockCounter).Scan(&counter, &nowMs); err != nil {
		return 0, 0, fmt.Errorf("lock counter: %w", err)
	}
	first, last = counter+1, counter+int64(len(batch))

	idx, err := s.checkIndexes(ctx, tx, batch)
	if err != nil {
		return 0, 0, err
	}
	if p.conditional {
		if err := s.checkConditions(ctx, tx, batch, first); err != nil {
			return 0, 0, err
		}
	}

	// The changelog. Each row carries the uid of the incarnation of its
	// index as of this Apply, so a tailer can tell a drop-and-recreate apart
	// from a continuing index without an extra query per batch.
	args := make([]any, 0, len(batch)*9)
	for i := range batch {
		c := &batch[i]
		st := idx[c.Index]
		args = append(args, first+int64(i), c.Index, c.Shard, string(c.Kind), c.ID, p.payload[i], nowMs, st.uid, st.mappingVersion)
	}
	if err := s.insertRows(ctx, tx, insertChanges, 9, args, ""); err != nil {
		return 0, 0, fmt.Errorf("insert changes: %w", err)
	}

	if err := s.applyState(ctx, tx, batch, p, first); err != nil {
		return 0, 0, err
	}

	if _, err := tx.ExecContext(ctx, s.q.updateCounter, last); err != nil {
		return 0, 0, fmt.Errorf("advance counter: %w", err)
	}
	if s.d.Notify != "" {
		if err := s.notify(ctx, tx, batch, first); err != nil {
			return 0, 0, err
		}
	}
	if s.beforeCommit != nil {
		s.beforeCommit(ctx, first, last)
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit: %w", err)
	}
	committed = true
	return first, last, nil
}

// insertChanges is the changelog insert; rows have 9 columns.
const insertChanges = "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES "

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
			err := tx.QueryRowContext(ctx, s.q.indexState, name).Scan(&st.uid, &st.mappingVersion, &mapping)
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
// *ConflictError listing every failure.
func (s *sqlStore) checkConditions(ctx context.Context, tx *sql.Tx, batch []Change, first int64) error {
	cur := make(map[recKey]int64)
	var conflict *ConflictError
	for i := range batch {
		c := &batch[i]
		k := keyOf(c)
		if c.IfSeq != 0 {
			seq, ok := cur[k]
			if !ok {
				q := s.q.docSeq
				if k.query {
					q = s.q.querySeq
				}
				err := tx.QueryRowContext(ctx, q, c.Index, c.Shard, c.ID).Scan(&seq)
				switch {
				case errors.Is(err, sql.ErrNoRows):
					seq = 0
				case err != nil:
					return fmt.Errorf("check if_seq: %w", err)
				}
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
		return conflict
	}
	return nil
}

// applyState writes the batch's net effect on sl_documents and sl_queries:
// the last change to each key wins.
func (s *sqlStore) applyState(ctx context.Context, tx *sql.Tx, batch []Change, p *prepared, first int64) error {
	lastPos := make(map[recKey]int, len(batch))
	for i := range batch {
		lastPos[keyOf(&batch[i])] = i
	}
	var docArgs, queryArgs []any
	type group struct {
		query bool
		index string
		shard int
	}
	deletes := make(map[group][]any)
	var order []group
	for i := range batch {
		c := &batch[i]
		k := keyOf(c)
		if lastPos[k] != i {
			continue
		}
		seq := first + int64(i)
		switch c.Kind {
		case KindUpsert:
			docArgs = append(docArgs, c.Index, c.Shard, c.ID, p.payload[i], seq)
		case KindQueryUpsert:
			queryArgs = append(queryArgs, c.Index, c.Shard, c.ID, string(p.query[i].Query), string(p.query[i].Meta), seq)
		case KindDelete, KindQueryDelete:
			g := group{query: k.query, index: c.Index, shard: c.Shard}
			if _, ok := deletes[g]; !ok {
				order = append(order, g)
			}
			deletes[g] = append(deletes[g], c.ID)
		}
	}
	if len(docArgs) > 0 {
		err := s.insertRows(ctx, tx, "INSERT INTO sl_documents (index_name, shard, id, body, seq) VALUES ", 5, docArgs,
			s.d.Upsert([]string{"index_name", "shard", "id"}, []string{"body", "seq"}))
		if err != nil {
			return fmt.Errorf("upsert documents: %w", err)
		}
	}
	if len(queryArgs) > 0 {
		err := s.insertRows(ctx, tx, "INSERT INTO sl_queries (index_name, shard, id, query, meta, seq) VALUES ", 6, queryArgs,
			s.d.Upsert([]string{"index_name", "shard", "id"}, []string{"query", "meta", "seq"}))
		if err != nil {
			return fmt.Errorf("upsert queries: %w", err)
		}
	}
	for _, g := range order {
		table := "sl_documents"
		if g.query {
			table = "sl_queries"
		}
		ids := deletes[g]
		per := min(maxRowsPerInsert, s.d.MaxParams-2)
		for start := 0; start < len(ids); start += per {
			part := ids[start:min(start+per, len(ids))]
			q := "DELETE FROM " + table + " WHERE index_name = ? AND shard = ? AND id IN (" +
				strings.TrimSuffix(strings.Repeat("?, ", len(part)), ", ") + ")"
			args := append([]any{g.index, g.shard}, part...)
			if _, err := tx.ExecContext(ctx, s.bind(q), args...); err != nil {
				return fmt.Errorf("delete from %s: %w", table, err)
			}
		}
	}
	return nil
}

// insertRows runs prefix + (?, ...), (?, ...) ... + suffix in chunks of at
// most maxRowsPerInsert rows; args holds ncols values per row.
func (s *sqlStore) insertRows(ctx context.Context, tx *sql.Tx, prefix string, ncols int, args []any, suffix string) error {
	rows := len(args) / ncols
	per := min(maxRowsPerInsert, s.d.MaxParams/ncols)
	row := "(" + strings.TrimSuffix(strings.Repeat("?, ", ncols), ", ") + ")"
	for start := 0; start < rows; start += per {
		end := min(start+per, rows)
		var b strings.Builder
		b.Grow(len(prefix) + (end-start)*(len(row)+2) + len(suffix))
		b.WriteString(prefix)
		for i := start; i < end; i++ {
			if i > start {
				b.WriteString(", ")
			}
			b.WriteString(row)
		}
		b.WriteString(suffix)
		if _, err := tx.ExecContext(ctx, s.bind(b.String()), args[start*ncols:end*ncols]...); err != nil {
			return err
		}
	}
	return nil
}

// notify announces each shard's highest new seq inside the transaction, so
// the notifications are delivered exactly when it commits.
func (s *sqlStore) notify(ctx context.Context, tx *sql.Tx, batch []Change, first int64) error {
	top := make(map[ShardID]int64)
	var order []ShardID
	for i := range batch {
		id := batch[i].ShardID()
		if _, ok := top[id]; !ok {
			order = append(order, id)
		}
		top[id] = first + int64(i)
	}
	q := s.bind(s.d.Notify)
	for _, id := range order {
		payload, err := json.Marshal(notification{Index: id.Index, Shard: id.Shard, Seq: top[id]})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, q, notifyChannel, string(payload)); err != nil {
			return fmt.Errorf("notify: %w", err)
		}
	}
	return nil
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
	rows, err := s.r.QueryContext(ctx, s.q.changesAfter, shard.Index, shard.Shard, seq, limit)
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
	err = s.r.QueryRowContext(ctx, s.q.horizon, shard.Index, shard.Shard).Scan(&below)
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
	if err := tx.QueryRowContext(ctx, s.q.readCounter).Scan(&asOf); err != nil {
		return 0, err
	}
	// The index's current incarnation and mapping, read once in this scan's
	// snapshot: the mapping is the first record, and the incarnation and
	// mapping version are stamped on every record.
	var uid string
	var mv int64
	var mapping []byte
	switch err := tx.QueryRowContext(ctx, s.q.indexState, shard.Index).Scan(&uid, &mv, &mapping); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return 0, err
	default:
		r := Record{Kind: RecordMapping, Index: shard.Index, Shard: shard.Shard, Body: mapping, IndexUID: uid, MappingVersion: mv}
		if err := fn(r); err != nil {
			return 0, err
		}
	}
	if err := scanRows(ctx, tx, s.q.scanDocs, shard, func(rows *sql.Rows) (Record, error) {
		r := Record{Kind: RecordDocument, Index: shard.Index, Shard: shard.Shard, IndexUID: uid, MappingVersion: mv}
		return r, rows.Scan(&r.ID, &r.Body, &r.Seq)
	}, fn); err != nil {
		return 0, err
	}
	if err := scanRows(ctx, tx, s.q.scanQueries, shard, func(rows *sql.Rows) (Record, error) {
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
	err = s.r.QueryRowContext(ctx, s.q.readHead).Scan(&seq, &nowMs)
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
	minQ := s.bind("SELECT MIN(seq) FROM sl_changes WHERE index_name = ? AND shard = ? AND seq < ?")
	delQ := s.bind("DELETE FROM sl_changes WHERE index_name = ? AND shard = ? AND seq >= ? AND seq < ?")
	for {
		var lo sql.NullInt64
		if err := s.r.QueryRowContext(ctx, minQ, shard.Index, shard.Shard, below).Scan(&lo); err != nil {
			return err
		}
		if !lo.Valid {
			return nil
		}
		hi := min(lo.Int64+pruneStep, below)
		if _, err := s.w.ExecContext(ctx, delQ, shard.Index, shard.Shard, lo.Int64, hi); err != nil {
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
	if err := tx.QueryRowContext(ctx, s.q.lockCounter).Scan(&counter, &nowMs); err != nil {
		return 0, err
	}
	belowSeq = min(belowSeq, counter+1)
	var cur int64
	err = tx.QueryRowContext(ctx, s.q.horizon, shard.Index, shard.Shard).Scan(&cur)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, s.bind("INSERT INTO sl_pruned (index_name, shard, below_seq) VALUES (?, ?, ?)"),
			shard.Index, shard.Shard, belowSeq)
	case err != nil:
	case cur >= belowSeq:
		belowSeq = cur
	default:
		_, err = tx.ExecContext(ctx, s.bind("UPDATE sl_pruned SET below_seq = ? WHERE index_name = ? AND shard = ?"),
			belowSeq, shard.Index, shard.Shard)
	}
	if err != nil {
		return 0, err
	}
	return belowSeq, tx.Commit()
}

// --- Postgres notifications --------------------------------------------------

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
	return s.d.Listen(ctx, conn, notifyChannel, ready, func(payload string) {
		var n notification
		if err := json.Unmarshal([]byte(payload), &n); err != nil {
			s.log.WarnContext(ctx, "ignoring a malformed change notification", slog.String("payload", payload))
			return
		}
		fn(Notification{Shard: ShardID{Index: n.Index, Shard: n.Shard}, Seq: n.Seq})
	})
}
