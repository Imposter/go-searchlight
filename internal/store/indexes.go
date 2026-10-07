package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// indexStore implements IndexStore over sl_indexes.
type indexStore struct{ s *sqlStore }

// newIndexUID mints a fresh incarnation id for a created or recreated index.
func newIndexUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func validMeta(m *IndexMeta) error {
	if err := validKey("index name", m.Name, MaxIndexName); err != nil {
		return err
	}
	if len(m.Mapping) == 0 {
		m.Mapping = []byte("{}")
	}
	if len(m.Settings) == 0 {
		m.Settings = []byte("{}")
	}
	if err := validJSON("mapping", m.Mapping); err != nil {
		return err
	}
	if err := validJSON("settings", m.Settings); err != nil {
		return err
	}
	_, err := m.Shards()
	return err
}

func (x *indexStore) Create(ctx context.Context, m IndexMeta) (out IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_create", attribute.String("index", m.Name))
	defer end(&err)
	if err := validMeta(&m); err != nil {
		return out, err
	}
	uid, err := newIndexUID()
	if err != nil {
		return out, err
	}
	create := s.d.Indexes.Create
	row, err := returningRow(ctx, s.w, create, []any{m.Name, string(m.Mapping), string(m.Settings), uid}, []any{m.Name})
	if err == nil {
		out, err = scanIndex(row, m.Name)
		if err == nil || create.Write != "" {
			return out, err // the insert is done; this is the read's outcome
		}
	}
	// The insert failed. A key violation looks different on every dialect;
	// look instead.
	if _, gerr := x.get(ctx, m.Name); gerr == nil {
		return IndexMeta{}, fmt.Errorf("index %q: %w", m.Name, ErrExists)
	}
	return IndexMeta{}, err
}

// scanIndex reads the dialect's IndexColumns.
func scanIndex(row interface{ Scan(...any) error }, name string) (IndexMeta, error) {
	m := IndexMeta{Name: name}
	var created int64
	err := row.Scan(&m.Mapping, &m.Settings, &m.Version, &created, &m.UID, &m.MappingVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return m, fmt.Errorf("index %q: %w", name, ErrNotFound)
	}
	m.CreatedAt = millis(created)
	return m, err
}

func (x *indexStore) get(ctx context.Context, name string) (IndexMeta, error) {
	return scanIndex(x.s.r.QueryRowContext(ctx, x.s.d.Indexes.Get, name), name)
}

func (x *indexStore) Get(ctx context.Context, name string) (m IndexMeta, err error) {
	ctx, end := x.s.start(ctx, "index_get", attribute.String("index", name))
	defer end(&err)
	return x.get(ctx, name)
}

func (x *indexStore) List(ctx context.Context) (out []IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_list")
	defer end(&err)
	rows, err := s.r.QueryContext(ctx, s.d.Indexes.List)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var m IndexMeta
		var created int64
		if err := rows.Scan(&m.Name, &m.Mapping, &m.Settings, &m.Version, &created, &m.UID, &m.MappingVersion); err != nil {
			return nil, err
		}
		m.CreatedAt = millis(created)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (x *indexStore) Update(ctx context.Context, m IndexMeta) (out IndexMeta, err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_update", attribute.String("index", m.Name))
	defer end(&err)
	if err := validMeta(&m); err != nil {
		return out, err
	}
	if s.closed.Load() {
		return out, ErrClosed
	}
	for attempt := 1; ; attempt++ {
		err = x.updateOnce(ctx, &m)
		if err == nil || attempt == applyAttempts || !s.retryable(err) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return out, err
	}
	return x.get(ctx, m.Name)
}

// updateOnce runs one attempt of Update. It takes the counter lock, like Apply,
// so a mapping change commits wholly before or after any Apply: every change
// committed after it carries its mapping version and follows it in the log.
func (x *indexStore) updateOnce(ctx context.Context, m *IndexMeta) error {
	s := x.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	var counter, nowMs int64
	if err := tx.QueryRowContext(ctx, s.d.Changelog.LockCounter).Scan(&counter, &nowMs); err != nil {
		return fmt.Errorf("lock counter: %w", err)
	}
	var oldMapping, oldSettings []byte
	var version, mappingVersion int64
	var uid string
	err = tx.QueryRowContext(ctx, s.d.Indexes.Current, m.Name).
		Scan(&oldMapping, &oldSettings, &version, &mappingVersion, &uid)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("index %q: %w", m.Name, ErrNotFound)
	}
	if err != nil {
		return err
	}
	if version != m.Version {
		return fmt.Errorf("index %q version %d: %w", m.Name, m.Version, ErrConflict)
	}
	// The shard count is fixed at creation (spec section 2): documents are placed
	// by it, so changing it would orphan them.
	oldShards, err := settingsShards(oldSettings)
	if err != nil {
		return err
	}
	if newShards, err := m.Shards(); err != nil {
		return err
	} else if newShards != oldShards {
		return invalidf("settings: shards is %d and cannot change (to %d)", oldShards, newShards)
	}
	remap := !bytes.Equal(oldMapping, m.Mapping)
	if remap {
		mappingVersion++
	}
	if _, err := tx.ExecContext(ctx, s.d.Indexes.Update, string(m.Mapping), string(m.Settings), mappingVersion, m.Name, m.Version); err != nil {
		return err
	}
	if remap {
		shards, err := m.Shards()
		if err != nil {
			return err
		}
		batch := make([]Change, shards)
		w := &dialect.Write{Changes: make([]dialect.ChangeRow, shards), Counter: counter + int64(shards)}
		for sh := range shards {
			batch[sh] = Change{Index: m.Name, Shard: sh, Kind: KindMapping, ID: MappingChangeID}
			w.Changes[sh] = dialect.ChangeRow{
				Seq: counter + 1 + int64(sh), Index: m.Name, Shard: sh, Kind: string(KindMapping), ID: MappingChangeID,
				Payload: string(m.Mapping), At: nowMs, IndexUID: uid, MappingVersion: mappingVersion,
			}
		}
		if w.Notify, err = s.notifications(batch, counter+1); err != nil {
			return err
		}
		if err := s.write(ctx, tx, w); err != nil {
			return fmt.Errorf("log mapping changes: %w", err)
		}
	}
	return tx.Commit()
}

func (x *indexStore) Drop(ctx context.Context, name string) (err error) {
	s := x.s
	ctx, end := s.start(ctx, "index_drop", attribute.String("index", name))
	defer end(&err)
	if err := validKey("index name", name, MaxIndexName); err != nil {
		return err
	}
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// Taking the counter lock orders the drop with Apply: no change for the
	// index commits after it.
	var counter, nowMs int64
	if err := tx.QueryRowContext(ctx, s.d.Changelog.LockCounter).Scan(&counter, &nowMs); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, s.d.Indexes.Drop, name)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("index %q: %w", name, ErrNotFound)
	}
	for _, q := range s.d.Indexes.DropData {
		if _, err := tx.ExecContext(ctx, q, name); err != nil {
			return fmt.Errorf("drop index %q: %w", name, err)
		}
	}
	return tx.Commit()
}
