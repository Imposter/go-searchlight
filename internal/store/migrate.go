package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// migration is one embedded NNNN_name.sql file.
type migration struct {
	version int
	name    string
	stmts   []string
}

// loadMigrations reads a dialect's migrations in version order.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return nil, err
	}
	out := make([]migration, 0, len(names))
	seen := make(map[int]string, len(names))
	for _, name := range names {
		base := strings.TrimSuffix(path.Base(name), ".sql")
		num, _, ok := strings.Cut(base, "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migration %s: name must be NNNN_description.sql", name)
		}
		if prev, dup := seen[v]; dup {
			return nil, fmt.Errorf("migrations %s and %s share version %d", prev, name, v)
		}
		seen[v] = name
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return nil, err
		}
		out = append(out, migration{version: v, name: base, stmts: splitStatements(string(body))})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// splitStatements splits a migration on semicolons that end a line, after
// dropping -- comment lines.
func splitStatements(body string) []string {
	var out []string
	var cur strings.Builder
	for _, line := range strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n") {
		t := strings.TrimSpace(line)
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		if strings.HasSuffix(t, ";") {
			cur.WriteString(strings.TrimSuffix(line, ";"))
			if st := strings.TrimSpace(cur.String()); st != "" {
				out = append(out, st)
			}
			cur.Reset()
			continue
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
	}
	if st := strings.TrimSpace(cur.String()); st != "" {
		out = append(out, st)
	}
	return out
}

// execer is what migrations run on: a transaction or a connection.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Migration retries: a migration whose DDL timed out waiting for a table lock
// (Maintenance.MigrateLockTimeout) is tried again, up to migrateAttempts times, after
// a pause growing from migrateRetryBase to migrateRetryCap.
const (
	migrateAttempts  = 10
	migrateRetryBase = time.Second
	migrateRetryCap  = 10 * time.Second
)

func (s *sqlStore) Migrate(ctx context.Context) (err error) {
	ctx, end := s.start(ctx, "migrate")
	defer end(&err)
	if s.closed.Load() {
		return ErrClosed
	}
	ms, err := loadMigrations(s.d.Migrations)
	if err != nil {
		return err
	}
	timedOut := s.d.Maintenance.LockTimedOut
	wait := migrateRetryBase
	for attempt := 1; ; attempt++ {
		err = s.migrateOnce(ctx, ms)
		if err == nil || timedOut == nil || !timedOut(err) || attempt == migrateAttempts {
			return err
		}
		s.log.WarnContext(ctx, "a migration timed out waiting for a table lock that queries hold; retrying",
			slog.Int("attempt", attempt), slog.Duration("wait", wait), slog.Any("error", err))
		if err := s.clock.Sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(2*wait, migrateRetryCap)
	}
}

// migrateOnce applies the pending migrations under the migration lock.
func (s *sqlStore) migrateOnce(ctx context.Context, ms []migration) error {
	m := &s.d.Maintenance
	if m.MigrateInTx {
		tx, err := s.w.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer rollback(tx)
		if m.MigrateLock != "" {
			if _, err := tx.ExecContext(ctx, m.MigrateLock); err != nil {
				return fmt.Errorf("migration lock: %w", err)
			}
		}
		if m.MigrateLockTimeout != "" {
			if _, err := tx.ExecContext(ctx, m.MigrateLockTimeout); err != nil {
				return fmt.Errorf("migration lock timeout: %w", err)
			}
		}
		if err := s.migrateOn(ctx, tx, ms, false); err != nil {
			return err
		}
		return tx.Commit()
	}

	conn, err := s.w.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, m.SessionLock).Scan(&got); err != nil {
		return fmt.Errorf("migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return errors.New("migration lock: timed out waiting for another node's migration")
	}
	defer func() {
		var released sql.NullInt64
		_ = conn.QueryRowContext(context.WithoutCancel(ctx), m.SessionUnlock).Scan(&released)
	}()
	if m.MigrateLockTimeout != "" {
		if _, err := conn.ExecContext(ctx, m.MigrateLockTimeout); err != nil {
			return fmt.Errorf("migration lock timeout: %w", err)
		}
		defer func() {
			if _, err := conn.ExecContext(context.WithoutCancel(ctx), m.MigrateLockReset); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn }) // not back into the pool
			}
		}()
	}
	return s.migrateOn(ctx, conn, ms, true)
}

// migrateOn applies the pending migrations, recording each version as it
// completes. resumable says a migration may have been interrupted part way (DDL is
// not transactional): a statement failing only because it was applied then is
// passed over.
func (s *sqlStore) migrateOn(ctx context.Context, ex execer, ms []migration, resumable bool) error {
	if _, err := ex.ExecContext(ctx, s.d.Maintenance.VersionTable); err != nil {
		return fmt.Errorf("create version table: %w", err)
	}
	rows, err := ex.QueryContext(ctx, s.d.Maintenance.AppliedMigrations)
	if err != nil {
		return err
	}
	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return err
		}
		applied[v] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	known := 0
	if len(ms) > 0 {
		known = ms[len(ms)-1].version
	}
	for v := range applied {
		if v > known {
			return fmt.Errorf("%w: it has migration %d, this binary knows up to %d", ErrNewerSchema, v, known)
		}
	}
	record := s.d.Maintenance.RecordMigration
	for _, m := range ms {
		if applied[m.version] {
			continue
		}
		applied := s.d.Maintenance.AlreadyApplied
		for i, st := range m.stmts {
			if _, err := ex.ExecContext(ctx, st); err != nil && (!resumable || applied == nil || !applied(err)) {
				return fmt.Errorf("migration %s statement %d: %w", m.name, i+1, err)
			}
		}
		if _, err := ex.ExecContext(ctx, record, m.version, m.name); err != nil {
			return fmt.Errorf("record migration %s: %w", m.name, err)
		}
		s.log.InfoContext(ctx, "applied migration", slog.Int("version", m.version), slog.String("name", m.name))
	}
	return nil
}
