// Package postgres is the PostgreSQL dialect of the SQL store, on
// jackc/pgx/v5 through database/sql.
//
// Apply locks the counter row with SELECT ... FOR UPDATE, so sequence numbers
// are assigned and committed in order. Migrations run in one transaction under
// a transaction-scoped advisory lock. Committed changes are announced with
// NOTIFY, which Listen turns into tailer wake-ups.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

//go:embed migrations/*.sql
var migrations embed.FS

// migrateLockKey is the advisory lock key migrations hold ("searchlight").
const migrateLockKey = 0x5345415243484c54

const now = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"

// Dialect returns the PostgreSQL dialect.
func Dialect() *dialect.Dialect {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	return &dialect.Dialect{
		Name:       "postgres",
		Open:       open,
		Migrations: sub,
		VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INTEGER NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at BIGINT NOT NULL
)`,
		MigrateInTx: true,
		MigrateLock: fmt.Sprintf("SELECT pg_advisory_xact_lock(%d)", int64(migrateLockKey)),
		Dollar:      true,
		ForUpdate:   " FOR UPDATE",
		Now:         now,
		Upsert:      onConflict,
		Claim:       claim,
		Greatest:    greatest,
		ApplyTx:     &sql.TxOptions{Isolation: sql.LevelReadCommitted},
		SnapshotTx:  &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true},
		MaxParams:   65535,
		Notify:      "SELECT pg_notify(?, ?)",
		Listen:      listen,
		Retryable:   retryable,
	}
}

// retryable reports serialization failures (40001) and deadlocks (40P01).
func retryable(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (pe.Code == "40001" || pe.Code == "40P01")
}

// Config parses a postgres:// or postgresql:// store URL into a pgx
// connection config. Query parameters are libpq's (sslmode, ...) or runtime
// parameters such as search_path.
func Config(u *url.URL) (*pgx.ConnConfig, error) {
	c := *u
	c.Scheme = "postgres"
	cfg, err := pgx.ParseConfig(c.String())
	if err != nil {
		// pgx errors can echo the connection string, password included.
		return nil, errors.New("invalid postgres store URL")
	}
	return cfg, nil
}

func open(u *url.URL) (dialect.Pools, error) {
	cfg, err := Config(u)
	if err != nil {
		return dialect.Pools{}, err
	}
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(16)
	return dialect.Pools{Write: db, Read: db}, nil
}

func onConflict(keys, update []string) string {
	var b strings.Builder
	b.WriteString(" ON CONFLICT (")
	b.WriteString(strings.Join(keys, ", "))
	b.WriteString(") DO UPDATE SET ")
	for i, c := range update {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c + " = EXCLUDED." + c)
	}
	return b.String()
}

var claimQuery = strings.ReplaceAll(`INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES ($1, $2, $3, $4, 'recovering', 0, NOW + $5, $6)
ON CONFLICT (index_name, shard, slot) DO UPDATE SET
	state = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.state ELSE EXCLUDED.state END,
	applied_seq = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.applied_seq ELSE 0 END,
	epoch = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.epoch ELSE EXCLUDED.epoch END,
	node_id = EXCLUDED.node_id,
	lease_until = EXCLUDED.lease_until
WHERE sl_shard_copies.node_id = EXCLUDED.node_id OR sl_shard_copies.lease_until < NOW`, "NOW", now)

func claim(a dialect.ClaimArgs) (string, []any) {
	return claimQuery, []any{a.Index, a.Shard, a.Slot, a.Node, a.TTLms, a.Epoch}
}

func greatest(a, b string) string { return "GREATEST(" + a + ", " + b + ")" }

// listen runs LISTEN on conn and calls fn for each notification until ctx
// ends. The connection is discarded afterwards rather than returned to the
// pool, because it stays subscribed.
func listen(ctx context.Context, conn *sql.Conn, channel string, ready func(), fn func(payload string)) error {
	err := conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("postgres listen: unexpected driver connection %T", dc)
		}
		pc := sc.Conn()
		if _, err := pc.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
			return err
		}
		if ready != nil {
			ready()
		}
		for {
			n, err := pc.WaitForNotification(ctx)
			if err != nil {
				return err
			}
			fn(n.Payload)
		}
	})
	// A subscribed connection must never be reused; the raw call either
	// failed (and database/sql may keep the conn) or ended with ctx. Closing
	// the pgx connection makes database/sql drop it.
	_ = conn.Raw(func(dc any) error {
		if sc, ok := dc.(*stdlib.Conn); ok {
			_ = sc.Close()
		}
		return nil
	})
	return err
}
