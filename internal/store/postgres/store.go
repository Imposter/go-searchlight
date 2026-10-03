// Package postgres is the PostgreSQL dialect of the SQL store, on
// jackc/pgx/v5 through database/sql.
//
// Apply locks the counter row with SELECT ... FOR UPDATE, so sequence numbers
// are assigned and committed in order, and writes a whole batch with one
// statement whose rows travel as arrays (see writeSQL). Writes that are read
// back use RETURNING. Migrations run in one transaction under a
// transaction-scoped advisory lock. Committed changes are announced with
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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

//go:embed migrations/*.sql
var migrations embed.FS

// migrateLockKey is the advisory lock key migrations hold ("searchlight").
const migrateLockKey = 0x5345415243484c54

// Dialect returns the PostgreSQL dialect.
func Dialect() *dialect.Dialect {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	return &dialect.Dialect{
		Name:        "postgres",
		Open:        open,
		Migrations:  sub,
		ApplyTx:     &sql.TxOptions{Isolation: sql.LevelReadCommitted},
		SnapshotTx:  &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true},
		Retryable:   retryable,
		Listen:      listen,
		Changelog:   changelog,
		Records:     records,
		Registry:    registry,
		Blobs:       blobs,
		Indexes:     indexes,
		Maintenance: maintenance,
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

// listenPingEvery is how long a listening connection waits for a notification
// before it pings the server, and listenPingTimeout how long the ping may take.
const (
	listenPingEvery   = 30 * time.Second
	listenPingTimeout = 10 * time.Second
)

// listen runs LISTEN on conn and calls fn for each notification until ctx
// ends. The connection is discarded afterwards rather than returned to the
// pool, because it stays subscribed.
func listen(ctx context.Context, conn *sql.Conn, ready func(), fn func(payload string)) error {
	err := conn.Raw(func(dc any) error {
		sc, ok := dc.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("postgres listen: unexpected driver connection %T", dc)
		}
		pc := sc.Conn()
		if _, err := pc.Exec(ctx, "LISTEN "+pgx.Identifier{notifyChannel}.Sanitize()); err != nil {
			return err
		}
		if ready != nil {
			ready()
		}
		for {
			// Wake now and then to ping: a half-open socket (the server or
			// the network gone without a reset) would otherwise wait forever.
			wctx, cancel := context.WithTimeout(ctx, listenPingEvery)
			n, err := pc.WaitForNotification(wctx)
			cancel()
			if err != nil {
				if ctx.Err() == nil && pgconn.Timeout(err) {
					pctx, cancel := context.WithTimeout(ctx, listenPingTimeout)
					err = pc.Ping(pctx)
					cancel()
					if err != nil {
						return fmt.Errorf("postgres listen: the connection does not answer: %w", err)
					}
					continue
				}
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
