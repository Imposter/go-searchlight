// Package mysql is the MySQL (InnoDB) dialect of the SQL store, on
// go-sql-driver/mysql.
//
// Apply locks the counter row with SELECT ... FOR UPDATE under READ
// COMMITTED, so sequence numbers are assigned and committed in order. MySQL
// DDL is not transactional, so migrations run on one connection holding a
// GET_LOCK named after the database, and each file is recorded as it
// completes. Identifiers and names are VARBINARY: ids compare byte for byte,
// as on the other dialects, instead of under a case-insensitive collation.
package mysql

import (
	"database/sql"
	"embed"
	"errors"
	"io/fs"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

//go:embed migrations/*.sql
var migrations embed.FS

const now = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)"

// Dialect returns the MySQL dialect.
func Dialect() *dialect.Dialect {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	return &dialect.Dialect{
		Name:       "mysql",
		Open:       open,
		Migrations: sub,
		VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INT NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	applied_at BIGINT NOT NULL
) ENGINE=InnoDB`,
		MigrateInTx:   false,
		SessionLock:   "SELECT GET_LOCK(CONCAT('searchlight.migrate.', DATABASE()), 120)",
		SessionUnlock: "SELECT RELEASE_LOCK(CONCAT('searchlight.migrate.', DATABASE()))",
		ForUpdate:     " FOR UPDATE",
		Now:           now,
		Upsert:        onDuplicateKey,
		Claim:         claim,
		ApplyTx:       &sql.TxOptions{Isolation: sql.LevelReadCommitted},
		SnapshotTx:    &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true},
		MaxParams:     65535,
		Retryable:     retryable,
	}
}

// retryable reports InnoDB deadlocks (1213) and lock wait timeouts (1205).
func retryable(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && (me.Number == 1213 || me.Number == 1205)
}

// Config converts a store URL of the form
// mysql://user:pass@host[:port]/db[?param=value...] into a driver config. The
// query parameters are go-sql-driver DSN parameters (tls, timeout, ...) or
// session variables. The store always interpolates parameters client-side
// (one round trip per statement), reports matched rather than changed rows,
// and talks UTC: the session time_zone is +00:00, so the database clock
// (UNIX_TIMESTAMP(NOW(3))) is never ambiguous across a DST change.
func Config(u *url.URL) (*mysql.Config, error) {
	host := u.Host
	if host == "" {
		return nil, errors.New("mysql store URL has no host")
	}
	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(strings.Trim(host, "[]"), "3306")
	}
	db := strings.TrimPrefix(u.EscapedPath(), "/")
	if db == "" {
		return nil, errors.New("mysql store URL has no database name")
	}
	dsn := "tcp(" + host + ")/" + db
	if u.RawQuery != "" {
		dsn += "?" + u.RawQuery
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("invalid mysql store URL: " + err.Error())
	}
	if u.User != nil {
		cfg.User = u.User.Username()
		cfg.Passwd, _ = u.User.Password()
	}
	cfg.InterpolateParams = true
	cfg.ClientFoundRows = true
	cfg.MultiStatements = false
	cfg.Loc = time.UTC
	if cfg.Params == nil {
		cfg.Params = make(map[string]string, 1)
	}
	cfg.Params["time_zone"] = "'+00:00'"
	return cfg, nil
}

// DSN returns the go-sql-driver DSN for a mysql:// store URL.
func DSN(u *url.URL) (string, error) {
	cfg, err := Config(u)
	if err != nil {
		return "", err
	}
	return cfg.FormatDSN(), nil
}

func open(u *url.URL) (dialect.Pools, error) {
	cfg, err := Config(u)
	if err != nil {
		return dialect.Pools{}, err
	}
	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return dialect.Pools{}, err
	}
	db := sql.OpenDB(conn)
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(30 * time.Minute)
	return dialect.Pools{Write: db, Read: db}, nil
}

func onDuplicateKey(_, update []string) string {
	var b strings.Builder
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, c := range update {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c + " = VALUES(" + c + ")")
	}
	return b.String()
}

// MySQL evaluates ON DUPLICATE KEY UPDATE assignments left to right, each
// seeing the ones before, so node_id changes after state, applied_seq and
// epoch have read the old owner, and lease_until last.
var claimQuery = strings.ReplaceAll(`INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES (?, ?, ?, ?, 'recovering', 0, NOW + ?, ?)
ON DUPLICATE KEY UPDATE
	state = IF(node_id = VALUES(node_id), state, IF(lease_until < NOW, VALUES(state), state)),
	applied_seq = IF(node_id = VALUES(node_id), applied_seq, IF(lease_until < NOW, 0, applied_seq)),
	epoch = IF(node_id = VALUES(node_id), epoch, IF(lease_until < NOW, VALUES(epoch), epoch)),
	node_id = IF(lease_until < NOW, VALUES(node_id), node_id),
	lease_until = IF(node_id = VALUES(node_id), VALUES(lease_until), lease_until)`, "NOW", now)

func claim(a dialect.ClaimArgs) (string, []any) {
	return claimQuery, []any{a.Index, a.Shard, a.Slot, a.Node, a.TTLms, a.Epoch}
}
