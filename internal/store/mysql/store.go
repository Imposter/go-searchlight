// Package mysql is the MySQL (InnoDB) dialect of the SQL store, on
// go-sql-driver/mysql.
//
// Apply locks the counter row with SELECT ... FOR UPDATE under READ
// COMMITTED, so sequence numbers are assigned and committed in order, and
// writes a batch as multi-row INSERTs sized to MySQL's placeholder limit.
// MySQL DDL is not transactional, so migrations run on one connection holding
// a GET_LOCK named after an MD5 hash of the database (a name can run to 64
// bytes, which a long database name alone could exceed), and each file is
// recorded as it completes. Identifiers and names are VARBINARY: ids compare
// byte for byte, as on the other dialects, instead of under a
// case-insensitive collation.
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

// Dialect returns the MySQL dialect.
func Dialect() *dialect.Dialect {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	return &dialect.Dialect{
		Name:        "mysql",
		Open:        open,
		Migrations:  sub,
		ApplyTx:     &sql.TxOptions{Isolation: sql.LevelReadCommitted},
		SnapshotTx:  &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true},
		Retryable:   retryable,
		Changelog:   changelog,
		Records:     records,
		Registry:    registry,
		Blobs:       blobs,
		Indexes:     indexes,
		Maintenance: maintenance,
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
