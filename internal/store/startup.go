package store

import (
	"errors"
	"io"
	"io/fs"
	"net"

	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/store/sqlite"
)

// Unreachable reports an error opening or migrating the store that waiting may cure:
// the database cannot be reached over the network yet, or takes no connection for now
// (starting up, shutting down, out of connections, locked).
func Unreachable(err error) bool {
	if err == nil || Misconfigured(err) {
		return false
	}
	var ne net.Error
	if errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	return postgres.Unreachable(err) || mysql.Unreachable(err) || sqlite.Unreachable(err)
}

// Misconfigured reports an error opening or migrating the store that retrying cannot
// cure: an invalid store URL, credentials the database refuses, a database that does
// not exist, a SQLite file that cannot be opened or written, or a schema newer than
// this binary.
func Misconfigured(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range []error{ErrInvalid, ErrNewerSchema, fs.ErrPermission, fs.ErrNotExist} {
		if errors.Is(err, e) {
			return true
		}
	}
	return postgres.Misconfigured(err) || mysql.Misconfigured(err) || sqlite.Misconfigured(err)
}
