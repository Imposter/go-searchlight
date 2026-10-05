package postgres

import (
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Unreachable reports a server that takes no connection for now: a connection
// exception (class 08), cannot_connect_now (57P03, starting up or shutting down) or
// too_many_connections (53300). Waiting may cure it.
func Unreachable(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	return strings.HasPrefix(pe.Code, "08") || pe.Code == "57P03" || pe.Code == "53300"
}

// Misconfigured reports a refusal retrying cannot cure: the credentials
// (invalid_password 28P01, invalid_authorization_specification 28000) or a database
// that does not exist (invalid_catalog_name 3D000).
func Misconfigured(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	switch pe.Code {
	case "28P01", "28000", "3D000":
		return true
	}
	return false
}
