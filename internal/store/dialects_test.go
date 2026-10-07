package store

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// optionalStatements are the statement fields a dialect may leave empty, by
// table and field name, with whether this dialect must set them.
func optionalStatements(d *dialect.Dialect) map[string]bool {
	m := d.Maintenance
	return map[string]bool{
		// A transaction that excludes other writers needs no lock (SQLite).
		"Maintenance.MigrateLock": false,
		// The session lock is for engines without transactional DDL.
		"Maintenance.SessionLock":   !m.MigrateInTx,
		"Maintenance.SessionUnlock": !m.MigrateInTx,
		// A lock timeout is for engines whose DDL queues behind queries (not
		// SQLite, whose writer lock excludes them), and resetting it for one set on
		// a pooled session.
		"Maintenance.MigrateLockTimeout": d.Name != "sqlite",
		"Maintenance.MigrateLockReset":   d.Name != "sqlite" && !m.MigrateInTx,
	}
}

// TestStatementTables checks every dialect fills its statement tables:
// every string is set (but the documented optional ones), every Returning
// has a read, a Returning without a separate write reads back with
// RETURNING, the builder is set, and Listen comes only with notifications.
func TestStatementTables(t *testing.T) {
	for _, scheme := range []string{"sqlite", "postgres", "mysql"} {
		d := dialectMust(scheme)
		optional := optionalStatements(d)
		tables := reflect.ValueOf(d).Elem()
		checked := 0
		for _, table := range []string{"Changelog", "Records", "Registry", "Blobs", "Indexes", "Maintenance"} {
			v := tables.FieldByName(table)
			for i := range v.NumField() {
				f, name := v.Field(i), table+"."+v.Type().Field(i).Name
				switch x := f.Interface().(type) {
				case string:
					must, isOptional := optional[name]
					if x == "" && (!isOptional || must) {
						t.Errorf("%s: %s is empty", scheme, name)
					}
					if x != "" && strings.TrimSpace(x) != x {
						t.Errorf("%s: %s has surrounding space", scheme, name)
					}
				case dialect.Returning:
					if x.Read == "" {
						t.Errorf("%s: %s has no read", scheme, name)
					}
					if x.Write == "" && !strings.Contains(x.Read, "RETURNING") {
						t.Errorf("%s: %s has no write and its read does not return from a write: %s", scheme, name, x.Read)
					}
				case []string:
					if len(x) == 0 {
						t.Errorf("%s: %s is empty", scheme, name)
					}
					for j, s := range x {
						if s == "" {
							t.Errorf("%s: %s[%d] is empty", scheme, name, j)
						}
					}
				case func(*dialect.Write, dialect.Limits) []dialect.Stmt:
					if x == nil {
						t.Errorf("%s: %s is nil", scheme, name)
					}
				case func(error) bool:
					// Optional: LockTimedOut comes with MigrateLockTimeout, and
					// AlreadyApplied with non-transactional DDL.
					if (x == nil) == (d.Name != "sqlite") && (name == "Maintenance.LockTimedOut" || !d.Maintenance.MigrateInTx) {
						t.Errorf("%s: %s set = %v", scheme, name, x != nil)
					}
				case dialect.Limits, bool:
				default:
					t.Errorf("%s: %s has unexpected type %T", scheme, name, x)
				}
				checked++
			}
		}
		if checked < 60 {
			t.Errorf("%s: only %d statement fields checked", scheme, checked)
		}
		if d.Open == nil || d.Migrations == nil {
			t.Errorf("%s: Open or Migrations is missing", scheme)
		}
		// Only Postgres has notifications, and its write sends them.
		if (d.Listen != nil) != (scheme == "postgres") {
			t.Errorf("%s: Listen set = %v", scheme, d.Listen != nil)
		}
	}
}
