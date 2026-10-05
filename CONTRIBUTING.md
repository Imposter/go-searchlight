# Contributing

## Tests

The suite has two tiers, sorted by [internal/testtier](internal/testtier/testtier.go).

| Target | Runs | Time on a developer machine |
| --- | --- | --- |
| `make test` | the short tier: `go test -short ./...` | under 2 minutes |
| `make test-heavy` | every test, the heavy tier included: `go test ./...` | about 3 minutes |
| `make test-all` | `test-heavy` with the property tests' long modes (`SEARCHLIGHT_LONG=1`) | longer |
| `make test-race` | `test-heavy` under `-race` (needs cgo and gcc) | several times longer |

`TESTFLAGS` adds flags to any of them, such as `TESTFLAGS=-race` or `TESTFLAGS=-count=2`.
CI runs `make test TESTFLAGS=-race` and `make test-race` as two Linux jobs against SQLite,
Postgres and MySQL. The heavy job runs every test, so it alone keeps the suite's coverage;
the short job answers first. On Windows CI runs the short tier, the smoke tests (the binary
built, run and restarted in `TestSmokeBinary`; every benchmark workload against the binary
in `TestEndToEndBinary`) and the heavy tier of `store`, `shard` and `segment`,
whose WAL checkpoints, merges, renames and large files behave differently on NTFS.

The T7 harnesses (`TestT7RefreshPhases`, `TestT7Diag`) are diagnostics, not checks: they run
only with `SEARCHLIGHT_T7DIAG=1` and outside `-short`.

The chaos suite (`test/chaos`) is outside both tiers: it builds only with the `chaos` tag,
needs Postgres (`SEARCHLIGHT_TEST_PG_URL`), and runs in its own workflow,
`.github/workflows/chaos.yml`.

### Which tier a test belongs to

A test belongs in the heavy tier when it waits on real time or real I/O for more than about
three seconds: cluster failover, rolling restarts, lease expiry, latency bounds, large
datasets, full matrices. Start it with `testtier.Heavy(t)`. A test that runs a matrix can run
part of it in the short tier with `testtier.Pick(short, heavy)`. When the heavy test is the
only coverage of a behaviour, keep a smaller case of it in the short tier.

A latency bound with a fixed margin scales the margin by `testtier.RaceSlowdown`: the race
detector slows commit paths several-fold, and `testtier.Race` reports when it is on.

### SQLite in tests

Test databases run with `synchronous=OFF`, since no test outlives the operating system that
holds its writes, and start as a copy of a template migrated once per test binary:

- outside package `store`, `storetest.SQLiteURL(storetest.Migrated(t, path))`;
- inside it, `sqliteHarness` and `forEachDialect`.

Tests of durability, crashes or fsyncs keep the store's `synchronous=FULL`:
`storetest.DurableSQLiteURL`, `durableSQLiteHarness`, or `forEachDurableDialect`.

Package `store` keeps its own copy of the template migration, since its tests cannot import
`storetest` (which imports `store`) without an import cycle.

### Parallel tests

The tests of `api`, `cluster`, `node`, `replica`, `shard` and `store` run serially. They
fsync (shard flushes, durable SQLite commits), and on Windows running them in parallel made
the suite slower, not faster: a busy disk stretched a new copy's first flush past the waits
and leases the tests hold (CreateIndex's wait for a new index's copies, a two-second lease),
and those tests failed. The suite's wall time is set by the cluster package's serial,
real-time tests in any case.

Elsewhere a test may call `t.Parallel()` when it is independent. Some state is
process-wide, and a test that changes it stays serial: `GOMAXPROCS`, the default `slog`
logger, the OpenTelemetry global providers (`telemetry.Setup`), `segment.SyncCounts`, the
shard package's default merge budget and filter cache, `search.SetThreads`, package-level
tuning variables, and environment variables.

### Before a pull request

```sh
make build test lint
GOOS=linux go vet ./...
go mod tidy -diff
```
