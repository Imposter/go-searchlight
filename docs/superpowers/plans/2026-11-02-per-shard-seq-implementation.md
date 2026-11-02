# Implementation plan: per-shard sequences (Change A, Searchlight 2.0.0)

**Source:** Phases 1 and 2 of [the write scaling plan](2026-11-01-write-scaling.md), as approved: D1 (one commit
per shard, per-item results, no 2PC), D2 (an opaque token in place of the scalar `seq`, `wait_for_seq` and
`percolated_seq`) and D4 (on all three dialects). Change B is out of scope. File and line references are in
[the code map](../specs/2026-11-01-write-scaling-code-map.md); [Phase 0's results](../specs/2026-11-01-write-scaling-phase0-results.md)
set the performance bar. The spike (`spike/write-scaling`, never merged) showed the shape and its shortcuts; none
of its code is reused as it stands.

**Branch:** `feat/per-shard-seq`, from `design/write-scaling` at `a0202bb`. Four milestones, each committed in small
steps and each leaving its own layer green:

| Milestone | Layer | Green at its end |
|---|---|---|
| M1 store | `internal/store` and its dialects | `go vet`, `golangci-lint`, `go test ./internal/store/...` on SQLite, Postgres and MySQL (where a server exists). Callers above compile; what still misbehaves there is listed under M1's "left for M2 and M3" |
| M2 node and replica | `internal/node`, `internal/replica`, `internal/shard`, `internal/cluster` | those packages' tests, both tiers |
| M3 API | `internal/api`, `api/openapi.yaml`, docs | `make build test lint`, the whole short and heavy tiers |
| M4 upgrade and verification | migration gate, chaos suite, benchmarks, release notes | everything, plus the chaos suite and Phase 0's benchmarks |

## Design decisions taken here

These refine the approved plan. Where one departs from it, the reason is given.

1. **`sl_shard_seq (index_name, shard)`, carrying `value`, `index_uid` and `mapping_version`.** The approved plan
   keys it by `index_uid`. Every other data table (`sl_changes`, `sl_documents`, `sl_pruned`, `sl_shard_copies`)
   is keyed by name, and so is everything a caller passes (`ShardID`). Keyed by name, Apply locks its row by what it
   already has, and the locked row returns the index's incarnation and mapping version as of the lock. That
   replaces `checkIndexes`' read of `sl_indexes` (Phase 1, item 3) outright: nothing is cached, so nothing goes
   stale and there is no conflict path to re-read on. The row is also the "local mapping copy" Change B's item 3
   asks for, minus the mapping text.
2. **`sl_counter` row 1 becomes the seq floor.** The migration seeds every shard at its value (the old global head).
   `Drop` raises it to the highest seq of the shards it removes, and `Create` seeds a new index's shards at it. So
   a name's shard never reissues a seq across a drop and recreate, as today, and a copy of a dropped incarnation
   never sees a head behind its applied seq. `HeadSeq`, `Tip` and `ScanShard` of a shard with no row (its index
   is gone) answer with the floor for the same reason. Row 2 still issues copy epochs.
3. **`Store.Apply` commits one transaction, whatever shards it names.** It locks each named shard's row in key order
   (index name by bytes, then shard), and gives each shard its own contiguous seqs. A direct Apply is therefore
   still all or nothing. The split by shard happens in the group committer, which is the node's only path to the
   store: on a dialect with several write connections, each shard has a lane and a request is cut into one part
   per shard. On SQLite (one write connection) there is one lane, and a request is never split: SQLite gains
   nothing from the split (D4) and would lose a fsync per shard. D1's guarantee is the floor: whole or nothing on
   one shard.
4. **Deadlock freedom.** Every transaction that takes shard rows takes them in the same key order: Apply's one or
   several rows, and `Update` and `Drop`, which take every row of their index in one `ORDER BY shard ... FOR UPDATE`
   (Postgres applies the ORDER BY before locking; InnoDB locks in primary key order). Settings-only updates take
   them too, as they took the counter lock before: `Update`'s optimistic `UPDATE ... WHERE version` doesn't check
   its row count, and only the lock keeps two updates from both succeeding.
5. **The committer pool.** A lane per shard, run by a goroutine while it has requests, and a semaphore bounding
   the transactions in flight: `GroupCommitOptions.MaxTransactions`, by default half the store's write pool (32 of
   Postgres's and MySQL's 64; Phase 0 found 32 better than 8 or 64). The batching window is waited out before a
   slot is taken, so a waiting lane holds no connection. A lane takes everything pending when its slot comes, up to
   the change and byte limits, so a backlog commits in full batches.
6. **Notifications leave the transaction.** After a commit, each shard's newest seq goes to a per-store notifier,
   which coalesces them and sends them in one statement of its own with `synchronous_commit` off. A lost
   notification costs at most one poll (500 ms). `Watcher`'s `ready` contract weakens accordingly: a change
   committed after ready is announced unless the committing node fails first.
7. **`sl_changes`' new primary key is `(index_name, shard, seq)`**, which makes the old second index redundant. A
   plain index on `seq` stays, for M4: until every node runs 2.0, 1.x nodes join `sl_changes` on `seq` alone at
   every poll (`ReadTip`, `ReadCounterMark`), and without it each poll scans the table.
8. **Partial results.** `GroupCommitter.Apply` returns a `*PartialError` when a request split over several shards
   had some fail: each failed shard with its changes' positions in the request and its error, positions
   translated. A request on one shard gets its error as before. Committed changes have their seqs set.

## M1: store

Test-first: each task writes or rewrites the tests named, sees them fail, then changes the code.

| # | Task | Files | Tests |
|---|---|---|---|
| 1.1 | This plan | `docs/superpowers/plans/2026-11-02-per-shard-seq-implementation.md` | |
| 1.2 | **Migration 0007** on each dialect: create `sl_shard_seq`; seed a row per shard of every index from `settings.shards` (1 when absent) at row 1's value with the index's uid and mapping version; swap `sl_changes`' key to `(index_name, shard, seq)`, drop `sl_changes_shard_seq`, add `sl_changes_seq (seq)`. Postgres in the migration transaction; MySQL in one `ALTER TABLE` (atomic DDL, so a rerun that fails with 1091 on the dropped index means it had completed: `alreadyApplied` learns 1091); SQLite rebuilds `sl_changes` `WITHOUT ROWID` in the migration transaction | `internal/store/{postgres,mysql,sqlite}/migrations/0007_shard_seq.sql`, `mysql/store.go` | `TestMigrateShardSeq`: a database at schema 6 with two indexes (1 and 3 shards) and changes migrates to rows at the old head, keeps every change readable, and takes a new change at head+1 on each shard; a rerun is a no-op |
| 1.3 | **Statements.** `LockShard` (one row, locked), `LockIndexShards` (an index's rows by shard, locked), `SeedShard`, `RaiseFloor`; `ReadHead`, `ReadTip` and `ReadCounterMark` (renamed `ReadShardMark`) per shard; `Write.Counter` becomes `Write.Counters` (a value and mapping version per shard); `Write.Notify` goes, and Postgres gains `Notify`; `DropData` deletes the index's rows | `internal/store/dialect/statements.go`, each dialect's `statements.go` and `build.go` | `TestStatementTables`, `TestSplitWrites`, `mysql/build_test.go`, `postgres/statements_test.go` |
| 1.4 | **Apply on shard rows.** Lock every touched shard in key order; the locked rows give each change's uid and mapping version (a missing row: re-read `sl_indexes` to say whether the index is gone, another incarnation, or the shard out of range); per-shard provisional seqs in `checkConditions`; per-shard contiguous seqs; `netState` and notifications from per-change seqs. `Store.Apply` returns `error` and sets each change's `Seq` | `internal/store/sql.go`, `store.go`, `types.go` | Rewritten: `checkLoad` (each shard's seqs tile its floor+1..head with no gap or overlap), `TestApplyAndRead`, `TestConcurrentWriters`, `TestConditionalWrites`, `TestBigBatch`, `TestIndexIncarnation`. New: `TestApplyShardOutOfRange`, `TestApplyAcrossShardsIsAtomic` |
| 1.5 | **Index operations.** `Create` inserts the index and its shard rows in one transaction, at the floor. `Update` locks every row of the index, then re-reads it; a remap logs one mapping change per shard at that shard's next seq and writes the new mapping version into each row. `Drop` locks every row, raises the floor to their highest value, and deletes them with the index's data | `internal/store/indexes.go` | Rewritten: `TestMappingChanges`, `TestMappingChangesOrderWithApply`, `TestShardCountImmutable`, `TestIndexes`. New: `TestRecreatedIndexStartsAboveItsPast`, `TestRemapAndDropDuringMultiShardWrites` (no deadlock, every shard's changes after the mapping change carry its version) |
| 1.6 | **Heads and horizons per shard.** `HeadSeq(ctx, shard)`; `Tip` reads the shard's head, its change at the head and at `At`, and its uid from its row; `ScanShard` reads the shard's head and marks; `raiseHorizon` takes the shard's row lock and clamps to the shard's head; a missing row answers with the floor. Temporary: `TopSeq` (the highest seq of any shard) for the node's single head, which M2 removes | `sql.go`, `store.go`, `types.go` | Rewritten: `TestHeadSeq`, `TestTip`, `TestScanShardSnapshot`, `TestPrune`, `TestLateCommitNotSkipped` and `TestTailerNeverSkips` (per shard, with a hook naming the shard), `TestTipOnPostgresReadsFewChangelogRows`, `TestTipPlanOnMySQL`, `TestCorruptBodyPrunedPast`, `TestChangeNonces` |
| 1.7 | **Notifier.** Post-commit, coalescing, one goroutine per watching store, stopped at `Close`; `synchronous_commit` off | `internal/store/notify.go`, `sql.go`, `postgres/statements.go` | `TestWatch` (a commit is announced, after commit); `TestNotifierCoalesces` |
| 1.8 | **Committer pool.** Lanes by shard (one lane when `MaxTransactions` is 1), a semaphore, the window before the slot; requests split and merged; `*PartialError` | `internal/store/groupcommit.go`, `types.go` | Kept: every `groupcommit_test.go` test (single lane). New: `TestGroupCommitLanesRunInParallel`, `TestGroupCommitBoundsTransactions`, `TestGroupCommitPartialFailure` (positions translated), `TestGroupCommitSplitsAcrossShards`; rewritten `TestGroupCommitConcurrentWriters`, `TestGroupCommitIsolation` |
| 1.9 | **Callers compile.** `Apply`'s and `HeadSeq`'s new signatures in test doubles; `cluster/maintenance.go`'s `ageFloor` and `logStart` search the shard's own head (already per shard); the node's commit loop takes `*PartialError` (temporary: committed shards answered, conflicted shards retried, any other failure a 5xx, as D1 allows) | `internal/node/{write,read,single}.go`, `internal/cluster/maintenance.go`, test doubles in `api`, `cluster`, `node`, `replica` | Those packages' short tiers, run and their failures listed |
| 1.10 | Benchmarks compile and run: `BenchmarkApply` (1, 8, 64 shards), `BenchmarkCommitLoad` | `bench_test.go`, `commit_load_test.go` | run once, `-benchtime 1x` |

**What M1's tests prove:** each shard's seqs are contiguous from its floor and commit in seq order under concurrent
writers on one node and several; a tailer of one shard never skips a change that commits late; a multi-shard
Apply is atomic and a split request reports exactly the shards that failed; remaps and drops interleave with
multi-shard writes without deadlock and every change after a remap carries its version; a recreated index's seqs
start above its past; heads, tips, snapshots and horizons are per shard; and a database from 1.x migrates with
every shard at the old head, on every dialect.

**Left for M2 and M3**, as M1 ended (the short tier of `api`, `cluster`, `node` and `replica` on SQLite and
Postgres, 2026-11-02). Everything compiles, `go vet` (also `GOOS=linux` and `-tags chaos`) and `golangci-lint` are
clean. What fails falls into three causes:

1. **`wait_for_seq` is a scalar waited on every shard (M3).** A seq taken on one shard is waited for on every shard,
   whose seqs are their own now, so a read with `wait_for_seq` on a multi-shard index times out (504). All 11
   `api` failures (`TestSearch`, `TestSavedQueriesAndPercolate`, `TestFieldsAndCluster`, `TestDateMath`,
   `TestEndToEndOnEveryDialect`, `TestReadYourWrites`, `TestNestedFields`, `TestUnchangedUpsertIsANoop`,
   `TestPointFields`, `TestMaxStalenessServesCachedResults`, `TestSuitesOnSingleNodeCluster`); in `node`
   `TestSearchDateMathFollowsTheClock`, `TestFilterAggsAndPostFilterDateMathFollowTheClock`, `TestPercolateBoxDateMath`,
   `TestSearchBoxFollowsTheClock`, `TestPatchMappingAddsAPointField`, `TestRequestCacheOnANode`,
   `TestRestartKeepsData`, `TestSearchMatchesBruteForce`, `TestDatabaseUnreachable`; in `cluster` the peer reads that
   answer 503 "no copy of shard N answered" (`TestReadRightAfterCreateOnAnotherNode`, the `OlderPeerRefused*`,
   `*ThroughPeers`, `TestWireNestedSubAggs`, `TestReadRereadsTheCopyTable`) and the 60 s waits
   (`TestMultiShardSearchAcrossNodes`, `TestFetchAfterPinLostRetries`, `TestFromAcrossNodes`,
   `TestDatePercolationMovesPastAnOlderCopy`). `TestStopStopsTheTailersBeforeMarkingRetiring` waits forever in a
   test helper's read with `wait_for_seq` and no deadline, so the `cluster` run hits its timeout.
2. **Tests that assert one global sequence (M2, M3).** `node`'s `TestWriteValidation` (contiguous seqs across a
   bulk) and `TestRewindLowersTheNodesHead` (the node's single head), `cluster`'s `TestConcurrentWritersOnEveryNode`
   (the changelog's global contiguity).
3. **The rewind suite (M2, task 2.3).** `replica`'s `TestChangelogRewindRebuildsTheCopy`,
   `TestRebuiltCopyReportsItsOwnSeq`, `TestStoppedAcrossARewindWhoseMarkRowWasPrunedRebuildsTheCopy` and
   `TestStoppedAcrossARewindAfterIdleAdvancesRebuildsTheCopy`: their `rewind` helper moves `sl_counter` row 1, not the
   shard's sequence row, and two of them stage idle advances over another shard's seqs, which no longer exist.

Temporary code M2 removes: `Store.TopSeq` and the node's single head that reads it (`read.go` `checkHead`,
`single.go`'s start), and the commit loop's PartialError handling in `node/write.go`, which M3 replaces with per-item
results. Moved forward from M2 (task 2.4): `waitWritten` waits each shard for its own seq.

## M2: node and replica

| # | Task | Files (code map §2, §4, §5) | Tests |
|---|---|---|---|
| 2.1 | **Heads per shard.** `noteHead`, `lowerHead`, `checkHead` and the node's `head` become per shard (a map keyed by `ShardID`, or a field of the shard's slot); `TailerEnv.Head` takes the shard; `TopSeq` goes | `internal/node/single.go`, `read.go`, `host.go`, `internal/store` | `node/single_test.go`'s head tests rewritten |
| 2.2 | **Tailers.** `Advance` over gaps goes: an idle shard's head is its own, so the tailer's advance is a no-op unless the head moved, and `Shard.Advance` and its generation-publishing path are removed with their tests. Lag is the shard's head minus applied | `internal/replica/tailer.go`, `internal/shard/shard.go`, `manifest.go`, `generation.go` | `replica/tailer_test.go`, `shard/seqmark_test.go` |
| 2.3 | **Rewind on own-shard anchors.** An anchor now always names an own-shard change, so `checkRewind`'s cross-shard cases and `ownCheck`'s separate state collapse into the anchor. Rewrite the rewind suite rather than adapt it | `internal/replica/rewind.go`, `recover.go` | `replica/rewind*_test.go` rewritten; `TestLateCommitNotSkipped` per shard in `replica` |
| 2.4 | **`querySeq`, `written`, results.** Keep both per shard (they are already in the slot); `res.Seq` becomes a per-shard map for M3's token; `waitWritten` waits each shard for its own seq | `internal/node/write.go`, `target.go` | `node/write_test.go`, `api/endpoints_test.go`'s wait cases |
| 2.5 | **Maintenance.** `ageFloor` and `logStart` search the shard's own head (done in M1), `pruneFloor` unchanged; check the retention tests | `internal/cluster/maintenance.go` | `cluster/maintenance_test.go` |
| 2.6 | **Group commit wiring.** `MaxTransactions` from config (`write_transactions`, default half the pool), with the committer's batch-size metric labelled by lane | `internal/config`, `internal/node/single.go` | config tests |

**What M2's tests prove:** a copy of an idle shard serves without advancing; a rewind is detected from own-shard
marks alone, on every path the old suite covered; per-shard waits never wait on another shard's seq.

## M3: API

| # | Task | Files | Tests |
|---|---|---|---|
| 3.1 | **The token.** An opaque, URL-safe string: a version byte, the index's uid, and sorted `(shard, seq)` pairs as varints; merged by per-shard maximum; at most one entry per shard (64 shards about 0.4 KB before base64). A token naming another incarnation is refused with 409 `index_recreated` | `internal/api/token.go` | `TestTokenRoundTrip`, `TestTokenMerge`, fuzz `FuzzTokenDecode` |
| 3.2 | **Writes return it.** Bulk, single-document and saved-query writes return `token` in place of `seq`; per-item `_seq` stays. The bulk's `errors` flag covers partial failure; a failed shard's items carry its error (a 5xx-class item error when the shard's transaction failed), and the response is 200 with `errors: true` unless every item failed for the same server-side reason | `internal/api/bulk.go`, `coordinator.go`, `internal/node/write.go` | `TestAbandonedBulkIsAllOrNothing` becomes `TestAbandonedBulkIsWholePerShard`; new `TestBulkPartialFailure` |
| 3.3 | **Reads wait on it.** `wait_for=<token>` on search, count, get and percolate waits only on the shards the request reads, 504 at the deadline; `wait_for_seq` is refused by name (400, `wait_for_seq` was removed in 2.0; use `wait_for`) | `internal/api/*`, `internal/node/target.go`, `internal/cluster/router.go`, `wire.go`, `peer_api.go` | `endpoints_test.go`: "wait_for waits on the shards it reads"; cluster `focus_test.go` |
| 3.4 | **`percolated_seq` goes.** Its replacement is `percolated: <token>`, the per-shard positions the percolation ran against | `internal/api`, `internal/node/target.go`, `router.go` | `versions_test.go` |
| 3.5 | **Docs.** Retry safety (a 5xx may have committed some shards: retry with `if_seq`, `version`, or idempotent upserts), the per-shard promise (whole or nothing per shard), the token, `wait_for` | `docs/api.md`, `api/openapi.yaml`, `docs/clustering.md`, `docs/architecture.md`, the design spec §1-§2, §8, §14, §17, `README.md`, `CHANGELOG.md` (2.0.0) | `openapi` lint test if present; doc links |
| 3.6 | Chaos harness: `wait_for_seq` uses become `wait_for` with the token | `test/chaos/harness_test.go`, `chaos_test.go` | compile with `-tags chaos` |

**What M3's tests prove:** a client reads its own writes with the token, waiting on only the shards it reads; a
partial bulk names exactly the items that failed; nothing accepts the removed scalars.

## M4: rolling upgrade and verification

| # | Task | Files | Tests |
|---|---|---|---|
| 4.1 | **Gated cutover.** A `shard_seqs` row in `sl_features`. Until it is on, 2.0 nodes lock row 1 as well as the shard row, and write row 1's next value as every seq, so 1.x nodes' global assumptions hold. The leader turns it on once every live node heartbeats 2.0, in a transaction holding row 1's lock that first raises every shard row to row 1's value and seeds rows for indexes a 1.x node created | `internal/store/sql.go`, `internal/cluster/membership.go` (the zstd_bodies gate's pattern) | `TestMixedVersionShardSeqs` in `store`; chaos `TestRollingUpgrade` from 1.8 to 2.0 under load |
| 4.2 | **Chaos suite** with partial bulks and the token: kill a node mid-bulk on a multi-shard index, restart the database mid-load | `test/chaos` | `TestKillMidBulk`, `TestDatabaseRestart` on 8 shards |
| 4.3 | **Benchmarks against Phase 0**, on NVMe with `fdatasync`: `BenchmarkCommitLoad` (64 × 4,096, 8 × 500, 64 × 500, 1-shard control) and `slbench writeload` at 1 and 64 shards, against base `c394860`. Exit bar: 64 × 4,096 at least 2× base sustained; the 1-shard control within 5% | `bench/` | Phase 0's script |
| 4.4 | **Release notes**: breaking changes, the upgrade order (all nodes to 2.0, then the flag), MySQL's table rebuild, Postgres's in-transaction key swap and its lock time | `CHANGELOG.md`, `RELEASING.md` if needed | |

**What M4's tests prove:** a 1.x cluster upgrades in place under load with no client error and nothing lost, and
the gain Phase 0 measured survives the real implementation.

## Open questions, with a recommendation

None blocks M1.

1. **What is the token's "epoch"?** The approved D2 names an epoch and a shard-map version. Under Change A the
   shard count never changes, so the index's uid is the only map version needed. A database failover that loses
   commits reissues seqs, so a token can point at a seq that now names another change. **Recommend:** no epoch in
   2.0's token; a token past a shard's head waits (as `wait_for_seq` does today) and documents that after a lossy
   failover a token may be satisfied by a reissued seq. Add a generation stamp when Change B makes per-store rewinds
   independent.
2. **Small batches over many shards.** Phase 0 lost up to 0.44× on a hard disk with 64 shards and 500-change bulks.
   **Recommend:** measure on NVMe in 4.3 first. If it still loses, let a lane take other shards' parts that are
   waiting and small into one transaction (Apply already locks several rows in order), capped at a few shards.
3. **The Postgres key swap holds `sl_changes`' lock while it builds.** The approved plan says to build the unique
   index concurrently, but the migration runner runs every pending migration in one transaction
   (`internal/store/migrate.go:127-147`), where `CREATE INDEX CONCURRENTLY` can't run. **Recommend:** keep it in the
   transaction, measure the hold on a production-sized changelog (it is pruned, so it should be small) in 4.3, and
   only if it passes a few seconds, teach the runner a non-transactional step.
4. **When does `sl_changes_seq` go?** It only serves 1.x nodes in the upgrade window. **Recommend:** drop it in
   2.1's first migration.
5. **The committer budget's surface.** **Recommend:** a node setting, `write_transactions`, default half the pool,
   documented with Phase 0's finding that more is not better.
6. **Percolation's "queries as they stand at its end"** narrows to the shards that committed (approved plan, Phase 2
   item 4). **Recommend:** percolate every committed document, report failed items without matches, and say so in
   `docs/api.md`.

## Where the approved plan is wrong or out of date

1. **`sl_shard_seq` keyed by `index_uid`.** See decision 1: by name, the lock read replaces the `sl_indexes` read,
   and the uid is a column.
2. **"Build the unique index concurrently and then swap"** can't run in the current migration runner (question 3).
3. **The rolling upgrade needs the old index on `seq`.** 1.x's `ReadTip` and `ReadCounterMark` join `sl_changes` on
   `seq` alone (`internal/store/postgres/statements.go:33,35`, and the same in the other dialects). With the key
   changed and no index on `seq`, every 1.x poll in the upgrade window scans `sl_changes` (decision 7).
4. **Phase 1, item 3 (cache `sl_indexes`)** is superseded by decision 1.
5. **D2's "shard-map version"** has no meaning under Change A, where the shard count is fixed (question 1).
6. **"Plain Apply locks one row"** (Phase 2, item 5). A direct Apply may name several shards; it takes their rows in
   the same order Update and Drop do, so the deadlock argument still holds (decision 4).
