# Implementation plan: per-shard sequences (Change A, Searchlight 2.0.0)

**Source:** Phases 1 and 2 of [the write scaling plan](2026-11-01-write-scaling.md), as approved: D1 (one commit
per shard, per-item results, no 2PC), D2 (an opaque token in place of the scalar `seq`, `wait_for_seq` and
`percolated_seq`) and D4 (on all three dialects). Change B is out of scope. File and line references are in
[the code map](../specs/2026-11-01-write-scaling-code-map.md); [Phase 0's results](../specs/2026-11-01-write-scaling-phase0-results.md)
set the performance bar. The spike (`spike/write-scaling`, never merged) showed the shape and its shortcuts; none
of its code is reused as it stands.

**Status: do not deploy this branch to a cluster.** Until M4's gated cutover lands, a 1.x node writing after
migration 0007 loses and collides with 2.x writes (see M4's ship blockers). A single 2.x node, or a cluster of 2.x
nodes on a fresh or fully migrated database, is what M1 and M2 test.

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
   the transactions in flight: `GroupCommitOptions.MaxTransactions`, the node's `write_transactions`, by default 16
   and at most half the store's write pool (1 on SQLite). Phase 0 found 32 faster than 8 or 64. Every
   transaction holds a connection of the node's one pool of 64, which its reads share, so a busy node can hold 64
   (plus `LISTEN`) whatever `write_transactions` is: `max_connections` must cover nodes × 65 (M2 review), and
   Postgres's default of 100 refuses a second busy node with 53300 (not retryable). The store's error names the
   pool size and `max_connections`, and `docs/operations.md` sizes it. The batching window is waited out before a
   slot is taken, so a waiting lane holds no connection. A lane takes everything pending when its slot comes, up to
   the change and byte limits, so a backlog commits in full batches.
6. **Notifications leave the transaction.** After a commit, each shard's newest seq goes to a per-store notifier,
   which coalesces them and sends them in one statement of its own with `synchronous_commit` off, once more if it
   fails. A lost notification costs a copy up to one poll, which while notifications flow is half `max_lag` (1 s by
   default), not `changelog_poll_interval`; lost ones are counted (`searchlight_store_notifications_lost_total`) and
   warned about at most once a minute. `Watcher`'s `ready` contract weakens accordingly: a change
   committed after ready is announced unless the committing node fails first.
7. **`sl_changes`' new primary key is `(index_name, shard, seq)`**, which makes the old second index redundant. A
   plain index on `seq` stays on Postgres and MySQL, for M4: until every node runs 2.0, 1.x nodes join `sl_changes`
   on `seq` alone at every poll (`ReadTip`, `ReadCounterMark`), and without it each poll scans the table. SQLite
   keeps none, since nodes of two versions never share its file. Its rebuilt `sl_changes` stays a rowid table: the
   rows carry document bodies, larger than `WITHOUT ROWID` suits, and the key's index serves the reads by shard and
   seq.
9. **Migration 0007's seeding.** Every index gets a row per shard of its `settings.shards` (a number, a string of
   one, or null or absent for one); a value that is none of those fails the migration rather than leaving the index
   without rows, and `Migrate` checks every index has exactly its shard count of rows. Postgres's migration locks
   `sl_counter` row 1 before it seeds, so no 1.x commit lands between the seeding and its end. MySQL's DDL is not
   transactional and its key swap is online, so a 1.x commit during the migration can take a seq the seeding missed:
   M4's gate must cover it there. `c33e22c` (an M1 review fix) rewrote 0007 after M1 was committed: a dev or CI
   database migrated by M1's first 0007 keeps that schema 7 (no null-shard seeding, the post-check never ran) and
   is not migrated again. Recreate such a database. No user has one: 0007 was never released.
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
| 2.1 | **Heads per shard.** The node's single head becomes a head in each shard's slot, raised by the node's commits and its copy's applied seq and lowered by its copy's rewind; `TailerEnv.Head` and `Rewound` are the slot's; `checkHead` checks a `wait_for_seq` against the heads of the shards a read waits on, asking the store for each (`HeadSeq`) on a miss; `querySeq` starts at the shard's head when the index opens. `Store.TopSeq` and `Single.Head` go | `internal/node/single.go`, `read.go`, `target.go`, `host.go`, `write.go`, `internal/store` | `TestRewindLowersTheNodesHead`, `TestSettingsPatchLogsNoMappingChange`, `TestWriteValidation` |
| 2.2 | **Tailers.** No cross-shard gaps are left, but `Shard.Advance` stays: a shard can stand above its last change (a new shard starts at the store's floor, a migrated one at the old global head), and an idle copy advances to its head once. The tailer is otherwise unchanged; its and the shard package's comments say so. Lag is the shard's head minus applied, as it already reads | `internal/replica/tailer.go`, `content_mark.go`, `internal/shard/shard.go` | `replica/tailer_test.go` |
| 2.3 | **Rewind on own-shard anchors.** Every mark now names an own-shard change, so the cross-shard cases are gone, but the own-shard check stays: the shard is pruned up to the oldest seq its copies applied, which can pass a copy's anchor, and only the own-shard check sees a rewind then. The rewind helpers rewind the shards' sequence rows; the two tests that staged marks on another shard's rows are replaced by one where another shard writes and prunes across a copy's rewind | `internal/replica/rewind.go`, `rewind*_test.go`, `internal/node/refresh_test.go` | `replica/rewind*_test.go`, `TestNoFalseRewinds` waits each copy for its own shard's head |
| 2.4 | **`querySeq`, `written`, results.** Per shard (in the slot); `waitWritten` waits each shard for its own seq (done in M1). The per-shard result for the token moves to M3 (3.2), where its shape is decided | `internal/node/write.go` | `cluster/focus_test.go`'s ack log keyed by shard and seq |
| 2.5 | **Maintenance.** `ageFloor` and `logStart` search the shard's own head (done in M1) | `internal/cluster/maintenance.go` | `cluster` maintenance tests |
| 2.6 | **`write_transactions`.** A node setting, default 0: half the store's write connections. Not labelled by lane: a label per shard would be unbounded | `internal/config`, `internal/node/single.go`, `docs/operations.md` | `config_test.go` |

**What M2's tests prove:** a node knows each shard's head and refuses a `wait_for_seq` past every head of the shards
a read waits on; a rewind lowers only its shard's head; a rewind is detected from own-shard marks alone, also when
another shard writes and prunes meanwhile; no rewind is counted under concurrent writers, prunes and a drop and
recreate; acknowledged writes are in each shard's changelog at their seqs, contiguous per shard.

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

**M3 as built.** Where it differs from the table:

- The token: version byte 1, the uid's 16 bytes, a uvarint count, then per entry the shard's uvarint delta from
  the previous shard (−1 before the first) and the seq's uvarint, shards ascending; base64url without padding. At
  most 1024 entries and 16 KiB of text. 30 bytes of text for 1 shard, 67 for 8, 366 for 64 (seqs near 10⁶). It
  names only the shards a write committed to; reads merge tokens by per-shard maximum.
- A token for another incarnation of the index is a 400 at `params.wait_for`, like a malformed one, a seq past its
  shard's head and a shard out of range (not 409 `index_recreated`): the coordinator asked for one clear 400.
- `percolated_seq` became `percolated_at`, a token of where each shard stood for the percolation. Only committed
  documents are percolated; the items of a failed shard carry its error and no matches.
- The M2 review's fixes: a rewind lowers a slot's `written` and `querySeq` with its head; a node that hosts no copy
  of a shard (and so hears of no rewind) lowers its `querySeq` to the store's head when a percolation's wait for it
  fails (a peer's 400, or a 504), and percolates once more when one fell; `Store.IndexHeads` reads an index's
  heads in one statement, for opening an index and for checking a token's shards; the 53300 error and the docs
  size `max_connections` for the shared pool of 64 (nodes × 65); a notification in flight at `Close` is sent,
  not counted as lost.

## M4: rolling upgrade and verification

| # | Task | Files | Tests |
|---|---|---|---|
| 4.1 | **Gated cutover.** A `shard_seqs` row in `sl_features`. Until it is on, 2.0 nodes lock row 1 as well as the shard row, and write row 1's next value as every seq, so 1.x nodes' global assumptions hold. The leader turns it on once every live node heartbeats 2.0, in a transaction holding row 1's lock that first raises every shard row to row 1's value and seeds rows for indexes a 1.x node created | `internal/store/sql.go`, `internal/cluster/membership.go` (the zstd_bodies gate's pattern) | `TestMixedVersionShardSeqs` in `store`; chaos `TestRollingUpgrade` from 1.8 to 2.0 under load |
| 4.2 | **Chaos suite** with partial bulks and the token: kill a node mid-bulk on a multi-shard index, restart the database mid-load | `test/chaos` | `TestKillMidBulk`, `TestDatabaseRestart` on 8 shards |
| 4.3 | **Benchmarks against Phase 0**, on NVMe with `fdatasync`: `BenchmarkCommitLoad` (64 × 4,096, 8 × 500, 64 × 500, 1-shard control) and `slbench writeload` at 1 and 64 shards, against base `c394860`. Exit bar: 64 × 4,096 at least 2× base sustained; the 1-shard control within 5% | `bench/` | Phase 0's script |
| 4.4 | **Release notes**: breaking changes, the upgrade order (all nodes to 2.0, then the flag), MySQL's table rebuild, Postgres's in-transaction key swap and its lock time | `CHANGELOG.md`, `RELEASING.md` if needed | |

**Ship blockers M4 must close** (from the M1 review, reproduced by its `TestReviewMixedVersionSkips`, kept at
`E:\tmp\m1-review\review_mixed_test.go` as M4's starting point). With 1.x and 2.x nodes writing after migration 0007:

1. A 1.x write takes row 1's next value on any shard; a tailer passes it, and a later 2.x write on the same shard
   takes a lower seq from the shard's row, which the tailer never reads. On a shard 1.x wrote several times, a 2.x
   write collides with 1.x's seqs.
2. Indexes 1.x creates have no sequence rows (and `Migrate`'s row check then fails a 2.x node's start).
3. A 1.x drop leaves the index's rows and raises no floor.
4. A 1.x remap leaves stale mapping versions in the rows.
5. 1.x's `ReadTip` and `ReadCounterMark` join `sl_changes` on `seq` alone, where a seq now names one row per shard,
   and pick any of them.

The gate's flip (4.1) must raise every shard row to row 1's value, seed rows for the indexes 1.x created, clear rows
a 1.x drop left, resync mapping versions and uids from `sl_indexes`, and refuse 1.x writes from then on. M4's tests
reproduce each of the five, before and after the flip.

**Measured for M4** (M1 review, on the workstation): the key swap holds Postgres's ACCESS EXCLUSIVE lock on
`sl_changes` for about 4.5 s per million rows, blocking reads too; MySQL's online rebuild takes about 18 s per
million rows; SQLite's rebuild 33 s per 3 million. Measure production's changelog size before release, and if it
is large, give the runner a non-transactional step for Postgres: `CREATE UNIQUE INDEX CONCURRENTLY`, then
`ADD PRIMARY KEY USING INDEX`.

**What M4's tests prove:** a 1.x cluster upgrades in place under load with no client error and nothing lost, and
the gain Phase 0 measured survives the real implementation.

## Open questions, decided

The coordinator accepted every recommendation (2026-11-02):

1. **No epoch in 2.0's token:** the index uid and the shards' seqs. A token past a shard's head after a lossy failover
   may be satisfied by a reissued seq; the docs say so. A generation stamp waits for Change B.
2. **Small batches over many shards** are measured in M4 (4.3) before anything changes.
3. **The Postgres key swap** stays in the migration's transaction; its lock time is measured in M4 (4.3).
4. **`sl_changes_seq`** is dropped in 2.1's first migration.
5. **`write_transactions`** is a node setting (M2, 2.6).
6. **Percolation** runs on the shards that committed, and `docs/api.md` says so (M3).

## Where the approved plan was wrong or out of date

Accepted, and corrected in the approved plan (2026-11-02):

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
