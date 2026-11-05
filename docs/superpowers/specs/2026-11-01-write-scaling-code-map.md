# Write scaling: what depends on one global seq and one database

Research for [the write scaling plan](../plans/2026-11-01-write-scaling.md). Read from the code at
`2de02f0` (v1.8.0 plus docs); nothing was run. Paths are relative to the repo root, and line numbers
are as of that commit.

- **Change A:** one sequence per shard in place of the one `sl_counter` row.
- **Change B:** `sl_changes` and `sl_documents` spread across several Postgres instances.

Lines marked **A:** or **B:** are inference about those changes. The rest states what the code or
docs say.

## Four findings that shape the plan

1. **A counter per shard alone gains little.** Three other things serialize commits:
   - **`pg_notify`.** `writeSQL` calls it inside every transaction (`internal/store/postgres/statements.go:88`).
     PostgreSQL's `PreCommit_Notify` then takes an `AccessExclusiveLock` on "database 0", released
     after commit (`src/backend/commands/async.c:921`, `REL_18_STABLE`, read 2026-11-01). The lock
     is on a shared object, so commits that notify run one at a time **across the whole instance**.
     Whether this holds in later PostgreSQL versions wasn't checked.
   - **One committer per node.** `GroupCommitter.run` calls `flush` synchronously, so one transaction
     is in flight per node (`internal/store/groupcommit.go:193-246`, `internal/node/single.go:133,519`).
   - **Mixed batches.** It concatenates requests with no regard to index or shard
     (`groupcommit.go:381-384`), and a whole request is one Apply (`internal/node/write.go:569`). A
     large bulk hashes onto nearly every shard, so it would hold nearly every shard's lock.
2. **Saved-query changes don't reach every shard.** A query lives on the one shard its id hashes to,
   and percolation fans out at read time (`internal/node/write.go:820-875`,
   `internal/store/records.go:291-292`). There's no global query stream to preserve.
3. **Ordering needs are per shard. What crosses shards is atomicity and the scalar seq in the API.**
4. **The default is one shard per index** (`internal/store/types.go:303,313`), and the shard count is
   fixed at creation (`internal/store/indexes.go:172-180`). An index needs several shards to gain
   anything, and existing indexes can't get more without a rebuild.

## 1. The commit path

`Apply` (`internal/store/sql.go:419-561`) validates and compresses outside the transaction
(`:419-428`), and retries up to 3 times on retryable errors (`:437-443`). Inside the transaction:

1. `:465` Begin with `ApplyTx`.
2. `:477` `LockCounter` returns the counter and one database `NOW`.
3. `:480-482` Reset each change's `Seq`, because retries re-enter here.
4. `:486` `checkIndexes` reads `sl_indexes` per distinct index (`:621`): a metadata read in a data
   transaction.
5. `:491-511` `checkConditions`, if conditional: reads document or query state (`:689-694`) and
   tombstones (`:730`), then drops skipped changes.
6. `:512` `first..last = counter+1..counter+len`, contiguous over kept changes.
7. `:513-527` Nothing kept: commit tombstones only, counter unchanged.
8. `:530-537` Build change rows with one `At`, plus uid, mapping version and nonce per row.
9. `:538-540` `netState`, `addTombs`, `notifications` (one per touched shard, `:888-910`).
10. `:543` Run the dialect's statements.
11. `:549` Commit.
12. `:553-559` Assign seqs back to the original batch positions.

Other assumptions: a provisional seq is `first+position` (`:758,804,806`); one batch may name many
indexes (`:612-655`) and shards (`:825-867`).

Group commit (`internal/store/groupcommit.go`): flushes at 2 ms, 4,096 changes or 16 MiB (`:20-28`).
Each request gets its own contiguous range (`:55`). An oversized request commits alone (`:233-236`).
Any non-conflict error fails every caller with one error (`:359-361`). The transaction runs on a
background context, cancelled only when all callers are gone (`:386-397`).

| Dialect | Lock | Write shape | Limits | Notify |
|---|---|---|---|---|
| Postgres | `FOR UPDATE`, read committed | One CTE statement with 44 array parameters | 64 MiB per statement | `pg_notify` in the statement |
| MySQL | `FOR UPDATE`, read committed | Multi-row statements, counter last | 65,535 params, 16 MiB | None |
| SQLite | `BEGIN IMMEDIATE` is the lock | One row per statement, counter last | None used | None |

Refs: `internal/store/postgres/statements.go:32,55-60,61-88,94-131`, `internal/store/postgres/store.go:48-52,63-66,94`,
`internal/store/mysql/statements.go:22,34,52-77`, `internal/store/mysql/build.go:21-75`,
`internal/store/sqlite/statements.go:24-28,44-80`, `internal/store/sqlite/store.go:4-25`.

**A:** a multi-shard Apply must lock rows in sorted order, or deadlocks consume the 3 retries.

## 2. Every use of the global seq

**Statements** (`internal/store/dialect/statements.go:145-162,293`):
- `LockCounter`.
- `ReadHead`.
- `ReadCounterMark`: `h ON h.seq = c.value` assumes a seq names one row across all shards
  (`postgres/statements.go:33`).
- `ReadTip`: the same assumption, and it joins `sl_pruned` and `sl_indexes` in one statement (`:35`).
- `NextEpoch`: row 2 of `sl_counter` (`:297`).

**Callers:**
- `sql.go:477` Apply, `:1005` `ScanShard`, `:1071` `HeadSeq`, `:1091` `Tip`, `:1191` `raiseHorizon`.
- `indexes.go:155,194-198` Update, contiguous with one seq per shard, `:227` Drop.
- `registry.go:216,244-257,368-371,400`: row 2 issues epochs, and its row lock is also the
  cluster-wide serializer of claims and retires.

**Contracts:**
- `store.go:10-14`: single counter row, commit order equals seq order.
- `store.go:71-87`: `HeadSeq` and `Tip` take no shard.
- `types.go:85-86`: "position in the global changelog".
- `dialect/dialect.go:27-29`: reads must reach the primary, because a tailer reads the head and then
  the changes.

**Schema** (`internal/store/{postgres,mysql,sqlite}/migrations/0001_init.sql`):
- `sl_documents.seq`, `sl_queries.seq`.
- `sl_changes.seq PRIMARY KEY`, and index `sl_changes_shard_seq (index, shard, seq)`.
- `sl_counter` rows 1 and 2, `sl_pruned.below_seq`, `sl_shard_copies.applied_seq`.
- `0006_body_hash.sql`: `hash_seq`. `0003_change_nonce.sql`: `nonce`.
- `sl_tombstones` has no seq (`0005_versions.sql:14-23`).

**A:** `sl_changes` needs a composite primary key. `sl_counter`'s integer id can't name a shard, so
this needs a new table.

**Node and shard state built on the scalar:**
- `internal/node/single.go:207-210,434,535,622,809,821`: `written`, `querySeq`, one head per node,
  `noteHead`, `lowerHead`.
- `internal/node/read.go:32-46`: `checkHead`.
- `internal/node/write.go:92-100,605`: head and result are maxima across shards.
- `internal/shard/shard.go:11-12,94-95,151,777,895,930`: gaps expected, `ErrSeqOrder`, `Advance`,
  `WaitRefreshed`.
- `internal/shard/manifest.go:33-35,49-50`, `internal/shard/generation.go:376-378,586`.

**Tests that write the counter directly:** `internal/store/{codec,nonce,tipplan,harness}_test.go`,
`internal/node/refresh_test.go:171`, `internal/replica/{rewind,rewindnonce}_test.go`.

## 3. Client-visible seq

| Field | Compared across | A and B |
|---|---|---|
| `seq` on bulk | Shards and documents (a maximum) | **Breaks** |
| `wait_for_seq` | Every shard, and across indexes | **Breaks** |
| `percolated_seq` | Shards (a minimum) | **Breaks** |
| `current_seq`, `if_seq` | One target only | Safe, if stored values stay valid |
| `_seq` on hits | One document; not unique across shards after A | Safe per document |
| `version`, tombstones | One target only | Safe |
| `applied_seq` and friends | One copy | Safe; lag changes meaning |

**`wait_for_seq` today.** Every shard of the index waits for the same N, including shards the write
never touched (`internal/node/target.go:222-244`). A local copy waits until `RefreshedSeq >= N`, with
504 at the deadline (`:348-377`). A peer runs its own `checkHead` first (`:293-319`). An idle shard
reaches N only because `Advance(head)` jumps over other shards' seqs (`internal/replica/tailer.go:703-712`,
`internal/shard/shard.go:891-893`). Remote: `internal/cluster/router.go:494`, `wire.go:42`,
`peer_api.go:219`. Docs: `docs/api.md:45,475,515,1121,1138`, `api/openapi.yaml:10,17,390,756`,
`docs/clustering.md:160-168`.

**`seq` in write responses.** `res.Seq` is the maximum across shards (`internal/node/write.go:96,100`),
waited on for every touched shard (`:120,671-707`). `written` (`:92-95`) already holds a per-shard map.
`docs/api.md:35-37`: "Seqs are global, contiguous and increase in commit order."

**`percolated_seq`.** The minimum across shards (`internal/node/target.go:190-198`). A remote target
reports the seq it was asked for, not one it observed (`internal/cluster/router.go:648`).

**`if_seq`.** Compared to one target's stored seq inside the transaction (`sql.go:784,789`). A stored
seq must stay meaningful, so existing rows stay valid after migration.

**`gc_deletes` tombstones.** Prune by index and age across all shards, by one database's clock
(`sql.go:1230-1247`, `postgres/statements.go:366`). **B:** must run on every shard database.

**Consumers outside this repo.** scrape-bot compares `mirrored_seq` with `percolated_seq`, compares
`decided_seq` with an answer's seq, stores them as bigints, and passes one seq as `wait_for_seq`
(`src/scrape_bot/watch_search.py:795-797,1024,1183`, `models.py:198-216`,
`searchlight/mirror.py:1341,1563,1587,1612`, `searchlight/reconcile.py:195,307`). Found by grep; call
sites not traced.

## 4. Tailers and copies

`internal/replica/tailer.go`:
- `:20-23` "seqs are global, so a shard's own have gaps".
- `:652-655` The head is read before the page, so a short page proves completeness up to the head.
  This assumes commit order equals seq order.
- `:703-712` `Advance` over gaps.
- `:445` Lag is global head minus applied, so it counts other shards' changes.
- `:84,198-199` 500 ms poll.

Already keyed by shard, and largely portable:
- `internal/replica/hub.go:137-145`: wake by shard if `n.Seq > Applied`. One subscription per node
  over one store (`:32-38`).
- Push hints: `internal/node/write.go:92-95,116-118`, `internal/cluster/router.go:699-714,865-946`,
  `wire.go:519-523`, `peer_api.go:194-203`, `internal/node/host.go:328-338`.
- `ChangesAfter`, `MinSeq`, `DeleteChanges`, `Horizon`: all by index and shard (`sql.go:924-983`,
  `postgres/statements.go:365,367`).
- `internal/cluster/maintenance.go:235-264`: `pruneFloor` compares within one shard.
- `internal/shard/cache_identity.go:18-25`, `internal/replica/content_mark.go:5-27`: independent of
  the global seq.

Not shard-scoped: the head row and at-seq joins in `ReadTip` and `ReadCounterMark`. `ageFloor` and
`logStart` binary-search `lo..HeadSeq` (`maintenance.go:270-271,284,311`), where a per-shard head is
enough. Retention: `sql.go:1145-1204`, with `raiseHorizon` under the counter lock.

## 5. Recovery and failure

`internal/replica/rewind.go`: a lost-commit failover makes the counter fall behind, and seqs are
reissued (`:17-41`). "A mark may name another shard's change" (`:33-35`). `checkRewind` looks for head
behind on two polls, own-shard change, or anchor mismatch (`:259-304`).
`internal/replica/recover.go:70-155,180-182,243-283,511-578`.
`ScanShard` takes one snapshot covering counter, marks, `sl_indexes`, documents and queries
(`sql.go:989-1044`).

Fencing: registry writes check the epoch (`registry.go:309,444,459`). Apply takes no epoch or lease,
so any node commits any write (`sql.go:464-561`). The counter update is in the transaction, so a
rollback leaves no gap (`sql.go:469-474`).

| Assumption | A (one database) | B (independent databases) |
|---|---|---|
| Counter and changelog rewind together | Holds per row | Holds per shard database only |
| All shards rewind together | Holds | **Breaks**: one shard can rewind alone, losing part of a bulk |
| Head behind means rewind | Holds with a per-shard head | Holds per shard |
| Anchor may name another shard | Collapses to own shard; logic can shrink | Same |
| Catalogue never rewinds separately from the log | Holds | **Breaks**: a metadata rewind can leave `sl_indexes` behind the logs |
| Epochs never reissued | Holds | **Breaks** on metadata rewind |
| `lowerHead` is node-wide | Must become per shard | Must become per shard |
| Snapshot's mapping matches its seq | Holds | **Breaks** unless the data database holds it |
| One error describes one commit outcome | Holds per transaction | **Breaks**: a multi-database request can partly commit |

Failure docs: `docs/clustering.md:280-281`, `docs/operations.md:765-780`, design spec `:356-357`.

## 6. Saved queries and percolation

Queries route by the same FNV-1a of their id (`internal/node/write.go:421,820-821`). Query changes
sit in their own shard's changelog.

A mixed bulk is one transaction, and every document percolates after commit, against queries as they
stand at the request's end, including its own `query_upsert`s (`docs/api.md:443-447`,
`internal/node/write.go:125-129,787-818`, test `internal/api/versions_test.go:54`). **A and B:** that
promise then depends on several commits completing.

Within a shard, order is total by seq. Across shards, nothing relies on it beyond the scalar
`wait_for_seq` and `percolated_seq` (`internal/node/read.go:443`, `write.go:601-603`,
`target.go:254-257,274`, global seed at `single.go:622`). A percolated document is passed in, not read
from the log, so only query visibility on each shard matters.

## 7. Index-level operations

`internal/store/indexes.go`:
- `:142-146` A mapping change holds the lock: "commits wholly before or after any Apply".
- `:147-210` One transaction updates `sl_indexes` and logs `KindMapping` to every shard.
- `:52-75` `Create` has no lock and no changelog row.
- `:212-245` `Drop` under the lock: "no change for the index commits after it".
- `:185` Settings-only updates take the lock but log nothing.

Copies adopt a mapping at its seq and require `version+1` (`internal/replica/apply.go:528-594,559`).
Any other version on a change forces a rebuild (`:581-585`). A uid mismatch forces a rebuild (`:508`).
New dynamic fields go into the stored mapping before the documents commit, in a separate transaction,
so ordinary writes can log a mapping change to every shard (`internal/node/write.go:291-296,316`,
`internal/node/mapping.go:61-154`).

These need ordering per shard, not across shards: each change must follow its own shard's mapping
row. What crosses shards is **atomicity**.

**A:** a `Remap` or `Drop` locks every shard row of the index.

**B:** if the version comes from a central catalogue and mapping rows fan out afterwards, an Apply can
stamp version k before that shard's log holds k, which triggers the rebuild at `apply.go:581-585`. Drop
has the matching problem: "no change commits after it" spans every shard database.

## 8. Table placement under B

- **One metadata database:** `sl_nodes`, `sl_features`, `sl_shard_copies`, `sl_counter` row 2,
  `sl_indexes` as the authority, `sl_blobs`, `sl_blob_uploads`, `sl_blob_chunks`.
- **Per data database:** `sl_changes`, `sl_documents`, `sl_queries`, `sl_tombstones`, `sl_pruned`, the
  sequences, and a local copy of each index's uid and mapping version.
- **Every database:** `sl_schema_migrations`.

| # | Operation | Location | Joins data and metadata? |
|---|---|---|---|
| 1 | Apply | `sql.go:464-561` | **Yes**: reads `sl_indexes` |
| 2 | Index update | `indexes.go:147-210` | **Yes**, on writes |
| 3 | Index drop | `indexes.go:214-245` | **Yes**, on writes |
| 4 | `ScanShard` snapshot | `sql.go:989-1044` | **Yes**, read snapshot |
| 5 | `ReadTip` statement | `sql.go:1085-1101` | **Yes**, one statement |
| 6 | `ListQueries` snapshot | `records.go:193-229` | **Yes**, read snapshot, cross-shard |
| 7 | `raiseHorizon` | `sql.go:1184-1204` | No, if both are per shard |
| 8 | `claimOnce` | `registry.go:157-242` | No |
| 9 | `retireOnce` | `registry.go:393-430` | No |
| 10 | `RenewLeases` (MySQL) | `registry.go:273-306` | No |
| 11 | Blob switch and delete | `blobs.go:197,428` | No |
| 12 | Migrate | `migrate.go:128,149` | Runs per database |

Linked by value, not joined: `ReportApplied` (`registry.go:434`), `pruneAll` (`maintenance.go:125-176`).

Single-database assumptions: one `StoreURL` (`internal/config/config.go:41-44`), one `Open` and
`Migrate` (`cmd/searchlight/main.go:281-283`), one store interface (`store.go:42-119`), "that one
clock" for leases and change times (`dialect/statements.go:10-13`), 64 connections per node
(`postgres/store.go:94`), which multiply per database. No `PREPARE TRANSACTION` exists anywhere today.

## 9. Reads from SQL

| Read | Location | Scope |
|---|---|---|
| Realtime `GET` | `internal/node/read.go:555-606,678-696`, `records.go:136-169` | Primary key, one shard |
| `if_seq`, `version`, query op checks | `sql.go:686-813` | Primary key, one shard, in the transaction |
| Unchanged detection | `write.go:181-234`, `records.go:63-129` | **One statement across shards**, outside the transaction |
| `ChangesAfter` | `sql.go:924-983` | One shard, plus horizon |
| SQL-scan recovery | `sql.go:989-1062` | One shard's rows, plus catalogue and head joins |
| List queries | `records.go:181-289`, `dialect/statements.go:216-230` | **`UNION ALL` across shards** in one snapshot |
| Queries by id, document hashes | `dialect/statements.go:233-266` | **`OR` across shards** |
| `ReadTip`, `ReadCounterMark` | `postgres/statements.go:33,35` | **Head row may be any shard** |
| Drop data, tombstone prune | `postgres/statements.go:354-361,366` | **All shards by index name** |

There are no joins between `sl_documents` and `sl_queries` or `sl_changes`.

## 10. Routing and atomicity promises

`ShardFor` is FNV-1a modulo n, "forever, so it never changes" (`internal/node/tailer.go:76-86`). There's
no `routing` parameter. Maximum 1,024 shards (`types.go:300`).

Promises to clients:
- `docs/api.md:413-414`: "Every operation in one request commits in one transaction."
- `docs/api.md:443-444`: "Documents and saved queries mix in one request, in one transaction."
- `docs/api.md:481-483`: "it is committed whole or not at all, never in part".
- `docs/api.md:398-400`: noops are outside the transaction.
- `docs/api.md:36`: "an acknowledged write is durable". `:1248`: a 5xx may or may not have committed.
- `docs/clustering.md:125-127,132-134`, `docs/architecture.md:98-105`, `api/openapi.yaml:371`.
- Design spec `:173`: "one transaction per request".

## 11. Tests and spec statements that pin this

Tests:
- `internal/store/conformance_test.go`: `checkLoad` ranges "tile 1..N with no gap or overlap"
  (`:269-303`), `TestConcurrentWriters` (`:308`), `TestGroupCommitConcurrentWriters` (`:326`),
  `TestLateCommitNotSkipped` (`:356`), `TestPrune` (`:690`), `TestHeadSeq` (`:1576`), `TestTip` (`:1604`),
  `TestGroupCommitIsolation` (`:1669`), `TestMappingChanges` (`:2071-2129`),
  `TestMappingChangesOrderWithApply` (`:2133`), `TestShardCountImmutable` (`:2195`).
- `internal/store/groupcommit_test.go:66-511`.
- `internal/api/cancel_test.go:42-49`: **`TestAbandonedBulkIsAllOrNothing`**.
- `internal/api/endpoints_test.go:177`: "wait_for_seq waits for the write, on every shard".
- `internal/api/hostile_test.go:419`, `internal/api/data_test.go:183,230,240`,
  `internal/node/single_test.go:1249`, `internal/cluster/focus_test.go:171-172,204-208`.
- `internal/replica/rewind*_test.go`, `internal/shard/seqmark_test.go`.
- `test/chaos/harness_test.go:675,848,903`, `test/chaos/chaos_test.go:133,244`.

Spec (`docs/superpowers/specs/2026-10-02-searchlight-design.md`):
- §1 `:39`: "**No coordination service:** no ZooKeeper, no etcd, no consensus library. The SQL database
  is the only coordination point."
- §2 `:64`, `:69`: fixed shards, one transaction per write.
- §8 `:308-316`: "`sl_counter` | the sequence row, locked per `Apply`". "Visibility order therefore equals
  `seq` order, and tailers never skip a late commit."
- §14 `:422`, §17 `:471,479`.

Others:
- `docs/superpowers/specs/2026-10-05-searchlight-part2-logs-roadmap.md:12,17`.
- `docs/clustering.md:51-56,74-75`.
- `README.md:33-36`: "write throughput is bounded by the database: scale the database for writes."

Nothing in `docs/superpowers` already discusses per-shard sequences or sharding the database.

**Doc drift to fix alongside:** `docs/clustering.md:166-168` says `wait_for_seq` is "trivially satisfied"
on `GET`, but the code runs `checkHead` and waits on the stale path. The spec table `:303-313` omits
`sl_tombstones` and `sl_pruned`.

## 12. What can measure a baseline today

- **`internal/store/bench_test.go:109,135`:** `BenchmarkApply`, `BenchmarkApplyMix`, changes/s across
  dialects. The most direct measure of the commit path alone.
- **`internal/cluster/bench_test.go:117`:** `BenchmarkWriteThroughput`, 1 and 3 nodes on a 3-shard index.
- **`internal/replica/bench_test.go:43,129`, `internal/api/bench_test.go:38,87`.**
- **`slbench writeload`** (`bench/cmd/slbench/writeload.go`): Postgres or MySQL through `--store-url`.
  Limits: `"shards": 1` is hard-coded (`:205`). One index (`:196`). Noops count as written (`:533`), so
  use `--changed 1`. For a database baseline, set `--percolate=false --readers 0` and raise
  `--gomaxprocs`. Setup uses a maximum seq with `wait_for_seq` (`:297-314`).
- **`slbench run`** (`runcmd.go:53-104`): has `--shards`. Postgres needs `--sl-url`.
- **Metric:** `searchlight_store_group_commit_batch_size`.

Measured so far: #69 records Postgres at 8.6-18.1k docs/s and MySQL at 1.3-1.4k, on a noisy Windows
box at 200k documents, fsync-bound. T11 (many nodes) is not measured.

## 13. What couldn't be determined from the code

- Where write time goes on Postgres: nothing separates the counter lock, the WAL fsync and `pg_notify`.
- scrape-bot's shard count in production, and so its exposure to multi-shard behaviour.
- `bench/workloads/searchlight.go` and `crosscheck.go` weren't read and may use the scalar seq.
- Not fully traced: settings propagation in `internal/cluster/membership.go`, the peer snapshot handler
  in `internal/cluster/peer_api.go`, and the chaos suite beyond its seq usage.
- Whether `Advance` publishes a generation with no new data: taken from comments, not the refresh path.
