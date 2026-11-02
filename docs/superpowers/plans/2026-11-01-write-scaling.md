# Plan: scaling writes with per-shard sequences and sharded databases

**Status:** proposed, no code written. Decisions D1 to D6 need the operator's approval before Phase 1.

**Research:** [what the code depends on](../specs/2026-11-01-write-scaling-code-map.md) and
[external research](../specs/2026-11-01-write-scaling-external-research.md).

## Context

Every write commits through one SQL database under one locked row, `sl_counter`. So the whole cluster commits one
batch at a time, and adding nodes adds search capacity but no write capacity. The aim is for write throughput to grow
with the hardware added, as it does with Elasticsearch's primary shards, while SQL stays the system of record and no
consensus service is added.

Two changes do this:
- **Change A:** one sequence per shard, so commits to different shards run in parallel.
- **Change B:** shards placed on several Postgres instances, so the WAL stops being one bottleneck.

Neither pays off fully alone. A leaves one WAL, and B needs A.

## What the research changed

1. **The counter isn't the only serializer.** `pg_notify` takes an instance-wide exclusive lock through commit, and each
   node runs one transaction at a time, mixing every shard. Removing the counter alone would show little gain. The
   parallelism has to come from all of it together.
2. **Multi-shard atomicity has to go.** A bulk in one transaction holds every shard's lock, which removes the
   parallelism. Across databases it needs 2PC, where one orphaned prepare holding a counter row stops all writes and
   tailing on that shard.
3. **A per-shard counter still holds through the WAL flush**, capping at roughly 670 commits/s per shard at a 1.5 ms
   flush (arithmetic, not measured). That's acceptable because each commit carries up to 4,096 changes, and rows per
   commit is the lever.
4. **Several databases in one Postgres instance gain little.** WAL is per instance, so B needs separate instances **on
   separate storage**. On one TrueNAS pool, B would add operational burden with little gain.
5. **Queries, tailers and prune floors are already per shard**, so a lot of the design ports cleanly. The hard parts are
   the public API, index-level operations and failure handling.

## Decisions for approval

| # | Decision | Recommendation | Why | Cost |
|---|---|---|---|---|
| **D1** | Bulk atomicity | **One commit per shard, per-item results. No 2PC.** A request is whole-or-nothing only when all its ops land on one shard | Same model as Elasticsearch. Failure is partial and reported, rather than a stalled shard | **Breaking.** Reverses `docs/api.md:413-414,481-483`. A 5xx may mean some shards committed, so clients must retry safely |
| **D2** | Read-your-writes | **Opaque token: a sparse vector of `(shard, seq)` plus epoch and shard-map version**, merged by per-shard maximum | No clock needed, stateless, waits only on shards a query hits | **Breaking.** Replaces scalar `wait_for_seq`, `seq` and `percolated_seq` on the bulk response. About 0.7 KB at 64 touched shards |
| **D3** | How to shard | **Application-level, in Searchlight, over plain Postgres** | Searchlight already routes by shard, and needs no router tier. Citus would add a coordinator and 2PC we'd avoid anyway, and Neki and Multigres are young or have no shared snapshot | We build placement and shard moves ourselves |
| **D4** | Dialects | **A on all three; B on Postgres only.** SQLite keeps one writer and gains nothing | MySQL measured 1.3-1.4k docs/s, so its limit is elsewhere (#69). Three dialects multiply failure testing | B-only features need a Postgres store |
| **D5** | Version | **Searchlight 2.0.0**, with no compatibility shim for the scalar | D1 and D2 break the API either way | Clients must migrate, including scrape-bot |
| **D6** | Target | **Set after Phase 0.** Proposed exit bar: 2 instances on separate storage give at least 1.7× one instance on a multi-shard bulk load | We need a number to stop against | Needs hardware for two instances |

**Rejected:**
- **2PC:** see above.
- **Consensus or Elasticsearch-style translogs:** drops "local disk is only a cache" and the no-consensus design
  (spec §1).
- **CockroachDB or YugabyteDB:** best-effort locks and costly per-commit consensus fight a counter-row design.
- **`synchronous_commit=off` by default:** breaks "acknowledged means durable". It stays an operator choice, once epochs
  make the rewind detectable.

## Phase 0: measure (about 1 week, no behaviour change)

Everything after this depends on the result.

1. Separate the time in a commit on Postgres: counter lock wait, statements, WAL flush, `pg_notify`.
2. Run on the real target storage, including ZFS under a VM, not only NVMe. Check flushes are honoured with
   `pg_test_fsync` in the guest.
3. Make `slbench writeload` take a shard count (it hard-codes `"shards": 1` at `:205`). Use `--changed 1
   --percolate=false --readers 0` and raise `--gomaxprocs`. Add `BenchmarkApply` at 1, 8 and 64 shards.
4. Try a spike: drop `pg_notify` from the transaction, and try several committers against per-shard rows on a throwaway
   branch. This is the cheapest test of whether Change A pays.
5. Close out #69 with those numbers, and record scrape-bot's production shard count.

**Gate:** if the spike doesn't show at least 2× on a multi-shard load on one instance, stop and find the real limit before
Phase 1. If one instance already far exceeds what's needed, stop altogether and write that down.

**Outcome (2026-11-02): met on NVMe, not on a hard disk; [results](../specs/2026-11-01-write-scaling-phase0-results.md).**
On NVMe with a real flush, per-shard counters sustained 4.0-4.4× base durably on one instance (64 shards with 4,096
changes, 8 with 500; one shard unchanged), and 2.2× end to end. There, base is limited by one backend's statement
CPU. On a 7,200 rpm disk, WAL bandwidth binds first and the gain was at most 1.32×. Check production's storage with
the results' script before Phase 1, and keep rows per commit high, since small batches over many shards lost.

## Phase 1: remove the serializers, inside one database (2-3 weeks)

Non-breaking and useful on its own.

1. **Move `pg_notify` out of the transaction.** Notify after commit. It's only a wake-up: push hints and the 500 ms poll
   cover a lost one, so a crash between commit and notify costs at most one poll. Measure wake latency.
2. **A committer per shard** (or a bounded pool keyed by shard), replacing the one `run` loop. Bound total connections: the
   64-per-node pool matters here.
3. **Stop `checkIndexes` reading `sl_indexes` per transaction** where a cached uid and mapping version will do, with the
   conflict path re-reading.

## Phase 2: per-shard sequences, Change A (3-5 weeks, breaking)

1. **Schema.** New `sl_shard_seq (index_uid, shard, value)`. `sl_changes` primary key becomes `(index_name, shard, seq)`.
   On Postgres, build the unique index concurrently and then swap. MySQL needs a table rebuild, so document the downtime.
2. **Seeding.** Every shard's counter starts at the old global head. Stored `seq` values and `if_seq` stay valid, and
   per-shard order stays monotonic.
3. **Commit.** Split a request by shard, then one transaction per shard, each taking one counter row. Conditions, tombstones
   and net state stay per shard. A request on one shard keeps full atomicity (D1).
4. **Bulk response.** Per-item status, with a token (D2). A failed shard fails its own items only. The bulk's `errors` flag
   covers partial failure. Documents percolate after their shards commit, and the "queries as they stand at its end" promise
   narrows to shards that committed.
5. **Index-level operations.** Update and Drop lock all of an index's shard rows, in sorted order, in one transaction. Plain
   Apply locks one row, so these can't deadlock with it.
6. **Heads.** `HeadSeq`, `Tip`, `checkHead`, `noteHead`, `lowerHead`, `ReadCounterMark` and `ReadTip` become per shard. `Advance`
   over gaps goes, because a shard's seqs are now its own. Lag becomes real per-shard lag. `ageFloor` and `logStart` search the
   shard's own head.
7. **Rewind.** Anchors now only name own-shard changes, so this logic shrinks. Rewrite the rewind tests rather than adapting them.
8. **API.** Token on writes; `wait_for=<token>` on reads, waiting only on the query's shards, with 504 on timeout and a defined
   response when the epoch or shard map changed. `if_seq`, `_seq` and `version` unchanged. Update `docs/api.md`, `api/openapi.yaml`,
   `docs/clustering.md`, the design spec §2, §8 and the README.
9. **Rolling upgrade.** Mixed versions take different locks, so this is a gated cutover through `sl_features`: new code keeps
   using the global row until every node is on the new version, then the flag flips in a transaction holding the old lock. Test
   with the chaos suite's rolling upgrade.

**Tests to rewrite:** `checkLoad`'s contiguity, `TestMappingChanges`, `TestAbandonedBulkIsAllOrNothing`, the rewind suite, chaos
harness `wait_for_seq` use. New: a deadlock-free stress of multi-shard requests against Remap and Drop; `TestLateCommitNotSkipped` per
shard.

**Exit:** D6's bar on one instance for lock contention, and no conformance or chaos regressions.

## Phase 3: scrape-bot and other clients (1-2 weeks, in that repo)

- Replace `mirrored_seq`, `percolated_seq` and `decided_seq` comparisons with per-shard or token forms
  (`watch_search.py:795-797,1024,1183`, `models.py:198-216`, `mirror.py`, `reconcile.py`).
- Handle partial bulk results: only rows whose item succeeded leave the mirror queue.
- Add a migration, and bump the Searchlight version floor.

Change A isn't released until this is ready, so the two ship together.

## Phase 4: decision point before Change B

Rerun Phase 0's measurements. Only go on if the WAL or the instance is now the measured limit, and separate storage exists for at
least 2 instances. Otherwise stop: Change A has shipped value on its own.

## Phase 5: sharded databases, Change B (2-3 months, Postgres only)

1. **Placement.** Configuration gains a metadata store URL and named data stores. `sl_shard_placement (index_uid, shard, store)`
   lives in the metadata database, chosen at index creation. A one-store config stays the default and behaves as today.
2. **Store split.** Tables placed per the code map §8. The `Store` interface splits into metadata and data parts. One pool, one
   `Listen` and one Hub subscription per data store. Set a connection budget per node.
3. **Local mapping copy.** Each data store keeps `(index_uid, shard, mapping_version, mapping)`, written with that shard's mapping row.
   This lets Apply, `ScanShard` and `ReadTip` stay single-database and avoids the rebuild at `apply.go:581-585`.
4. **Fan-out protocols, resumable and idempotent:**
   - **Mapping change:** metadata records the new version as pending, then each shard store applies it under its counter lock, then
     metadata marks it complete. Changes are additive, so a shard ahead of another is safe for writes that follow it. A write's new
     field commits only after its own shard has the mapping row. A sweeper finishes pending work after a crash.
   - **Create and Drop:** Drop marks the uid dropped in metadata, so writes are refused by uid. Then each store deletes, and a sweeper
     resumes.
5. **Cross-shard reads become coordinator merges:** `ListQueries`, `DocumentHashes`, `DropData`, tombstone prune. `ListQueries` loses its
   single snapshot, so document its weaker listing guarantee.
6. **Independent failure.**
   - **A data store's rewind:** handled per shard by the rewind logic. A bulk may lose only that shard's part, and acknowledged tokens may
     now point past the head. Define that response. Epoch in tokens is per store.
   - **A metadata rewind:** the catalogue can fall behind the logs, and epochs can be reissued. Detect through a generation stamp in each
     data store and refuse, needing operator action. This is new work with no equivalent today.
   - **A data store outage:** its shards answer 503 for writes and stale for reads, and others carry on. Metadata outage: per-spec behaviour.
7. **Moving a shard between stores.** Copy under a fence: snapshot, tail the source's changelog into the target, stop writes for that shard
   briefly, flip placement with an epoch bump. Needed for growth, and it's the riskiest item here. Copies on nodes keep working by uid.
8. **Clock.** Leases use the metadata clock. Change times and tombstone age use each store's own clock, so document the skew tolerance.
9. **Operations.** Migrations per store, with mixed-state handling. Per-store health, metrics and backups (a consistent backup across stores
   isn't possible, so document point-in-time per store and how rewind handles restore). Add a chaos suite: kill or rewind one store of three.

**Exit:** D6's bar across instances, with chaos tests for single-store outage, rewind, metadata rewind and a move during writes.

## Effort and risk

The estimates are rough and assume one person. Roughly **8-10 weeks** through Phase 3, and **another 2-3 months** for Phase 5.

| Risk | Mitigation |
|---|---|
| The gain doesn't appear because the limit is elsewhere | Phase 0 gate, before anything breaks |
| Clients mishandle partial bulks | Phase 3 ships with Change A; per-item results are explicit |
| Connection counts rise (per-shard committers, per-store pools) | Connection budget in Phases 1 and 5; PgBouncer needs care around `LISTEN` |
| Rewind and metadata failures are subtle | Chaos tests are exit criteria |
| Shard counts are fixed, so existing indexes gain nothing | Document it: reindexing to more shards needs a new index until `_split` or reindex exist (out of scope) |
| B brings little without separate storage | Phase 4 gate requires it |

## Out of scope

Changing the shard count of an existing index (`_split`, reindex), rebalancing shard copies across nodes, Elasticsearch API compatibility,
custom routing beyond what D1 needs, and MySQL or SQLite support for Change B.

## Open questions

1. What write throughput is the goal? D6 needs a real number, and scrape-bot's needs alone may not justify Change B.
2. Is hardware for 2-3 Postgres instances on separate storage available? The planned 2-3 Compose hosts could provide it, but today's TrueNAS
   pool can't.
3. Should D1 offer custom routing, so callers can opt into atomicity by landing on one shard? Recommended as a small addition if a client needs it;
   nobody asks for it today.
