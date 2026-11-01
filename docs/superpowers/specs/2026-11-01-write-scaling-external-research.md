# Write scaling: external research

Research for [the write scaling plan](../plans/2026-11-01-write-scaling.md), gathered 2026-11-01.
Nothing here was run. **S** is what a source states; **I** is inference. Sources are of mixed quality
and some are dated, and the last section lists what couldn't be verified.

## 1. A commit-ordered changelog in Postgres

**`nextval` doesn't work.**
- **S:** Values aren't reclaimed on abort, and sequences "cannot be used to obtain gapless sequences"
  ([PG18 docs](https://www.postgresql.org/docs/current/functions-sequence.html)).
- **S:** Readers cursoring on a sequence can skip rows that commit late
  ([Sequin, 2024-07](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/)).

**A row-lock counter per partition works, and sets the shape of the limit.**
- **S:** Sequin calls serialising writes "usually the way to go", though its lock durations reached
  seconds.
- **S:** Kozlovski's `log_counter` design hit 243k msg/s across 30 partitions on 96 vCPU at about 10%
  CPU, capped near 8k msg/s per table, on io2 EBS rather than NVMe
  ([2025-10](https://topicpartition.io/blog/postgres-pubsub-queue-benchmarks)).
- **S:** Commit runs `RecordTransactionCommit`, then `ProcArrayEndTransaction`, and only then releases
  locks ([xact.c](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/access/transam/xact.c)).
- **I:** A waiter gets the row only after the holder is visible, so seq order equals visibility order.
- **I:** The lock spans the WAL flush, so group commit can't batch two commits on one counter.
  Per-shard commits cap near 1/(flush + lock-held round trips), about 670/s at a 1.5 ms flush. **Rows per
  commit, not commits, is the lever.**

**Alternatives.**
- **Advisory locks.** **S:** MessageDB takes a per-category lock, so one category has no gaps but `$all`
  can ([Protean ADR, 2026-07](https://docs.proteanhq.com/adr/0025-all-subscription-gap-safety/), a secondary
  source). **I:** Equivalent to a per-shard counter.
- **xmin horizon.** **S:** Dudycz filters on `pg_snapshot_xmin(pg_current_snapshot())`
  ([2022-10](https://event-driven.io/en/ordering_in_postgres_outbox/)). **S:** Sequin rejected it because any
  long transaction holds back reads. **I:** Never passes an uncommitted row, but gives neither commit
  order nor a gapless per-shard sequence. Unrelated work and prepared transactions could stall every
  shard. Xids being cluster-wide is unverified.
- **Gap detection.** **S:** In [Marten #4953 (2026-07)](https://github.com/JasperFx/marten/discussions/4953),
  wall-clock skipping couldn't tell slow from rolled back, and committed events went unprojected. **I:**
  Avoid.
- **Logical decoding.** **S:** The WAL sender emits transactions in commit-LSN order
  ([a Debezium issue](https://issues.redhat.com/browse/DBZ-9203), not the PG docs). Slots allow one
  consumer, can replay after a crash, and pin WAL
  ([docs](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html)). A June 2026
  [hackers thread](https://www.postgresql.org/message-id/d99c688994ab3a998afe26e61fe4f69f@oss.nttdata.com)
  reports a Debezium skip risk; its resolution is unconfirmed.

**`NOTIFY` serializes commits.** **S:** `PreCommit_Notify` takes `AccessExclusiveLock` on "database 0",
"so that it'll automatically be released after either commit or abort", to keep queue entries in commit
order ([async.c, REL_18_STABLE, around line 921](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c)).
**I:** Commits that notify run one at a time across the instance, whatever the row locks allow.

## 2. Throughput limits

**Commit settings.**
- **S:** `commit_delay`: start at half the `pg_test_fsync` flush time. It helps less at high client
  counts, because group commit already occurs
  ([docs](https://www.postgresql.org/docs/current/wal-configuration.html)).
- **S:** `synchronous_commit=off`: the risk window is up to 3×`wal_writer_delay`, with loss but no
  corruption, in commit order. `PREPARE` is always synchronous
  ([docs](https://www.postgresql.org/docs/current/wal-async-commit.html)).
- **I:** With async commit the lock releases before the flush, so the per-shard cap rises. A crash can
  then rewind a sequence tailers already read, so positions need an epoch, like Elasticsearch's primary
  terms. It also breaks "acknowledged means durable".

**Measured rates.** None is a clean single-row-insert commit ceiling.

| Source | Setup | Result |
|---|---|---|
| [ClickHouse, 2026-08](https://clickhouse.com/blog/posette-talk-recap-postgres-isnt-slow-your-storage-is) | 16 vCPU, local NVMe, pgbench updates | 16,030 TPS; WAL fsync about 1.5 ms per transaction |
| [pgsql-hackers, 2024-01](https://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg159404.html) | 64 vCPU EPYC, one NVMe | 12-26k TPS sync; about 30k async. More WAL insert locks didn't help |
| [Vondra, 2022-10](https://www.enterprisedb.com/blog/postgres-vs-file-systems-performance-comparison) | 16-core Xeon, ext4 on NVMe | 58.7k TPS read-write at scale 1000 |

**Hot rows.** **S:** Single-row `update-one` scaled only 2.86× at 48 clients on PG18 with fsync disabled
([Callaghan, 2025-10](https://smalldatum.blogspot.com/2025/10/measuring-scaleup-for-postgres-180-with.html)).

**Partitioning.**
- **S:** Hash partitioning gave 7.2× throughput by spreading relation-extension locks
  ([EDB, 2026-10](https://www.enterprisedb.com/blog/finding-and-fixing-silent-throughput-ceiling-postgresql)).
  The article's config text and code disagree.
- **I:** Partitioning `sl_changes` helps extension, index contention and retention. It doesn't help the
  counter row or the WAL.

**WAL.** **I:** WAL is per cluster. Change A leaves one WAL. Change B scales it only with **separate
instances on separate storage**, not several databases in one cluster. No credible WAL ceiling figure was
found.

## 3. ZFS

**Guidance conflict.**
- **S:** OpenZFS advises `recordsize=32K` (64K or 128K also fine), lz4, separate data and WAL datasets,
  `full_page_writes=off`, and trying `logbias=throughput` on data
  ([docs](https://openzfs.github.io/openzfs-docs/Performance%20and%20Tuning/Workload%20Tuning.html)).
- **S:** PG warns that turning off `full_page_writes` risks silent corruption
  ([docs](https://www.postgresql.org/docs/current/runtime-config-wal.html)).

**Measurements.** **S:** Vondra (2022, one NVMe, ZFS 2.1, recordsize 8K), read-write TPS:

| Scale | ZFS default | ZFS tuned | ext4 |
|---|---|---|---|
| 1000 | 32.6k | 36.1k | 58.7k |
| 10000 | 3.8k | 6.1k | 30.5k |

These were saturation tests and are dated. ZFS jitter was notably low on his RAID machine.

**SLOG and sync.**
- **S:** A SLOG matters only for sync writes ([TrueNAS](https://www.truenas.com/docs/references/slog/)).
- **S:** A forum post says ESXi over iSCSI doesn't request sync writes, so `sync=always` is needed
  ([thread](https://www.truenas.com/community/threads/do-i-need-slog.114572/)). That applies only to that
  hypervisor, and the source post wasn't read in full.
- No measured SLOG-with-Postgres figure or authoritative `volblocksize` guidance was found.

**Under a VM.** **I:**
- The atomic-write argument covers Postgres directly on ZFS. Through a guest filesystem and virtual disk,
  a 4K-block guest may split an 8K page, so keep `full_page_writes=on` and consider `wal_compression`.
- Verify the flush path with `pg_test_fsync` in the guest. Implausibly fast results suggest flushes aren't
  honoured.
- A shared pool defeats Change B's WAL isolation.

## 4. Sharding options

Per-shard order is **I** throughout.

| Option | Atomicity | Burden and single points | Resharding | Per-shard order |
|---|---|---|---|---|
| **Citus 14.3** | 2PC, coordinator recovery from metadata; a worker blocks if the coordinator is permanently lost ([2017](https://www.citusdata.com/blog/2017/11/22/how-citus-executes-distributed-transactions/)) | Self-hosted HA; coordinator failover is DIY ([docs](https://docs.citusdata.com/en/stable/admin_guide/cluster_management.html)) | Online via logical replication; needs PK or replica identity | Yes, if counter and rows are colocated |
| **App-level** | None unless built | Multiplied ops; no router tier | Your own | Yes, trivially |
| **Neki** | [Not atomic; no 2PC; no shared read snapshot](https://planetscale.com/docs/neki/transactions) | Router, sidecars, control plane; available | Online, per an HN comment only | Likely; real Postgres per shard |
| **Multigres v0.1.0** (2026-05, Apache-2.0) | [Design](https://github.com/multigres/multigres/blob/main/docs/sharding/design.md) puts 2PC out of scope | Alpha | Execution out of scope | Likely; too early to rely on |
| **PgDog v0.1.62** (AGPL) | [2PC: "very high chance" atomic; partial reads possible](https://docs.pgdog.dev/features/sharding/2pc/); [crash recovery experimental, blocks startup](https://docs.pgdog.dev/features/sharding/2pc/crash-recovery/) | Proxy needs a durable WAL disk | [Online](https://github.com/pgdogdev/pgdog), but [a competitor calls it manual](https://planetscale.com/blog/the-history-of-postgres-sharding) | Yes, if routed to one shard |
| **PgCat** | Not verified | Sharding [experimental](https://github.com/levkk/pgcat); last commit 2025-02 | Not found | Avoid |
| **CockroachDB v26.4** | Native; about one consensus round plus reads ([docs](https://docs.cockroachlabs.com/docs/stable/architecture/transaction-layer)) | Different engine | Not researched | Serializable counter should hold, at consensus cost per commit |
| **YugabyteDB** | Native; multi-shard uses a [status tablet](https://docs.yugabyte.com/architecture/transactions/distributed-txns) | Different engine | Not researched | Probably, with `CACHE 1` or a counter row; costly |

**Citus status.** **S:** AGPL-3.0, v14.3.0 released 2026-10-23, [14.1 supports PG16-18](https://www.citusdata.com/updates/v14-1/),
schema-based sharding [since 12.0](https://docs.citusdata.com/en/stable/get_started/concepts.html). Managed:
Azure [Elastic Clusters](https://github.com/citusdata/citus);
[Cosmos DB for PostgreSQL is on a retirement path](https://learn.microsoft.com/en-us/azure/cosmos-db/postgresql/introduction).
Activity by GitHub API (2026 partial): commits 868, 479, 115, 298, 189 for 2022-2026; merged PRs 619, 457, 196,
273, 289. That shows reduced activity from the 2022 peak, then partial recovery.

**CockroachDB.** **S:** `FOR UPDATE` under SERIALIZABLE uses best-effort locks, dropped on lease transfer or
range split, surfacing as 40001 retries ([docs](https://docs.cockroachlabs.com/docs/stable/select-for-update)).
[Sequences](https://docs.cockroachlabs.com/docs/stable/create-sequence) can have gaps and cause hotspots.
[Changefeeds](https://docs.cockroachlabs.com/docs/stable/changefeed-messages) order per key only.

**YugabyteDB.** **S:** [Based on PG15 since 2025.1](https://docs.yugabyte.com/stable/api/ysql/pg15-features/).
[`nextval` is slow unless cached](https://docs.yugabyte.com/latest/api/ysql/commands/ddl_create_sequence).
[Read Committed needs a flag](https://docs.yugabyte.com/stable/architecture/transactions/read-committed/). No
measured write latency was found for either engine.

## 5. Search precedents and read-your-writes tokens

**Elasticsearch.**
- **S:** The primary assigns each op a seq from its counter, and primary terms change on promotion.
  `if_seq_no` plus `if_primary_term` make a write conditional
  ([docs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control)).
- **S:** The local checkpoint is the fully processed prefix on a copy. The global checkpoint is aligned
  across active copies. Ops above it may be rolled back
  ([2017](https://www.elastic.co/blog/elasticsearch-sequence-ids-6-0)).
- **S:** `refresh=wait_for` parks the response until the next refresh, and past 1,000 listeners per shard
  forces a refresh ([docs](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/refresh-parameter)).
- **S:** Fleet search has `wait_for_checkpoints` per shard
  ([client docs](https://elasticsearch-py.readthedocs.io/en/stable/api/fleet.html)).
- **I:** Fleet is the closest precedent for a per-shard position vector.

**Other engines.**
- **Quickwit:** [`commit=wait_for|force`](https://quickwit.io/docs/reference/rest-api). Split metadata and
  source checkpoint [publish atomically](https://deepwiki.com/quickwit-oss/quickwit/4.3-source-configuration)
  to a Postgres metastore (secondary source).
- **Vespa:** [no multi-document transactions, not linearizable](https://docs.vespa.ai/en/content/consistency.html).
- **ParadeDB:** index inside Postgres, [WAL-logged as of 0.24.0](https://api.pgxn.org/src/pg_search/pg_search-0.24.2/docs/welcome/guarantees.mdx).

**Token precedents.**
- **Cosmos DB:** tokens are [partition-bound; a missing token falls back to eventual](https://learn.microsoft.com/en-us/azure/cosmos-db/consistency-levels).
- **MongoDB:** a node [waits until its oplog reaches `afterClusterTime`](https://www.mongodb.com/docs/manual/reference/read-concern/).
- **Zanzibar:** an ["opaque byte sequence encoding a globally meaningful timestamp"](https://www.usenix.org/system/files/atc19-pang.pdf),
  opaque to stop clients picking arbitrary times.
- **Spanner:** [timestamp bounds](https://cloud.google.com/spanner/docs/timestamp-bounds).
- **Kafka:** per-partition offsets.

**Options.** All **I**.

| Option | Size | Trade-off |
|---|---|---|
| Sparse vector `{epoch, shardmap_ver, [(shard, seq)]}`, opaque, merged by per-shard maximum | About 5-10 bytes per touched shard; 64 shards is about 0.7 KB base64 | No clock, stateless, waits only on shards the query hits |
| Server-held session ID | Tiny | Needs shared state and expiry |
| Single timestamp (Zanzibar, MongoDB) | Fixed | Needs a clock across shards, which Change B removes |
| No token; block the write (`wait_for`) | None | Simplest; slower writes; no cross-client propagation |

## 6. Atomic multi-shard bulks

**Non-atomic engines.** Elasticsearch has per-item `status` and a top-level `errors` flag
([API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk)); "[not atomic](https://www.elastic.co/guide/en/elasticsearch/guide/master/bulk.html)"
comes from the older Definitive Guide. DynamoDB's `BatchWriteItem`, Vespa and Neki follow the same model.

**Atomic options.**
- **DynamoDB:** [`TransactWriteItems`: 100 actions, two underlying writes per item, charged even when cancelled, "avoid" for bulk ingest](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html).
- **Kafka:** [commit markers on each partition](https://www.confluent.io/blog/transactions-apache-kafka/).
  [A hanging transaction freezes the last stable offset and `read_committed` consumers](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/158873417/KIP-664+Provide+tooling+to+detect+and+abort+hanging+transactions).

**2PC on Postgres.**
- **S:** A prepared transaction "continues to hold whatever locks it held", hinders VACUUM, and in extreme
  cases can force shutdown for wraparound. The docs advise keeping the feature off without a transaction
  manager ([docs](https://www.postgresql.org/docs/current/sql-prepare-transaction.html)).
- **I:** This is the Kafka failure in Postgres form. An orphaned prepare holding a shard's counter row **stops
  all writes and tailing on that shard**. Prepare also adds a synchronous flush under the lock, and visibility
  differs across shards during phase 2.

**The alternative.** **I:** One commit per shard, per-item results, a per-shard position in the response, and a
client idempotency key for retries. A tailer never needs cross-shard ordering, and the failure is partial and
reported rather than a stall. Callers who need atomicity use a single routing key, as
[Neki advises](https://planetscale.com/ref/keep-ledger-transactions-on-one-neki-shard).

## 7. Couldn't verify, or dated

- No controlled single-row-insert or WAL-ceiling benchmark on NVMe, on any filesystem.
- [Kafgres's 600k events/s](https://rynr.dev/blog/700mbskafgres/) is one author's result and bypasses heap
  storage, so it isn't used. A 2021 ZFS pgbench was discarded for ambiguous labels.
- Licences and rebalancing weren't researched for CockroachDB and YugabyteDB. Neki's source availability is
  unchecked.
- Citus 2PC mechanics rest on a 2017 post and an old regression test, plus the 14.1 notes. Whether current Citus
  lacks a distributed read snapshot is unconfirmed ([PR #6489](https://github.com/citusdata/citus/pull/6489), merge
  state unchecked). No source documents a specific event in Citus's community history.
- Dated: Elastic 2017, Vondra 2022; the OpenZFS page is undated. Per-shard commit caps in §1 are arithmetic, not
  measurements.
- The `NOTIFY` lock was read on `REL_18_STABLE`. Later versions weren't checked.
