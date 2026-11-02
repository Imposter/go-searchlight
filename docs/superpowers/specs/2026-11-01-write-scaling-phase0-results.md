# Write scaling Phase 0: results

Phase 0 of [the write scaling plan](../plans/2026-11-01-write-scaling.md), measured 2026-11-01 and 2026-11-02 on
one Windows workstation. Read with the [code map](2026-11-01-write-scaling-code-map.md) and the
[external research](2026-11-01-write-scaling-external-research.md).

## Verdict

**The gate is not met on this machine.** No sustained multi-shard load on one instance doubled. The serializers
can be removed: on one node, the spike's per-shard counters took the lock wait from up to 183 ms (under step b's
lanes) to about 1 ms. They reached a median 1.9-2.3× in **bursts of 2-15 seconds**, but only on a WAL flush the disk
doesn't honour. Over a million
changes the best gain was **1.32× durable and 1.30× with that unsafe flush**. Several loads got slower: small bulks
over many shards, and three nodes.

**On this machine, the limit is the disk, not the serializers.** The data directory is on a 7,200 rpm hard disk.
A durable flush costs 8.5 ms, and WAL plus checkpoint writes run at only 15-45 MB/s. Each change writes about 2 KB
of WAL, which caps the instance near 7-25k changes/s whatever Searchlight does. With fsync and synchronous commit
both off, the cap comes from WAL writes: 22-24k changes/s, and 20-28k with the spike.

**The question isn't settled for production or NVMe.** The base design has a second, CPU limit: one backend writes
the whole 4,096-change statement, at about 27 µs per change. That caps one node near 25-35k changes/s on any
storage. The spike removes that cap, so it could pay on storage that sustains more than about 70 MB/s of WAL. This
disk can't show it. **Before Phase 1, rerun the script in "Running it on production's storage"** on production's
ZFS (and, if wanted, on this machine's NVMe drive), and apply the gate to that. If production also caps near
these numbers, stop. Production's needs are another reason not to hurry: scrape-bot's 2026-11-01 load test found
Searchlight and Postgres "nowhere near a limit in steady state", and scrape-bot creates its indexes with one shard
(the default). With one shard, Change A gains nothing.

## Environment

| Item | Value |
|---|---|
| CPU | AMD Ryzen 7 3700X, 8 cores and 16 threads |
| Memory | 64 GB |
| OS | Windows 11 Pro 10.0.22621 |
| Data directory | `E:\tmp\writescale-phase0\data`, NTFS with 4 KB clusters, on a **WDC WD10EZEX: 1 TB, 7,200 rpm SATA hard disk** shared with other files. Node directories and Searchlight's shard files were on the same disk |
| Postgres | 17.6, the EDB Windows binaries, local TCP |
| Go | 1.25.14; Searchlight built with async preemption off, as `make` builds it on Windows |
| Other load | Desktop applications. Total CPU averaged 8% over the 4,040 five-second samples, and showed no sustained second load. Windows Defender real-time protection was on; its exclusions couldn't be read without admin rights |

**`pg_test_fsync`** on the data disk, at 3 s per test:

| Method | 8 kB write ops/s | µs/op |
|---|---|---|
| open_datasync (**Windows' default**) | 11,305 | 88 |
| fdatasync | 116.9 | 8,553 |
| fsync | 38.4 | 26,016 |
| fsync_writethrough, open_sync | n/a | |

At 88 µs, `open_datasync` is implausible for a spinning disk, so its writes stop in the drive's volatile cache.
`fdatasync` matches one revolution at 7,200 rpm, so it really flushes. Postgres on Windows defaults to
`open_datasync`, so **a default Postgres on this machine isn't durable**. Linux defaults to `fdatasync`.

**Postgres settings.** Every setting that isn't a default:

| Setting | Value | Why |
|---|---|---|
| `port`, `listen_addresses` | 55440, 127.0.0.1 | scratch instance |
| `max_connections` | 300 | as `deploy/compose.postgres.yml` |
| `shared_buffers` | 256MB | as `deploy/compose.postgres.yml` |
| `wal_sync_method` | `fdatasync` (**durable** mode) or `open_datasync` (**cache** mode, flush not honoured) | see above |
| `track_io_timing`, `track_wal_io_timing` | on | WAL timings in `pg_stat_wal` |
| `timezone`, `log_checkpoints` | UTC, on | |
| initdb | `-E UTF8 --locale=C -A trust` | |

Two sensitivity configurations ran only where marked:
- **tuned:** `shared_buffers=8GB`, `max_wal_size=32GB`, `checkpoint_timeout=30min`.
- **no-flush:** tuned, plus `fsync=off` and `synchronous_commit=off`. This is a bound, not a setting anyone should
  run.

Everything else was at its default: `wal_buffers` 8MB, `max_wal_size` 1GB, `full_page_writes` on,
`wal_compression` off, `checkpoint_timeout` 5min, `synchronous_commit` on, `commit_delay` 0, autovacuum on. Before
the measurements, a 1.5 GB insert warmed the WAL to 64 recycled segments. With the default `max_wal_size`, the log
warned "checkpoints are occurring too frequently" 39 times over the session.

## Method

**Tools**, on `design/write-scaling` (`967b62f`, `6ff2daa`, `c394860`; no production code changed):
- **`BenchmarkCommitLoad`** (`internal/store/commit_load_test.go`) loads the commit path alone. Writers per node
  send bulks of rewrites over a 50,000-document pool through a group committer (or straight to `Apply`, marked
  "direct"), with no node, tailers or HTTP in the way. A pgx tracer on the write pool times each statement from
  the client:
  - begin;
  - lock: the counter's `SELECT ... FOR UPDATE`, so its wait;
  - index: the `sl_indexes` read;
  - write: the one CTE statement;
  - commit;
  - notify, when it runs apart from the transaction.

  It also reads `pg_stat_wal` before and after, and samples `pg_stat_activity` every 50 ms. Documents are about
  900 bytes, compressed as a cluster stores them. One iteration is 1,000 changes. Short runs are 100,000 changes
  (2-15 s); "sustained" runs are 1,000,000 (20-150 s).
- **`slbench writeload --shards N --pg-url ...`** runs end to end: a real node with its copies tailing and
  indexing, over HTTP. It adds the group committer's transactions, the copies' lag, and Postgres wait events and
  WAL statistics to its result. Every run used `--changed 1 --percolate=false --readers 0 --gomaxprocs 8 --docs
  20k --duration 30s --client-timeout 60s`.
- **`BenchmarkApply`** now runs at 1, 8 and 64 shards. **`BenchmarkWriteThroughput`** takes a shard count and a bulk
  size, and runs on Postgres when `SEARCHLIGHT_TEST_PG_URL` is set.

**Spike**, on the local branch `spike/write-scaling`, which is never merged or pushed. Each step adds to the one
before:

| Step | Commit | What changed |
|---|---|---|
| base | `c394860` | `design/write-scaling` as it is |
| a | `11f85f8` | `pg_notify` leaves the transaction. A goroutine per store coalesces shards' newest seqs and sends them after commit, in one statement with `synchronous_commit` off. `SL_SPIKE_NOTIFY=sync` sends them from the committer, flush included, and `tx` puts them back in the transaction |
| b | `1f5ec8f` | 8 group-commit lanes, keyed by a request's first shard. All of them still take `sl_counter` row 1 |
| c | `f544340`, `4c7207c` | `sl_shard_seq (index_name, shard, value)` in place of row 1; the `sl_changes` key becomes `(index_name, shard, seq)` and the second index goes. A request is split per shard, with one lane and one transaction per shard. At most 32 transactions run per node (`SL_SPIKE_COMMITTERS`), and the batching window is settable (`SL_SPIKE_DELAY_MS`). Tailers follow through a per-shard `ReadTip`, so end-to-end runs had tailers on, and every run ended with the copies caught up or within 16,000 changes. A test checks that concurrent multi-shard bulks leave each shard's seqs contiguous from 1. The spike's slbench waits on `_count`, not `wait_for_seq` |

Out of scope, as asked: rewind, mapping changes under load (the spike's `Update` takes shard rows in order but
wasn't exercised), migrations, MySQL and SQLite.

**Runs.** The variants ran round robin in each round, so they shared the same drift. Every cell has at least three
runs. Cells over 20% apart were run again where the verdict depends on them, which gave 5 or 6 runs. Tables show
**median [min-max] n**. Each store-level case has a fresh schema, and each end-to-end run a fresh database. A
`CHECKPOINT` came before every run. The runner, analysis script, raw JSON lines (`runs.jsonl`), machine samples and
summaries are kept in `E:\tmp\writescale-phase0\` (`scripts\`, `results\`).

## Where a commit's time goes

One writer and one committer, 64 shards, changes/s, with each phase's mean milliseconds per transaction. With one
committer the lock is never contended, so its wait is zero by construction.

| Mode, bulk | Changes/s | begin | lock | index | write | commit | WAL flush (server) |
|---|---|---|---|---|---|---|---|
| durable, 4,096 | 11,105 [10,380-12,629] 3 | 0.1 | 0.15 | 0.10 | **117.5** | **207.0** | 111 ms per sync, 5 syncs/s |
| durable, 4,096, notify off | 10,578 [10,482-11,961] 3 | 0.1 | 0.15 | 0.08 | 117.9 | 222.6 | |
| durable, 4,096, synchronous_commit off | 15,643 [13,673-16,404] 3 | 0.1 | 0.27 | 0.13 | 212.5 | **1.2** | |
| durable, 500 | 5,812 [5,796-6,541] 3 | 0.1 | 0.01 | 0.05 | 15.2 | 61.8 | |
| cache, 4,096 | 18,349 [17,941-18,528] 3 | 0.1 | 0.10 | 0.11 | **115.5** | **63.2** | WAL writes 23 ms each |
| cache, 500 | 16,156 [15,554-16,207] 3 | 0.1 | 0.01 | 0.05 | 15.2 | 7.1 | |

- **The write statement** takes 107-118 ms for 4,096 changes (26-29 µs each) on one backend. This is CPU and WAL
  insertion, the same with any flush. One committer per node can't beat about 35k changes/s even with free flushes.
- **COMMIT** is the WAL write and flush of the transaction's 8 MB of WAL: 170-220 ms durable, 55-65 ms in cache
  mode. That is 60-65% and 35% of the cycle.
- **The counter lock and the `sl_indexes` read** cost 0.1-0.3 ms together, under 0.2% of a 4,096-change commit.
- **`pg_notify` in the transaction** costs nothing measurable with one committer: within noise at 500 and 4,096, and
  3% at one change per commit (1,286 against 1,326 commits/s, cache mode).

**Lock wait appears only when several committers share the row.** Three nodes with four writers each, 64 shards, 500-change bulks:

| Mode | Changes/s, 1 node | Changes/s, 3 nodes | Lock wait per transaction, 3 nodes (mean, p95) |
|---|---|---|---|
| durable | 10,360 [9,967-12,092] 3 | 10,176 [9,751-10,557] 3 | 236 ms, 393 ms |
| cache | 20,001 [19,927-20,435] 3 | 19,865 [19,400-20,512] 3 | 107 ms, 207 ms |

Three nodes commit what one node does, because the row lets one transaction through at a time. Each waits about
one other node's commit cycle. Wait events show it as `Lock/transactionid` (and `Lock/tuple` under step b).

## Per-row commit cap, against the research's 670/s

The research inferred that a counter row held through the flush caps commits per row near 1/(flush + round trips
under the lock), about 670/s at a 1.5 ms flush. Direct `Apply` of one change per transaction:

| Mode | Writers on the row | Commits/s | Mean commit | Lock wait |
|---|---|---|---|---|
| durable (8.5 ms flush) | 1 | 84 [75-98] 6 | 11.3 ms | 0 |
| durable | 16 | 75 [70-77] 3 | 12.2 ms | 198 ms |
| durable | 64 | 63 [59-67] 3 | 14.2 ms | 960 ms |
| cache (about 0.1 ms flush) | 1 | 1,286 [831-1,384] 6 | 0.2 ms | 0 |
| cache | 16 | 777 [740-814] 3 | 0.2 ms | 18.8 ms |
| cache | 64 | 606 [590-641] 3 | 0.3 ms | 98 ms |
| **step c, 64 shards, 16 writers**, durable | spread over 64 rows | 481 [461-507] 3 | 31 ms | 0.2 ms |
| **step c, 64 shards, 16 writers**, cache | spread over 64 rows | 5,299 [5,259-5,382] 3 | 0.5 ms | 0.25 ms |

**The arithmetic holds for one writer:** 1/(11.3 + 0.5 ms) is 85/s, against 84 measured. **Contention lowers it by
a further 10-50%**, because each hand-off of the lock costs a wake-up and a round trip: 777/s with 16 waiters in
cache mode, against 1,286/s for one. So the research's 670/s at 1.5 ms is an upper bound for one row, and a
contended row should be planned at about half the formula's figure. **Per-shard rows multiply commits nearly
linearly** while the flush is shared (5,299/s over 64 rows), since Postgres's own group commit batches different
rows' flushes. The research doc's §1 bullet on the cap should read: "near 1/(flush + lock-held round trips) for
one writer; 10-50% lower with 16-64 writers queued on the row (measured)".

## Spike: what each step gained

Store level, one node, 16 writers, short runs (100,000 changes). Changes/s:

| Mode | Shards, bulk | base | a | b | c | c/base |
|---|---|---|---|---|---|---|
| durable | 64, 4,096 | 14,744 [13,380-16,295] 6 | 14,400 [13,852-14,935] 3 | 14,947 [14,393-15,433] 3 | 16,160 [9,953-19,131] 6 | **1.10** |
| durable | 8, 500 | 14,722 [12,630-15,364] 6 | 12,897 [12,826-14,358] 3 | 13,217 [12,898-13,349] 3 | 15,588 [12,875-17,631] 6 | **1.06** |
| durable | 64, 500 | 14,270 [13,283-14,800] 6 | 14,220 [13,605-14,342] 3 | 12,175 [11,853-12,279] 3 | 7,330 [6,420-8,515] 6 | **0.51** |
| durable | 1, 500 (control) | 14,910 [14,209-15,185] 3 | 15,550 [15,116-15,953] 3 | 14,950 [14,066-15,947] 3 | 16,424 [16,001-16,680] 3 | 1.10 |
| cache | 64, 4,096 | 22,313 [21,171-22,667] 6 | 20,959 [20,905-22,079] 3 | 21,546 [21,383-21,722] 3 | 50,820 [45,160-58,132] 6 | **2.28** |
| cache | 8, 500 | 22,112 [21,770-22,775] 6 | 21,814 [21,098-22,365] 3 | 20,739 [19,562-21,140] 3 | 42,458 [38,906-47,380] 6 | **1.92** |
| cache | 64, 500 | 22,082 [21,297-22,999] 6 | 21,632 [21,340-21,738] 3 | 20,355 [20,221-20,985] 3 | 32,035 [28,635-38,813] 6 | **1.45** |
| cache | 1, 500 (control) | 23,869 [23,088-24,036] 3 | 23,572 [22,829-23,808] 3 | 22,943 [22,811-23,030] 3 | 23,730 [22,250-24,466] 3 | 0.99 |

Lower concurrency, 64 shards, 500-change bulks:

| Mode | Load | base | a | b | c | c/base |
|---|---|---|---|---|---|---|
| durable | 1 node, 4 writers | 10,360 [9,967-12,092] 3 | 10,351 [9,733-10,794] 3 | 7,707 [7,512-7,874] 3 | 4,546 [4,261-4,739] 3 | 0.44 |
| durable | 3 nodes, 4 writers each | 10,176 [9,751-10,557] 3 | 11,054 [9,960-12,350] 3 | 7,633 [7,492-7,916] 3 | 6,466 [6,389-7,510] 3 | 0.64 |
| cache | 1 node, 4 writers | 20,001 [19,927-20,435] 3 | 20,962 [20,256-21,750] 3 | 19,886 [19,102-20,278] 3 | 12,508 [11,649-13,538] 3 | 0.63 |
| cache | 3 nodes, 4 writers each | 19,865 [19,400-20,512] 3 | 19,926 [19,548-20,831] 3 | 19,294 [19,128-19,932] 3 | 15,955 [15,367-16,637] 3 | 0.80 |

Lock wait per transaction (mean ms), 16 writers, 64 shards, 500-change bulks:

| Mode | base | a | b | c |
|---|---|---|---|---|
| durable | 0.20 | 0.36 | 182.7 | 0.72 |
| cache | 0.25 | 0.30 | 106.2 | 1.26 |

What each step did:
- **a (notify after commit): nothing measurable, alone.** With one committer per node, nothing contends for the
  notify lock. Sending notifications after commit *with* a flush (`SL_SPIKE_NOTIFY=sync`) cost 17% durable (11,816
  against 14,220). A notifying transaction takes an xid, so it writes a commit record of its own to flush
  (`PreCommit_Notify`, which still holds `GetCurrentTransactionId` and the database-0 lock on master as on 17). The
  wake-up should go out with `synchronous_commit` off, as the spike does.
- **a is a precondition for c.** With per-shard transactions, putting `pg_notify` back in the transaction (`c-tx`)
  dropped c from 7,330 to 2,299 (64 shards, 500, durable), and from 16,160 to 8,534 (64 shards, 4,096, durable).
  In cache mode it was too noisy to call ([14,236-49,025]). So the instance-wide notify lock is real, but it only
  bites once several transactions commit at once.
- **b (lanes on one row): worse.** Lanes queue on row 1: 105-183 ms of lock wait per transaction, and smaller
  batches per commit (2,222 changes against 4,000). Throughput fell 0-26%, and up to 54% end to end. Committers
  without their own rows only add contention.
- **c (per-shard rows): 1.9-2.3× in short bursts with an unhonoured flush, 1.06-1.10× durable, and much worse with
  small per-shard batches.** Rows per commit fell from about 4,000 to 15-460, so it is a trade:
  - Where shards get enough rows each (8 shards with 500-change bulks, or 64 shards with 4,096), the parallel
    write statements beat one backend's CPU limit.
  - Where they don't (64 shards with 500 or fewer, or few writers), each bulk becomes 64 tiny commits that each
    wait out the 2 ms window and their own WAL flush, and throughput falls.

  A larger window gave little and was noisy: with 64 shards, 500-change bulks and 16 writers, 38,812 at 5 ms and
  33,497 at 20 ms, against 32,035 (cache mode). Capping concurrent transactions at 8 or raising it to 64 was worse
  than 32 (cache: 22,624 and 9,203 against 32,035).

**Sustained** (1,000,000 changes, 20-150 s), 16 writers. This is where the gate is decided:

| Config | Shards, bulk | base | c | c/base |
|---|---|---|---|---|
| durable | 64, 4,096 | 7,386 [7,293-7,545] 3 | 5,906 [5,554-6,086] 3 | 0.80 |
| durable | 8, 500 | 7,630 [6,649-8,938] 5 | 6,617 [5,793-7,466] 5 | 0.87 |
| durable | 64, 500 | 6,997 [6,697-7,572] 5 | 4,405 [3,991-5,603] 5 | 0.63 |
| cache | 64, 4,096 | 8,756 [8,514-9,167] 3 | 7,642 [7,512-8,662] 3 | 0.87 |
| cache | 8, 500 | 8,769 [8,398-9,572] 3 | 8,296 [8,148-9,730] 3 | 0.95 |
| durable, tuned | 64, 4,096 | 12,685 [11,841-12,705] 3 | 16,741 [16,110-16,766] 3 | **1.32** |
| durable, tuned | 8, 500 | 12,568 [11,997-12,573] 3 | 14,581 [14,285-15,642] 3 | 1.16 |
| durable, tuned | 64, 500 | 12,590 [11,849-12,648] 3 | 11,228 [11,055-11,503] 3 | 0.89 |
| cache, tuned | 64, 4,096 | 18,192 [6,875-18,994] 5 | 23,705 [15,953-24,695] 5 | **1.30** |
| cache, tuned | 8, 500 | 19,048 [18,065-19,233] 5 | 24,294 [22,803-24,860] 5 | **1.28** |
| cache, tuned | 64, 500 | 18,412 [17,793-18,681] 3 | 22,190 [21,736-22,653] 3 | 1.21 |
| no-flush, tuned | 64, 4,096 | 22,395 [19,843-25,315] 5 | 23,818 [22,152-34,438] 5 | 1.06 |
| no-flush, tuned | 8, 500 | 21,753 [21,176-23,166] 5 | 28,459 [21,019-33,020] 5 | 1.31 |
| no-flush, tuned | 64, 500 | 23,699 [21,635-25,754] 3 | 20,345 [20,005-20,632] 3 | 0.86 |

Over a million changes, everything falls to what the disk sustains. The short runs' 2× came from shared buffers,
the OS cache and the drive cache absorbing the first seconds.
- **At compose settings:** checkpoints every few seconds, `IO/DataFileWrite`, `IO/DataFileRead` and `Lock/extend`.
- **Tuned:** the WAL alone. WAL ran at 35-45 MB/s, with `LWLock/WALWrite` 9-15 and `LWLock/WALInsert` 6-11
  backends waiting under c.

The no-flush bound shows the flush isn't what limits it on this disk: WAL *writes* are.

## End to end (slbench writeload)

Docs/s with a real node, tailers and indexing on, 30 s after a 20,000-listing preload. All runs had 0 noops, and
the copies' lag at the end was 0-16,000 changes.

Baseline, 500-document bulks:

| Mode | Shards | 1 writer | 4 writers | 16 writers |
|---|---|---|---|---|
| durable | 1 | 2,598 [2,566-2,982] 3 | 3,060 [2,945-3,303] 3 | 5,216 [4,928-5,521] 3 |
| durable | 8 | 1,323 [1,258-1,330] 3 | 2,009 [1,961-2,022] 3 | 3,369 [3,058-3,372] 3 |
| durable | 64 | 838 [816-895] 3 | 1,617 [1,576-1,665] 3 | 3,313 [3,208-3,414] 3 |
| cache | 1 | | | 6,899 [6,846-7,227] 3 |
| cache | 8 | | | 4,772 [4,303-5,761] 5 |
| cache | 64 | | 2,340 [2,249-2,523] 3 | 4,136 [4,071-4,182] 3 |

With 4,096-document bulks, durable:

| Shards | 4 writers | 16 writers |
|---|---|---|
| 8 | 3,267 [3,174-3,577] 3 | 3,295 [3,179-3,295] 3 |
| 64 | 3,219 [3,052-3,251] 3 | 2,991 [2,910-3,094] 3 |

At 16 writers, 520-570 bulks a run were refused with 429, the copies' write buffers being full, and sent again.

Steps, 16 writers unless marked:

| Mode | Shards, bulk | base | a | b | c | c/base |
|---|---|---|---|---|---|---|
| durable | 64, 4,096 | 2,991 [2,910-3,094] 3 | 3,061 [2,906-3,109] 3 | failed at preload, 504 (3 of 3) | 5,082 [4,699-5,534] 3 | **1.70** |
| durable | 8, 500 | 3,369 [3,058-3,372] 3 | 3,308 [3,258-3,495] 3 | 1,539 [1,534-1,614] 3 | 4,058 [3,941-4,144] 3 | 1.20 |
| durable | 64, 500 | 3,313 [3,208-3,414] 3 | 3,259 [2,956-3,508] 3 | 1,523 [1,495-1,601] 3 | 2,859 [2,802-2,916] 3 | 0.86 |
| durable | 64, 500, 4 writers | 1,617 [1,576-1,665] 3 | 1,788 [1,719-1,831] 3 | 1,173 [1,166-1,177] 3 | 1,091 [1,021-1,205] 3 | 0.67 |
| durable | 1, 500 | 5,216 [4,928-5,521] 3 | 5,178 [4,890-5,229] 3 | 5,027 [4,823-5,593] 3 | 5,282 [5,130-5,492] 3 | 1.01 |
| cache | 64, 4,096 | 4,226 [3,822-4,274] 3 | 4,165 [3,923-4,235] 3 | 3,955 [3,888-4,022] 2 | 7,264 [6,259-7,294] 3 | **1.72** |
| cache | 8, 500 | 4,772 [4,303-5,761] 5 | 5,141 [4,632-5,536] 3 | 2,352 [2,087-2,547] 3 | 5,475 [4,538-5,644] 5 | 1.15 |
| cache | 64, 500 | 4,136 [4,071-4,182] 3 | 4,309 [3,615-4,397] 3 | 2,284 [2,179-2,312] 3 | 3,913 [3,619-4,090] 3 | 0.95 |

End to end runs at 20-35% of the store level, so the commit path isn't its limit:
- With one shard and 16 writers, Postgres committed about 1.3 transactions/s of 3,900 changes with **263 ms per
  flush**, against 96 ms at store level. The node's own shard files sit on the same disk and fsync too.
- **More shards cost throughput before any spike step:** 5,216 docs/s at 1 shard against 3,313 at 64, durable.
  Postgres showed 4-11 backends `active` with no wait event at 64 shards, against about 1 at one shard. These are
  likely the copies' per-shard changelog polls and the unchanged-detection reads, but that wasn't profiled.

c's 1.7× at 64 shards with 4,096-document bulks restores roughly the one-shard figure. Step b's 64-shard,
4,096-document runs failed at preload with 504s in all three durable runs and one cache run: its lanes queued
behind the one row past the request deadline.

## Cluster benchmark (`BenchmarkWriteThroughput`, Postgres)

Four writers per node and short runs. The documents are about 100 bytes, so the WAL per change is far smaller than
in the loads above. Docs/s:

| Mode | Nodes, shards, bulk | base | a | c | c/base |
|---|---|---|---|---|---|
| durable | 1, 3, 500 | 9,360 [8,702-9,440] 3 | 9,068 [8,924-9,270] 3 | 9,477 [9,026-9,995] 3 | 1.01 |
| durable | 3, 3, 500 | 8,483 [8,196-8,599] 3 | 8,285 [8,274-8,492] 3 | 9,694 [9,261-9,763] 3 | 1.14 |
| durable | 3, 64, 500 | 5,159 [4,998-5,356] 3 | 5,544 [5,372-5,555] 3 | 4,774 [4,525-5,527] 3 | 0.93 |
| durable | 3, 64, 100 | 1,561 [1,542-1,716] 3 | 1,897 [1,822-1,907] 3 | 1,754 [1,664-1,820] 3 | 1.12 |
| cache | 1, 3, 500 | 27,747 [25,269-28,253] 3 | 23,656 [21,427-25,497] 3 | 39,124 [37,071-44,693] 3 | 1.41 |
| cache | 3, 3, 500 | 21,275 [18,645-24,125] 3 | 21,893 [21,468-22,727] 3 | 48,110 [47,988-48,943] 3 | **2.26** |
| cache | 3, 64, 500 | 8,463 [8,302-8,630] 3 | 9,190 [8,913-10,156] 3 | 5,082 [5,077-5,307] 3 | 0.60 |
| cache | 3, 64, 100 | 3,843 [3,574-4,076] 3 | 4,155 [3,679-4,398] 3 | 2,541 [2,463-2,559] 3 | 0.66 |

Three nodes on one Postgres give no more than one today: in cache mode, 21,275 against 27,747 at 3 shards. With
small documents and few shards, c doubled a three-node cluster in a short burst (2.26×). With 64 shards and
100-500-document bulks, it halved it again.

## Wait events

These are sampled from `pg_stat_activity`, as the mean number of backends in each state.
- **base, any mode:** one backend at a time, `IO/WalSync` 0.3-0.5 (durable) or `IO/WalWrite` 0.3 (cache), CPU
  0.3-0.6, and the walwriter. With 3 nodes, add `Lock/transactionid` 0.5-1.0, the counter row's queue.
- **b:** `Lock/transactionid` 0.95 and `Lock/tuple` up to 13.7 (16 direct writers on one row), the same queue made
  longer.
- **c, short runs (64 shards, 4,096, durable):** `LWLock/WALWrite` 13.9-15.7, `LWLock/WALInsert` 9.5-10.9,
  `LWLock/BufferContent` 0.7-2.7, `Lock/extend` 0.3-1.3. That is up to 26 backends queued on WAL. In cache mode,
  `WALWrite` 8.8-11.1 and `WALInsert` 6.2-9.0. **The serializer moves from the counter row to the WAL.**
- **c sustained, compose settings:** add `IO/DataFileWrite` 2.2-5.0, `IO/DataFileRead` 0.8-1.5 and `Lock/extend`
  3.5-4.0: the checkpointer and buffer evictions on the same spindle.
- **End to end, base:** `checkpointer` always active, `LWLock/WALWrite` 1-3.5, `IO/WalSync` 0.8, and at 64 shards 4-11
  backends on CPU.

`pg_stat_wal`: 1,650-2,200 bytes of WAL per change (900-byte document compressed, a changelog row, two index
entries, the document row and full-page images). Durable flushes averaged 85-170 ms because each carried several MB
of WAL. Sustained WAL never passed 45 MB/s.

## Where the WAL flush limit lands

- **Commit rate:** one flush costs 8.5-11 ms durable on this disk, so about 85-100 group flushes/s for the
  instance. Postgres shares a flush across concurrent transactions, so per-shard rows reach 480-660 commits/s.
  Commits aren't the limit at 4,096 rows each.
- **Bytes:** the binding limit here is WAL bandwidth, 25-45 MB/s sustained, which is 12-25k changes/s at about
  2 KB per change. More parallel committers can't beat it, and the spike's WAL-lock queues are the sign.
- **Off this disk:** a single base committer is CPU-bound near 35k changes/s at a free flush. So per-shard commits
  pay only where the storage sustains more than about 70 MB/s of WAL with flushes near a millisecond. That is
  plausible on NVMe with power-loss protection or a ZFS SLOG, and not shown here.

## MySQL and SQLite

Not measured: no MySQL server was available, and Docker wasn't used. #69 recorded MySQL at 1.3-1.4k docs/s,
fsync-bound, ten times below Postgres here. Its multi-statement writes and the counter-last order don't change what
this phase found, that the disk binds first. **Neither is worth Change A work now:** MySQL's limit is per-commit
fsync cost, and SQLite has one writer by design. Keep D4.

## How far this is from production's storage

Production runs Postgres 17 in a Docker container in a 4-core VM on TrueNAS Scale, with the pool on ZFS. Its
`pg_test_fsync`, recordsize, sync setting and SLOG are unknown. This machine is a 7,200 rpm disk under NTFS:
- **Flushes:** durable flushes cost 8.5 ms here. ZFS with a SLOG would likely be 0.2-2 ms; without a SLOG, on hard
  disks, similar to here.
- **Throughput:** the WAL here is held near 40 MB/s by one spindle shared with data and shard files.
- **CPU:** production has a quarter of the cores.
- **Which result carries over:** the per-row cap formula and the step-by-step ordering likely do. The absolute
  throughputs don't.
- **What would change the verdict:** on production storage, base sustained far below 35k changes/s while WAL
  bandwidth had headroom. That needs the run below.

## Issue #69

- **Postgres store-level numbers at equal durability (`fdatasync`):**
  - 12-15k changes/s in short runs.
  - 7-8k sustained over a million changes at compose settings.
  - 12.6k sustained when tuned.
  - 22-24k with no flush at all, as a bound.
- **Cache mode** (Windows' default `open_datasync`, not durable on this disk): 18-24k short, 8.8k sustained, 18-19k
  sustained when tuned.
- **End to end** (scrape-bot shaped listings, a real node): 2.6-5.2k docs/s on one shard, 0.8-3.4k on 64. That fits
  #69's 8.6-18.1k bulk-load range, given small documents there and a disk shared with the shards here.
- **MySQL** wasn't remeasured, as above.
- **Against Elasticsearch:** not run.
- **Single-thread build:** not touched.

## Not verified

- **Production storage, and NVMe:** the verdict is for this disk only.
- **The causes of end-to-end losses:** why more shards slow the baseline (4-11 backends on CPU at 64 shards), and how
  much the node's shard fsyncs on the same disk take. Neither was profiled.
- **Sampled CPU per process:** Postgres backends exit when a run's pool closes, so their CPU seconds didn't add
  up. Machine-wide CPU averaged 8% outside runs and never stayed saturated in the samples, but those were 5-10 s
  apart.
- **Notify wake-up latency** under step a (Phase 1's item) wasn't measured.
- **The spike leaves untested:** mapping changes under load; conflicts in a split request (only the first error is
  returned, with positions not translated); rewind; `HeadSeq` taken as the maximum over shards; retention on
  per-shard seqs.
- **`wait_for_seq` end to end:** the true figure, with read-your-writes, needs Phase 2's per-shard token. The
  spike's slbench didn't use `wait_for_seq` at all.
- **Production's shard count:** scrape-bot's code sets no `shards`, so its indexes are likely one shard each. The
  live index's settings weren't read.
- **The research's `NOTIFY` claim:** confirmed on `REL_17_STABLE` (`async.c:921`) and on master, so it is not
  removed in later versions.

## Running it on production's storage

Run these on the VM that hosts Postgres, against a **throwaway Postgres container on the same storage, never the
live database**. Pick a quiet hour, since the load competes with the live database for the pool's I/O. Build the
Linux binaries on the workstation, where the spike branch exists:

```sh
# On the workstation (Git Bash). The spike branch is local only.
cd /e/code/go-searchlight-writescale
GOOS=linux GOARCH=amd64 go build -o out/searchlight-base ./cmd/searchlight
GOOS=linux GOARCH=amd64 go build -o out/slbench ./bench/cmd/slbench
GOOS=linux GOARCH=amd64 go test -c -o out/store-base.test ./internal/store
cd /e/code/go-searchlight-writescale-spike && git checkout -q 4c7207c
GOOS=linux GOARCH=amd64 go build -o ../go-searchlight-writescale/out/searchlight-c ./cmd/searchlight
GOOS=linux GOARCH=amd64 go test -c -o ../go-searchlight-writescale/out/store-c.test ./internal/store
git checkout -q spike/write-scaling
scp -r /e/code/go-searchlight-writescale/out <host>:/tmp/slphase0
```

```sh
# On the production VM. DIR must be on the same pool and dataset (or zvol) as the live Postgres
# volume; find it with: docker volume inspect <project>_pgdata
set -eu
DIR=/path/on/the/same/dataset/sl-phase0
mkdir -p "$DIR/data"

# 1. Is the flush honoured? Compare fdatasync against the drive: implausibly fast means it is not.
docker run --rm -v "$DIR:/t" postgres:17 /usr/lib/postgresql/17/bin/pg_test_fsync -s 5 -f /t/fsync.tmp | tee "$DIR/pg_test_fsync.txt"

# 2. A throwaway Postgres on the same storage, on a port nothing else uses.
docker run -d --name sl-phase0-pg --shm-size 256m -p 127.0.0.1:55499:5432 \
  -e POSTGRES_HOST_AUTH_METHOD=trust -v "$DIR/data:/var/lib/postgresql/data" postgres:17 \
  postgres -c max_connections=300 -c shared_buffers=256MB -c track_wal_io_timing=on -c track_io_timing=on
sleep 10
PG=postgres://postgres@127.0.0.1:55499/postgres?sslmode=disable

# 3. The commit path alone: base, then the spike's step c. A million changes each, three times.
cd /tmp/slphase0
for i in 1 2 3; do for v in base c; do
  SEARCHLIGHT_TEST_PG_URL=$PG ./store-$v.test -test.run '^$' -test.benchtime 1000x \
    -test.bench '^BenchmarkCommitLoad$/^nodes=1$/^writers=16$/^shards=(8|64)$/^bulk=(500|4096)$/^notify=on$/^flush=sync$/^commit=group$' \
    | tee -a "$DIR/store-$v.txt"
done; done

# 4. End to end, base, at 1 and 64 shards (add the c binary for a comparison).
for s in 1 64; do for i in 1 2 3; do
  docker exec sl-phase0-pg psql -U postgres -c "DROP DATABASE IF EXISTS wl WITH (FORCE)" -c "CREATE DATABASE wl"
  ./slbench writeload --sl-bin ./searchlight-base --dir "$DIR/wl" --store-url "${PG%/postgres*}/wl?sslmode=disable" \
    --pg-url "${PG%/postgres*}/wl?sslmode=disable" --gomaxprocs 2 --docs 20k --bulk 500 --duration 30s --writers 16 \
    --shards $s --changed 1 --percolate=false --readers 0 --client-timeout 60s \
    --settings cluster_token=phase0-token-0123456789 --json "$DIR/wl-s$s-$i.json"
  rm -rf "$DIR/wl"
done; done

# 5. Clean up: the container and its data, keeping the result files.
docker rm -f sl-phase0-pg && rm -rf "$DIR/data"
```

**Apply the gate to step 3's sustained figures:** c at least 2× base on 8 or 64 shards. Also note whether base
stays well under 35k changes/s while `wal_MB/s` has headroom. `--gomaxprocs 2` leaves the 4-core VM's other half
to Postgres.

## Raw data

`E:\tmp\writescale-phase0\results\`:
- `runs.jsonl`: every run, with its metrics, wait events and WAL deltas;
- `store_summary.txt`, `e2e_summary.txt`, `cluster_summary.txt`;
- `machine.csv`: CPU and disk every 5 s;
- `pg_test_fsync_E.txt`.

`scripts\runner.py` and `scripts\analyze.py` reproduce them.
