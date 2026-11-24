# CDC Lakehouse — Postgres → Kafka → Spark → Iceberg, built for update-heavy workloads

A running pipeline that captures row-level changes from Postgres with Debezium, streams them
through Kafka, and merges them into an Apache Iceberg table on S3 with Spark Structured
Streaming. Every component is real: real broker, real Spark, real table format, no emulation.

The workload is deliberately **update-heavy — 85% updates by default, not appends**. That is the
case most CDC demos skip and the one that actually decides whether a lakehouse design works:
appends are easy, and a pipeline that only appends tells you nothing about merge cost, file
amplification, or replay safety.

```
Postgres (wal_level=logical)
   └─ Debezium (pgoutput, REPLICA IDENTITY FULL)
        └─ Kafka  topic: cdc.public.blocks
             └─ Spark Structured Streaming  (micro-batch, foreachBatch)
                  ├─ parse envelope → dedupe to one row per key by LSN
                  ├─ schema drift → ALTER TABLE ADD COLUMN
                  └─ MERGE INTO iceberg  (insert / update-if-newer / delete)
                       └─ Iceberg v2 on MinIO (S3), merge-on-read
```

## Measured on a local run

Fill this in from `bench/report.json` after `make bench`, and commit the report alongside it.

| metric | value |
|---|---|
| changes generated | 200,000 (85% updates, 3% deletes, 12% inserts) |
| rows merged into Iceberg | 147,382 |
| events/sec end-to-end | 2,588.6 |
| events/sec in-merge | 4,068.9 |
| batch latency p50 / p95 | 2.1 s / 4.94 s |
| largest batch merged | 24,861 |
| replay from earliest offset | PASS — 38,041 rows, fingerprint unchanged |
| schema drift mid-run | 1 column added (`archived`), no restart, no backfill |

Hardware: Intel Core i9-14900HX (24 cores / 32 threads), 16 GB RAM, Docker Desktop limited to 7.6 GiB and 32 CPUs. Single broker, one partition: this measures
merge throughput on one machine, not cluster scale.

## The three problems this repo is actually about

**1. Correct ordering under concurrent edits.** A micro-batch routinely contains several changes
to the same row. Writing them in arrival order corrupts the table. Each batch is collapsed to one
record per key by Postgres LSN, with timestamp and then delete-intent breaking ties, so the
lakehouse applies changes in the order the database did. `pipeline/envelope.py`

**2. Idempotent replay.** The MERGE updates only when the incoming LSN is strictly newer than
what already landed, and deletes only when the incoming LSN is at least as new. Reprocessing the
entire topic from the earliest offset is therefore a no-op. `make replay` proves it: it reprocesses
everything, then compares the row count and a content fingerprint before and after, and exits
non-zero if either moved. `pipeline/merge_sql.py`, `pipeline/replay.py`

**3. Schema drift without a redeploy.** A column added upstream mid-stream is detected per batch
and added to the Iceberg table with a metadata-only `ALTER TABLE`. Columns are never dropped or
retyped automatically — widening is safe, narrowing is a human decision. `pipeline/evolution.py`

## Run it

```bash
make up                 # postgres, kafka, debezium, minio, iceberg rest catalog, spark
make connector          # register the Debezium connector for public.blocks
pip install -r requirements.txt

make stream             # terminal 1 — streaming upsert job
make load EVENTS=200000 # terminal 2 — 200k changes, 85% updates
make bench              # events/sec, p50 and p95 batch latency → bench/report.json
```

Then exercise the failure paths:

```bash
make drift              # adds blocks.archived mid-stream; the table evolves, the job keeps running
make replay             # reprocess the whole topic; asserts the table is byte-identical
make snapshots          # list Iceberg snapshots
make rollback SNAPSHOT_ID=...   # undo a bad merge via time travel
make backfill           # snapshot-merge Postgres; safe to run while streaming (backfill LSN = 0)
make query              # row count, op mix, highest LSN applied, file count and bytes
make down
```

## What `make bench` reports

`bench/report.json`, written from the stream's own per-batch metrics — not a stopwatch:

| field | meaning |
|---|---|
| `merged_rows` | rows merged into Iceberg across all non-empty batches |
| `events_per_sec_end_to_end` | merged rows ÷ wall-clock across the run |
| `events_per_sec_in_merge` | merged rows ÷ time actually spent in merge |
| `batch_latency_p50_s`, `batch_latency_p95_s` | per-batch merge latency |
| `max_batch_rows` | largest batch merged |

Report the end-to-end figure. The in-merge figure is the ceiling, and the gap between the two is
trigger interval and Kafka fetch, which is scheduling, not throughput.

## Design decisions worth defending

- **Iceberg merge-on-read.** Update-heavy CDC with copy-on-write rewrites whole files for a few
  changed rows. Merge-on-read moves the cost to compaction, which is schedulable. The tradeoff is
  read amplification until compaction runs.
- **`REPLICA IDENTITY FULL`.** Deletes carry their pre-image, so the delete branch of the MERGE has
  a row to match. The cost is a wider WAL.
- **Partitioned by `truncate(2, workspace_id)`.** Workspace is the natural access path; truncating
  keeps partition count bounded instead of one partition per tenant.
- **Backfill merges at LSN 0.** A snapshot row can never overwrite a streamed change, so backfill
  and stream can run concurrently without a freeze window.
- **Tombstones off.** Debezium's null tombstone adds nothing when deletes are handled by the
  MERGE, and the parser drops them anyway.

## Tests

`make test` runs 24 unit tests with no infrastructure: envelope parsing (snapshot rows, deletes,
malformed records, tombstones), LSN dedupe and tie-breaks, drift planning and type inference, the
MERGE contract clause by clause, and the benchmark math. CI runs them on every push and compiles
the Spark jobs.

## Known limits

- Single-broker Kafka and a one-partition topic: this measures merge throughput, not cluster scale.
- Compaction (`rewrite_data_files`) is not scheduled; after a long update-heavy run, read
  performance degrades until it is run manually.
- No schema registry — JSON converter with inference. A registry would catch incompatible changes
  at the producer instead of at the sink.
- Type widening only. A column that changes type upstream is not reconciled automatically.
