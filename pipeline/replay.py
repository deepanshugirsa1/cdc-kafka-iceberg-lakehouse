"""Two recovery paths, both exercised by `make replay`.

replay   - reprocess the whole topic in batch. The row count and every content
           hash must be unchanged, which is what proves the MERGE is idempotent.
rollback - move the table back to an earlier Iceberg snapshot, for the case
           where bad data was merged and needs to be undone, not reprocessed.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from pipeline.merge_sql import build_merge_sql
from pipeline.spark_session import BOOTSTRAP, CATALOG, TABLE, TOPIC, build_spark, target_columns
from pipeline.stream_to_iceberg import dedupe_latest, flatten_envelopes


def fingerprint(spark) -> tuple[int, str]:
    df = spark.table(TABLE)
    digest = df.select(F.sha2(F.concat_ws("|", *sorted(df.columns)), 256).alias("h")).agg(
        F.sha2(F.concat_ws("", F.sort_array(F.collect_list("h"))), 256).alias("f")
    )
    return df.count(), digest.collect()[0]["f"]


def replay(spark) -> None:
    before = fingerprint(spark)
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
        .filter(F.col("value").isNotNull())
    )
    latest = dedupe_latest(flatten_envelopes(raw, spark))
    latest.createOrReplaceTempView("replay_changes")
    columns = [c for c in target_columns(spark) if c in latest.columns]
    spark.sql(build_merge_sql(TABLE, "replay_changes", "id", columns))

    after = fingerprint(spark)
    print(f"[replay] before rows={before[0]} fingerprint={before[1][:12]}")
    print(f"[replay] after  rows={after[0]} fingerprint={after[1][:12]}")
    if before != after:
        raise SystemExit("REPLAY NOT IDEMPOTENT - table changed on reprocessing")
    print("[replay] PASS - full-topic reprocessing left the table byte-identical")


def rollback(spark, snapshot_id: str) -> None:
    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{TABLE}', {snapshot_id})")
    print(f"[rollback] {TABLE} restored to snapshot {snapshot_id}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("replay", "rollback", "snapshots"), default="replay")
    parser.add_argument("--snapshot-id", default=os.getenv("SNAPSHOT_ID"))
    args = parser.parse_args()

    spark = build_spark("cdc-replay")
    if args.mode == "snapshots":
        spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {TABLE}.snapshots").show(50, False)
    elif args.mode == "rollback":
        if not args.snapshot_id:
            raise SystemExit("--snapshot-id is required for rollback")
        rollback(spark, args.snapshot_id)
    else:
        replay(spark)
    spark.stop()


if __name__ == "__main__":
    main()
