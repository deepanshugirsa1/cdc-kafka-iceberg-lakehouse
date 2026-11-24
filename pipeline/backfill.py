"""Snapshot backfill from Postgres, merged by the same LSN rule as the stream.

Backfill rows carry _lsn = 0, so they can never overwrite a change the stream
has already applied. Backfill and stream can therefore run at the same time.
"""
from __future__ import annotations

import os

from pyspark.sql import functions as F

from pipeline.merge_sql import build_merge_sql
from pipeline.spark_session import TABLE, build_spark, ensure_table, target_columns


def main() -> None:
    spark = build_spark("cdc-backfill")
    ensure_table(spark)
    source = (
        spark.read.format("jdbc")
        .option("url", os.getenv("PG_JDBC", "jdbc:postgresql://postgres:5432/app"))
        .option("dbtable", os.getenv("PG_TABLE", "public.blocks"))
        .option("user", os.getenv("PG_USER", "app"))
        .option("password", os.getenv("PG_PASSWORD", "app"))
        .option("driver", "org.postgresql.Driver")
        .option("numPartitions", os.getenv("JDBC_PARTITIONS", "4"))
        .option("partitionColumn", "version")
        .option("lowerBound", "1")
        .option("upperBound", os.getenv("JDBC_UPPER", "1000"))
        .load()
        .withColumn("_op", F.lit("r"))
        .withColumn("_lsn", F.lit(0).cast("bigint"))
        .withColumn("_ts_ms", F.lit(0).cast("bigint"))
    )
    for column in ("created_at", "updated_at"):
        if column in source.columns:
            source = source.withColumn(column, F.col(column).cast("string"))

    source.createOrReplaceTempView("backfill_rows")
    columns = [c for c in target_columns(spark) if c in source.columns]
    spark.sql(build_merge_sql(TABLE, "backfill_rows", "id", columns))
    print(f"[backfill] merged {source.count()} snapshot rows into {TABLE}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
