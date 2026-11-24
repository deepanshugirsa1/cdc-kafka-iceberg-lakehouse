"""Kafka (Debezium) -> Spark Structured Streaming -> Iceberg MERGE upserts.

Update-heavy CDC is an upsert problem, not an append problem: every micro-batch
collapses to one row per key and is merged by LSN, so the job is idempotent and
the topic can be replayed from the earliest offset without changing the table.
"""
from __future__ import annotations

import json
import os
import pathlib
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.evolution import plan_schema_evolution
from pipeline.merge_sql import build_merge_sql
from pipeline.spark_session import (
    BOOTSTRAP,
    METRICS_PATH,
    TABLE,
    TOPIC,
    build_spark,
    ensure_table,
    target_columns,
)

_SAMPLES = {T.BooleanType(): True, T.LongType(): 0, T.IntegerType(): 0, T.DoubleType(): 0.0}


def _sample_row(schema: T.StructType) -> dict:
    return {f.name: _SAMPLES.get(f.dataType, "s") for f in schema.fields}


def flatten_envelopes(raw: DataFrame, spark) -> DataFrame:
    """Parse Debezium envelopes, letting Spark infer the row schema.

    Inference is deliberate: a column added upstream shows up here instead of
    being silently dropped by a hardcoded schema.
    """
    parsed = spark.read.json(raw.select("value").rdd.map(lambda r: r.value))
    if "op" not in parsed.columns:
        return parsed.limit(0)
    image = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    return (
        parsed.withColumn("_image", image)
        .filter(F.col("_image").isNotNull() & F.col("_image.id").isNotNull())
        .select(
            F.col("_image.*"),
            F.col("op").alias("_op"),
            F.coalesce(F.col("source.lsn").cast("bigint"), F.lit(0)).alias("_lsn"),
            F.coalesce(F.col("ts_ms").cast("bigint"), F.lit(0)).alias("_ts_ms"),
        )
    )


def dedupe_latest(df: DataFrame) -> DataFrame:
    """One row per key: highest LSN, then newest timestamp, delete breaking a tie."""
    order = [
        F.col("_lsn").desc(),
        F.col("_ts_ms").desc(),
        F.when(F.col("_op") == "d", 1).otherwise(0).desc(),
    ]
    ranked = df.withColumn("_rn", F.row_number().over(Window.partitionBy("id").orderBy(*order)))
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def evolve(spark, df: DataFrame) -> None:
    for statement in plan_schema_evolution(TABLE, target_columns(spark), [_sample_row(df.schema)]):
        print(f"[drift] {statement}", flush=True)
        spark.sql(statement)


def process_batch(batch: DataFrame, batch_id: int) -> None:
    started = time.time()
    spark = batch.sparkSession
    if batch.rdd.isEmpty():
        return
    changes = flatten_envelopes(batch, spark)
    if not changes.columns:
        return
    evolve(spark, changes)
    latest = dedupe_latest(changes).persist()
    rows = latest.count()
    if rows:
        columns = [c for c in target_columns(spark) if c in latest.columns]
        latest.createOrReplaceTempView("batch_changes")
        spark.sql(build_merge_sql(TABLE, "batch_changes", "id", columns))
    latest.unpersist()
    _record({"batch_id": batch_id, "rows": rows, "started": started, "ended": time.time()})


def _record(metric: dict) -> None:
    path = pathlib.Path(METRICS_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        handle.write(json.dumps(metric) + "\n")
    print(f"[batch {metric['batch_id']}] {metric['rows']} rows in "
          f"{metric['ended'] - metric['started']:.2f}s", flush=True)


def main() -> None:
    spark = build_spark("cdc-to-iceberg")
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)
    (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", os.getenv("STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", os.getenv("MAX_OFFSETS", "50000"))
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
        .filter(F.col("value").isNotNull())
        .writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT", "/opt/bench/_checkpoint"))
        .trigger(processingTime=os.getenv("TRIGGER", "5 seconds"))
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
