"""Shared Spark + Iceberg wiring."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG = os.getenv("ICEBERG_CATALOG", "lake")
TABLE = os.getenv("TARGET_TABLE", f"{CATALOG}.cdc.blocks")
TOPIC = os.getenv("TOPIC", "cdc.public.blocks")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
METRICS_PATH = os.getenv("METRICS_PATH", "/opt/bench/batch_metrics.jsonl")


def build_spark(app: str) -> SparkSession:
    rest = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3 = os.getenv("S3_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", rest)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", os.getenv("WAREHOUSE", "s3://warehouse/"))
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", s3)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate()
    )


def ensure_table(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.cdc")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id string, workspace_id string, parent_id string, type string,
            content string, version bigint, is_deleted boolean,
            created_at string, updated_at string,
            _op string, _lsn bigint, _ts_ms bigint
        ) USING iceberg
        PARTITIONED BY (truncate(2, workspace_id))
        TBLPROPERTIES (
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read',
            'format-version'='2'
        )
        """
    )


def target_columns(spark) -> list[str]:
    return [f.name for f in spark.table(TABLE).schema.fields]
