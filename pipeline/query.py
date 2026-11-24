"""Read-side sanity checks against the Iceberg table."""
from pipeline.spark_session import TABLE, build_spark

spark = build_spark("cdc-query")
spark.sql(f"SELECT count(*) AS rows, count(DISTINCT workspace_id) AS workspaces FROM {TABLE}").show()
spark.sql(f"SELECT _op, count(*) AS n FROM {TABLE} GROUP BY _op ORDER BY n DESC").show()
spark.sql(f"SELECT max(_lsn) AS highest_lsn_applied FROM {TABLE}").show()
spark.sql(f"SELECT count(*) AS files, sum(file_size_in_bytes) AS bytes FROM {TABLE}.files").show()
spark.stop()
