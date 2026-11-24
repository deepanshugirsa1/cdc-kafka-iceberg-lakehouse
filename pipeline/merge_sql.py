"""MERGE statement construction. Pure, so the upsert contract is unit tested."""
from __future__ import annotations

from typing import Iterable

from pipeline.envelope import CONTROL_COLUMNS


def build_merge_sql(
    target: str,
    source_view: str,
    key: str = "id",
    columns: Iterable[str] = (),
) -> str:
    """Build the idempotent CDC upsert.

    Three properties matter, and each is a line below:
      * a delete removes the row rather than leaving a tombstone;
      * an update applies only when its LSN is newer than what landed, so
        replaying the topic from the earliest offset is a no-op;
      * an unmatched non-delete inserts.
    """
    columns = [c for c in columns if c != key]
    if not columns:
        raise ValueError("merge needs at least one non-key column")
    assignments = ", ".join(f"t.{c} = s.{c}" for c in columns)
    insert_cols = ", ".join([key, *columns])
    insert_vals = ", ".join(f"s.{c}" for c in [key, *columns])
    return (
        f"MERGE INTO {target} t\n"
        f"USING {source_view} s\n"
        f"ON t.{key} = s.{key}\n"
        f"WHEN MATCHED AND s._op = 'd' AND s._lsn >= t._lsn THEN DELETE\n"
        f"WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET {assignments}\n"
        f"WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


def control_columns() -> tuple[str, ...]:
    return CONTROL_COLUMNS
