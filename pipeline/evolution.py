"""Schema-drift planning: decide what the target table is missing. Pure."""
from __future__ import annotations

from typing import Any, Iterable

from pipeline.envelope import CONTROL_COLUMNS

_TYPE_MAP = {bool: "boolean", int: "bigint", float: "double", str: "string"}
RESERVED = set(CONTROL_COLUMNS)


def infer_type(value: Any) -> str:
    """Map a JSON value to an Iceberg type. Unknown and null widen to string."""
    if value is None:
        return "string"
    return _TYPE_MAP.get(type(value), "string")


def plan_schema_evolution(
    table: str,
    known_columns: Iterable[str],
    rows: Iterable[dict[str, Any]],
) -> list[str]:
    """Return ALTER TABLE statements for columns that appeared upstream.

    Iceberg adds columns by name without rewriting data, so a producer that adds
    a field mid-stream costs one DDL and no backfill. Columns are never dropped
    or retyped here: that is a human decision, not a streaming one.
    """
    known = {c.lower() for c in known_columns} | RESERVED
    additions: dict[str, str] = {}
    for row in rows:
        for column, value in row.items():
            name = column.lower()
            if name in known or name in additions:
                continue
            additions[name] = infer_type(value)
    return [
        f"ALTER TABLE {table} ADD COLUMN {name} {additions[name]}"
        for name in sorted(additions)
    ]
