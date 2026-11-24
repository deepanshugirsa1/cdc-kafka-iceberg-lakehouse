"""Pure Debezium-envelope logic. No Spark, no Kafka - unit tested in CI."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

CONTROL_COLUMNS = ("_op", "_lsn", "_ts_ms", "_source_table")
DELETE, UPDATE, INSERT, SNAPSHOT = "d", "u", "c", "r"


@dataclass(frozen=True)
class ChangeRecord:
    key: str
    op: str
    lsn: int
    ts_ms: int
    payload: dict[str, Any]

    @property
    def is_delete(self) -> bool:
        return self.op == DELETE


def parse_envelope(raw: dict[str, Any] | None, key_field: str = "id") -> ChangeRecord | None:
    """Turn one Debezium envelope into a ChangeRecord.

    Returns None for tombstones, heartbeats and anything without a usable key:
    a malformed record must never stall the stream.
    """
    if not raw:
        return None
    envelope = raw.get("payload", raw)
    op = envelope.get("op")
    if op not in (INSERT, UPDATE, DELETE, SNAPSHOT):
        return None

    row = envelope.get("before") if op == DELETE else envelope.get("after")
    if not row or row.get(key_field) in (None, ""):
        return None

    source = envelope.get("source") or {}
    # Snapshot rows carry no LSN; they must lose to any streamed change for the
    # same key, so they are ordered at 0.
    lsn = source.get("lsn")
    return ChangeRecord(
        key=str(row[key_field]),
        op=op,
        lsn=int(lsn) if lsn is not None else 0,
        ts_ms=int(envelope.get("ts_ms") or source.get("ts_ms") or 0),
        payload=dict(row),
    )


def dedupe_latest(records: Iterable[ChangeRecord]) -> list[ChangeRecord]:
    """Collapse a micro-batch to one record per key: the highest LSN wins.

    Postgres hands out LSNs monotonically, so this is the same ordering the
    database applied. Ties break on ts_ms, then on delete, because a delete that
    shares an LSN with an update is the later intent.
    """
    winners: dict[str, ChangeRecord] = {}
    for record in records:
        current = winners.get(record.key)
        if current is None or _rank(record) > _rank(current):
            winners[record.key] = record
    return [winners[k] for k in sorted(winners)]


def _rank(record: ChangeRecord) -> tuple[int, int, int]:
    return (record.lsn, record.ts_ms, 1 if record.is_delete else 0)


def to_row(record: ChangeRecord) -> dict[str, Any]:
    """Flatten a ChangeRecord into the shape written to the lakehouse."""
    row = dict(record.payload)
    row["_op"] = record.op
    row["_lsn"] = record.lsn
    row["_ts_ms"] = record.ts_ms
    return row


def update_ratio(records: Iterable[ChangeRecord]) -> float:
    """Share of changes that are updates - the workload property this repo exists to test."""
    records = list(records)
    if not records:
        return 0.0
    return sum(1 for r in records if r.op == UPDATE) / len(records)
