from pipeline.envelope import (
    ChangeRecord,
    dedupe_latest,
    parse_envelope,
    to_row,
    update_ratio,
)


def envelope(op, lsn, block_id="b1", content="x", ts=10):
    row = {"id": block_id, "workspace_id": "w1", "type": "text", "content": content}
    return {
        "op": op,
        "before": row if op == "d" else None,
        "after": None if op == "d" else row,
        "ts_ms": ts,
        "source": {"lsn": lsn, "table": "blocks"},
    }


def test_parses_update():
    record = parse_envelope(envelope("u", 500))
    assert record == ChangeRecord(key="b1", op="u", lsn=500, ts_ms=10, payload=record.payload)
    assert record.payload["content"] == "x"


def test_delete_uses_before_image():
    record = parse_envelope(envelope("d", 700))
    assert record.is_delete and record.key == "b1"


def test_snapshot_rows_order_below_streamed_changes():
    snapshot = parse_envelope({**envelope("r", None), "source": {"table": "blocks"}})
    streamed = parse_envelope(envelope("u", 1))
    assert snapshot.lsn == 0
    assert dedupe_latest([streamed, snapshot]) == [streamed]


def test_tombstones_and_junk_are_skipped():
    assert parse_envelope(None) is None
    assert parse_envelope({"op": "u", "after": None}) is None
    assert parse_envelope({"op": "heartbeat"}) is None
    assert parse_envelope({"op": "u", "after": {"id": None}}) is None


def test_unwraps_payload_wrapper():
    assert parse_envelope({"payload": envelope("c", 3)}).lsn == 3


def test_dedupe_keeps_highest_lsn_per_key():
    batch = [
        parse_envelope(envelope("c", 1, content="v1")),
        parse_envelope(envelope("u", 9, content="v9")),
        parse_envelope(envelope("u", 4, content="v4")),
        parse_envelope(envelope("c", 2, block_id="b2")),
    ]
    latest = {r.key: r for r in dedupe_latest(batch)}
    assert latest["b1"].payload["content"] == "v9"
    assert len(latest) == 2


def test_delete_wins_a_tie_on_the_same_lsn():
    update = parse_envelope(envelope("u", 42))
    delete = parse_envelope(envelope("d", 42))
    assert dedupe_latest([update, delete])[0].is_delete


def test_to_row_adds_control_columns():
    row = to_row(parse_envelope(envelope("u", 77)))
    assert row["_op"] == "u" and row["_lsn"] == 77 and row["_ts_ms"] == 10


def test_update_ratio_measures_the_workload():
    batch = [parse_envelope(envelope("u", i)) for i in range(8)]
    batch += [parse_envelope(envelope("c", i, block_id=f"n{i}")) for i in range(2)]
    assert update_ratio(batch) == 0.8
    assert update_ratio([]) == 0.0
