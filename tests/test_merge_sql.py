import pytest

from pipeline.merge_sql import build_merge_sql

COLUMNS = ["id", "workspace_id", "content", "_op", "_lsn", "_ts_ms"]


def sql():
    return build_merge_sql("lake.blocks", "batch_changes", "id", COLUMNS)


def test_delete_branch_removes_the_row():
    assert "WHEN MATCHED AND s._op = 'd' AND s._lsn >= t._lsn THEN DELETE" in sql()


def test_update_applies_only_to_newer_lsn():
    # This single predicate is what makes a full-topic replay a no-op.
    assert "WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE SET" in sql()


def test_insert_skips_deletes():
    assert "WHEN NOT MATCHED AND s._op <> 'd' THEN INSERT" in sql()


def test_key_is_not_reassigned_on_update():
    update_clause = sql().split("THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    assert "t.id = s.id" not in update_clause
    assert "t.content = s.content" in update_clause


def test_requires_a_payload_column():
    with pytest.raises(ValueError):
        build_merge_sql("lake.blocks", "batch_changes", "id", ["id"])
