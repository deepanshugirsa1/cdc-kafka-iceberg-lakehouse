from pipeline.evolution import infer_type, plan_schema_evolution

KNOWN = ["id", "workspace_id", "type", "content"]


def test_no_drift_means_no_ddl():
    assert plan_schema_evolution("lake.blocks", KNOWN, [{"id": "1", "content": "a"}]) == []


def test_new_column_is_added_once_and_typed():
    rows = [{"id": "1", "archived": True}, {"id": "2", "archived": False, "depth": 3}]
    assert plan_schema_evolution("lake.blocks", KNOWN, rows) == [
        "ALTER TABLE lake.blocks ADD COLUMN archived boolean",
        "ALTER TABLE lake.blocks ADD COLUMN depth bigint",
    ]


def test_control_columns_are_never_proposed():
    rows = [{"id": "1", "_op": "u", "_lsn": 9, "_ts_ms": 1, "_source_table": "blocks"}]
    assert plan_schema_evolution("lake.blocks", KNOWN, rows) == []


def test_case_insensitive_against_existing_columns():
    assert plan_schema_evolution("lake.blocks", ["ID", "Content"], [{"id": "1", "content": "a"}]) == []


def test_nulls_widen_to_string_rather_than_guessing():
    assert infer_type(None) == "string"
    assert plan_schema_evolution("lake.blocks", KNOWN, [{"id": "1", "note": None}]) == [
        "ALTER TABLE lake.blocks ADD COLUMN note string"
    ]


def test_type_mapping():
    assert (infer_type(True), infer_type(5), infer_type(1.5), infer_type("s")) == (
        "boolean",
        "bigint",
        "double",
        "string",
    )
