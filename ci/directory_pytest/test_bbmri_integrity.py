import json
from pathlib import Path

import pytest


def _load_optional_fixture(path: str):
    p = Path(path)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def test_biobanks_table_exists_and_has_rows(emx2, schema_names):
    print("test_biobanks_table_exists_and_has_rows: entered")
    schema = schema_names["bbmri"]
    data = emx2(schema, "query { Biobanks { id } }")
    # If you genuinely have an empty dataset, change this to >= 0
    assert len(data["Biobanks"]) > 0


def test_no_duplicate_biobank_ids_in_first_n(emx2, schema_names):
    """
    If your dataset is huge and GraphQL returns many rows, you may want paging.
    This checks the first N ids returned are unique (good smoke-test).
    """
    print("test_no_duplicate_biobank_ids_in_first_n: entered")
    schema = schema_names["bbmri"]
    data = emx2(schema, "query { Biobanks { id } }")
    ids = [r["id"] for r in data["Biobanks"]]
    assert len(ids) == len(set(ids)), "Duplicate Biobanks.id detected in returned rows"


@pytest.mark.parametrize(
    "table,field,fixture_path",
    [
        # Put baseline IDs you expect to never disappear in these fixture files.
        ("Biobanks", "id", "/directory_reference/biobanks_ids.json"),
        ("Collections", "id", "/directory_reference/collections_ids.json"),
        ("CollectionFacts", "id", "/directory_reference/collection_facts_ids.json"),
    ],
)
def test_expected_ids_still_present_if_fixture_exists(emx2, schema_names, table, field, fixture_path):
    """
    Optional: if you provide fixtures listing expected IDs, assert they are still present.
    If the fixture file doesn't exist, the test is skipped (so you can add them gradually).
    """
    print(f"test_expected_ids_still_present_if_fixture_exists: Checking {table}.{field} fixture: {fixture_path}")
    expected = _load_optional_fixture(fixture_path)
    print(f"test_expected_ids_still_present_if_fixture_exists: expected: {expected}")
    if expected is None:
        pytest.skip(f"No fixture file: {fixture_path}")
    
    print(f"test_expected_ids_still_present_if_fixture_exists: Expected {len(expected)} {table}.{field} values")

    schema = schema_names["bbmri"]
    data = emx2(schema, f"query {{ {table} {{ {field} }} }}")
    actual = {r[field] for r in data[table]}

    print(f"test_expected_ids_still_present_if_fixture_exists: Found {len(actual)} {table}.{field} values")

    missing = [x for x in expected if x not in actual]
    assert not missing, f"Missing {table}.{field} values after sync, e.g.: {missing[:10]}"

def test_table_row_counts_if_fixture_exists(emx2, schema_names):
    print("test_table_row_counts_if_fixture_exists: entered")
    cfg = _load_optional_fixture("/directory_reference/table_counts.json")
    if cfg is None:
        pytest.skip("No table_counts.json fixture")

    schema = schema_names["bbmri"]

    for table, rule in cfg.items():
        print(f"test_table_row_counts_if_fixture_exists: Checking rule: {rule}")
        # Adjust this to your actual aggregate query shape
        data = emx2(schema, f"query {{ {table}_agg {{ count }} }}")
        count = data[f"{table}_agg"]["count"]
        print(f"test_table_row_counts_if_fixture_exists: {table}: {count}")

        if "exact" in rule:
            assert count == rule["exact"], f"{table}: {count} != {rule['exact']}"
        if "min" in rule:
            assert count >= rule["min"], f"{table}: {count} < {rule['min']}"
        if "max" in rule:
            assert count <= rule["max"], f"{table}: {count} > {rule['max']}"