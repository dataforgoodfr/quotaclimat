"""Conventions of the curate schema (clean tables for the teams and their Claude, see the CLAUDE.md), checked on
the dbt manifest: no database needed, only the dbt project (dbt parse). A model added to models/curate/ without
its full documentation fails here."""
import json
import os
import re
import subprocess
from pathlib import Path

import pytest

PROJECT_DIR = Path(__file__).resolve().parents[1]
CURATE_PATH = "curate/"
MODEL_META = {"libelle"}
COLUMN_META = {"libelle", "unite", "agregation", "type_metabase", "cle_etrangere"}
AGGREGATIONS = {"somme", "moyenne", "aucune"}
# sections of a model description, after its first line "Une ligne = ..."
DESCRIPTION_SECTIONS = ["Usage :", "Pièges :", "Source :"]
NUMERIC_TYPES = re.compile(r"^(smallint|integer|bigint|int|numeric|decimal|real|double precision|float)\b")
COLUMN_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@pytest.fixture(scope="module")
def manifest() -> dict:
    subprocess.run(
        ["dbt", "parse", "--project-dir", str(PROJECT_DIR), "--profiles-dir", str(PROJECT_DIR / "dbt")],
        check=True, capture_output=True, text=True, env={**os.environ},
    )
    return json.loads((PROJECT_DIR / "target" / "manifest.json").read_text())


def curate_models(manifest: dict) -> dict:
    """The curate models by table name (alias: a model named like a model of another schema is named
    curate_<table>, with alias <table>)."""
    return {
        node["alias"] or node["name"]: node
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model" and node["original_file_path"].startswith(f"models/{CURATE_PATH}")
    }


def column_tests(manifest: dict, model: dict) -> dict:
    """Generic data tests of each column of the model, by column name: {column: {test name: test node}}."""
    tests = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] == "test" and node.get("attached_node") == model["unique_id"] and node.get("column_name"):
            tests.setdefault(node["column_name"], {})[node["test_metadata"]["name"]] = node
    return tests


def test_curate_has_models(manifest):
    assert {"chaine", "publicite", "cas_mesinformation", "emission_jour"} <= set(curate_models(manifest))


def test_curate_model_config(manifest):
    for name, model in curate_models(manifest).items():
        config = model["config"]
        assert model["schema"] == "curate", name
        assert config["materialized"] == "table", f"{name}: a view would be dropped by the rebuild of its sources"
        assert config["contract"]["enforced"], f"{name}: the contract freezes the column names and types"
        assert config["persist_docs"] == {"relation": True, "columns": True}, name


def test_curate_model_description(manifest):
    for name, model in curate_models(manifest).items():
        description = model["description"]
        assert description.startswith("Une ligne = "), f"{name}: the description starts with the grain"
        for section in DESCRIPTION_SECTIONS:
            assert section in description, f"{name}: section « {section} » missing in the description"
        assert set(model["meta"]) <= MODEL_META, f"{name}: unknown meta {set(model['meta']) - MODEL_META}"
        assert model["meta"].get("libelle"), f"{name}: meta.libelle (name shown in Metabase) missing"


def test_curate_column_documentation(manifest):
    for name, model in curate_models(manifest).items():
        assert model["columns"], f"{name}: no documented column"
        for column_name, column in model["columns"].items():
            label = f"{name}.{column_name}"
            meta = column["meta"]
            assert COLUMN_NAME.match(column_name), f"{label}: snake_case without accents"
            assert column["description"].strip(), f"{label}: description missing"
            assert column["data_type"], f"{label}: data_type missing (contract)"
            assert set(meta) <= COLUMN_META, f"{label}: unknown meta {set(meta) - COLUMN_META}"
            assert meta.get("libelle"), f"{label}: meta.libelle missing"
            if "type_metabase" in meta:
                assert meta["type_metabase"].startswith("type/"), label


def test_curate_column_naming(manifest):
    for name, model in curate_models(manifest).items():
        for column_name, column in model["columns"].items():
            label = f"{name}.{column_name}"
            data_type = column["data_type"].lower()
            if data_type.startswith("bool"):
                assert column_name.startswith("est_"), f"{label}: a boolean starts with est_"
            if data_type.startswith("timestamp"):
                assert column_name.endswith("_paris"), f"{label}: a time is in Paris time, named ..._paris"


def test_curate_numeric_columns_have_unit_and_aggregation(manifest):
    for name, model in curate_models(manifest).items():
        for column_name, column in model["columns"].items():
            meta = column["meta"]
            if not NUMERIC_TYPES.match(column["data_type"].lower()) or meta.get("type_metabase") in ("type/PK", "type/FK"):
                continue
            label = f"{name}.{column_name}"
            assert meta.get("unite"), f"{label}: meta.unite missing for a number"
            assert meta.get("agregation") in AGGREGATIONS, f"{label}: meta.agregation among {AGGREGATIONS}"


def test_curate_primary_key(manifest):
    for name, model in curate_models(manifest).items():
        keys = [c for c, column in model["columns"].items() if column["meta"].get("type_metabase") == "type/PK"]
        assert len(keys) == 1, f"{name}: exactly one column with type_metabase: type/PK"
        tests = column_tests(manifest, model).get(keys[0], {})
        assert {"unique", "not_null"} <= set(tests), f"{name}.{keys[0]}: unique and not_null tests"


def test_curate_foreign_keys(manifest):
    models = curate_models(manifest)
    for name, model in models.items():
        tests = column_tests(manifest, model)
        for column_name, column in model["columns"].items():
            meta = column["meta"]
            label = f"{name}.{column_name}"
            if "cle_etrangere" not in meta:
                assert meta.get("type_metabase") != "type/FK", f"{label}: type/FK without cle_etrangere"
                continue
            assert meta.get("type_metabase") == "type/FK", f"{label}: cle_etrangere with type_metabase: type/FK"
            table, target = meta["cle_etrangere"].split(".")
            assert table in models and target in models[table]["columns"], f"{label}: {meta['cle_etrangere']} not in curate"
            assert models[table]["columns"][target]["meta"].get("type_metabase") == "type/PK", label
            relationship = tests.get(column_name, {}).get("relationships")
            assert relationship is not None, f"{label}: relationships test missing"
            assert relationship["test_metadata"]["kwargs"]["field"] == target, label
            assert f"ref('{models[table]['name']}')" in relationship["test_metadata"]["kwargs"]["to"], label


def test_curate_is_the_last_layer(manifest):
    """Nothing in dbt reads a curate table: it can change without breaking a model, and is never dropped by
    the cascade of a rebuild."""
    curate_ids = {model["unique_id"] for model in curate_models(manifest).values()}
    for node in manifest["nodes"].values():
        if node["resource_type"] in ("model", "snapshot") and node["unique_id"] not in curate_ids:
            assert not curate_ids & set(node["depends_on"]["nodes"]), f"{node['name']} reads the curate schema"
