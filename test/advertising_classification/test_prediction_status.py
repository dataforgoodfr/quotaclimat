"""The values of advertising.ad.prediction_status are declared in one place (tools/prediction_status.py).

- every status the pipeline writes or reads is declared final or non final, and every declared status is used;
- the dbt var final_prediction_statuses (analytics.publicites) is the Python list.

No database: the statuses are read in the code (ast), the dbt var in dbt_project.yml.
"""

import ast
from pathlib import Path

import yaml

from quotaclimat.data_ingestion.advertising.tools.prediction_status import (
    AD_CONTENT_TYPE,
    CONTENT_TYPES,
    FINAL_PREDICTION_STATUSES,
    LEGACY_PREDICTION_STATUSES,
    NON_FINAL_PREDICTION_STATUSES,
)

ADVERTISING_DIR = Path("quotaclimat/data_ingestion/advertising")
CLASSIFICATION_DIR = ADVERTISING_DIR / "s03_classification"
# its dictionary matches give the status written by e3_dict_match.py
DICTIONARY_MODULE = ADVERTISING_DIR / "tools" / "brand_dictionary.py"
DBT_PROJECT = Path("my_dbt_project/dbt_project.yml")
PUBLICITES_MODEL = Path("my_dbt_project/models/analytics/publicites.sql")


def _strings(node: ast.AST) -> list[str]:
    """String literals of a node: a constant, or the constants of a list / tuple / set."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.IfExp):
        return _strings(node.body) + _strings(node.orelse)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [s for element in node.elts for s in _strings(element)]
    return []


def _attribute_name(node: ast.AST) -> str | None:
    return node.attr if isinstance(node, ast.Attribute) else None


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return _attribute_name(node)


def statuses_in_code(
    tree: ast.AST, classification: bool, dictionary: bool = False
) -> set[str]:
    """Status literals of a module.

    Everywhere: prediction_status=... and "prediction_status": ... in a dict (writes), comparisons with a
    prediction_status attribute and its .in_(...) (reads). In s03_classification (classification=True) also
    the constants named *_STATUS or *_STATUSES. In the brand dictionary (dictionary=True), the method of a
    match, written as the status by e3_dict_match.py. Not the status of a StageResult: it only counts the
    results of a stage in its progress bar.
    """
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "prediction_status":
                    found.update(_strings(keyword.value))
            if (
                _name(node.func) == "in_"
                and isinstance(node.func, ast.Attribute)
                and _attribute_name(node.func.value) == "prediction_status"
                and node.args
            ):
                found.update(_strings(node.args[0]))
        elif isinstance(node, ast.Dict):
            keys = {"prediction_status"} | ({"method"} if dictionary else set())
            for key, value in zip(node.keys, node.values):
                if isinstance(key, ast.Constant) and key.value in keys:
                    found.update(_strings(value))
        elif isinstance(node, ast.Compare):
            sides = [node.left, *node.comparators]
            if any(_attribute_name(side) == "prediction_status" for side in sides):
                for side in sides:
                    found.update(_strings(side))
        elif classification and isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.endswith(
                    ("_STATUS", "_STATUSES")
                ):
                    found.update(_strings(node.value))
    return found


def pipeline_statuses() -> set[str]:
    found = set()
    for path in ADVERTISING_DIR.rglob("*.py"):
        if path.name == "prediction_status.py":
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        found |= statuses_in_code(
            tree,
            classification=CLASSIFICATION_DIR in path.parents,
            dictionary=path == DICTIONARY_MODULE,
        )
    return found


def test_final_non_final_and_legacy_are_disjoint():
    lists = [
        FINAL_PREDICTION_STATUSES,
        NON_FINAL_PREDICTION_STATUSES,
        LEGACY_PREDICTION_STATUSES,
    ]
    all_statuses = [status for statuses in lists for status in statuses]
    assert len(set(all_statuses)) == len(all_statuses)


def test_legacy_statuses_are_no_longer_used():
    """A legacy status is only in the database: the code writing it was removed."""
    assert set(LEGACY_PREDICTION_STATUSES).isdisjoint(pipeline_statuses())


def test_every_pipeline_status_is_declared():
    """A new status written by the pipeline must be added to FINAL_PREDICTION_STATUSES or
    NON_FINAL_PREDICTION_STATUSES (tools/prediction_status.py): final ones are shown in
    analytics.publicites and in the screenshots, non final ones are left out."""
    declared = set(FINAL_PREDICTION_STATUSES) | set(NON_FINAL_PREDICTION_STATUSES)
    undeclared = pipeline_statuses() - declared
    assert not undeclared, (
        f"Statuses neither final nor non final: {sorted(undeclared)}. Declare them in "
        "tools/prediction_status.py (and in the status table of s03_classification/sector_and_brand/README.md)."
    )


def test_every_declared_status_is_used():
    """A declared status that the code never writes or reads is stale (or the scan of the code missed it)."""
    declared = set(FINAL_PREDICTION_STATUSES) | set(NON_FINAL_PREDICTION_STATUSES)
    assert declared - pipeline_statuses() == set()


def test_scan_finds_each_kind_of_status():
    """The scan of the code finds a status written in each way: a status constant, a dictionary match, the
    values of an UPDATE, a read in a comparison."""
    assert statuses_in_code(
        ast.parse('DONE_STATUS = "a"\nPENDING_STATUSES = ("b", "c")'),
        classification=True,
    ) == {"a", "b", "c"}
    assert statuses_in_code(
        ast.parse('{"method": "d", "sector_code": s}'),
        classification=False,
        dictionary=True,
    ) == {"d"}
    assert statuses_in_code(
        ast.parse('{"prediction_status": DONE if ok else "e"}'), classification=False
    ) == {"e"}
    assert statuses_in_code(
        ast.parse('Ad.prediction_status.in_(["f", "g"])'), classification=False
    ) == {"f", "g"}
    assert statuses_in_code(
        ast.parse('x.values(prediction_status="h")'), classification=False
    ) == {"h"}
    assert statuses_in_code(
        ast.parse('Ad.prediction_status == "i"'), classification=False
    ) == {"i"}
    # the status of a StageResult only counts the results of a stage
    assert (
        statuses_in_code(ast.parse('StageResult(ad_id, "ok")'), classification=True)
        == set()
    )
    # outside the classification, status is another column (e.g. advertising.ad_screenshot)
    assert (
        statuses_in_code(ast.parse('Screenshot(status="done")'), classification=False)
        == set()
    )


def test_dbt_var_is_the_python_list():
    dbt_vars = yaml.safe_load(DBT_PROJECT.read_text())["vars"]
    assert dbt_vars["final_prediction_statuses"] == list(FINAL_PREDICTION_STATUSES)


def test_publicites_uses_the_dbt_list():
    sql = PUBLICITES_MODEL.read_text()
    assert "prediction_status IN ({{ final_prediction_statuses() }})" in sql
    assert f"a.fragment_type = '{AD_CONTENT_TYPE}'" in sql
    for status in FINAL_PREDICTION_STATUSES:
        assert f"'{status}'" not in sql, (
            f"{status} written in publicites.sql instead of the macro"
        )


def test_ad_content_type_is_a_content_type_of_the_classification():
    assert AD_CONTENT_TYPE in CONTENT_TYPES
