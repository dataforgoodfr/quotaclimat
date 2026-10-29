"""The values of advertising.ad.prediction_status are declared in one place (s03_classification/prediction_status.py).

- every status the pipeline writes or reads is declared final or non final, and every declared status is used;
- the dbt var final_prediction_statuses (analytics.publicites) is the Python list.

No database: the statuses are read in the code (ast), the dbt var in dbt_project.yml.
"""

import ast
from pathlib import Path
from typing import get_args

import yaml

from quotaclimat.data_ingestion.advertising.s03_classification.ad_classification_schema import (
    ContentType,
)
from quotaclimat.data_ingestion.advertising.s03_classification.prediction_status import (
    AD_CONTENT_TYPE,
    FINAL_PREDICTION_STATUSES,
    LEGACY_PREDICTION_STATUSES,
    NON_FINAL_PREDICTION_STATUSES,
)

ADVERTISING_DIR = Path("quotaclimat/data_ingestion/advertising")
CLASSIFICATION_DIR = ADVERTISING_DIR / "s03_classification"
DBT_PROJECT = Path("my_dbt_project/dbt_project.yml")
PUBLICITES_MODEL = Path("my_dbt_project/models/analytics/publicites.sql")
# results of a stage that are never written to prediction_status (s03_classification/pipeline/runner.py)
RUNNER_ONLY_STATUSES = {"ok", "error"}


def _strings(node: ast.AST) -> list[str]:
    """String literals of a node: a constant, or the constants of a list / tuple / set."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [s for element in node.elts for s in _strings(element)]
    return []


def _attribute_name(node: ast.AST) -> str | None:
    return node.attr if isinstance(node, ast.Attribute) else None


def _name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    return _attribute_name(node)


def statuses_in_code(tree: ast.AST, classification: bool) -> set[str]:
    """Status literals of a module.

    Everywhere: prediction_status=... (writes), comparisons with a prediction_status attribute and its .in_(...)
    (reads). In s03_classification (classification=True) also the results of the stages: status=... in a call
    (StageResult), assignments to a status variable, and the method of a DictMatch (written as the status by
    e03_dict_match.py).
    """
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            callee = _name(node.func)
            for keyword in node.keywords:
                if keyword.arg == "prediction_status" or (
                    classification and keyword.arg == "status"
                ):
                    found.update(_strings(keyword.value))
                if classification and callee == "DictMatch" and keyword.arg == "method":
                    found.update(_strings(keyword.value))
            if classification and callee == "DictMatch" and node.args:
                found.update(_strings(node.args[0]))
            if (
                callee == "in_"
                and isinstance(node.func, ast.Attribute)
                and _attribute_name(node.func.value) == "prediction_status"
                and node.args
            ):
                found.update(_strings(node.args[0]))
        elif isinstance(node, ast.Compare):
            sides = [node.left, *node.comparators]
            names = {"prediction_status"} | ({"status"} if classification else set())
            if any(_attribute_name(side) in names for side in sides):
                for side in sides:
                    found.update(_strings(side))
        elif classification and isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "status":
                    found.update(_strings(node.value))
                elif isinstance(target, ast.Tuple) and isinstance(
                    node.value, ast.Tuple
                ):
                    for element, value in zip(target.elts, node.value.elts):
                        if isinstance(element, ast.Name) and element.id == "status":
                            found.update(_strings(value))
    return found


def pipeline_statuses() -> set[str]:
    found = set()
    for path in ADVERTISING_DIR.rglob("*.py"):
        if path.name == "prediction_status.py":
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        found |= statuses_in_code(
            tree, classification=CLASSIFICATION_DIR in path.parents
        )
    return found - RUNNER_ONLY_STATUSES


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
    NON_FINAL_PREDICTION_STATUSES (s03_classification/prediction_status.py): final ones are shown in
    analytics.publicites and in the screenshots, non final ones are left out."""
    declared = set(FINAL_PREDICTION_STATUSES) | set(NON_FINAL_PREDICTION_STATUSES)
    undeclared = pipeline_statuses() - declared
    assert not undeclared, (
        f"Statuses neither final nor non final: {sorted(undeclared)}. Declare them in "
        "s03_classification/prediction_status.py (and in the README status reference)."
    )


def test_every_declared_status_is_used():
    """A declared status that the code never writes or reads is stale (or the scan of the code missed it)."""
    declared = set(FINAL_PREDICTION_STATUSES) | set(NON_FINAL_PREDICTION_STATUSES)
    assert declared - pipeline_statuses() == set()


def test_scan_finds_each_kind_of_status():
    """The scan of the code finds a status written in each way: a status variable, a DictMatch, a StageResult,
    a read in a comparison."""
    assert statuses_in_code(
        ast.parse('status, method = "a", "a"'), classification=True
    ) == {"a"}
    assert statuses_in_code(
        ast.parse('DictMatch("b", sector)'), classification=True
    ) == {"b"}
    assert statuses_in_code(
        ast.parse('DictMatch(method="c")'), classification=True
    ) == {"c"}
    assert statuses_in_code(
        ast.parse('StageResult(ad_id=i, status="d")'), classification=True
    ) == {"d"}
    assert statuses_in_code(
        ast.parse('Ad.prediction_status.in_(["e", "f"])'), classification=False
    ) == {"e", "f"}
    assert statuses_in_code(
        ast.parse('x.values(prediction_status="g")'), classification=False
    ) == {"g"}
    assert statuses_in_code(
        ast.parse('Ad.prediction_status == "h"'), classification=False
    ) == {"h"}
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
        assert (
            f"'{status}'" not in sql
        ), f"{status} written in publicites.sql instead of the macro"


def test_ad_content_type_is_a_content_type_of_the_classification():
    assert AD_CONTENT_TYPE in get_args(ContentType)
