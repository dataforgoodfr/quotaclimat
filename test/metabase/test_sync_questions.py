import copy

import pytest

from quotaclimat.metabase.questions import METABASE_DIR, load_queries, read_query, render, sql_version, tag_names
from quotaclimat.metabase.sync_questions import marker, native_query, plan, sync

SQL = "SELECT {{debut}}, {{niveau}} [[, {{nb}}]] FROM analytics.t WHERE {{secteur}}"
VARIABLES = {
    "debut": {"type": "date"},
    "niveau": {"type": "text", "values": ["marque", "entreprise"], "default": "entreprise"},
    "nb": {"type": "number"},
    "secteur": {"type": "dimension", "field": "analytics.t.secteur", "widget": "string/="},
}
FIELD_IDS = {"secteur": 12}


def mbql5_card(sql: str, tags: list, parameters: list, description=None) -> dict:
    return {
        "name": "test",
        "description": description,
        "dataset_query": {
            "lib/type": "mbql/query",
            "database": 2,
            "stages": [{"lib/type": "mbql.stage/native", "native": sql, "template-tags": tags}],
        },
        "parameters": parameters,
    }


# a question created by hand: niveau as a number widget with its own default, an obsolete variable
NIVEAU = {"id": "id-niveau", "name": "niveau", "display-name": "Échelle", "type": "text", "default": ["marque"]}
OBSOLETE = {"id": "id-old", "name": "old", "display-name": "Old", "type": "text"}
NIVEAU_PARAMETER = {"id": "id-niveau", "type": "string/=", "slug": "niveau", "name": "Échelle",
                    "target": ["variable", ["template-tag", "niveau"]], "default": ["marque"]}
OLD_PARAMETER = {"id": "id-old", "type": "string/=", "slug": "old", "target": ["variable", ["template-tag", "old"]]}


def test_plan_keeps_existing_variables_and_adds_new_ones():
    card = mbql5_card("SELECT {{niveau}}, {{old}}", [NIVEAU, OBSOLETE], [NIVEAU_PARAMETER, OLD_PARAMETER])
    change = plan(card, "q.sql", SQL, VARIABLES, FIELD_IDS)
    # never synchronized: its SQL may have been edited in Metabase
    assert change["status"] == "edited in Metabase"
    sql, tags = native_query(change["payload"]["dataset_query"])
    assert sql == SQL
    assert list(tags) == ["debut", "niveau", "nb", "secteur"]
    assert tags["niveau"] == NIVEAU
    assert tags["debut"]["type"] == "date"
    assert tags["secteur"]["dimension"][2] == 12
    assert tags["secteur"]["widget-type"] == "string/="
    # stored as a list, like on the question
    assert isinstance(change["payload"]["dataset_query"]["stages"][0]["template-tags"], list)
    parameters = {p["slug"]: p for p in change["payload"]["parameters"]}
    assert parameters["niveau"] == NIVEAU_PARAMETER
    assert "old" not in parameters
    assert parameters["secteur"]["target"] == ["dimension", ["template-tag", "secteur"]]
    assert parameters["secteur"]["isMultiSelect"] is True
    assert parameters["nb"]["type"] == "number/="
    assert change["payload"]["description"] == marker("q.sql", SQL)
    assert change["summary"][0] == "SQL"
    assert "variable old retirée" in change["summary"]


def test_plan_after_synchronization():
    card = mbql5_card("SELECT {{niveau}}", [NIVEAU], [NIVEAU_PARAMETER], "Ma question.")
    first = plan(card, "q.sql", SQL, VARIABLES, FIELD_IDS)
    assert first["payload"]["description"] == "Ma question.\n\n" + marker("q.sql", SQL)
    synchronized = {**card, **copy.deepcopy(first["payload"])}
    assert plan(synchronized, "q.sql", SQL, VARIABLES, FIELD_IDS)["status"] == "up to date"
    # new version of the file: plain update, the marker is replaced
    new_sql = SQL + " ORDER BY 1"
    change = plan(synchronized, "q.sql", new_sql, VARIABLES, FIELD_IDS)
    assert change["status"] == "update"
    assert change["payload"]["description"] == "Ma question.\n\n" + marker("q.sql", new_sql)
    assert sql_version(new_sql) in change["payload"]["description"]
    # SQL edited in Metabase since the synchronization
    edited = copy.deepcopy(synchronized)
    edited["dataset_query"]["stages"][0]["native"] = SQL + " LIMIT 10"
    assert plan(edited, "q.sql", new_sql, VARIABLES, FIELD_IDS)["status"] == "edited in Metabase"


def test_plan_legacy_dataset_query_and_retyped_variable():
    # secteur was a text variable, it becomes a field filter; legacy shape with template tags by name
    card = {
        "name": "test",
        "description": None,
        "dataset_query": {
            "type": "native",
            "database": 2,
            "native": {"query": SQL, "template-tags": {"secteur": {"id": "x", "name": "secteur", "type": "text"}}},
        },
        "parameters": [],
    }
    change = plan(card, "q.sql", SQL, VARIABLES, FIELD_IDS)
    # same SQL as the file: nothing of the question's SQL is lost
    assert change["status"] == "update"
    tags = change["payload"]["dataset_query"]["native"]["template-tags"]
    assert tags["secteur"]["type"] == "dimension"
    assert tags["secteur"]["dimension"] == ["field", 12, None]
    assert tags["secteur"]["id"] != "x"
    assert "variable secteur recréée en dimension" in change["summary"]


class FakeMetabase:
    def __init__(self, cards):
        self.cards = cards
        self.updated = {}

    def get(self, path):
        return copy.deepcopy(self.cards[int(path.split("/")[1])])

    def put(self, path, body):
        self.updated[int(path.split("/")[1])] = body

    def field_id(self, database, field):
        return 12


def test_sync_leaves_questions_edited_in_metabase_unless_forced(monkeypatch):
    monkeypatch.setattr("quotaclimat.metabase.sync_questions.read_query", lambda file: SQL)
    queries = {"q.sql": {"questions": [1, 2], "variables": VARIABLES, "database": 2}}
    synchronized = mbql5_card("SELECT 1", [], [], marker("q.sql", "SELECT 1"))
    edited = mbql5_card("SELECT 2", [], [], marker("q.sql", "SELECT 1"))

    metabase = FakeMetabase({1: synchronized, 2: edited})
    assert sync(metabase, queries, apply=False, force=False) is False
    assert metabase.updated == {}

    assert sync(metabase, queries, apply=True, force=False) is False
    assert list(metabase.updated) == [1]

    metabase = FakeMetabase({1: synchronized, 2: edited})
    assert sync(metabase, queries, apply=True, force=True) is True
    assert list(metabase.updated) == [1, 2]


def test_render():
    query, values = render(SQL, VARIABLES, {"debut": "2025-01-01", "secteur": ["Auto", "Banque"]})
    # nb has no value: its optional clause is left out; niveau takes its default, as a varchar
    assert query == (
        'SELECT CAST(%s AS date), CAST(%s AS varchar)  FROM analytics.t '
        'WHERE "analytics"."t"."secteur" IN (%s, %s)'
    )
    assert [str(v) for v in values] == ["2025-01-01", "entreprise", "Auto", "Banque"]
    query, values = render("SELECT 100 % [[{{nb}}]] {{secteur}}", VARIABLES, {"nb": 3})
    assert query == "SELECT 100 %% %s 1 = 1"
    with pytest.raises(ValueError):
        render("SELECT {{debut}}", VARIABLES, {})


def test_manifest_declares_every_variable():
    queries = load_queries()
    # every query is in the manifest of its folder
    assert {p.relative_to(METABASE_DIR).as_posix() for p in METABASE_DIR.rglob("*.sql")} == set(queries)
    assert {file.split("/")[0] for file in queries} == {"advertising", "mesinformation"}
    for file, query in queries.items():
        assert set(tag_names(read_query(file))) == set(query["variables"]), file
        for case in query["tests"]:
            assert set(case["params"]) <= set(query["variables"]), file
