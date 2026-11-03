import copy

from quotaclimat.metabase import sync_curated
from quotaclimat.metabase.sync_curated import curated_tables, field_description, plan, sync, table_footer


def model(name: str, columns: dict, libelle: str) -> dict:
    return {
        "resource_type": "model",
        "name": name,
        "alias": name,
        "schema": "curated",
        "original_file_path": f"models/curated/{name}.sql",
        "description": f"Une ligne = un objet {name}.",
        "meta": {"libelle": libelle},
        "columns": {
            column: {"name": column, "description": f"Description de {column}.", "meta": meta}
            for column, meta in columns.items()
        },
    }


MANIFEST = {
    "nodes": {
        "model.p.chaine": model("chaine", {"chaine_id": {"libelle": "Identifiant", "type_metabase": "type/PK"}}, "Chaînes"),
        "model.p.publicite": model(
            "publicite",
            {
                "chaine_id": {"libelle": "Chaîne", "type_metabase": "type/FK", "cle_etrangere": "chaine.chaine_id"},
                "duree_sec": {"libelle": "Durée (s)", "unite": "seconde", "agregation": "somme"},
            },
            "Publicités",
        ),
        "model.p.publicites": {**model("publicites", {"x": {}}, "x"), "schema": "analytics",
                               "original_file_path": "models/analytics/publicites.sql"},
        "test.p.unique": {"resource_type": "test", "original_file_path": "models/curated/_curated.yml"},
    }
}


def metabase_table(table_id: int, name: str, fields: dict) -> dict:
    return {
        "id": table_id, "db_id": 2, "schema": "curated", "name": name, "display_name": name.title(),
        "description": None,
        "fields": [
            {"id": field_id, "name": field, "display_name": field, "description": None, "semantic_type": None,
             "fk_target_field_id": None}
            for field, field_id in fields.items()
        ],
    }


class FakeMetabase:
    def __init__(self, tables: list[dict]):
        self.by_id = {t["id"]: t for t in tables}
        self.updated = {}
        self.synced = []

    def tables(self, database, schema):
        return [
            {k: t[k] for k in ("id", "db_id", "schema", "name")} for t in self.by_id.values()
            if t["db_id"] == database and t["schema"] == schema
        ]

    def get(self, path):
        return copy.deepcopy(self.by_id[int(path.split("/")[1])])

    def put(self, path, body):
        kind, metabase_id = path.split("/")
        self.updated[(kind, int(metabase_id))] = body
        if kind == "table":
            self.by_id[int(metabase_id)].update(body)
        else:
            for table in self.by_id.values():
                for field in table["fields"]:
                    if field["id"] == int(metabase_id):
                        field.update(body)

    def post(self, path, body=None):
        self.synced.append(path)


def test_curated_tables_reads_the_curated_models_only():
    tables = curated_tables(MANIFEST)
    assert set(tables) == {"chaine", "publicite"}
    publicite = tables["publicite"]
    assert publicite["display_name"] == "Publicités"
    assert publicite["description"].startswith("Une ligne = un objet publicite.\n\n")
    assert publicite["description"].endswith(table_footer(MANIFEST["nodes"]["model.p.publicite"]))
    assert publicite["fields"]["chaine_id"] == {
        "display_name": "Chaîne", "description": "Description de chaine_id.", "semantic_type": "type/FK",
        "fk_target": "chaine.chaine_id",
    }
    # no semantic type in dbt: left to Metabase
    assert "semantic_type" not in publicite["fields"]["duree_sec"]


def test_field_description_adds_unit_and_aggregation():
    column = {"description": "Durée du spot. ", "meta": {"unite": "seconde", "agregation": "somme"}}
    assert field_description(column) == (
        "Durée du spot.\n\nUnité : seconde. Agrégation : somme (additive : se somme sur les lignes et les groupes)."
    )
    assert field_description({"description": "Nom.", "meta": {}}) == "Nom."


def test_plan_resolves_foreign_keys_and_skips_what_is_up_to_date():
    metabase = FakeMetabase([
        metabase_table(10, "chaine", {"chaine_id": 101}),
        metabase_table(11, "publicite", {"chaine_id": 111, "duree_sec": 112}),
    ])
    tables = curated_tables(MANIFEST)
    known = sync_curated.known_tables(metabase, 2, tables)
    updates = plan(tables, known)
    bodies = {(kind, metabase_id): body for kind, metabase_id, body, _ in updates}
    assert bodies[("field", 111)]["fk_target_field_id"] == 101
    assert bodies[("field", 111)]["semantic_type"] == "type/FK"
    assert bodies[("table", 11)]["display_name"] == "Publicités"
    assert set(bodies[("field", 112)]) == {"display_name", "description"}
    # once applied, nothing left to do
    assert sync(metabase, MANIFEST, [2], apply=True, wait=0) is True
    assert plan(tables, sync_curated.known_tables(metabase, 2, tables)) == []


def test_sync_preview_changes_nothing():
    metabase = FakeMetabase([
        metabase_table(10, "chaine", {"chaine_id": 101}),
        metabase_table(11, "publicite", {"chaine_id": 111, "duree_sec": 112}),
    ])
    assert sync(metabase, MANIFEST, [2], apply=False, wait=0) is True
    assert metabase.updated == {}


def test_sync_waits_for_new_tables_and_columns(monkeypatch):
    monkeypatch.setattr(sync_curated.time, "sleep", lambda seconds: None)
    # publicite exists, but without its new column duree_sec; chaine is not known yet
    metabase = FakeMetabase([metabase_table(11, "publicite", {"chaine_id": 111})])
    assert sync(metabase, MANIFEST, [2], apply=True, wait=0) is False
    assert metabase.synced == ["database/2/sync_schema"]
    # publicite has a foreign key to the missing table chaine: left for the next run
    assert metabase.updated == {}

    # after the sync of the schema, Metabase knows both tables
    metabase = FakeMetabase([metabase_table(11, "publicite", {"chaine_id": 111})])
    known_after_sync = [metabase_table(10, "chaine", {"chaine_id": 101}),
                        metabase_table(11, "publicite", {"chaine_id": 111, "duree_sec": 112})]
    metabase.post = lambda path, body=None: metabase.by_id.update({t["id"]: t for t in known_after_sync})
    assert sync(metabase, MANIFEST, [2], apply=True, wait=60) is True
    assert ("field", 112) in metabase.updated
