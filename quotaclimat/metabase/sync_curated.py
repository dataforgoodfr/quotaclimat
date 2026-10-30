"""Pushes the documentation of the dbt models of the curated schema (my_dbt_project/models/curated/_curated.yml) to
the metadata of their Metabase tables, read by the teams and by their Claude through the Metabase MCP.

    python -m quotaclimat.metabase.sync_curated --database 2            # shows what would change, changes nothing
    python -m quotaclimat.metabase.sync_curated --database 2 --apply    # updates the tables and their fields

Environment: METABASE_URL and METABASE_API_KEY, as for sync_questions; the group of the API key must be allowed to
edit the table metadata of the database (Admin > Table Metadata). Reads the dbt manifest (target/manifest.json of
`dbt parse`, `dbt run` or `dbt test`).

dbt is the source: the table name shown (meta.libelle), its description, and for each column the name shown, the
description (with its unit and aggregation, meta.unite and meta.agregation), the semantic type (meta.type_metabase)
and the foreign key target (meta.cle_etrangere) overwrite what was set in Metabase. A semantic type not set in dbt
is left to Metabase. Tables or columns not yet known to Metabase (new model, new column) trigger a sync of the
database schema, then the script waits for them, at most --wait seconds.
"""
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

from quotaclimat.metabase.sync_questions import Metabase

DEFAULT_MANIFEST = Path(__file__).resolve().parents[2] / "my_dbt_project" / "target" / "manifest.json"
CURATED_PATH = "models/curated/"
AGGREGATIONS = {
    "somme": "somme (additive : se somme sur les lignes et les groupes)",
    "moyenne": "moyenne (ne pas sommer)",
    "aucune": "aucune (ne pas sommer ni moyenner sur plusieurs lignes)",
}


def table_footer(model: dict) -> str:
    return (
        f"Table du schéma curated, construite par dbt (my_dbt_project/{model['original_file_path']} du dépôt "
        "dataforgoodfr/quotaclimat). Sa documentation se modifie dans le dépôt : une modification faite ici est "
        "écrasée à la prochaine synchronisation."
    )


def field_description(column: dict) -> str:
    meta = column.get("meta") or {}
    details = []
    if meta.get("unite"):
        details.append(f"Unité : {meta['unite']}.")
    if meta.get("agregation"):
        details.append(f"Agrégation : {AGGREGATIONS[meta['agregation']]}.")
    return "\n\n".join(part for part in [column["description"].strip(), " ".join(details)] if part)


def curated_tables(manifest: dict) -> dict:
    """Metadata of the curated tables as dbt documents them, by table name: {"schema", "display_name",
    "description", "fields": {column: {"display_name", "description", "semantic_type", "fk_target"}}}, fk_target
    being "table.column" in the same schema; semantic_type is absent when dbt leaves it to Metabase."""
    tables = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or not node["original_file_path"].startswith(CURATED_PATH):
            continue
        fields = {}
        for name, column in node["columns"].items():
            meta = column.get("meta") or {}
            field = {"display_name": meta.get("libelle") or name, "description": field_description(column)}
            if meta.get("type_metabase"):
                field["semantic_type"] = meta["type_metabase"]
            if meta.get("cle_etrangere"):
                field["fk_target"] = meta["cle_etrangere"]
            fields[name] = field
        tables[node["alias"] or node["name"]] = {
            "schema": node["schema"],
            "display_name": (node.get("meta") or {}).get("libelle") or node["name"],
            "description": "\n\n".join([node["description"].strip(), table_footer(node)]),
            "fields": fields,
        }
    return tables


def changes(current: dict, wanted: dict, keys: list[str]) -> dict:
    return {key: wanted[key] for key in keys if key in wanted and current.get(key) != wanted[key]}


def plan(tables: dict, known: dict) -> list[tuple[str, int, dict, str]]:
    """Updates to bring Metabase in line with dbt: [("table" or "field", Metabase id, PUT body, label)].
    known: the Metabase tables, by name, each {"id", "display_name", "description", "fields": {name: field}}."""
    updates = []
    for name, table in tables.items():
        current = known[name]
        body = changes(current, table, ["display_name", "description"])
        if body:
            updates.append(("table", current["id"], body, f"{table['schema']}.{name}"))
        for column, field in table["fields"].items():
            current_field = current["fields"][column]
            wanted = dict(field)
            if "fk_target" in wanted:
                target_table, target_column = wanted.pop("fk_target").split(".")
                wanted["fk_target_field_id"] = known[target_table]["fields"][target_column]["id"]
            body = changes(current_field, wanted, ["display_name", "description", "semantic_type", "fk_target_field_id"])
            if body:
                updates.append(("field", current_field["id"], body, f"{table['schema']}.{name}.{column}"))
    return updates


def known_tables(metabase: Metabase, database: int, tables: dict) -> dict:
    """The curated tables known to Metabase in this database, by name, with their fields; a table missing one of
    the columns documented in dbt is left out (not synced yet)."""
    known = {}
    for schema in sorted({table["schema"] for table in tables.values()}):
        for table in metabase.tables(database, schema):
            if table["name"] not in tables:
                continue
            metadata = metabase.get(f"table/{table['id']}/query_metadata")
            fields = {field["name"]: field for field in metadata["fields"]}
            if set(tables[table["name"]]["fields"]) <= set(fields):
                known[table["name"]] = {**metadata, "fields": fields}
    return known


def wait_for_tables(metabase: Metabase, database: int, tables: dict, wait: int, apply: bool) -> dict:
    """known_tables, after a sync of the database schema when some tables or columns are not known yet."""
    known = known_tables(metabase, database, tables)
    missing = set(tables) - set(known)
    if not missing or not apply:
        return known
    logging.info(f"base {database} : {', '.join(sorted(missing))} pas encore dans Metabase, synchronisation du schéma")
    metabase.post(f"database/{database}/sync_schema")
    deadline = time.monotonic() + wait
    while missing and time.monotonic() < deadline:
        time.sleep(15)
        known = known_tables(metabase, database, tables)
        missing = set(tables) - set(known)
    return known


def sync(metabase: Metabase, manifest: dict, databases: list[int], apply: bool, wait: int) -> bool:
    """Synchronizes the documentation of the curated tables in each database; False if a table is still unknown
    to Metabase or an update failed."""
    tables = curated_tables(manifest)
    if not tables:
        logging.error("aucun modèle curated dans le manifeste dbt")
        return False
    ok = True
    for database in databases:
        try:
            known = wait_for_tables(metabase, database, tables, wait, apply)
        except requests.RequestException as error:
            logging.error(f"base {database} : {error}")
            ok = False
            continue
        missing = set(tables) - set(known)
        if missing:
            logging.error(f"base {database} : {', '.join(sorted(missing))} absente(s) de Metabase ou incomplète(s)")
            ok = False
        # foreign keys to a missing table cannot be resolved: those tables wait for the next run
        documented = {
            name: table for name, table in tables.items()
            if name in known and all(f["fk_target"].split(".")[0] in known for f in table["fields"].values() if "fk_target" in f)
        }
        updates = plan(documented, known)
        if not updates:
            logging.info(f"base {database} : documentation à jour ({len(documented)} tables)")
        for kind, metabase_id, body, label in updates:
            logging.info(f"base {database} : {label} : {', '.join(body)}")
            if apply:
                try:
                    metabase.put(f"{kind}/{metabase_id}", body)
                except requests.RequestException as error:
                    logging.error(f"base {database} : {label} : échec de la mise à jour : {error}")
                    ok = False
    return ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", type=int, action="append", required=True,
                        help="Metabase database id (repeat for several connections to the same Postgres database)")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="dbt manifest.json")
    parser.add_argument("--apply", action="store_true", help="update Metabase (default: only show changes)")
    parser.add_argument("--wait", type=int, default=900, help="seconds to wait for Metabase to sync new tables")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    url, api_key = os.environ.get("METABASE_URL"), os.environ.get("METABASE_API_KEY")
    if not url or not api_key:
        logging.error("METABASE_URL and METABASE_API_KEY are required")
        return 2
    if not args.apply:
        logging.info("aperçu : Metabase n'est pas modifié (--apply pour mettre à jour)")
    manifest = json.loads(args.manifest.read_text())
    return 0 if sync(Metabase(url, api_key), manifest, args.database, args.apply, args.wait) else 1


if __name__ == "__main__":
    sys.exit(main())
