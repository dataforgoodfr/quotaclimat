"""Synchronizes the Metabase questions listed in the manifests metabase/*/questions.yml with their query files.

    python -m quotaclimat.metabase.sync_questions            # shows what would change, changes nothing
    python -m quotaclimat.metabase.sync_questions --apply    # updates the questions
    python -m quotaclimat.metabase.sync_questions --apply --force  # also overwrites questions edited in Metabase

Environment: METABASE_URL (ex. https://metabase.example.org) and METABASE_API_KEY (Metabase API key, Admin >
Authentication > API keys, in a group allowed to edit the questions' collections).

For each question: the SQL text becomes the file's, the variables still in the query keep their settings
(type, widget, default, list of values), the new ones are created from the manifest, the ones no longer in the
query are removed; the description gets a line naming the file and the version (hash) of the synchronized SQL.
A question whose SQL was changed in Metabase since its last synchronization (its SQL no longer matches the
version in its description, or no version yet) is left as it is and reported, unless --force.
"""
import argparse
import difflib
import logging
import os
import re
import sys
import uuid

import requests

from quotaclimat.metabase.questions import load_queries, read_query, sql_version, tag_names

MARKER = re.compile(r"^Requête versionnée : metabase/\S+ \(version (\w+)\).*$", re.M)
PARAMETER_TYPES = {"text": "string/=", "number": "number/=", "date": "date/single", "boolean": "boolean/="}


def marker(file: str, sql: str) -> str:
    return (
        f"Requête versionnée : metabase/{file} (version {sql_version(sql)}) dans le dépôt "
        "dataforgoodfr/quotaclimat. La modifier dans le dépôt : une modification faite ici bloque sa synchronisation."
    )


def native_query(dataset_query: dict) -> tuple[str, dict]:
    """SQL text and template tags by name, from a legacy (native.query) or an MBQL 5 (stages) dataset_query."""
    if "stages" in dataset_query:
        stage = dataset_query["stages"][0]
        tags = stage.get("template-tags") or {}
        if isinstance(tags, list):
            tags = {t["name"]: t for t in tags}
        return stage["native"], tags
    native = dataset_query["native"]
    return native["query"], native.get("template-tags") or {}


def with_native_query(dataset_query: dict, sql: str, tags: dict) -> dict:
    """Copy of dataset_query with this SQL text and these template tags, in the same shape."""
    if "stages" in dataset_query:
        stage = dataset_query["stages"][0]
        stored_as_list = isinstance(stage.get("template-tags"), list)
        new_stage = {**stage, "native": sql, "template-tags": list(tags.values()) if stored_as_list else tags}
        return {**dataset_query, "stages": [new_stage, *dataset_query["stages"][1:]]}
    return {**dataset_query, "native": {**dataset_query["native"], "query": sql, "template-tags": tags}}


def _default(variable: dict):
    # Metabase stores the default of a date as a string, of the other types as a list of values
    return variable["default"] if variable["type"] == "date" else [variable["default"]]


def new_tag(name: str, variable: dict, field_id: int | None, mbql5: bool) -> dict:
    tag = {
        "id": str(uuid.uuid4()),
        "name": name,
        "display-name": variable.get("display_name", name.replace("_", " ").title()),
        "type": variable["type"],
    }
    if variable["type"] == "dimension":
        tag["dimension"] = field_reference(field_id, mbql5)
        tag["widget-type"] = variable.get("widget", "string/=")
        if tag["widget-type"] == "string/contains":
            tag["options"] = {"case-sensitive": False}
    if "default" in variable:
        tag["default"] = _default(variable)
    if variable.get("required"):
        tag["required"] = True
    return tag


def field_reference(field_id: int, mbql5: bool) -> list:
    return ["field", {"lib/uuid": str(uuid.uuid4())}, field_id] if mbql5 else ["field", field_id, None]


def reference_field_id(reference: list) -> int | None:
    ids = [x for x in reference[1:] if isinstance(x, int)]
    return ids[0] if ids else None


def new_parameter(tag: dict, variable: dict) -> dict:
    dimension = tag["type"] == "dimension"
    parameter = {
        "id": tag["id"],
        "type": tag["widget-type"] if dimension else PARAMETER_TYPES[tag["type"]],
        "target": ["dimension" if dimension else "variable", ["template-tag", tag["name"]]],
        "name": tag["display-name"],
        "slug": tag["name"],
        "isMultiSelect": dimension,
    }
    if "options" in tag:
        parameter["options"] = tag["options"]
    if "values" in variable:
        parameter["values_query_type"] = "list"
        parameter["values_source_type"] = "static-list"
        parameter["values_source_config"] = {"values": [[str(v)] for v in variable["values"]]}
    for key in ("default", "required"):
        if key in tag:
            parameter[key] = tag[key]
    return parameter


def plan(card: dict, file: str, sql: str, variables: dict, field_ids: dict) -> dict:
    """Changes to bring the card in line with the query file: {"status": "up to date" | "update" | "edited in
    Metabase", "payload": fields for PUT /api/card/:id, "summary": list of changes, "diff": SQL diff}."""
    dataset_query = card["dataset_query"]
    card_sql, card_tags = native_query(dataset_query)
    mbql5 = "stages" in dataset_query
    tags, summary = {}, []
    for name in tag_names(sql):
        variable = variables[name]
        tag = card_tags.get(name)
        if tag is not None and (tag["type"] == "dimension") != (variable["type"] == "dimension"):
            summary.append(f"variable {name} recréée en {variable['type']}")
            tag = None
        if tag is None:
            if name not in card_tags:
                summary.append(f"variable {name} ajoutée")
            tag = new_tag(name, variable, field_ids.get(name), mbql5)
        elif variable["type"] == "dimension" and reference_field_id(tag["dimension"]) != field_ids[name]:
            summary.append(f"filtre {name} déplacé sur {variable['field']}")
            tag = {**tag, "dimension": field_reference(field_ids[name], mbql5)}
        tags[name] = tag
    summary += [f"variable {name} retirée" for name in card_tags if name not in tags]

    parameters = [p for p in card.get("parameters") or [] if p["id"] in {t["id"] for t in tags.values()}]
    parameters += [
        new_parameter(tag, variables[name])
        for name, tag in tags.items()
        if tag["id"] not in {p["id"] for p in parameters}
    ]

    description = card.get("description") or ""
    found = MARKER.search(description)
    new_description = (
        MARKER.sub(marker(file, sql), description)
        if found
        else (description.rstrip() + "\n\n" + marker(file, sql)).strip()
    )
    if card_sql.strip() != sql.strip():
        summary.insert(0, "SQL")
    if new_description != description:
        summary.append("description")
    if not summary and parameters == (card.get("parameters") or []):
        return {"status": "up to date", "summary": [], "payload": {}, "diff": ""}

    diff = "".join(difflib.unified_diff(
        card_sql.strip().splitlines(True), sql.strip().splitlines(True), "Metabase", f"metabase/{file}", n=1
    ))
    edited = card_sql.strip() != sql.strip() and (found is None or found.group(1) != sql_version(card_sql))
    return {
        "status": "edited in Metabase" if edited else "update",
        "summary": summary or ["paramètres"],
        "payload": {
            "dataset_query": with_native_query(dataset_query, sql, tags),
            "parameters": parameters,
            "description": new_description,
        },
        "diff": diff,
    }


class Metabase:
    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["x-api-key"] = api_key
        self._fields = {}

    def get(self, path: str):
        response = self.session.get(f"{self.url}/api/{path}", timeout=60)
        response.raise_for_status()
        return response.json()

    def put(self, path: str, body: dict):
        response = self.session.put(f"{self.url}/api/{path}", json=body, timeout=60)
        response.raise_for_status()
        return response.json()

    def post(self, path: str, body: dict | None = None):
        response = self.session.post(f"{self.url}/api/{path}", json=body, timeout=60)
        response.raise_for_status()
        return response.json()

    def field_id(self, database: int, field: str) -> int:
        """Id of a column schema.table.column, as synced by Metabase."""
        schema, table, column = field.split(".")
        if (database, schema, table) not in self._fields:
            tables = [
                t for t in self.get("table")
                if t["db_id"] == database and t["schema"] == schema and t["name"] == table
            ]
            if len(tables) != 1:
                raise ValueError(f"table {schema}.{table} not found in Metabase database {database}")
            metadata = self.get(f"table/{tables[0]['id']}/query_metadata")
            self._fields[(database, schema, table)] = {f["name"]: f["id"] for f in metadata["fields"]}
        return self._fields[(database, schema, table)][column]


def sync(metabase: Metabase, queries: dict, apply: bool, force: bool) -> bool:
    """Synchronizes all the questions of the manifests; False if a question was left out or failed."""
    ok = True
    for file, query in queries.items():
        sql = read_query(file)
        variables = query["variables"]
        field_ids = {
            name: metabase.field_id(query["database"], v["field"])
            for name, v in variables.items()
            if v["type"] == "dimension" and query["questions"]
        }
        for card_id in query["questions"]:
            try:
                card = metabase.get(f"card/{card_id}")
                if card["dataset_query"].get("database") != query["database"]:
                    raise ValueError(f"question on database {card['dataset_query'].get('database')}")
                change = plan(card, file, sql, variables, field_ids)
            except (requests.RequestException, ValueError, KeyError) as error:
                logging.error(f"question {card_id} ({file}): {error}")
                ok = False
                continue
            label = f"question {card_id} « {card.get('name')} » ({file})"
            if change["status"] == "up to date":
                logging.info(f"{label} : à jour")
                continue
            if change["status"] == "edited in Metabase":
                logging.warning(
                    f"{label} : SQL modifié dans Metabase depuis sa dernière synchronisation (ou jamais synchronisée), "
                    f"écart avec le fichier :\n{change['diff']}"
                )
                if not force:
                    logging.warning(f"{label} : laissée telle quelle, reporter la modification dans le fichier ou --force")
                    ok = False
                    continue
            logging.info(f"{label} : {', '.join(change['summary'])}")
            if apply:
                try:
                    metabase.put(f"card/{card_id}", change["payload"])
                    logging.info(f"{label} : mise à jour")
                except requests.RequestException as error:
                    logging.error(f"{label} : échec de la mise à jour : {error}")
                    ok = False
    return ok


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="update the questions (default: only show changes)")
    parser.add_argument("--force", action="store_true", help="also overwrite the questions edited in Metabase")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    url, api_key = os.environ.get("METABASE_URL"), os.environ.get("METABASE_API_KEY")
    if not url or not api_key:
        logging.error("METABASE_URL and METABASE_API_KEY are required")
        return 2
    if not args.apply:
        logging.info("aperçu : aucune question modifiée (--apply pour mettre à jour)")
    return 0 if sync(Metabase(url, api_key), load_queries(), args.apply, args.force) else 1


if __name__ == "__main__":
    sys.exit(main())
