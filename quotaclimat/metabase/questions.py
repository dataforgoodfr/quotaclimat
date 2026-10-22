"""Native SQL queries of Metabase questions versioned in the subfolders of metabase/ (advertising/,
mesinformation/...), and their manifests, one questions.yml per subfolder.

A manifest lists, for each query file of its folder, the Metabase questions running it, the type of each variable
({{name}}) and the test values. render() substitutes the variables the way Metabase does, to run the queries in
the tests.
"""
import datetime
import hashlib
import re
from decimal import Decimal
from pathlib import Path

import yaml

METABASE_DIR = Path(__file__).resolve().parents[2] / "metabase"

VARIABLE_TYPES = {"text", "number", "date", "boolean", "dimension"}
# {{name}}; snippets ({{snippet: x}}) and saved questions ({{#12}}) are not used in our queries
TAG = re.compile(r"\{\{\s*([A-Za-z_]\w*)\s*\}\}")
OPTIONAL_CLAUSE = re.compile(r"\[\[(.*?)\]\]", re.S)


def load_queries(root: Path = METABASE_DIR) -> dict:
    """Queries of all the manifests, by path relative to metabase/ (ex. advertising/x.sql), each with the
    Metabase database of its manifest."""
    queries = {}
    for manifest_path in sorted(root.rglob("questions.yml")):
        with open(manifest_path) as f:
            manifest = yaml.safe_load(f)
        folder = manifest_path.parent.relative_to(root)
        for name, query in manifest["queries"].items():
            file = (folder / name).as_posix()
            for tag, variable in query["variables"].items():
                if variable["type"] not in VARIABLE_TYPES:
                    raise ValueError(f"{file}: variable {tag} of unknown type {variable['type']}")
                if variable["type"] == "dimension" and len(variable["field"].split(".")) != 3:
                    raise ValueError(f"{file}: field filter {tag} needs field: schema.table.column")
            queries[file] = {**query, "database": manifest["database"]}
    return queries


def read_query(file: str) -> str:
    return (METABASE_DIR / file).read_text()


def tag_names(sql: str) -> list[str]:
    """Variables of the query, in order of first use."""
    return list(dict.fromkeys(TAG.findall(sql)))


def sql_version(sql: str) -> str:
    """Short hash of a query text, written in the description of the questions synchronized with it."""
    return hashlib.sha256(sql.strip().encode()).hexdigest()[:12]


def _is_set(value) -> bool:
    return value is not None and value != [] and value != ""


def render(sql: str, variables: dict, params: dict) -> tuple[str, list]:
    """Query and parameters for psycopg2, with the variables substituted the way Metabase does it:
    an optional clause [[ ... ]] is kept only when all its variables have a value; a text variable is sent as a
    varchar (hence the CASTs in the queries); a field filter becomes "schema"."table"."column" IN (...), which
    only works if the query reads the table under its full name, without alias, or 1 = 1 without value."""

    def keep_clause(match):
        names = TAG.findall(match.group(1))
        return match.group(1) if all(_is_set(params.get(n)) for n in names) else ""

    # % is the psycopg2 placeholder character
    sql = OPTIONAL_CLAUSE.sub(keep_clause, sql.replace("%", "%%"))
    values = []

    def substitute(match):
        name = match.group(1)
        variable = variables[name]
        value = params.get(name)
        if variable["type"] == "dimension":
            if not _is_set(value):
                return "1 = 1"
            column = ".".join(f'"{part}"' for part in variable["field"].split("."))
            value = value if isinstance(value, list) else [value]
            if variable.get("widget", "string/=") == "string/contains":
                values.extend(f"%{v.lower()}%" for v in value)
                return "(" + " OR ".join(f"LOWER({column}) LIKE %s" for _ in value) + ")"
            values.extend(value)
            return f"{column} IN (" + ", ".join("%s" for _ in value) + ")"
        if not _is_set(value):
            value = variable.get("default")
        if not _is_set(value):
            raise ValueError(f"variable {name} without value nor default, outside of an optional clause")
        if variable["type"] == "date":
            values.append(datetime.date.fromisoformat(str(value)))
            return "CAST(%s AS date)"
        if variable["type"] == "number":
            values.append(Decimal(str(value)))
            return "%s"
        if variable["type"] == "boolean":
            values.append(bool(value))
            return "%s"
        values.append(str(value))
        return "CAST(%s AS varchar)"

    return TAG.sub(substitute, sql), values
