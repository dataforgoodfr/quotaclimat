"""Load reference data from private Google Sheets into Postgres before dbt runs.

Sources are listed in my_dbt_project/external_sources.yml, by spreadsheet name. The spreadsheets are
looked up by name in a Google Drive folder (id or link in the env variable named by `folder_env`),
shared as viewer with a Google service account. They are read with the Google Drive and Sheets APIs
with read-only scopes: no public link is needed, and only displayed cell values are read (never
formulas or files).

Every tab of a sheet is loaded into its own table, <table_prefix><tab name in snake_case>, replaced in
its own transaction, so a failed download or validation leaves the previous version in place.
"""

import datetime
import hashlib
import json
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import yaml
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account
from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.utils.sentry import sentry_init

DEFAULT_CONFIG_PATH = Path("my_dbt_project/external_sources.yml")
LOAD_HISTORY_TABLE = "ref_external_source_load"
SCHEMA = "public"
TABLE_PREFIX_REQUIRED = "ref_"
SHEETS_API_URL = "https://sheets.googleapis.com/v4/spreadsheets"
DRIVE_FILES_API_URL = "https://www.googleapis.com/drive/v3/files"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet by name in the folder
    "https://www.googleapis.com/auth/spreadsheets.readonly",  # read its cell values
]
SPREADSHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet"
CREDENTIALS_ENV = "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON"
DOWNLOAD_TIMEOUT_SEC = 60
MAX_CELLS_PER_SHEET = 1_000_000
DRIVE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{10,}$")
DRIVE_FOLDER_URL_PATTERN = re.compile(
    r"^https://drive\.google\.com/drive/(?:u/\d+/)?folders/([A-Za-z0-9_-]{10,})(?:[/?#].*)?$"
)


class ExternalSourceError(Exception):
    pass


def to_snake_case(name: str) -> str:
    """'Catégories Transversales' -> 'categories_transversales'"""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def parse_folder_id(value: str) -> str:
    """Accepts a Drive folder id or a https://drive.google.com/drive/folders/<id> link."""
    value = value.strip()
    if DRIVE_ID_PATTERN.match(value):
        return value
    match = DRIVE_FOLDER_URL_PATTERN.match(value)
    if match:
        return match.group(1)
    raise ExternalSourceError("not a Google Drive folder id or https://drive.google.com/drive/folders/<id> link")


def drive_query_literal(value: str) -> str:
    """Quotes a value for a Drive API search query."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def get_session() -> AuthorizedSession:
    credentials_json = os.environ.get(CREDENTIALS_ENV, "").strip()
    if not credentials_json:
        raise ExternalSourceError(f"no Google service account credentials (env {CREDENTIALS_ENV} not set)")
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(credentials_json), scopes=GOOGLE_SCOPES
    )
    return AuthorizedSession(credentials)


def find_spreadsheet_id(session: AuthorizedSession, folder_id: str, spreadsheet_name: str) -> str:
    """Id of the spreadsheet named exactly spreadsheet_name in the folder (not in sub-folders)."""
    response = session.get(
        DRIVE_FILES_API_URL,
        params={
            "q": (
                f"{drive_query_literal(folder_id)} in parents"
                f" and name = {drive_query_literal(spreadsheet_name)}"
                f" and mimeType = '{SPREADSHEET_MIME_TYPE}' and trashed = false"
            ),
            "fields": "files(id,name)",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        },
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    response.raise_for_status()
    files = response.json().get("files", [])
    if len(files) != 1:
        raise ExternalSourceError(
            f"{len(files)} spreadsheets named {spreadsheet_name!r} in the folder, expected exactly 1"
        )
    return files[0]["id"]


def fetch_google_sheet(folder_id: str, spreadsheet_name: str) -> dict[str, list[list[str]]]:
    """Returns {tab name: rows of displayed values} for every grid tab of the spreadsheet."""
    session = get_session()
    spreadsheet_id = find_spreadsheet_id(session, folder_id, spreadsheet_name)
    response = session.get(
        f"{SHEETS_API_URL}/{spreadsheet_id}",
        params={"fields": "sheets.properties(title,sheetType)"},
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    response.raise_for_status()
    titles = [
        s["properties"]["title"]
        for s in response.json().get("sheets", [])
        if s["properties"].get("sheetType", "GRID") == "GRID"
    ]
    if not titles:
        return {}
    # quote tab names in A1 notation: 'Tab name', with ' doubled
    ranges = ["'" + title.replace("'", "''") + "'" for title in titles]
    response = session.get(
        f"{SHEETS_API_URL}/{spreadsheet_id}/values:batchGet",
        params={
            "ranges": ranges,
            "valueRenderOption": "FORMATTED_VALUE",
            "majorDimension": "ROWS",
        },
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    response.raise_for_status()
    value_ranges = response.json().get("valueRanges", [])
    return {title: vr.get("values", []) for title, vr in zip(titles, value_ranges)}


def rows_to_dataframe(rows: list[list[str]]) -> pd.DataFrame:
    """First row is the header; the API drops trailing empty cells, so rows are padded."""
    if not rows:
        return pd.DataFrame()
    n_cells = sum(len(r) for r in rows)
    if n_cells > MAX_CELLS_PER_SHEET:
        raise ExternalSourceError(f"{n_cells} cells, more than the {MAX_CELLS_PER_SHEET} limit")
    width = max(len(r) for r in rows)
    header = [str(c).strip() if c is not None else "" for c in rows[0]] + [""] * (width - len(rows[0]))
    header = [c or f"unnamed_{i}" for i, c in enumerate(header)]
    body = [list(r) + [None] * (width - len(r)) for r in rows[1:]]
    return pd.DataFrame(body, columns=header, dtype=object)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype(object).where(df.notna(), None)
    df.columns = [str(c).strip() for c in df.columns]
    if len(set(df.columns)) != len(df.columns):
        raise ExternalSourceError(f"duplicated column names {list(df.columns)}")
    # columns without header and without values (spreadsheet leftovers)
    df = df[[c for c in df.columns if not (c.startswith("unnamed_") and df[c].isna().all())]]
    df = df.apply(lambda col: col.map(lambda v: str(v).strip() if v is not None else None))
    df = df.replace("", None)
    return df.dropna(how="all")


def validate(df: pd.DataFrame, settings: dict) -> None:
    missing = [c for c in settings.get("columns", []) if c not in df.columns]
    if missing:
        raise ExternalSourceError(f"missing columns {missing}, got {list(df.columns)}")
    if df.empty:
        raise ExternalSourceError("no rows")
    unique = settings.get("unique") or []
    if unique:
        if df[unique].isna().any().any():
            raise ExternalSourceError(f"empty values in unique columns {unique}")
        duplicated = df[df.duplicated(subset=unique, keep=False)]
        if not duplicated.empty:
            raise ExternalSourceError(
                f"duplicated values for {unique}: {duplicated[unique].drop_duplicates().values.tolist()}"
            )


def target_tables(source: dict, sheets: dict) -> dict[str, tuple[str, dict]]:
    """Returns {tab name: (table name, validation settings)} for the tabs to load."""
    sheets_settings = source.get("sheets") or {}
    unknown = set(sheets_settings) - set(sheets)
    if unknown:
        logging.error(
            "External source %s: tabs %s configured but not found, got %s",
            source["name"], sorted(unknown), list(sheets),
        )
    targets = {}
    for sheet_name in sheets:
        settings = sheets_settings.get(sheet_name) or {}
        if settings.get("skip"):
            continue
        table = settings.get("table") or f"{source['table_prefix']}{to_snake_case(sheet_name)}"
        # only ever create or replace reference tables, whatever the tab names or the config say
        if not re.fullmatch(rf"{TABLE_PREFIX_REQUIRED}[a-z0-9_]+", table) or len(table) > 63:
            logging.error("External source %s / %s: invalid table name %s, skipped", source["name"], sheet_name, table)
            continue
        targets[sheet_name] = (table, {**settings, "label": f"{source['name']} / {sheet_name}"})
    return targets


def record_load(conn, name: str, table: str, row_count: int, content_sha256: str) -> None:
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{LOAD_HISTORY_TABLE} (
            name text, table_name text, loaded_at timestamp, row_count integer, content_sha256 text
        )
    """))
    conn.execute(
        text(f"""
            INSERT INTO {SCHEMA}.{LOAD_HISTORY_TABLE}
            VALUES (:name, :table_name, :loaded_at, :row_count, :content_sha256)
        """),
        {
            "name": name,
            "table_name": table,
            "loaded_at": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
            "row_count": row_count,
            "content_sha256": content_sha256,
        },
    )


def load_source(engine, source: dict, fetch=fetch_google_sheet) -> dict[str, bool]:
    """Returns {table name: loaded}. A table that fails validation is left unchanged."""
    name = source["name"]
    folder = os.environ.get(source.get("folder_env", ""), "").strip()
    if not folder:
        logging.warning("External source %s: env %s not set, skipped", name, source.get("folder_env"))
        return {}
    try:
        sheets = fetch(parse_folder_id(folder), source["spreadsheet"])
    except Exception as e:
        # the error message never contains the credentials, only the HTTP status / reason
        logging.error("External source %s: %s, tables left unchanged", name, e)
        return {}

    results = {}
    for sheet_name, (table, settings) in target_tables(source, sheets).items():
        rows = sheets[sheet_name]
        try:
            df = clean(rows_to_dataframe(rows))
            validate(df, settings)
            content_sha256 = hashlib.sha256(json.dumps(rows, ensure_ascii=False).encode()).hexdigest()
            with engine.begin() as conn:
                df.to_sql(table, conn, schema=SCHEMA, if_exists="replace", index=False)
                record_load(conn, name, table, len(df), content_sha256)
        except Exception as e:
            logging.error("External source %s: %s, table %s left unchanged", settings["label"], e, table)
            results[table] = False
            continue
        logging.info("External source %s: %s rows loaded into %s.%s", settings["label"], len(df), SCHEMA, table)
        results[table] = True
    return results


def load_external_sources(config_path: Path = DEFAULT_CONFIG_PATH, engine=None) -> dict[str, bool]:
    config = yaml.safe_load(Path(config_path).read_text())
    engine = engine or connect_to_db()
    results = {}
    for source in config.get("sources", []):
        results.update(load_source(engine, source))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
    sentry_init()
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CONFIG_PATH
    results = load_external_sources(config_path)
    logging.info("External sources loaded: %s", results)
