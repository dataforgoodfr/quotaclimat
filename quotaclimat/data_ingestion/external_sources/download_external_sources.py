"""Download reference data from private Google Sheets as dbt seeds, before `dbt seed` loads them.

Sources are listed in my_dbt_project/external_sources.yml, by spreadsheet name. The spreadsheets are
looked up by name (quotaclimat/utils/google_sheets.py, shared with the jobs that write to Google Sheets) in a Google Drive folder (id in the env variable named by `folder_env`),
shared as viewer with a Google service account. They are read with the Google Drive and Sheets APIs
with read-only scopes: no public link is needed, and only displayed cell values are read (never
formulas or files). A spreadsheet readable without credentials (shared "anyone with the link" or
published on the web) is refused.

Every tab is written to my_dbt_project/seeds/ref/<table_prefix><tab name in snake_case>.csv, then
`dbt seed --select path:seeds/ref` loads it into the table of the same name (schema advertising), and the seed tests
(my_dbt_project/seeds/ref/_ref_seeds.yml) check it. When a spreadsheet cannot be downloaded, no CSV
is written for it and its tables keep their previous version.
"""

import csv
import logging
import os
import re
import unicodedata
from pathlib import Path

import yaml
from google.auth.transport.requests import AuthorizedSession

# re-exported: the advertising jobs and the tests import them from here
from quotaclimat.utils.google_sheets import (  # noqa: F401
    DRIVE_FILES_API_URL,
    SHEETS_API_URL,
    GoogleSheetsError,
    drive_query_literal,
    ensure_not_public,
    find_spreadsheet,
    parse_folder_id,
    quote_tab,
    service_account_session,
)
from quotaclimat.utils.sentry import sentry_init

DEFAULT_CONFIG_PATH = Path("my_dbt_project/external_sources.yml")
SEEDS_DIR = Path("my_dbt_project/seeds/ref")
TABLE_PREFIX_REQUIRED = "ref_"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet by name in the folder
    "https://www.googleapis.com/auth/spreadsheets.readonly",  # read its cell values
]
CREDENTIALS_ENV = "GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON"
DOWNLOAD_TIMEOUT_SEC = 60
MAX_CELLS_PER_SHEET = 1_000_000
# tabs whose name starts with this prefix are for humans (documentation, metadata): never downloaded
HUMAN_TAB_PREFIX = "_"

# a single error class, so that the errors of the shared helpers are caught like the ones raised here
ExternalSourceError = GoogleSheetsError


def to_snake_case(name: str) -> str:
    """'Catégories Transversales' -> 'categories_transversales'"""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def is_human_tab(sheet_name: str) -> bool:
    """'_lisez-moi' -> True: tab for humans only, not downloaded."""
    return sheet_name.startswith(HUMAN_TAB_PREFIX)


def get_session() -> AuthorizedSession:
    return service_account_session(CREDENTIALS_ENV, GOOGLE_SCOPES)


def fetch_google_sheet(folder_id: str, spreadsheet_name: str) -> dict[str, list[list[str]]]:
    """Returns {tab name: rows of displayed values} for every grid tab of the spreadsheet."""
    session = get_session()
    spreadsheet = find_spreadsheet(session, folder_id, spreadsheet_name)
    ensure_not_public(spreadsheet)
    spreadsheet_id = spreadsheet["id"]
    response = session.get(
        f"{SHEETS_API_URL}/{spreadsheet_id}",
        params={"fields": "sheets.properties(title,sheetType)"},
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    response.raise_for_status()
    titles = [
        s["properties"]["title"]
        for s in response.json().get("sheets", [])
        if s["properties"].get("sheetType", "GRID") == "GRID" and not is_human_tab(s["properties"]["title"])
    ]
    if not titles:
        return {}
    ranges = [quote_tab(title) for title in titles]
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


def rows_to_csv_rows(rows: list[list[str]]) -> list[list[str]]:
    """First row is the header. Pads rows (the API drops trailing empty cells), strips values,
    drops empty rows and columns without header nor values."""
    n_cells = sum(len(r) for r in rows)
    if n_cells > MAX_CELLS_PER_SHEET:
        raise ExternalSourceError(f"{n_cells} cells, more than the {MAX_CELLS_PER_SHEET} limit")
    if not rows or not any(str(c).strip() for c in rows[0]):
        raise ExternalSourceError("no header row")
    width = max(len(r) for r in rows)
    padded = [[str(c).strip() for c in r] + [""] * (width - len(r)) for r in rows]
    header, body = padded[0], [r for r in padded[1:] if any(r)]
    kept = [i for i, name in enumerate(header) if name or any(r[i] for r in body)]
    if any(not header[i] for i in kept):
        raise ExternalSourceError("a column has values but no header")
    names = [header[i] for i in kept]
    if len(set(names)) != len(names):
        raise ExternalSourceError(f"duplicated column names {names}")
    if not body:
        raise ExternalSourceError("no rows")
    return [names] + [[r[i] for i in kept] for r in body]


def select_columns(csv_rows: list[list[str]], columns: dict[str, str]) -> list[list[str]]:
    """Keeps only the configured columns of a tab, renamed: {header in the sheet: column name}, in the
    order of the config. The other columns (long texts, computed columns...) are left out. A configured
    header missing from the tab is an error: the table is left unchanged rather than loaded without it."""
    header = csv_rows[0]
    missing = [name for name in columns if name not in header]
    if missing:
        raise ExternalSourceError(f"configured columns {missing} not found, got {header}")
    names = list(columns.values())
    if len(set(names)) != len(names):
        raise ExternalSourceError(f"duplicated column names {names}")
    indexes = [header.index(name) for name in columns]
    body = [kept for kept in ([row[i] for i in indexes] for row in csv_rows[1:]) if any(kept)]
    if not body:
        raise ExternalSourceError("no rows in the configured columns")
    return [names] + body


def target_tables(source: dict, sheets: dict) -> dict[str, str]:
    """Returns {tab name: table name} for the tabs to download."""
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
        if settings.get("skip") or is_human_tab(sheet_name):
            continue
        table = settings.get("table") or f"{source['table_prefix']}{to_snake_case(sheet_name)}"
        # only ever create or replace reference tables, whatever the tab names or the config say
        if not re.fullmatch(rf"{TABLE_PREFIX_REQUIRED}[a-z0-9_]+", table) or len(table) > 63:
            logging.error("External source %s / %s: invalid table name %s, skipped", source["name"], sheet_name, table)
            continue
        if table in targets.values():
            logging.error("External source %s / %s: table %s already used by another tab, skipped", source["name"], sheet_name, table)
            continue
        targets[sheet_name] = table
    return targets


def download_source(source: dict, seeds_dir: Path = SEEDS_DIR, fetch=fetch_google_sheet) -> dict[str, bool]:
    """Writes one seed CSV per tab. Returns {table name: written}."""
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

    seeds_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    for sheet_name, table in target_tables(source, sheets).items():
        try:
            csv_rows = rows_to_csv_rows(sheets[sheet_name])
            columns = ((source.get("sheets") or {}).get(sheet_name) or {}).get("columns")
            if columns:
                csv_rows = select_columns(csv_rows, columns)
        except ExternalSourceError as e:
            logging.error("External source %s / %s: %s, table %s left unchanged", name, sheet_name, e, table)
            results[table] = False
            continue
        with open(seeds_dir / f"{table}.csv", "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(csv_rows)
        logging.info("External source %s / %s: %s rows written for %s", name, sheet_name, len(csv_rows) - 1, table)
        results[table] = True
    return results


def download_external_sources(config_path: Path = DEFAULT_CONFIG_PATH, seeds_dir: Path = SEEDS_DIR) -> dict[str, bool]:
    config = yaml.safe_load(Path(config_path).read_text())
    # seeds of a previous run in the same container must not be loaded again
    for old_csv in seeds_dir.glob("ref_*.csv"):
        old_csv.unlink()
    results = {}
    for source in config.get("sources", []):
        results.update(download_source(source, seeds_dir))
    return results


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
    sentry_init()
    results = download_external_sources()
    logging.info("External sources downloaded: %s", results)
