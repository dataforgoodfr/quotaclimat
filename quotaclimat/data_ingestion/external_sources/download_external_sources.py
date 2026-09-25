"""Download reference data from private Google Sheets as dbt seeds, before `dbt seed` loads them.

Sources are listed in my_dbt_project/external_sources.yml, by spreadsheet name. The spreadsheets are
looked up by name in a Google Drive folder (id or link in the env variable named by `folder_env`),
shared as viewer with a Google service account. They are read with the Google Drive and Sheets APIs
with read-only scopes: no public link is needed, and only displayed cell values are read (never
formulas or files). A spreadsheet readable without credentials (shared "anyone with the link" or
published on the web) is refused.

Every tab is written to my_dbt_project/seeds/ref/<table_prefix><tab name in snake_case>.csv, then
`dbt seed --select path:seeds/ref` loads it into the table of the same name, and the seed tests
(my_dbt_project/seeds/ref/_ref_seeds.yml) check it. When a spreadsheet cannot be downloaded, no CSV
is written for it and its tables keep their previous version.
"""

import csv
import json
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from quotaclimat.utils.sentry import sentry_init

DEFAULT_CONFIG_PATH = Path("my_dbt_project/external_sources.yml")
SEEDS_DIR = Path("my_dbt_project/seeds/ref")
TABLE_PREFIX_REQUIRED = "ref_"
SHEETS_API_URL = "https://sheets.googleapis.com/v4/spreadsheets"
DRIVE_FILES_API_URL = "https://www.googleapis.com/drive/v3/files"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",  # find the spreadsheet by name in the folder
    "https://www.googleapis.com/auth/spreadsheets.readonly",  # read its cell values
]
SPREADSHEET_MIME_TYPE = "application/vnd.google-apps.spreadsheet"
# Drive permission ids of the "anyone" permissions (public on the web / anyone with the link)
PUBLIC_PERMISSION_IDS = {"anyone", "anyoneWithLink"}
ANONYMOUS_EXPORT_URL = "https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
GOOGLE_LOGIN_HOSTS = {"accounts.google.com"}
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


def find_spreadsheet(session: AuthorizedSession, folder_id: str, spreadsheet_name: str) -> dict:
    """Drive file (id, name, permissionIds) of the spreadsheet named exactly spreadsheet_name in the
    folder (not in sub-folders)."""
    response = session.get(
        DRIVE_FILES_API_URL,
        params={
            "q": (
                f"{drive_query_literal(folder_id)} in parents"
                f" and name = {drive_query_literal(spreadsheet_name)}"
                f" and mimeType = '{SPREADSHEET_MIME_TYPE}' and trashed = false"
            ),
            "fields": "files(id,name,permissionIds)",
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
    return files[0]


def ensure_not_public(spreadsheet: dict, anonymous_get=None) -> None:
    """Refuses a spreadsheet readable by anyone, checked in two independent ways. Fails closed: when
    the anonymous check cannot conclude, the spreadsheet is refused too."""
    public_permissions = PUBLIC_PERMISSION_IDS & set(spreadsheet.get("permissionIds") or [])
    if public_permissions:
        raise ExternalSourceError(f"spreadsheet is shared publicly ({sorted(public_permissions)}), refused")

    # try to read it without any credentials, like anyone who got the link
    response = (anonymous_get or requests.get)(
        ANONYMOUS_EXPORT_URL.format(spreadsheet_id=spreadsheet["id"]),
        allow_redirects=False,
        stream=True,  # never download the content
        timeout=DOWNLOAD_TIMEOUT_SEC,
    )
    try:
        status = response.status_code
        location_host = urlparse(response.headers.get("Location", "")).hostname
        content_type = response.headers.get("Content-Type", "")
    finally:
        response.close()
    if status in (401, 403, 404) or (300 <= status < 400 and location_host in GOOGLE_LOGIN_HOSTS):
        return  # login required: private
    if (status == 200 and "text/html" not in content_type) or 300 <= status < 400:
        raise ExternalSourceError("spreadsheet is readable without credentials, refused")
    raise ExternalSourceError(f"could not check that the spreadsheet is private (HTTP {status}), refused")


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
        if settings.get("skip"):
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
