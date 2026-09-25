"""Load external reference data (Google Sheets shared by link, CSV files) into Postgres before dbt runs.

Sources are listed in my_dbt_project/external_sources.yml. Each table is fully replaced in a single
transaction, so a failed download or validation leaves the previous version in place.

Two kinds of sources:
- spreadsheet: a Google Sheet (any link to it). The whole workbook is exported as xlsx in one request
  and every tab is loaded in its own table, named <table_prefix><tab name as snake_case>. Per-tab
  settings (table name, required columns, unique key) go under `sheets`, keyed by tab name.
- csv: a single CSV file (URL or local path).
"""

import datetime
import hashlib
import io
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from sqlalchemy import text

from postgres.database_connection import connect_to_db
from quotaclimat.utils.sentry import sentry_init

DEFAULT_CONFIG_PATH = Path("my_dbt_project/external_sources.yml")
LOAD_HISTORY_TABLE = "ref_external_source_load"
SCHEMA = "public"
DOWNLOAD_TIMEOUT_SEC = 60
GOOGLE_SHEET_ID_PATTERN = re.compile(r"^/spreadsheets/d/([A-Za-z0-9_-]+)")


class ExternalSourceError(Exception):
    pass


def to_snake_case(name: str) -> str:
    """'Catégories Transversales' -> 'categories_transversales'"""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def spreadsheet_export_url(url: str) -> str:
    """Any Google Sheet link (/edit, /view, ?gid=...) -> xlsx export of the whole workbook.
    Other URLs and local paths are returned unchanged."""
    parsed = urlparse(url)
    match = GOOGLE_SHEET_ID_PATTERN.match(parsed.path)
    if parsed.scheme == "https" and parsed.hostname == "docs.google.com" and match:
        return f"https://docs.google.com/spreadsheets/d/{match.group(1)}/export?format=xlsx"
    return url


def resolve_url(source: dict) -> str | None:
    if source.get("url_env"):
        url = os.environ.get(source["url_env"], "").strip()
        if url:
            return url
    return source.get("url")


def download(url: str) -> bytes:
    if url.startswith(("http://", "https://")):
        response = requests.get(url, timeout=DOWNLOAD_TIMEOUT_SEC)
        response.raise_for_status()
        # A sheet that is not shared by link redirects to a Google login HTML page
        if "text/html" in response.headers.get("Content-Type", ""):
            raise ExternalSourceError("got an HTML page instead of a file, check the sheet sharing")
        return response.content
    return Path(url).read_bytes()


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.astype(object).where(df.notna(), None)
    df.columns = [str(c).strip() for c in df.columns]
    # columns without header and without values (spreadsheet leftovers)
    df = df[[c for c in df.columns if not (c.startswith("Unnamed:") and df[c].isna().all())]]
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


def read_tables(source: dict, content: bytes) -> dict[str, tuple[pd.DataFrame, dict]]:
    """Returns {table name: (raw dataframe, validation settings)}."""
    if source.get("type", "csv") == "spreadsheet":
        sheets_settings = source.get("sheets") or {}
        frames = pd.read_excel(io.BytesIO(content), sheet_name=None, dtype=str)
        unknown = set(sheets_settings) - set(frames)
        if unknown:
            logging.error(
                "External source %s: tabs %s configured but not found, got %s",
                source["name"], sorted(unknown), list(frames),
            )
        tables = {}
        for sheet_name, df in frames.items():
            settings = sheets_settings.get(sheet_name) or {}
            if settings.get("skip"):
                continue
            table = settings.get("table") or f"{source.get('table_prefix', 'ref_')}{to_snake_case(sheet_name)}"
            tables[table] = (df, {**settings, "label": f"{source['name']} / {sheet_name}"})
        return tables
    df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    return {source["table"]: (df, {**source, "label": source["name"]})}


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


def load_source(engine, source: dict) -> dict[str, bool]:
    """Returns {table name: loaded}. A table that fails validation is left unchanged."""
    name = source["name"]
    url = resolve_url(source)
    if not url:
        logging.warning("External source %s: no url (env %s not set), skipped", name, source.get("url_env"))
        return {}
    if source.get("type", "csv") == "spreadsheet":
        url = spreadsheet_export_url(url)
    try:
        content = download(url)
        tables = read_tables(source, content)
    except Exception as e:
        logging.error("External source %s: %s, tables left unchanged", name, e)
        return {}

    content_sha256 = hashlib.sha256(content).hexdigest()
    results = {}
    for table, (raw_df, settings) in tables.items():
        try:
            df = clean(raw_df)
            validate(df, settings)
        except Exception as e:
            logging.error("External source %s: %s, table %s left unchanged", settings["label"], e, table)
            results[table] = False
            continue
        with engine.begin() as conn:
            df.to_sql(table, conn, schema=SCHEMA, if_exists="replace", index=False)
            record_load(conn, name, table, len(df), content_sha256)
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
