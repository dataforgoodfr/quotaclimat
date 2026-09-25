"""Load external CSV sources (e.g. Google Sheets shared by link) into Postgres before dbt runs.

Sources are listed in my_dbt_project/external_sources.yml. Each table is fully replaced in a single
transaction, so a failed download or validation leaves the previous version in place.
"""

import datetime
import hashlib
import io
import logging
import os
import sys
from pathlib import Path

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


class ExternalSourceError(Exception):
    pass


def resolve_url(source: dict) -> str | None:
    if source.get("url_env"):
        url = os.environ.get(source["url_env"], "").strip()
        if url:
            return url
    return source.get("url")


def download_csv(url: str) -> bytes:
    if url.startswith(("http://", "https://")):
        response = requests.get(url, timeout=DOWNLOAD_TIMEOUT_SEC)
        response.raise_for_status()
        # A sheet that is not shared by link redirects to a Google login HTML page
        if "text/html" in response.headers.get("Content-Type", ""):
            raise ExternalSourceError("got an HTML page instead of a CSV, check the sheet sharing")
        return response.content
    return Path(url).read_bytes()


def parse_and_validate(content: bytes, source: dict) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    df = df.apply(lambda col: col.str.strip()).replace("", None)
    df = df.dropna(how="all")

    missing = [c for c in source.get("columns", []) if c not in df.columns]
    if missing:
        raise ExternalSourceError(f"missing columns {missing}, got {list(df.columns)}")
    if df.empty:
        raise ExternalSourceError("no rows")

    unique = source.get("unique") or []
    if unique:
        if df[unique].isna().any().any():
            raise ExternalSourceError(f"empty values in unique columns {unique}")
        duplicated = df[df.duplicated(subset=unique, keep=False)]
        if not duplicated.empty:
            raise ExternalSourceError(
                f"duplicated values for {unique}: {duplicated[unique].drop_duplicates().values.tolist()}"
            )
    return df


def load_source(engine, source: dict) -> bool:
    name = source["name"]
    url = resolve_url(source)
    if not url:
        logging.warning(
            "External source %s: no URL (env %s not set), table %s left unchanged",
            name, source.get("url_env"), source["table"],
        )
        return False
    try:
        content = download_csv(url)
        df = parse_and_validate(content, source)
    except Exception as e:
        logging.error("External source %s: %s, table %s left unchanged", name, e, source["table"])
        return False

    with engine.begin() as conn:
        df.to_sql(source["table"], conn, schema=SCHEMA, if_exists="replace", index=False)
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
                "table_name": source["table"],
                "loaded_at": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
                "row_count": len(df),
                "content_sha256": hashlib.sha256(content).hexdigest(),
            },
        )
    logging.info("External source %s: %s rows loaded into %s.%s", name, len(df), SCHEMA, source["table"])
    return True


def load_external_sources(config_path: Path = DEFAULT_CONFIG_PATH, engine=None) -> dict[str, bool]:
    config = yaml.safe_load(Path(config_path).read_text())
    engine = engine or connect_to_db()
    return {s["name"]: load_source(engine, s) for s in config.get("sources", [])}


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
    sentry_init()
    config_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CONFIG_PATH
    results = load_external_sources(config_path)
    logging.info("External sources loaded: %s", results)
