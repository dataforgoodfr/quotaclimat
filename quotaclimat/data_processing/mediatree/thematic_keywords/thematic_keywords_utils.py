
import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd
import numpy as np
import sys
import hashlib
import json
import re
import unicodedata
from datetime import date

# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS


### RUNS REGISTER ###
PG_MAX_IDENTIFIER = 63

def make_destination_table_name(start_date: date, kw_list: list[str], country: str) -> str:
    cleaned = list(dict.fromkeys(k.strip().lower() for k in kw_list if k.strip()))
    if not cleaned:
        raise ValueError("kw_list is empty")
    country = country.strip().lower()
    if not country:
        raise ValueError("country is empty")

    # first keyword as given (input order), identifier-safe ascii slug (accents removed, max 30 chars)
    first = unicodedata.normalize("NFKD", cleaned[0]).encode("ascii", "ignore").decode("ascii")
    first = re.sub(r"[^a-z0-9]+", "_", first).strip("_")[:30] or "kw"

    # sorted: the hash does not depend on the keywords order
    payload = json.dumps({"start": start_date.isoformat(), "country": country, "kw": sorted(cleaned)},
                         ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    table_name = f"thematic_keywords_{first}_{digest}"
    if len(table_name) > PG_MAX_IDENTIFIER:  # 18 + 30 + 1 + 12 = 61 max
        raise ValueError(f"table name too long: {table_name}")
    return table_name


def configure_thematic_registry(con: duckdb.DuckDBPyConnection, dry_run: bool,
                                postgres_db: str, registry_table: str) -> None:
    """Creates the registry table if it does not exist yet."""
    try:
        if dry_run:
            return
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {postgres_db}.{registry_table} (
                table_name VARCHAR NOT NULL,
                start_date DATE NOT NULL,
                country VARCHAR NOT NULL,
                keywords VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (table_name)
            )
        """)
        logging.info(f"[{registry_table}] TABLE CREATED OR ALREADY EXISTS")
    except Exception as e:
        logging.error(f"[{registry_table}] ERROR CONFIGURING REGISTRY:")
        logging.error(e)
        raise
    
def _table_exists(con: duckdb.DuckDBPyConnection, postgres_db: str, table: str) -> bool:
    return con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_name = ?",
        [postgres_db, table.split(".")[-1]],
    ).fetchone()[0] > 0

def register_run(con: duckdb.DuckDBPyConnection, dry_run: bool, postgres_db: str,
                 destination_table: str, start_date: date, country: str,
                 kw_list: list[str], registry_table: str) -> None:
    """Writes destination_table -> start date, country and keywords in the registry.
    Raises if destination_table is already registered with another start date, country or keywords."""
    try:
        if dry_run:
            return
        country = country.strip().lower()
        keywords = json.dumps(sorted({k.strip().lower() for k in kw_list if k.strip()}),
                              ensure_ascii=False)
        row = con.execute(
            f"SELECT start_date, country, keywords FROM {postgres_db}.{registry_table} "
            f"WHERE table_name = ?", [destination_table]).fetchone()
        if row is None:
            con.execute(
                f"INSERT INTO {postgres_db}.{registry_table} "
                f"(table_name, start_date, country, keywords) VALUES (?, ?, ?, ?)",
                [destination_table, start_date, country, keywords])
            logging.info(f"[{registry_table}] {destination_table} REGISTERED")
        elif tuple(row) != (start_date, country, keywords):
            raise ValueError(f"{destination_table} already registered with other parameters: {row}")
        else:
            logging.info(f"[{registry_table}] {destination_table} ALREADY REGISTERED (same parameters)")
    except Exception as e:
        logging.error(f"[{registry_table}] ERROR REGISTERING RUN:")
        logging.error(e)
        raise

def get_registered_run(con: duckdb.DuckDBPyConnection, postgres_db: str,
                       destination_table: str, registry_table: str) -> dict:
    """Returns {"start_date", "country", "keywords"} registered for destination_table.
    Raises LookupError if the table is not in the registry, ValueError if its country is missing."""
    try:
        row = con.execute(
            f"SELECT start_date, country, keywords FROM {postgres_db}.{registry_table} "
            f"WHERE table_name = ?", [destination_table.split(".")[-1]]).fetchone()
        if row is None:
            raise LookupError(f"{destination_table} not found in {registry_table}")
        if row[1] is None:
            raise ValueError(f"{destination_table} registered without country (legacy row): fill it in the registry")
        return {"start_date": row[0], "country": row[1], "keywords": json.loads(row[2])}
    except Exception as e:
        logging.error(f"[{registry_table}] ERROR READING REGISTRY FOR {destination_table}:")
        logging.error(e)
        raise
    

### OME DICTIONNARY ###
def get_keywords_in_ome(keywords_to_look_for: list[str], language: str) -> list[str]:
    """Returns keywords as found in OME dictionary (for the given language), in input order.
    Raises if at least one keyword is missing, to avoid a long run on a partial list."""
    # dict.fromkeys: dedupe while keeping the input order
    keywords_lower = list(dict.fromkeys(m.strip().lower() for m in keywords_to_look_for if m.strip()))
    wanted = set(keywords_lower)
    try:
        found = {
            entree["keyword"].lower()
            for entrees in THEME_KEYWORDS.values()
            for entree in entrees
            if entree["language"] == language and entree["keyword"].lower() in wanted
        }
    except Exception as e:
        logging.error("[OME DICTIONARY] ERROR READING OME DICTIONARY:")
        logging.error(e)
        raise

    not_found = [k for k in keywords_lower if k not in found]
    if not_found:
        raise ValueError(
            f"[OME DICTIONARY] Keywords not found in OME dictionary (language={language}): {not_found}"
        )

    result = [k for k in keywords_lower if k in found]  # input order
    logging.info("[OME DICTIONARY] All keywords found in OME dictionary: %s", result)
    return result

### SOURCE ###
def configure_barometre_source(con: duckdb.DuckDBPyConnection, source_table: str,
                               postgres_user: str, postgres_password: str, 
                               postgres_host: str, postgres_port: str, 
                               postgres_db: str) -> None:
    """Attach the Barometre database (once: serves both as source and destination)."""
    barometre_dsn=f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    try:
        # Install and load the Postgres extension for DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{barometre_dsn}' AS {postgres_db} (TYPE POSTGRES);")
        logging.info(f"[{source_table}] CONNECTION OK")

    except Exception as e:
        logging.error(f"[{source_table}] CONNECTION ERROR:")
        logging.error(e)
        raise e
    
def read_barometre_keywords(con: duckdb.DuckDBPyConnection,
                            keywords_to_look_for: list[str],
                            start_date: date, end_date: date,
                            country: str,
                            source_table: str, postgres_db: str) -> pd.DataFrame:
    """Reads barometre.keywords to retrieve (start, channel) of every thematic chunk to keep"""
    keywords_list = keywords_to_look_for
    kewords_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in keywords_list)
    try:
        out = con.sql(f"""
            WITH exploded AS (
                SELECT k.id, k.channel_name,
                    k.start AT TIME ZONE 'Europe/Paris' AS start,
                    k.plaintext,
                    unnest(from_json(k.keywords_with_timestamp::JSON,
                                        '[{{"keyword":"VARCHAR","theme":"VARCHAR"}}]')) AS kwt
                FROM {postgres_db}.{source_table} k
                WHERE k.start >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                    AND k.start <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                    AND k.country = '{country}'
            )
            SELECT e.id, e.channel_name, e.start, e.plaintext,
                string_agg(DISTINCT e.kwt.keyword, ',' ORDER BY e.kwt.keyword) AS keywords,
                string_agg(DISTINCT e.kwt.theme, ',' ORDER BY e.kwt.theme) AS themes
            FROM exploded e
            WHERE e.kwt.keyword IN ({kewords_sql})
            GROUP BY e.id, e.channel_name, e.start, e.plaintext
            ORDER BY e.start, e.channel_name
    """).df()
        return out
    except Exception as e:
        logging.error(f"[{source_table}] ERROR READING KEYWORDS:")
        logging.error(e)
        raise e
    
    
### DESTINATION ### 
def get_max_start_date_from_destination(con: duckdb.DuckDBPyConnection, postgres_db: str, destination_table: str) -> date:
    """Get the maximum start date from the destination table, or None if empty."""
    try:
        result = con.sql(f"SELECT MAX(start) FROM {postgres_db}.{destination_table}").fetchone()
        if result and result[0]:
            return result[0].date()
        else:
            return None
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR GETTING MAX START DATE: {e}")
        raise e


def configure_barometre_destination(con: duckdb.DuckDBPyConnection, 
                                    dry_run: bool,
                                    source_table: str, destination_table: str, 
                                    postgres_db: str) -> None:
    """The database is already attached by configure_barometre_source: we only prepare the table."""
    try:
        logging.info(f"[{destination_table}] CONFIGURATION: Using {postgres_db}")
        # <!> Asserting source_table won't be cleared
        if destination_table == source_table or\
                destination_table.split(".")[-1].lower() == source_table.split(".")[-1].lower():
            raise ValueError(
                f"DESTINATION_TABLE == SOURCE_TABLE ({destination_table}): "
                "the source table would be cleared by DELETE"
            )
        if not dry_run:
            existed = _table_exists(con, postgres_db, destination_table)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {postgres_db}.{destination_table} (
                    this_id VARCHAR NOT NULL,
                    channel_name VARCHAR NOT NULL,
                    channel_title VARCHAR NOT NULL,
                    start TIMESTAMP NOT NULL,
                    thematic_chunk BOOLEAN NOT NULL,
                    keywords VARCHAR,
                    themes VARCHAR,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    plaintext VARCHAR NOT NULL,
                    source_keywords_id VARCHAR,
                    PRIMARY KEY (this_id)
                )
            """)
            if existed:
                logging.info(f"[{destination_table}] TABLE ALREADY EXISTS: resuming, rows will be appended")
            else:
                logging.info(f"[{destination_table}] TABLE CREATED")

    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CONNECTING:")
        logging.error(e)
        raise e

### S3 ###
def _configure_s3(con: duckdb.DuckDBPyConnection, 
                  s3_endpoint_url: str, s3_region: str, 
                  s3_access_key: str, s3_secret_key: str) -> None:
    endpoint = s3_endpoint_url.removeprefix("https://").removeprefix("http://")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_region='{s3_region}';
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{s3_access_key}';
            SET s3_secret_access_key='{s3_secret_key}';
            SET s3_url_style='path';
        """)
        logging.info("[S3] CONFIGURATION OK")

    except Exception as e:
        logging.error("[S3] CONFIGURATION ERROR:")
        logging.error(e)
        raise e


def read_day_from_s3(con: duckdb.DuckDBPyConnection, day: date, channels: list[str], country: str,
                     s3_bucket_name: str) -> pd.DataFrame:
    """Read parquet files for a specific day and channels: channel_name, start, plaintext."""
    if country != "france":
        globs = ", ".join(
            f"'s3://{s3_bucket_name}/country={country}/year={day.year}/month={day.month}/day={day.day}"
            f"/channel={ch}/*.parquet'"
            for ch in channels
        )
    else:
        globs = ", ".join(
            f"'s3://{s3_bucket_name}/year={day.year}/month={day.month}/day={day.day}"
            f"/channel={ch}/*.parquet'"
            for ch in channels
        )
    try:
        files = [r[0] for r in con.sql(f"SELECT file FROM glob([{globs}])").fetchall()]
        if not files:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        # Channels without any files on that day
        missing = [ch for ch in channels if not any(f"/channel={ch}/" in f for f in files)]
        if missing:
            logging.warning(f"[S3] No S3 data for {day}: {', '.join(missing)}")

        file_list = ", ".join(f"'{f}'" for f in files)
        df = con.sql(
            f"SELECT channel_name, start, plaintext FROM read_parquet([{file_list}], union_by_name=true)"
        ).df()

        # Mediatree API uses sud-radio or sudradio: standardize before deduplicating
        df["channel_name"] = df["channel_name"].replace("sudradio", "sud-radio")
        if df.empty:
            logging.warning(f"[S3] No S3 chunks for {day}")
            raise FileNotFoundError(f"No S3 chunks for {day}")
        else:
            logging.info(f"[S3] {day}: {len(df)} chunks read from S3")

    except Exception as e:
        logging.error(f"[S3] ERROR READING S3 FOR {day}: {e}")
        raise e
    df = df.drop_duplicates(subset=["channel_name", "start"], keep="last").reset_index(drop=True)

    if df["start"].dt.tz is not None:
        df["start"] = df["start"].dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    if df.duplicated(subset=["channel_name", "start"]).any():
        raise ValueError(f"[S3] {day}: duplicate start after local time conversion (daylight saving time switch?)")

    return df

### GETTING ADJACENT CHUNKS ###
def extract_adjacent_chunks_day(keywords_day: pd.DataFrame, s3_day_df: pd.DataFrame,
                                gap: pd.Timedelta, country: str, channel_titles: dict[str, str]) -> pd.DataFrame:
    """Keeps thematic chunks (from the keywords table) and their adjacents (within gap) fetched from s3"""
    try:
        gap_ns = pd.Timedelta(gap).value  # integer, nanoseconds
        keys = ["start", "channel_name"]

        # All chunks coming from the keywords table are thematic chunks
        kw = (
            keywords_day[keys + ["id", "plaintext", "keywords", "themes"]]
            .drop_duplicates(subset=keys)
            .rename(columns={"id": "source_keywords_id"})
            .assign(thematic_chunk=True)
        )

        # S3 chunks within gap of at least one thematic chunk (the chunk itself, 0 gap, is included)
        parts = []
        for channel, s3_chan in s3_day_df.groupby("channel_name"):
            kw_starts = (
                pd.to_datetime(kw.loc[kw["channel_name"] == channel, "start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )
            if len(kw_starts) == 0:
                continue
            s3_starts = (
                pd.to_datetime(s3_chan["start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )
            ecart = np.abs(s3_starts[:, None] - kw_starts[None, :])
            parts.append(s3_chan[(ecart <= gap_ns).any(axis=1)])

        if not parts:
            raise ValueError(
                f"No chunk in S3 found on {len(kw)} thematic chunks: "
                f"missing channels in S3 or timezone shifting?"
            )
        near = pd.concat(parts, ignore_index=True)

        # All thematic chunks must be in S3
        n_found = len(kw.merge(near[keys], on=keys))
        if n_found != len(kw):
            raise ValueError(f"{len(kw) - n_found} thematic chunks missing from S3")

        # For the same channel and start time, source plaintext must match S3 plaintext
        check = kw.merge(near, on=keys, suffixes=("_kw", "_s3"))
        mismatch = check[check["plaintext_kw"] != check["plaintext_s3"]]
        if not mismatch.empty:
            raise ValueError(
                f"{len(mismatch)} thematic chunks have mismatched plaintext between source table and S3 "
                f"at the same channel and start time: issue is likely caused by a timezone offset. "
                f"Example: {mismatch[['channel_name', 'start']].iloc[0].to_dict()}"
            )

        # Pure adjacents: in S3 near a thematic chunk, absent from the keywords table
        pure = near.merge(kw[keys], on=keys, how="left", indicator=True)
        pure = pure[pure["_merge"] == "left_only"].drop(columns="_merge")
        pure = pure.assign(source_keywords_id=None, keywords=None, themes=None,
                           thematic_chunk=False)

        out = pd.concat([kw, pure], ignore_index=True)

        # id = primary key: channel + start
        out["this_id"] = (out["channel_name"] + "_"
                          + out["start"].dt.strftime("%Y%m%d%H%M%S"))
        out["channel_title"] = out["channel_name"].map(channel_titles[country])
        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_chunk",
                "source_keywords_id", "keywords", "themes", "plaintext"]
        return out.sort_values(["start", "channel_name"])[cols].reset_index(drop=True)

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e