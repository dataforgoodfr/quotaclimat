### Fonctions utilitaires du script de création de la table thématique
### (configuration, dictionnaire OME, connexions Postgres/S3, lecture et extraction des chunks).
### Le point d'entrée (run + main) est dans main.py.
###
### <!> Les variables de configuration (START_DATE, DRY_RUN, POSTGRES_DB, etc.) sont définies
### dans ce module par load_configuration(). Depuis main.py, il faut y accéder via `utils.NOM`
### (et non `from utils import NOM`, qui figerait la valeur avant l'appel à load_configuration).

import logging
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date
import pandas as pd
import numpy as np
import sys
from urllib.parse import quote

# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS


def get_keyword_ome_hrfp_theme(keywords_to_look_for: list[str], language: str) -> dict[str, dict]:
    """Only keeps keywords in OME dictionnary"""
    keywords_lower = {m.lower() for m in keywords_to_look_for}
    try:
        keywords_hrfp_theme = {}
        for theme, entrees in THEME_KEYWORDS.items():
            for entree in entrees:
                k = entree["keyword"].lower()
                if k not in keywords_lower or entree["language"] != language:
                    continue
                hrfp = entree["high_risk_of_false_positive"]
                if k not in keywords_hrfp_theme:
                    keywords_hrfp_theme[k] = {"theme": [theme], "hrfp": hrfp}
                else:
                    if theme not in keywords_hrfp_theme[k]["theme"]:
                        keywords_hrfp_theme[k]["theme"].append(theme)

        not_found = list(keywords_lower - keywords_hrfp_theme.keys())
        if not_found:
            logging.info("[OME DICTIONARY] Keywords not found in OME dictionary: %s", not_found)
        else:
            logging.info("[OME DICTIONARY] All keywords found in OME dictionary")
        if keywords_hrfp_theme:
            logging.info("[OME DICTIONARY] Starting research on keywords on: %s", list(keywords_hrfp_theme.keys()))

    except Exception as e:
        logging.error("[OME DICTIONARY] ERROR READING OME DICTIONARY:")
        logging.error(e)
        raise e
    if not keywords_hrfp_theme:
        raise ValueError("No keywords found in OME dictionary")

    return keywords_hrfp_theme


def confirm(message: str, assume_yes: bool = False) -> bool:
    """Prompts for y/N confirmation in terminal. Returns True if accepted.
    Without interactive terminal (job, CI), rejects unless assume_yes is True."""
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        logging.error(f"[RUN] Confirmation required but no interactive terminal: {message} "
                      f"Rerun with --yes to confirm.")
        return False
    return input(f"{message} [y/N] ").strip().lower() in ("y", "yes", "o", "oui")


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
                                    clear_destination_table: bool, dry_run: bool,
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
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {postgres_db}.{destination_table} (
                    this_id VARCHAR NOT NULL,
                    channel_name VARCHAR NOT NULL,
                    channel_title VARCHAR NOT NULL,
                    start TIMESTAMP NOT NULL,
                    thematic_core_chunk BOOLEAN NOT NULL,
                    adjacent_to_thematic_core BOOLEAN NOT NULL,
                    keywords VARCHAR,
                    themes VARCHAR,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    plaintext VARCHAR NOT NULL,
                    source_keywords_id VARCHAR,
                    PRIMARY KEY (this_id)
                )
            """)
            logging.info(f"[{destination_table}] TABLE CREATED OR ALREADY EXISTS")
            if clear_destination_table:
                con.execute(
                    f"DELETE FROM {postgres_db}.{destination_table} "
                )
                logging.info(f"[{destination_table}] TABLE CLEARED")
    except Exception as e:
        logging.error(f"[{destination_table}] ERROR CONNECTING:")
        logging.error(e)
        raise e


def read_barometre_keywords(con: duckdb.DuckDBPyConnection,
                            keywords_to_look_for: dict[str, dict],
                            start_date: date, end_date: date,
                            country: str,
                            source_table: str, postgres_db: str) -> pd.DataFrame:
    """Reads barometre.keywords to retrieve (start, channel) of every thematic chunk to keep"""
    keywords_list = list(keywords_to_look_for.keys())
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
               string_agg(DISTINCT e.kwt.theme, ',' ORDER BY e.kwt.theme) AS themes,
               /* thematic core = at least one direct theme (no "_indirectes" suffix):
                  either no hrfp, or granted hrfp with the suffix removed.
                  A keyword can have several themes, the chunk is core if at least one is direct */
               COALESCE(bool_or(NOT ends_with(e.kwt.theme, '_indirectes')), FALSE) AS thematic_core_chunk
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


def extract_adjacent_chunks_day(keywords_day: pd.DataFrame, s3_day_df: pd.DataFrame, 
                                gap: pd.Timedelta, country: str, channel_titles: dict[str, str]) -> pd.DataFrame:
    """According to keywords reading, extracts core, adjacent and keywords_containing chunks from s3"""
    try:
        gap_ns = pd.Timedelta(gap).value  # integer, nanoseconds
        keys = ["start", "channel_name"]

        # All chunks coming from the keywords table: thematic cores and indirect-only chunks
        kw = (
            keywords_day[keys + ["id", "plaintext", "keywords", "themes", "thematic_core_chunk"]]
            .drop_duplicates(subset=keys)
            .rename(columns={"id": "source_keywords_id"})
        )
        cores = kw[kw["thematic_core_chunk"]]

        if cores.empty:
            # No core: nothing to fetch on S3, non-core chunks are kept as they are
            out = kw.assign(adjacent_to_thematic_core=False)
        else:
            # S3 chunks within gap of at least one core (core itself, 0 gap, is included)
            parts = []
            for channel, s3_chan in s3_day_df.groupby("channel_name"):
                core_starts = (
                    pd.to_datetime(cores.loc[cores["channel_name"] == channel, "start"])
                    .dt.as_unit("ns").astype("int64").to_numpy()
                )
                if len(core_starts) == 0:
                    continue
                s3_starts = (
                    pd.to_datetime(s3_chan["start"])
                    .dt.as_unit("ns").astype("int64").to_numpy()
                )
                ecart = np.abs(s3_starts[:, None] - core_starts[None, :])
                parts.append(s3_chan[(ecart <= gap_ns).any(axis=1)])

            if not parts:
                raise ValueError(
                    f"No chunk in S3 found on {len(cores)} core chunks: "
                    f"missing channels in S3 or timezone shifting?"
                )
            near = pd.concat(parts, ignore_index=True)

            # All core chunks must be in S3
            n_core_found = len(cores.merge(near[keys], on=keys))
            if n_core_found != len(cores):
                raise ValueError(f"{len(cores) - n_core_found} core chunks missing from S3")

            # For the same channel and start time, source plaintext must match S3 plaintext
            check = cores.merge(near, on=keys, suffixes=("_kw", "_s3"))
            mismatch = check[check["plaintext_kw"] != check["plaintext_s3"]]
            if not mismatch.empty:
                raise ValueError(
                    f"{len(mismatch)} core chunks have mismatched plaintext between source table and S3 "
                    f"at the same channel and start time: issue is likely caused by a timezone offset. "
                    f"Example: {mismatch[['channel_name', 'start']].iloc[0].to_dict()}"
                )

            # adjacent= true if within gap of a core, and not a core itself
            # (also applies to non-core chunks from keywords that sit in a core window)
            kw = kw.merge(near[keys].assign(in_window=True), on=keys, how="left")
            kw["adjacent_to_thematic_core"] = kw["in_window"].eq(True) & ~kw["thematic_core_chunk"]
            kw = kw.drop(columns="in_window")

            # Pure neighbours: in S3 near a core, absent from the keywords table
            pure = near.merge(kw[keys], on=keys, how="left", indicator=True)
            pure = pure[pure["_merge"] == "left_only"].drop(columns="_merge")
            pure = pure.assign(source_keywords_id=None, keywords=None, themes=None,
                               thematic_core_chunk=False, adjacent_to_thematic_core=True)

            out = pd.concat([kw, pure], ignore_index=True)

        # id = primary key: channel + start
        out["this_id"] = (out["channel_name"] + "_"
                          + out["start"].dt.strftime("%Y%m%d%H%M%S"))
        out["channel_title"] = out["channel_name"].map(channel_titles[country])
        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_core_chunk", "adjacent_to_thematic_core",
                "source_keywords_id", "keywords", "themes", "plaintext"]
        return out.sort_values(["start", "channel_name"])[cols].reset_index(drop=True)

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e