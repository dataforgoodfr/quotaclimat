### Creates a thematic table with
### - chunks that contain given keywords and passing OME methodology (as themetic_core_chunk = True)
### - neighbour chunks (+/- 2 minutes ) fetched in s3 (as adjacent_to_thematic_core = True)
### - chunks that contain given keyword (as adjacent = False and core = False)

### given folowing arguments
### - start_date
### - end_date
### - keywords to look for (has to be in OME dictionnary)
### - country (default france)
### - clear_destination_table : to start destination table from scratch (--assume_yes to confirm clearing)


import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date, timedelta
import pandas as pd
import numpy as np
import argparse
import sys
from urllib.parse import quote

# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import channel_titles_france
from quotaclimat.data_processing.mediatree.i8n.belgium.channel_titles import channel_titles_belgium
from quotaclimat.data_processing.mediatree.i8n.belgium_flanders.channel_titles import channel_titles_belgium_flanders

GAP = pd.Timedelta(minutes=2, seconds=30)
CHANNEL_TITLES={'france':channel_titles_france, 'belgium': channel_titles_belgium, 'belgium_flanders': channel_titles_belgium_flanders}
LANGUAGES = {"france":"french", "belgium": "french", "belgium_flanders": "dutch"}

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "S3_REGION",
    "KEYWORDS","DESTINATION_TABLE",
]

def _env_bool(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "y", "oui")

def _env_date(name: str):
    value = os.getenv(name)
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError(f"{name} must be YYYY-MM-DD, got: {value!r}")
    

def load_configuration() -> None:
    """Read environment variables (credentials and parameters), expose them as globals.
    Raises if a required one is missing or invalid."""
    global START_DATE, END_DATE, KEYWORDS, COUNTRY, LANGUAGE
    global CLEAR_DESTINATION_TABLE, ASSUME_YES
    global DRY_RUN
    global POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    global S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_REGION
    global DESTINATION_TABLE, SOURCE_TABLE
    global LOGGING_SOURCE_TABLE, LOGGING_DESTINATION_TABLE

    ### CHECK REQUIRED VARIABLES BEFORE USING ANY
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    ### PARAMETERS (formerly command line arguments)
    START_DATE = _env_date("START_DATE")
    END_DATE = _env_date("END_DATE") or date.today()
    KEYWORDS = [k.strip() for k in os.environ["KEYWORDS"].split(",") if k.strip()]
    COUNTRY = (os.getenv("COUNTRY") or "france").lower()
    if COUNTRY not in LANGUAGES:
        raise ValueError(f"COUNTRY must be one of {list(LANGUAGES)}, got: {COUNTRY!r}")
    LANGUAGE = LANGUAGES[COUNTRY]
    CLEAR_DESTINATION_TABLE = _env_bool("CLEAR_DESTINATION_TABLE")
    ASSUME_YES = _env_bool("ASSUME_YES")
    if not KEYWORDS:
        raise ValueError("KEYWORDS is set but contains no keyword")
    if START_DATE and END_DATE <= START_DATE:
        raise ValueError("END_DATE must be after START_DATE")
    DRY_RUN = _env_bool("DRY_RUN")

    ### BAROMETRE CREDENTIALS (single database: source and destination)
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = quote(os.environ["POSTGRES_PASSWORD"], safe="")
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    POSTGRES_PORT = os.environ["POSTGRES_PORT"]
    POSTGRES_DB = os.environ["POSTGRES_DB"]
    # env var just for local
    SOURCE_TABLE = os.environ["SOURCE_TABLE"] or "public.keywords"
    DESTINATION_TABLE = os.environ["DESTINATION_TABLE"]

    ### S3 CREDENTIALS
    S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
    S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
    S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
    S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
    S3_REGION = os.environ["S3_REGION"]
    
    # If they look like file paths, read the file content
    if S3_ACCESS_KEY and S3_ACCESS_KEY.startswith('/'):
        try:
            with open(S3_ACCESS_KEY, 'r') as f:
                S3_ACCESS_KEY = f.read().strip()
        except Exception as e:
            raise ValueError(f"Could not read S3_ACCESS_KEY from file {S3_ACCESS_KEY}: {e}")
    if S3_SECRET_KEY and S3_SECRET_KEY.startswith('/'):
        try:
            with open(S3_SECRET_KEY, 'r') as f:
                S3_SECRET_KEY = f.read().strip()
        except Exception as e:
            raise ValueError(f"Could not read S3_SECRET_KEY from file {S3_SECRET_KEY}: {e}")

    ### LOGGING
    LOGGING_SOURCE_TABLE = ("SRC" + ":" + SOURCE_TABLE).upper()
    LOGGING_DESTINATION_TABLE = ("DEST" + ":" + DESTINATION_TABLE).upper()


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


def get_max_start_date_from_destination(con: duckdb.DuckDBPyConnection) -> date:
    """Get the maximum start date from the destination table, or None if empty."""
    try:
        result = con.sql(f"SELECT MAX(start) FROM {POSTGRES_DB}.{DESTINATION_TABLE}").fetchone()
        if result and result[0]:
            return result[0].date()
        else:
            return None
    except Exception as e:
        logging.error(f"[{LOGGING_DESTINATION_TABLE}] ERROR GETTING MAX START DATE: {e}")
        raise e
    
    
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


def _barometre_dsn() -> str:
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def configure_barometre_source(con: duckdb.DuckDBPyConnection) -> None:
    """Attach the Barometre database (once: serves both as source and destination)."""
    try:
        # Install and load the Postgres extension for DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{_barometre_dsn()}' AS {POSTGRES_DB} (TYPE POSTGRES);")
        logging.info(f"[{LOGGING_SOURCE_TABLE}] CONNECTION OK")
        
    except Exception as e:
        logging.error(f"[{LOGGING_SOURCE_TABLE}] CONNECTION ERROR:")
        logging.error(e)
        raise e
        

def configure_barometre_destination(con: duckdb.DuckDBPyConnection, clear_destination_table: bool) -> None:
    """The database is already attached by configure_barometre_source: we only prepare the table."""
    try:
        logging.info(f"[{LOGGING_DESTINATION_TABLE}] CONFIGURATION: Using {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB} as {POSTGRES_USER}")
        # <!> Asserting source_table won't be cleared
        if DESTINATION_TABLE == SOURCE_TABLE or\
                DESTINATION_TABLE.split(".")[-1].lower() == SOURCE_TABLE.split(".")[-1].lower():
            raise ValueError(
                f"DESTINATION_TABLE == SOURCE_TABLE ({DESTINATION_TABLE}): "
                "the source table would be cleared by DELETE"
            )            
        if not DRY_RUN:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {POSTGRES_DB}.{DESTINATION_TABLE} (
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
            logging.info(f"[{LOGGING_DESTINATION_TABLE}] TABLE CREATED OR ALREADY EXISTS")
            if clear_destination_table:
                con.execute(
                    f"DELETE FROM {POSTGRES_DB}.{DESTINATION_TABLE} "
                )
                logging.info(f"[{LOGGING_DESTINATION_TABLE}] TABLE {DESTINATION_TABLE} CLEARED")
    except Exception as e:
        logging.error(f"[{LOGGING_DESTINATION_TABLE}] ERROR CONNECTING:")
        logging.error(e)
        raise e

        
def read_barometre_keywords(con: duckdb.DuckDBPyConnection, 
                        keywords_to_look_for: dict[str, dict], 
                        start_date:date, 
                        end_date:date,
                        country: str) -> pd.DataFrame:
    """Reads barometre.keywords to retrieve (start, channel) of every thematic chunk to keep"""
    keywords_list = list(keywords_to_look_for.keys())
    kewords_sql = ", ".join("'" + m.replace("'", "''") + "'" for m in keywords_list)
    try:
        out= con.sql(f"""
        WITH exploded AS (
            SELECT k.id, k.channel_name, 
            k.start AT TIME ZONE 'Europe/Paris' AS start,
            k.plaintext,
                unnest(from_json(k.keywords_with_timestamp::JSON,
                        '[{{"keyword":"VARCHAR","theme":"VARCHAR"}}]')) AS kwt
            FROM {POSTGRES_DB}.{SOURCE_TABLE} k
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
        logging.error(f"[{LOGGING_SOURCE_TABLE}] ERROR READING KEYWORDS:")
        logging.error(e)
        raise e


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    endpoint = S3_ENDPOINT_URL.removeprefix("https://").removeprefix("http://")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_region='{S3_REGION}';
            SET s3_endpoint='{endpoint}';
            SET s3_access_key_id='{S3_ACCESS_KEY}';
            SET s3_secret_access_key='{S3_SECRET_KEY}';
            SET s3_url_style='path';
        """)
        logging.info("[S3] CONFIGURATION OK")

    except Exception as e:
        logging.error("[S3] CONFIGURATION ERROR:")
        logging.error(e)
        raise e
    
    
def read_day_from_s3(con: duckdb.DuckDBPyConnection, day: date, channels: list[str], country:str) -> pd.DataFrame:
    """Read parquet files for a specific day and channels: channel_name, start, plaintext."""
    if country != "france":
        globs = ", ".join(
            f"'s3://{S3_BUCKET_NAME}/country={country}/year={day.year}/month={day.month}/day={day.day}"
            f"/channel={ch}/*.parquet'"
            for ch in channels
        )
    else:
        globs = ", ".join(
            f"'s3://{S3_BUCKET_NAME}/year={day.year}/month={day.month}/day={day.day}"
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


def extract_adjacent_chunks_day(keywords_day: pd.DataFrame, s3_day_df: pd.DataFrame, gap: pd.Timedelta, country: str) -> pd.DataFrame:
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
        out["channel_title"] = out["channel_name"].map(CHANNEL_TITLES[country])
        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_core_chunk", "adjacent_to_thematic_core",
                "source_keywords_id", "keywords", "themes", "plaintext"]
        return out.sort_values(["start", "channel_name"])[cols].reset_index(drop=True)

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e


def run(start_date: date, end_date: date,
        keywords_to_look_for: list[str], country: str, language: str,
        clear_destination_table: bool, assume_yes: bool) -> None:
    
    logging.info(f"[RUN] {start_date} -> {end_date}, {keywords_to_look_for}, {country}, {language}")
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")
    
    _con = duckdb.connect()
        
    ### Checking keywords to process
    kw_dict = get_keyword_ome_hrfp_theme(keywords_to_look_for, language)
    
    ### Configuring source DB
    configure_barometre_source(_con)
            
    if not start_date and clear_destination_table:
        raise ValueError("Destination table will be cleared. start_date required.")
    if not start_date:
        max_start = get_max_start_date_from_destination(_con)
        if max_start is None:
            raise ValueError("Empty destination: provide --start_date")
        start_date = max_start
        logging.info(f"Deducted start date from {LOGGING_DESTINATION_TABLE}: {start_date}")
        if not confirm(f"No --start_date: resuming at {start_date} (last day in table). "
                       f"Continue?", assume_yes):
            raise SystemExit("Cancelled by user")

    if start_date >= end_date: 
        logging.warning("[END] Deducted start_date >= end_date.")
        return

    if clear_destination_table and not DRY_RUN:
        if not confirm(f"WARNING: {LOGGING_DESTINATION_TABLE} will be cleared. "
                       f"Continue?", assume_yes):
            raise SystemExit("Cancelled by user")

    ### Configuring s3 connection
    _configure_s3(_con)
    ### Configuring destination table
    configure_barometre_destination(_con, clear_destination_table)

    table = f"{POSTGRES_DB}.{DESTINATION_TABLE}"
    counter_to_ingest=0
    ingested = 0
    stopped_day = None
    
    ### For each day: 
    ### 1. reading source table keywords
    ### 2. fetching all needed days and channels on s3
    ### 3. extracting only thematic chunks and neighbour chunks (+/- 2 minutes)
    ### 4. ingesting the result in destination table
    day = start_date
    while day < end_date:
        # 1. Reading keywords
        try:
            logging.info(f"[RUN] ------ Processing day {day} ------")
            logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: reading source table")
            keywords_day = read_barometre_keywords(_con, kw_dict, day, day + timedelta(days=1), country)
        except Exception:
            logging.exception(f"[{LOGGING_SOURCE_TABLE}] {day}: failed")
            stopped_day = day
            break

        if keywords_day.empty:
            logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: no chunk found")
            day += timedelta(days=1)
            continue
        n_core = int(keywords_day["thematic_core_chunk"].sum())
        logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: {len(keywords_day)} chunks found, {n_core} thematic core chunks")

        # 2. Fetching data in s3, only for channels with at least one core chunk
        s3_day_df = None
        if n_core:
            logging.info(f"[S3] {day}: Fetching s3 data on days and channels from {n_core} core chunks")
            day_channels = (keywords_day.loc[keywords_day["thematic_core_chunk"], "channel_name"]
                            .unique().tolist())
            try:
                s3_day_df = read_day_from_s3(_con, day, day_channels, country)
            except Exception:
                logging.exception(f"[S3] {day}: failed to read S3")
                stopped_day = day
                break

        try:
            # 3. Extracting core chunks, neighbour chunks and non-core chunks
            logging.info(f"[EXTRACT_ADJACENT_CHUNKS] Starting selection of chunks to keep (core + adjacent_to_core + others)")
            day_to_ingest_df = extract_adjacent_chunks_day(keywords_day, s3_day_df, GAP, country)
            n_core_day = int(day_to_ingest_df["thematic_core_chunk"].sum())
            n_adjacent_day = int(day_to_ingest_df["adjacent_to_thematic_core"].sum())
            n_other_day = len(day_to_ingest_df) - n_core_day - n_adjacent_day
            logging.info(
                f"[{LOGGING_DESTINATION_TABLE}] {day}: {len(day_to_ingest_df)} chunks selected for ingestion into destination "
                f"({n_core_day} core, {n_adjacent_day} adjacent, {n_other_day} other)"
            )
            counter_to_ingest+=len(day_to_ingest_df)
            n_expected_day = day_to_ingest_df["this_id"].nunique()


            if DRY_RUN:
                day += timedelta(days=1)
                continue
            # 4. Ingesting into destination table
            _con.begin()
            try:
                # checks if consistent table (if extra, probably previous runs with other kw)
                _con.execute(f"""
                    INSERT INTO {table} BY NAME
                    SELECT *,now() AT TIME ZONE 'Europe/Paris' AS created_at
                    FROM day_to_ingest_df
                    ON CONFLICT (this_id) DO NOTHING
                """)
                n_extra = _con.execute(f"""
                    SELECT count(*) FROM {table}
                    WHERE start >= ? AND start < ?
                      AND this_id NOT IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
                
                # checks if n_ingest = n_to_ingest
                n_in_table = _con.execute(f"""
                    SELECT count(*) FROM {table}
                    WHERE start >= ? AND start < ?
                      AND this_id IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
                if n_in_table != n_expected_day:
                    raise RuntimeError(
                        f"{day}: {n_in_table} chunks found in the table after ingestion, "
                        f"{n_expected_day} expected"
                    )
                _con.commit()
            except Exception:
                _con.rollback()
                raise

            ingested += len(day_to_ingest_df)
            if n_extra:
                logging.warning(
                    f"[{LOGGING_DESTINATION_TABLE}] {day}: {n_extra} chunks already in table "
                    f"and missing from this run ({n_expected_day} expected), likely ingested with other keywords"
                )
            else:
                logging.info(f"[{LOGGING_DESTINATION_TABLE}] {day}: {n_expected_day} chunks ingested. Table consistent.")
        except Exception:
            logging.exception(f"[{LOGGING_DESTINATION_TABLE}] {day}: failed")
            stopped_day = day
            break

        day += timedelta(days=1)
        
    # If dry_run, no writing in destination table
    if DRY_RUN:
        logging.info(f"[END] DRY_RUN: {counter_to_ingest} chunks would have been ingested")
    else:
        logging.info(f"[END] {ingested} chunks inserted or ignored (already present rows are ignored)")
    if stopped_day:
        logging.error(
            f"[END] RUN STOPPED at day {stopped_day} (end_date requested: {end_date}, "
            f"so the run did NOT reach end_date). Fix the cause, then rerun without --start_date "
            f"to resume from the last ingested day."
        )
        raise RuntimeError(f"Run stopped at {stopped_day}, end_date {end_date} not reached")
    
         
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True
    )
    load_configuration()
    try:
        run(start_date=START_DATE, end_date=END_DATE, keywords_to_look_for=KEYWORDS,
            country=COUNTRY, language=LANGUAGE,
            clear_destination_table=CLEAR_DESTINATION_TABLE, assume_yes=ASSUME_YES)
    except Exception as e:
        logging.error(f"[RUN] ERROR: {e}")
        raise