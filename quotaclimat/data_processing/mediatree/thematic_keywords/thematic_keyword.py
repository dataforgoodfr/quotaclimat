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

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import (
    channel_titles_france,
)

# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)


### BAROMETRE CREDENTIALS (single database: source and destination)
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = quote(os.getenv("POSTGRES_PASSWORD"), safe="")
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_DATABASE = os.getenv("POSTGRES_DB")
SOURCE_TABLE="public.keywords"
DESTINATION_TABLE=os.getenv("DESTINATION_TABLE")

### S3 CREDENTIALS
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")

### GLOBALS
DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
GAP = "2 minutes 30 seconds"
LOGGING_SOURCE_TABLE=(PG_DATABASE+"."+SOURCE_TABLE).upper()
LOGGING_DESTINATION_TABLE=(PG_DATABASE+"."+DESTINATION_TABLE).upper()


def get_keyword_ome_hrfp_theme(keywords_to_look_for: list, language: str) -> dict:
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


def _barometre_dsn() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


def configure_barometre_source(con: duckdb.DuckDBPyConnection):
    """Attach the Barometre database (once: serves both as source and destination)."""
    try:
        # Install and load the Postgres extension for DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{_barometre_dsn()}' AS {PG_DATABASE} (TYPE POSTGRES);")
        logging.info(f"[{LOGGING_SOURCE_TABLE}] CONNECTION OK")
        
    except Exception as e:
        logging.error(f"[{LOGGING_SOURCE_TABLE}] CONNECTION ERROR:")
        logging.error(e)
        raise e
        

def configure_barometre_destination(con: duckdb.DuckDBPyConnection, clear_destination_table: bool) -> None:
    """The database is already attached by configure_barometre_source: we only prepare the table."""
    try:
        logging.info(f"[{LOGGING_DESTINATION_TABLE}] CONFIGURATION: Using {PG_HOST}:{PG_PORT}/{PG_DATABASE} as {PG_USER}")
        # <!> Asserting source_table won't be cleared
        if DESTINATION_TABLE == SOURCE_TABLE or\
                DESTINATION_TABLE.split(".")[-1].lower() == SOURCE_TABLE.split(".")[-1].lower():
            raise ValueError(
                f"DESTINATION_TABLE == SOURCE_TABLE ({DESTINATION_TABLE}): "
                "the source table would be cleared by DELETE"
            )            
        if not DRY_RUN:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {PG_DATABASE}.{DESTINATION_TABLE} (
                    this_id VARCHAR NOT NULL,
                    channel_name VARCHAR NOT NULL,
                    channel_title VARCHAR NOT NULL,
                    start TIMESTAMP NOT NULL,
                    thematic_core_chunk BOOLEAN NOT NULL,
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
                    f"DELETE FROM {PG_DATABASE}.{DESTINATION_TABLE} "
                )
                logging.info(f"[{LOGGING_DESTINATION_TABLE}] TABLE {DESTINATION_TABLE} CLEARED")
    except Exception as e:
        logging.error(f"[{LOGGING_DESTINATION_TABLE}] ERROR CONNECTING:")
        logging.error(e)
        raise e

        
def read_barometre_keywords(con: duckdb.DuckDBPyConnection, 
                        keywords_to_look_for: dict, 
                        start_date:date, 
                        end_date:date,
                        country: str) -> pd.DataFrame:
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
            FROM {PG_DATABASE}.{SOURCE_TABLE} k
            WHERE k.start >= TIMESTAMP '{start_date}' AT TIME ZONE 'Europe/Paris'
                AND k.start <  TIMESTAMP '{end_date}'   AT TIME ZONE 'Europe/Paris'
                AND k.country = '{country}'
        )
        SELECT e.id, e.channel_name, e.start, e.plaintext,
               string_agg(DISTINCT e.kwt.keyword, ',' ORDER BY e.kwt.keyword) AS keywords,
               string_agg(DISTINCT e.kwt.theme, ',' ORDER BY e.kwt.theme) AS themes
        FROM exploded e
        WHERE e.kwt.keyword IN ({kewords_sql})
            -- keeping only direct keywords -> either no hrfp (no "_indirectes" suffix), or granted hrfp with suffix deleted
            -- a keyword can have several themes, we keep the chunk if at least one is direct
            AND NOT ends_with(e.kwt.theme, '_indirectes')
        GROUP BY e.id, e.channel_name, e.start, e.plaintext
        ORDER BY e.start, e.channel_name
    """).df()
        return out
    except Exception as e:
        logging.error(f"[{LOGGING_SOURCE_TABLE}] ERROR READING KEYWORDS:")
        logging.error(e)
        raise e



# SOURCE: GET S3 CREDENTIALS
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
    
    
# SOURCE - READ FROM S3: ONE DAY AT A TIME
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
        logging.error(f"[S3] ❌ ERROR READING S3 FOR {day}: {e}")
        raise e 
    df = df.drop_duplicates(subset=["channel_name", "start"], keep="last").reset_index(drop=True)

    if df["start"].dt.tz is not None:
        df["start"] = df["start"].dt.tz_convert("Europe/Paris").dt.tz_localize(None)
    if df.duplicated(subset=["channel_name", "start"]).any():
       raise ValueError(f"[S3] {day}: duplicate start after local time conversion (daylight saving time switch?)")
   
    return df

def extract_adjacent_chunks_day(thematic_core_chunks: pd.DataFrame, s3_day_df: pd.DataFrame, gap: str) -> pd.DataFrame:
    try:
        gap_ns = pd.Timedelta(gap).value  # integer, nanoseconds
        keys = ["start", "channel_name"]
        core_keys = (
            thematic_core_chunks[keys + ["id", "plaintext", "keywords", "themes"]]
            .drop_duplicates(subset=keys)
            .rename(columns={"plaintext": "core_plaintext", "id": "source_keywords_id"})
            .assign(thematic_core_chunk=True)
        )
        
        parts = []
        for channel, s3_chan in s3_day_df.groupby("channel_name"):
            core_starts = (
                pd.to_datetime(core_keys.loc[core_keys["channel_name"] == channel, "start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )
            if len(core_starts) == 0:
                continue
            s3_starts = (
                pd.to_datetime(s3_chan["start"])
                .dt.as_unit("ns").astype("int64").to_numpy()
            )

            # Difference between each S3 chunk and each core chunk of the channel: matrix (n_s3 x n_core), in ns
            ecart = np.abs(s3_starts[:, None] - core_starts[None, :])

            # Keep chunk if within gap distance of at least one core (core itself, 0 gap, is included)
            parts.append(s3_chan[(ecart <= gap_ns).any(axis=1)])

        if not parts:
            raise ValueError(
                f"No chunk in S3 found on {len(core_keys)} core chunks: "
                f"missing channels in S3 or timezone shifting?"
    )

        out = pd.concat(parts, ignore_index=True)

        # thematic_core_chunk = True if key is in core_chunks, False if adjacent chunk
        out = out.merge(core_keys, on=keys, how="left")
        out["thematic_core_chunk"] = out["thematic_core_chunk"].eq(True)
        
        n_core_found = out["thematic_core_chunk"].sum()
        if n_core_found != len(core_keys):
            raise ValueError(f"{len(core_keys) - n_core_found} core chunks missing from S3")

        # For the same channel and start time, source plaintext must match S3 plaintext
        cores = out[out["thematic_core_chunk"]]
        mismatch = cores[cores["plaintext"] != cores["core_plaintext"]]
        if not mismatch.empty:
            raise ValueError(
                f"{len(mismatch)} core chunks have mismatched plaintext between source table and S3 "
                f"at the same channel and start time: issue is likely caused by a timezone offset. "
                f"Example: {mismatch[['channel_name', 'start']].iloc[0].to_dict()}"
            )

        # id = primary key: channel + start
        out["this_id"] = (out["channel_name"] + "_"
                          + out["start"].dt.strftime("%Y%m%d%H%M%S"))
        out["channel_title"] = out["channel_name"].map(channel_titles_france)

        cols = ["this_id", "channel_name", "channel_title", "start",
                "thematic_core_chunk", "source_keywords_id", "keywords", "themes", "plaintext"]      
        return out.sort_values(keys)[cols].reset_index(drop=True)

    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ❌ ERROR EXTRACTING ADJACENT CHUNKS: {e}")
        raise e
    
def get_max_start_date_from_destination(con: duckdb.DuckDBPyConnection) -> date:
    """Get the maximum start date from the destination table, or None if empty."""
    try:
        result = con.sql(f"SELECT MAX(start) FROM {PG_DATABASE}.{DESTINATION_TABLE}").fetchone()
        if result and result[0]:
            return result[0].date()
        else:
            return None
    except Exception as e:
        logging.error(f"[{LOGGING_DESTINATION_TABLE}] ❌ ERROR GETTING MAX START DATE: {e}")
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


def run(start_date: date, end_date: date,
        keywords_to_look_for: list,
        country: str, language: str,
        clear_destination_table: bool, assume_yes: bool) -> None:
    logging.info("------------------------------")
    logging.info(f"[RUN] {start_date} -> {end_date}, {keywords_to_look_for}, {country}, {language}")
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")
    
    _con = duckdb.connect()
        

    kw_dict = get_keyword_ome_hrfp_theme(keywords_to_look_for, language)
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

    _configure_s3(_con)
    configure_barometre_destination(_con, clear_destination_table)

    table = f"{PG_DATABASE}.{DESTINATION_TABLE}"
    counter_to_ingest=0
    ingested = 0
    stopped_day = None

    day = start_date
    while day < end_date:
        try:
            logging.info(f"[RUN] ------ Processing day {day} ------")
            logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: reading source table")
            core_day = read_barometre_keywords(_con, kw_dict, day, day + timedelta(days=1), country)
        except Exception:
            logging.exception(f"[{LOGGING_SOURCE_TABLE}] {day}: failed")
            stopped_day = day
            break

        if core_day.empty:
            logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: no core chunk found, nothing to do")
            day += timedelta(days=1)
            continue
        logging.info(f"[{LOGGING_SOURCE_TABLE}] {day}: {len(core_day)} core chunk found")

        logging.info(f"[S3] {day}: processing {len(core_day)} core chunks")
        
        # channels to read
        day_channels = core_day["channel_name"].unique().tolist()

        try:
            s3_day_df = read_day_from_s3(_con, day, day_channels, country)
        except Exception:
            logging.exception(f"[S3] {day}: failed to read S3")
            stopped_day = day
            break

        try:
            day_to_ingest_df = extract_adjacent_chunks_day(core_day, s3_day_df, GAP)
            logging.info(f"[{LOGGING_DESTINATION_TABLE}] {day}: {len(day_to_ingest_df)} chunks (core + adjacent) to ingest")
            counter_to_ingest+=len(day_to_ingest_df)
            n_expected_day = day_to_ingest_df["this_id"].nunique()

            if DRY_RUN:
                day += timedelta(days=1)
                continue

            _con.begin()
            try:
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
    
        
        
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="1. Extract keywords from the source table, "
                    "2. Retrieve adjacent chunks from S3, "
                    "3. Insert into destination table."
    )
    parser.add_argument("--start_date", type=date.fromisoformat,
                        help="Start date (included), YYYY-MM-DD")
    parser.add_argument("--end_date", type=date.fromisoformat, default=date.today(),
                        help="End date (excluded), YYYY-MM-DD (default: today)")
    parser.add_argument("--keywords", type=str, nargs="+", required=True,
                        help="Keywords to look for")
    parser.add_argument("--country", type=str, default="france", help="Country name")
    parser.add_argument("--language", type=str, default="french", help="Language name")
    parser.add_argument("--clear_destination_table",action="store_true",
                        help="Clear the destination table before inserting")
    parser.add_argument("--assume_yes", action="store_true",
                        help="Automatically confirm prompts (non-interactive usage)")
    args = parser.parse_args()

    if args.start_date and args.end_date <= args.start_date:
        parser.error("--end_date must be after --start_date")
    return args


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True
    )
    args = parse_args()
    try:
        run(start_date=args.start_date, end_date=args.end_date, keywords_to_look_for=args.keywords,
            country=args.country, language=args.language, 
            clear_destination_table=args.clear_destination_table, assume_yes=args.assume_yes)
    except Exception as e:
        logging.error(f"[RUN] ERROR: {e}")
        raise