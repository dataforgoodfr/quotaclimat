### Creates a thematic table {thematic}_art_{country}_{start YYYYMMDD}_{end YYYYMMDD} with
### - factiva articles whose title, snippet or body contain given keywords

### given following environment variables
### - START_DATE (mandatory, included): YYYY-MM-DD
### - END_DATE (mandatory, excluded): YYYY-MM-DD
### - THEMATIC (mandatory): keywords fetched in yaml
### - COUNTRY (default france): one of COUNTRIES
### - DRY_RUN (default false): reads and counts, writes nothing
### - SOURCE_TABLE (default public.factiva_articles): local only
### - MODE (default new): one of MODES
###     - new: creates the table, raises if it already exists
###     - resume: restarts from the last ingested day, raises if the table does not exist
###     - reset: clears the table (or creates it) and restarts from START_DATE

import logging
import os
from pathlib import Path
from dotenv import load_dotenv
import duckdb
from datetime import date, timedelta
import pandas as pd
import numpy as np
import sys
from urllib.parse import quote

# only local env
ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env", override=False)
THEMATIC_YML = ROOT / "quotaclimat" / "data_processing" / "thematic" / "thematic_dict.yml"

import quotaclimat.data_processing.thematic.thematic_utils as utils
import quotaclimat.data_processing.thematic.thematic_articles_utils as art_utils

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "START_DATE", "END_DATE", "THEMATIC"
]

DESTINATION_SCHEMA = "thematic_extracts"
SOURCE_TABLE = "public.factiva_articles"
MEDIA_TYPE = "presse"
LANGUAGES = {"france": "french", "belgium": "french", "belgium_flanders": "dutch"}
COUNTRIES = {"france": "fr", "belgium": "be", "belgium_flanders": "befl" }
MODES = ("new", "resume", "reset")

def load_env() -> None:
    """Read environment variables (credentials and parameters) and expose them as module globals.
    Raises if a required one is missing or invalid."""
    global START_DATE, END_DATE, THEMATIC, COUNTRY, LANGUAGE, DRY_RUN
    global SOURCE_TABLE
    global POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    global MODE
    ### CHECK REQUIRED VARIABLES BEFORE USING ANY
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    ### DATES (mandatory: checked in REQUIRED_ENV)
    try:
        START_DATE = date.fromisoformat(os.environ["START_DATE"])
        END_DATE = date.fromisoformat(os.environ["END_DATE"])
    except ValueError as e:
        raise ValueError(f"START_DATE and END_DATE must be YYYY-MM-DD: {e}")
    if END_DATE <= START_DATE:
        raise ValueError("END_DATE must be after START_DATE")

    ### Keywords to look for from thematic
    THEMATIC = os.environ["THEMATIC"]
    
    ### COUNTRY AND LANGUAGE
    COUNTRY = (os.getenv("COUNTRY") or "france").strip().lower()
    if COUNTRY not in COUNTRIES:
        raise ValueError(f"COUNTRY must be one of {list(COUNTRIES)}, got: {COUNTRY!r}")
    LANGUAGE = LANGUAGES[COUNTRY]

    ### FLAGS
    DRY_RUN = (os.getenv("DRY_RUN") or "").strip().lower() in ("1", "true", "yes", "y", "oui")
    MODE = (os.getenv("MODE") or "new").strip().lower()
    if MODE not in MODES:
        raise ValueError(f"MODE must be one of {list(MODES)}, got: {MODE!r}")

    ### TABLES
    SOURCE_TABLE = os.getenv("SOURCE_TABLE") or SOURCE_TABLE  # env var just for local

    ### BAROMETRE CREDENTIALS (single database: source and destination)
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = quote(os.environ["POSTGRES_PASSWORD"], safe="")
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    POSTGRES_PORT = os.environ["POSTGRES_PORT"]
    POSTGRES_DB = os.environ["POSTGRES_DB"]
    
def run() -> None:
    
    logging.info(
        "[RUN]\n"
        f"start_date included = {START_DATE}\n"
        f"end_date excluded= {END_DATE}\n"
        f"thematic = {THEMATIC}\n"
        f"country = {COUNTRY}\n"
        f"language = {LANGUAGE}"
    )
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")

    destination_table=utils.make_destination_table_name(THEMATIC, MEDIA_TYPE,
                                      COUNTRIES[COUNTRY], START_DATE, END_DATE)
    start_date = START_DATE
    logging.info(f"[RUN] MODE={MODE} on {destination_table}")

    kw_list = utils.get_keywords(THEMATIC_YML, THEMATIC, MEDIA_TYPE, LANGUAGE)
    if not kw_list:
        raise ValueError(f"No keywords found for '{THEMATIC}' in {THEMATIC_YML}.")
    
    ### Connection and source
    _con = duckdb.connect()
    _con.execute("SET TimeZone='Europe/Paris'")
    utils.configure_barometre_source(_con, SOURCE_TABLE,
                                    POSTGRES_USER, POSTGRES_PASSWORD,
                                    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)
    
    ### Check existence BEFORE creating the table
    table_exists = utils.destination_table_exists(_con, destination_table, POSTGRES_DB, DESTINATION_SCHEMA)

    ### NEW: the table must not exist yet
    if MODE == "new":
        if table_exists:
            raise ValueError(
                f"[RUN] {destination_table} already exists: rerun with MODE=resume or MODE=reset"
            )
        art_utils.create_thematic_articles_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA)

    ### RESUME: the table must exist, restart from its last ingested day
    elif MODE == "resume":
        if not table_exists:
            raise ValueError(
                f"[RUN] {destination_table} does not exist: nothing to resume "
                "(check START_DATE / END_DATE, or rerun with MODE=new)"
            )
        max_start = art_utils.get_max_start_date_from_thematic_articles(_con, POSTGRES_DB, destination_table, DESTINATION_SCHEMA)
        if max_start is None:
            logging.info(f"[RUN] {destination_table} is empty: resuming from START_DATE")
        else:
            # last day is reprocessed: ON CONFLICT DO NOTHING makes it safe
            start_date = max(START_DATE, max_start)

    ### RESET: empty the table (or create it) and restart from START_DATE
    elif MODE == "reset":
        if table_exists:
            logging.warning(f"[RUN] Reset mode. Destination table {destination_table} will be cleared.")
            try:
                utils.clear_destination_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA)
            except Exception as e:
                raise ValueError(f"[RUN] Failed to clear {destination_table}") from e
        else:
            logging.warning(f"[RUN] MODE=reset but {destination_table} does not exist: creating it")
            art_utils.create_thematic_articles_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA)

    logging.info(f"[RUN] Processing from {start_date} to {END_DATE}")
    if start_date >= END_DATE:
        logging.warning("[END] start_date >= end_date: table already complete.")
        return
        
    counter_to_ingest = 0
    ingested = 0
    stopped_day = None
    
    ### For each day:
    ### 1. reading source table factiva articles
    ### 2. ingesting the result in destination table
    days = [start_date + timedelta(days=i) for i in range((END_DATE - start_date).days)]
    for day in days:
        # 1. Reading keywords
        try:
            logging.info(f"[RUN] ------ Processing day {day} ------")
            logging.info(f"[{SOURCE_TABLE}] {day}: reading source table")
            articles_day = art_utils.read_factiva_articles(
                                                            _con, kw_list,
                                                            day, day + timedelta(days=1),
                                                            SOURCE_TABLE, POSTGRES_DB)
                    
        except Exception:
            logging.exception(f"[{SOURCE_TABLE}] {day}: failed")
            stopped_day = day
            break

        if articles_day.empty:
            logging.info(f"[{SOURCE_TABLE}] {day}: no article found")
            continue
        
        n_expected_day = len(articles_day)
        if articles_day["an"].nunique() < n_expected_day:
            n_dup = n_expected_day - articles_day["an"].nunique()
            logging.error(f"[{SOURCE_TABLE}] {day}: {n_dup} duplicated 'an' in the articles read")
            stopped_day = day
            break
        logging.info(f"[{SOURCE_TABLE}] {day}: {n_expected_day} articles found")
        counter_to_ingest += n_expected_day

        if DRY_RUN:
            continue

        # 2. Ingesting into destination table
        n_expected_day = articles_day["an"].nunique()
        _con.begin()
        try:
            _con.execute(f"""
                INSERT INTO {POSTGRES_DB}.{DESTINATION_SCHEMA}.{destination_table} BY NAME
                SELECT *, now() AS created_at
                FROM articles_day
                ON CONFLICT (an) DO NOTHING
            """)

            # checks that every article of the day is now in the table
            n_in_table = _con.execute(f"""
                SELECT count(DISTINCT an) FROM {POSTGRES_DB}.{DESTINATION_SCHEMA}.{destination_table}
                WHERE an IN (SELECT an FROM articles_day)
            """).fetchone()[0]
            if n_in_table != n_expected_day:
                raise RuntimeError(
                    f"{day}: {n_in_table} articles found in the table after ingestion, "
                    f"{n_expected_day} expected"
                )
            _con.commit()
            logging.info(f"[{destination_table}] {day}: {n_in_table} articles ingested.")

            ingested += n_expected_day
        except Exception:
            _con.rollback()
            logging.exception(f"[{destination_table}] {day}: failed")
            stopped_day = day
            break        

    if DRY_RUN:
        logging.info(f"[END] DRY_RUN: {counter_to_ingest} articles would have been ingested")
    else:
        logging.info(f"[END] {ingested} articles inserted or ignored (already present rows are ignored)")
    if stopped_day:
        logging.error(
            f"[END] RUN STOPPED at day {stopped_day} (end_date requested: {END_DATE}, "
            f"so the run did NOT reach end_date). Fix the cause, then rerun with MODE=resume "
            f"to resume from the last ingested day."
        )
        raise RuntimeError(f"Run stopped at {stopped_day}, end_date {END_DATE} not reached")

    ### Checking results (in dry run the table may not exist)
    if not DRY_RUN:
        art_utils.check_articles(_con, POSTGRES_DB, destination_table, DESTINATION_SCHEMA)



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True
    )
    try:
        load_env()
        run()
    except Exception as e:
        logging.error(f"[RUN] ERROR: {e}")
        raise