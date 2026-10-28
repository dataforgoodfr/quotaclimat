### Fills a thematic table {thematic}_keywords_{country} (e.g. incendies_keywords_fr) with,
### for every period of the thematic and country in thematic_jobs_config.yml (column period):
### - chunks that contain given keywords
### - neighbour chunks (+/- GAP) fetched in s3

### given following environment variables
### - THEMATIC (mandatory): keywords and periods fetched in yaml (period start included, end excluded)
### - COUNTRY (default france): one of utils.COUNTRIES
### - DRY_RUN (default false): reads and counts, writes nothing
### - SOURCE_TABLE (default public.keywords): local only, e.g. public.test_keywords
### - MODE (default new): one of MODES
###     - new: creates the table, raises if it already exists
###     - resume: restarts each period from its last ingested day (a period without rows from its start),
###       raises if the table does not exist. Use it to add a period to an existing table.
###       Changing the dates or id of an ingested period requires MODE=reset.
###     - reset: clears the table (or creates it) and restarts from the start of each period

import logging
from datetime import timedelta
import duckdb
import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote

import quotaclimat.data_processing.thematic_extracts.thematic_keywords_utils as kw_utils
import quotaclimat.data_processing.thematic_extracts.thematic_utils as utils

# only local env
ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env", override=False)
THEMATIC_YML = ROOT / "quotaclimat" / "data_processing" / "thematic_extracts" / "thematic_jobs_config.yml"

# timedelta that defines the range in which a chunk is considered adjacent to core chunk
GAP = timedelta(minutes=2, seconds=30)

SOURCE_TABLE = "public.keywords"
DESTINATION_SCHEMA = "thematic_extracts"
# Postgres users granted SELECT on the destination table at its creation (quoted: names contain hyphens)
USERS_TO_GRANT = ['"climateguard-reader-user"', '"metabase-analytics-prod"']
MEDIA_TYPE="audiovisuel"
MODES = ("new", "resume", "reset")

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "S3_REGION",
    "THEMATIC"
]


def load_env() -> None:
    """Read environment variables (credentials and parameters) and expose them as module globals.
    Raises if a required one is missing or invalid."""
    global THEMATIC, COUNTRY, COUNTRY_CONF, DRY_RUN
    global SOURCE_TABLE
    global POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    global S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_REGION
    global MODE
    ### CHECK REQUIRED VARIABLES BEFORE USING ANY
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    ### Keywords to look for from thematic
    THEMATIC = os.environ["THEMATIC"]
    
    ### COUNTRY AND LANGUAGE
    COUNTRY = (os.getenv("COUNTRY") or "france").strip().lower()
    if COUNTRY not in utils.COUNTRIES:
        raise ValueError(f"COUNTRY must be one of {list(utils.COUNTRIES)}, got: {COUNTRY!r}")
    COUNTRY_CONF = utils.COUNTRIES[COUNTRY]

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

    ### S3 CREDENTIALS (if they look like file paths, read the file content)
    S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
    S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
    if S3_ACCESS_KEY.startswith("/"):
        with open(S3_ACCESS_KEY, "r") as f:
            S3_ACCESS_KEY = f.read().strip()
    if S3_SECRET_KEY.startswith("/"):
        with open(S3_SECRET_KEY, "r") as f:
            S3_SECRET_KEY = f.read().strip()
    S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
    S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
    S3_REGION = os.environ["S3_REGION"]


def run() -> None:

    logging.info(
        "[PARAMS]\n"
        f"THEMATIC = {THEMATIC}\n"
        f"COUNTRY = {COUNTRY} (deducted LANGUAGE = {COUNTRY_CONF['language']})\n"
        f"MODE={MODE}\n"
    )    
    logging.warning(f"[PARAMS] DRY_RUN={DRY_RUN}\n")

    destination_table=utils.make_destination_table_name(THEMATIC, MEDIA_TYPE, COUNTRY_CONF["country_short"])
    logging.info(f"[RUN] MODE={MODE} on {destination_table}")

    ## Getting periods (end excluded)
    periods = utils.get_periods(THEMATIC_YML, THEMATIC, COUNTRY)
    logging.info("[RUN] Periods: " + ", ".join(f"{p['id']} ({p['start']} -> {p['end']})" for p in periods))

    ## Getting kw
    kw_list = utils.get_keywords(THEMATIC_YML, THEMATIC, MEDIA_TYPE, COUNTRY_CONF["language"])
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
        kw_utils.create_thematic_keywords_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA, USERS_TO_GRANT)

    ### RESUME: the table must exist, restart from its last ingested day
    elif MODE == "resume":
        if not table_exists:
            raise ValueError(
                f"[RUN] {destination_table} does not exist: nothing to resume "
                "(check THEMATIC / COUNTRY, or rerun with MODE=new)"
            )
        logging.info("[RUN] Resuming each period from its last ingested day")


    ### RESET: empty the table (or create it) and restart from the start of each period
    elif MODE == "reset":
        if table_exists:
            logging.warning(f"[RUN] Reset mode. Destination table {destination_table} will be cleared.")
            try:
                utils.clear_destination_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA)
            except Exception as e:
                raise ValueError(f"[RUN] Failed to clear {destination_table}") from e
        else:
            logging.warning(f"[RUN] MODE=reset but {destination_table} does not exist: creating it")
            kw_utils.create_thematic_keywords_table(_con, DRY_RUN, destination_table, POSTGRES_DB, DESTINATION_SCHEMA, USERS_TO_GRANT)

    ### Configuring s3 connection
    kw_utils._configure_s3(_con, S3_ENDPOINT_URL, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY)

    counter_to_ingest = 0
    n_plaintext_mismatch = 0
    ingested = 0
    stopped_day = None

    ### For each period, for each day:
    ### 1. reading source table keywords
    ### 2. fetching all needed days and channels on s3
    ### 3. extracting only thematic chunks and neighbour chunks (+/- 2 minutes)
    ### 4. ingesting the result in destination table
    for period in periods:
        start_date = period["start"]
        if MODE == "resume":
            # last ingested day of the period is reprocessed: ON CONFLICT DO NOTHING makes it safe
            max_start = kw_utils.get_max_start_from_thematic_keywords(
                _con, POSTGRES_DB, destination_table, DESTINATION_SCHEMA, period["id"])
            if max_start:
                start_date = max(start_date, max_start)
            logging.info(f"[RUN] Period {period['id']}: resuming from {start_date}")
        end_date = period["end"]  # excluded
        if start_date >= end_date:
            logging.info(f"[RUN] Period {period['id']}: already complete")
            continue
        logging.info(f"[RUN] ====== Period {period['id']}: from {start_date} to {end_date} (excluded) ======")
        days = [start_date + timedelta(days=i) for i in range((end_date - start_date).days)]
        for day in days:
            # 1. Reading keywords
            try:
                logging.info(f"[RUN] ------ Processing day {day} ------")
                logging.info(f"[{SOURCE_TABLE}] {day}: reading source table")

                keywords_day_df = kw_utils.read_barometre_keywords(_con, kw_list,
                                                             day, day + timedelta(days=1),
                                                             COUNTRY_CONF["country"], COUNTRY_CONF["channels"],
                                                             SOURCE_TABLE, POSTGRES_DB)
            except Exception:
                logging.exception(f"[{SOURCE_TABLE}] {day}: failed")
                stopped_day = day
                break

            if keywords_day_df.empty:
                logging.info(f"[{SOURCE_TABLE}] {day}: no chunk found")
                continue
        
            logging.info(f"[{SOURCE_TABLE}] {day}: {len(keywords_day_df)} chunks found")

            # 2. Fetching data in s3, for channels with at least one chunk
            day_channels = keywords_day_df["channel_name"].unique().tolist()
            logging.info(f"[S3] {day}: Fetching s3 data on {len(day_channels)} channels from {len(keywords_day_df)} chunks")
            try:
                s3_day_df = kw_utils.read_day_from_s3(_con, day, day_channels, COUNTRY_CONF["country"], S3_BUCKET_NAME)
            except Exception:
                logging.exception(f"[S3] {day}: failed to read S3")
                stopped_day = day
                break

            try:
                # 3. Extracting thematic chunks and neighbour chunks
                logging.info("[KEYWORDS/S3] Starting selection of chunks to keep (thematic + adjacent)")
                day_to_ingest_df, n_mismatch_day = kw_utils.extract_adjacent_chunks_day(keywords_day_df, s3_day_df, GAP)
                n_plaintext_mismatch += n_mismatch_day
                # COUNTRY of the job kept as a control, period id to tell periods apart in the table
                day_to_ingest_df = day_to_ingest_df.assign(country=COUNTRY_CONF["country"], period=period["id"])
                n_thematic_day = int(day_to_ingest_df["thematic_chunk"].sum())
                n_adjacent_day = len(day_to_ingest_df) - n_thematic_day
                logging.info(
                    f"[{destination_table}] {day}: {len(day_to_ingest_df)} chunks selected for ingestion into destination "
                    f"({n_thematic_day} thematic, {n_adjacent_day} adjacent)"
                )
                counter_to_ingest += len(day_to_ingest_df)
                n_expected_day = day_to_ingest_df["this_id"].nunique()

                if DRY_RUN:
                    continue

                # 4. Ingesting into destination table
                _con.begin()
                try:
                    _con.execute(f"""
                        INSERT INTO {POSTGRES_DB}.{DESTINATION_SCHEMA}.{destination_table} BY NAME
                        SELECT *, now() AS created_at
                        FROM day_to_ingest_df
                        ON CONFLICT (this_id) DO NOTHING
                    """)

                    # checks if n_ingest == n_to_ingest
                    n_in_table = _con.execute(f"""
                        SELECT count(*) FROM {POSTGRES_DB}.{DESTINATION_SCHEMA}.{destination_table}
                        WHERE start >= TIMESTAMP '{day}' AT TIME ZONE 'Europe/Paris'
                            AND start <  TIMESTAMP '{day + timedelta(days=1)}' AT TIME ZONE 'Europe/Paris'
                            AND this_id IN (SELECT this_id FROM day_to_ingest_df)
                    """).fetchone()[0]
                    if n_in_table != n_expected_day:
                        raise RuntimeError(
                            f"{day}: {n_in_table} chunks found in the table after ingestion, "
                            f"{n_expected_day} expected"
                        )
                    _con.commit()
                    logging.info(f"[{destination_table}] {day}: {n_in_table} chunks ingested.")

                    ingested += len(day_to_ingest_df)
                except Exception:
                    _con.rollback()
                    raise

            except Exception:
                logging.exception(f"[{destination_table}] {day}: failed")
                stopped_day = day
                break
        if stopped_day:
            break

    if DRY_RUN:
        logging.info(f"[END] DRY_RUN: {counter_to_ingest} chunks would have been ingested")
    else:
        logging.info(f"[END] {ingested} chunks inserted or ignored (already present rows are ignored)")
    if stopped_day:
        logging.error(
            f"[END] RUN STOPPED at day {stopped_day} (period {period['id']}, end requested: {period['end']}, "
            f"so the run did NOT reach the end of the periods). Fix the cause, then rerun with MODE=resume "
            f"to resume from the last ingested day."
        )
        raise RuntimeError(f"Run stopped at {stopped_day}, period {period['id']} not completed")
    
    ### Checking results
    kw_utils.check_keywords(_con, POSTGRES_DB, destination_table, DESTINATION_SCHEMA, GAP, DRY_RUN, SOURCE_TABLE, n_plaintext_mismatch)
    

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