### Creates a thematic table with
### - chunks that contain given keywords
### - neighbour chunks (+/- 2 minutes ) fetched in s3

### given folowing arguments
### - start_date [Optional]: if not start_date, resumes drom max date
### - end_date
### - thematic: keywords fetched in yaml
### - country (default france)

import logging
from datetime import date, timedelta
import duckdb
from pandas import Timedelta
import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote
import yaml

# only local env
ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env", override=False)

import quotaclimat.data_processing.thematic.thematic_keywords_utils as kw_utils
import quotaclimat.data_processing.thematic.thematic_utils as utils
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import channel_titles_france
from quotaclimat.data_processing.mediatree.i8n.belgium.channel_titles import channel_titles_belgium
from quotaclimat.data_processing.mediatree.i8n.belgium_flanders.channel_titles import channel_titles_belgium_flanders
 
# timedelta that defines the range in which a chunk is considered adjacent to core chunk
GAP = timedelta(minutes=2, seconds=30)
CHANNEL_TITLES = {'france': channel_titles_france, 'belgium': channel_titles_belgium, 'belgium_flanders': channel_titles_belgium_flanders}
LANGUAGES = {"france": "french", "belgium": "french", "belgium_flanders": "dutch"}
COUNTRIES = {"france": "fr","belgium": "be", "belgium_flanders": "befl" }
THEMATIC_YML = ROOT / "quotaclimat" / "data_processing" / "thematic" / "thematic.yml"
SCHEMA = "public"

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "S3_REGION",
    "END_DATE", "THEMATIC"
]


def load_env() -> None:
    """Read environment variables (credentials and parameters) and expose them as module globals.
    Raises if a required one is missing or invalid."""
    global START_DATE, END_DATE, THEMATIC, COUNTRY, LANGUAGE
    global DRY_RUN
    global SOURCE_TABLE, DESTINATION_TABLE
    global POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    global S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_REGION

    ### CHECK REQUIRED VARIABLES BEFORE USING ANY
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    ### DATES
    START_DATE, END_DATE = None, date.today()
    try:
        if os.getenv("START_DATE"):
            START_DATE = date.fromisoformat(os.environ["START_DATE"])
        if os.getenv("END_DATE"):
            END_DATE = date.fromisoformat(os.environ["END_DATE"])
    except ValueError as e:
        raise ValueError(f"START_DATE and END_DATE must be YYYY-MM-DD: {e}")
    if START_DATE and END_DATE <= START_DATE:
        raise ValueError("END_DATE must be after START_DATE")

    THEMATIC = os.getenv("THEMATIC")
    
    ### COUNTRY
    COUNTRY = (os.getenv("COUNTRY") or "france").lower()
    if COUNTRY not in LANGUAGES:
        raise ValueError(f"COUNTRY must be one of {list(LANGUAGES)}, got: {COUNTRY!r}")
    LANGUAGE = LANGUAGES[COUNTRY]

    ### FLAGS
    DRY_RUN = os.getenv("DRY_RUN", "").strip().lower() in ("1", "true", "yes", "y", "oui")

    ### TABLES
    SOURCE_TABLE = os.getenv("SOURCE_TABLE") or "public.keywords"  # env var just for local

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
        "[RUN]\n"
        f"start_date = {START_DATE}\n"
        f"end_date = {END_DATE}\n"
        f"thematic = {THEMATIC}\n"
        f"country = {COUNTRY}\n"
        f"language = {LANGUAGE}"
    )    
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")

    ### 1. Pure computation and validation, no DB access yet
    destination_table = f"{THEMATIC}_thematic_kw_{COUNTRIES[COUNTRY]}"
    is_new_run = START_DATE is not None
    start_date = START_DATE

    # <!> Asserting source_table won't be cleared
    if destination_table.lower() == SOURCE_TABLE.split(".")[-1].lower():
        raise ValueError(
            f"destination_table == SOURCE_TABLE ({destination_table}): "
            "the source table would be cleared"
        )

    kw_list = utils.get_keywords(THEMATIC_YML, THEMATIC)
    if not kw_list:
        raise ValueError(f"No keywords found for '{THEMATIC}' in {THEMATIC_YML}.")

    ### 2. Connection and source
    _con = duckdb.connect()
    utils.configure_barometre_source(_con, SOURCE_TABLE,
                                    POSTGRES_USER, POSTGRES_PASSWORD,
                                    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

    ### 3. Check existence BEFORE creating the table
    table_exists = kw_utils.destination_table_exists(_con, destination_table, POSTGRES_DB, SCHEMA)

    ### 4. Create the table if needed (new run only: a resume needs an existing one)
    if is_new_run:
        kw_utils.configure_barometre_thematic_keywords(_con, DRY_RUN,
                                            SOURCE_TABLE, destination_table,
                                            POSTGRES_DB, SCHEMA)

    ### 5. New run vs resume
    if is_new_run:
        logging.info(f"[RUN] NEW RUN with {start_date} and {THEMATIC}")
        if table_exists:
            try:
                utils.clear_destination_table(_con, DRY_RUN, destination_table, POSTGRES_DB, SCHEMA)
            except Exception as e:
                raise ValueError(f"[RUN] Failed to clear {destination_table}") from e
            logging.warning(f"[RUN] Table {destination_table} cleared")
    else:
        logging.info("[RUN] No start date given, attempting to resume.")
        if not table_exists:
            raise ValueError(
                f"[RUN] {destination_table} does not exist in {POSTGRES_DB}: "
                "rerun as a new run with START_DATE"
            )
        max_start = kw_utils.get_max_start_from_thematic_keywords(_con, POSTGRES_DB, destination_table)
        if max_start is None:
            raise ValueError("[END] Empty destination: rerun as a new run with START_DATE")
        start_date = max_start
        logging.info(f"Deducted start date from {destination_table}: {start_date}")

    logging.info(f"[RUN] Processing from {start_date} to {END_DATE}")
    if start_date >= END_DATE:
        logging.warning("[END] start_date >= end_date.")
        return
        
    ### Configuring s3 connection
    kw_utils._configure_s3(_con, S3_ENDPOINT_URL, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY)

    counter_to_ingest = 0
    ingested = 0
    stopped_day = None

    ### For each day:
    ### 1. reading source table keywords
    ### 2. fetching all needed days and channels on s3
    ### 3. extracting only thematic chunks and neighbour chunks (+/- 2 minutes)
    ### 4. ingesting the result in destination table
    day = start_date
    while day < END_DATE:
        # 1. Reading keywords
        try:
            logging.info(f"[RUN] ------ Processing day {day} ------")
            logging.info(f"[{SOURCE_TABLE}] {day}: reading source table")
            keywords_day = kw_utils.read_barometre_keywords(_con, kw_list,
                                                         day, day + timedelta(days=1),
                                                         COUNTRY, SOURCE_TABLE, POSTGRES_DB)
        except Exception:
            logging.exception(f"[{SOURCE_TABLE}] {day}: failed")
            stopped_day = day
            break

        if keywords_day.empty:
            logging.info(f"[{SOURCE_TABLE}] {day}: no chunk found")
            day += timedelta(days=1)
            continue
        
        logging.info(f"[{SOURCE_TABLE}] {day}: {len(keywords_day)} chunks found")

        # 2. Fetching data in s3, for channels with at least one chunk
        day_channels = keywords_day["channel_name"].unique().tolist()
        logging.info(f"[S3] {day}: Fetching s3 data on {len(day_channels)} channels from {len(keywords_day)} chunks")
        try:
            s3_day_df = kw_utils.read_day_from_s3(_con, day, day_channels, COUNTRY, S3_BUCKET_NAME)
        except Exception:
            logging.exception(f"[S3] {day}: failed to read S3")
            stopped_day = day
            break

        try:
            # 3. Extracting thematic chunks and neighbour chunks
            logging.info("[EXTRACT_ADJACENT_CHUNKS] Starting selection of chunks to keep (thematic + adjacent)")
            day_to_ingest_df = kw_utils.extract_adjacent_chunks_day(keywords_day, s3_day_df, GAP,
                                                                 COUNTRY, CHANNEL_TITLES)
            n_thematic_day = int(day_to_ingest_df["thematic_chunk"].sum())
            n_adjacent_day = len(day_to_ingest_df) - n_thematic_day
            logging.info(
                f"[{destination_table}] {day}: {len(day_to_ingest_df)} chunks selected for ingestion into destination "
                f"({n_thematic_day} thematic, {n_adjacent_day} adjacent)"
            )
            counter_to_ingest += len(day_to_ingest_df)
            n_expected_day = day_to_ingest_df["this_id"].nunique()

            if DRY_RUN:
                day += timedelta(days=1)
                continue

            # 4. Ingesting into destination table
            _con.begin()
            try:
                _con.execute(f"""
                    INSERT INTO {POSTGRES_DB}.{SCHEMA}.{destination_table} BY NAME
                    SELECT *, now() AT TIME ZONE 'Europe/Paris' AS created_at
                    FROM day_to_ingest_df
                    ON CONFLICT (this_id) DO NOTHING
                """)

                # checks if n_ingest == n_to_ingest
                n_in_table = _con.execute(f"""
                    SELECT count(*) FROM {POSTGRES_DB}.{SCHEMA}.{destination_table}
                    WHERE start >= ? AND start < ?
                      AND this_id IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
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

        day += timedelta(days=1)

    if DRY_RUN:
        logging.info(f"[END] DRY_RUN: {counter_to_ingest} chunks would have been ingested")
    else:
        logging.info(f"[END] {ingested} chunks inserted or ignored (already present rows are ignored)")
    if stopped_day:
        logging.error(
            f"[END] RUN STOPPED at day {stopped_day} (end_date requested: {END_DATE}, "
            f"so the run did NOT reach end_date). Fix the cause, then rerun with DESTINATION_TABLE and without START_DATE "
            f"to resume from the last ingested day."
        )
        raise RuntimeError(f"Run stopped at {stopped_day}, end_date {END_DATE} not reached")


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