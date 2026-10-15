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
from datetime import date, timedelta
import duckdb
from pandas import Timedelta
import os
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import quote

# only local env
load_dotenv(Path(__file__).resolve().parents[4] / ".env", override=False)

import quotaclimat.data_processing.mediatree.thematic_keywords.thematic_keywords_utils as utils
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import channel_titles_france
from quotaclimat.data_processing.mediatree.i8n.belgium.channel_titles import channel_titles_belgium
from quotaclimat.data_processing.mediatree.i8n.belgium_flanders.channel_titles import channel_titles_belgium_flanders

# timedelta that defines the range in which a chunk is considered adjacent to core chunk
GAP = Timedelta(minutes=2, seconds=30)
CHANNEL_TITLES = {'france': channel_titles_france, 'belgium': channel_titles_belgium, 'belgium_flanders': channel_titles_belgium_flanders}
LANGUAGES = {"france": "french", "belgium": "french", "belgium_flanders": "dutch"}

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "S3_REGION",
    "KEYWORDS", "END_DATE"
]

# Globals set by load_env():
# START_DATE, END_DATE, KEYWORDS, COUNTRY, LANGUAGE,
# CLEAR_DESTINATION_TABLE, ASSUME_YES, DRY_RUN,
# SOURCE_TABLE, DESTINATION_TABLE,
# POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
# S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, S3_BUCKET_NAME, S3_REGION


def load_env() -> None:
    """Read environment variables (credentials and parameters) and expose them as module globals.
    Raises if a required one is missing or invalid."""
    global START_DATE, END_DATE, KEYWORDS, COUNTRY, LANGUAGE
    global CLEAR_DESTINATION_TABLE, ASSUME_YES, DRY_RUN
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

    ### KEYWORDS
    KEYWORDS = [k.strip() for k in os.environ["KEYWORDS"].split(",") if k.strip()]
    if not KEYWORDS:
        raise ValueError("KEYWORDS is set but contains no keyword")

    ### COUNTRY
    COUNTRY = (os.getenv("COUNTRY") or "france").lower()
    if COUNTRY not in LANGUAGES:
        raise ValueError(f"COUNTRY must be one of {list(LANGUAGES)}, got: {COUNTRY!r}")
    LANGUAGE = LANGUAGES[COUNTRY]

    ### FLAGS
    true_values = ("1", "true", "yes", "y", "oui")
    CLEAR_DESTINATION_TABLE = os.getenv("CLEAR_DESTINATION_TABLE", "").strip().lower() in true_values
    ASSUME_YES = os.getenv("ASSUME_YES", "").strip().lower() in true_values
    DRY_RUN = os.getenv("DRY_RUN", "").strip().lower() in true_values

    ### TABLES
    SOURCE_TABLE = os.getenv("SOURCE_TABLE") or "public.keywords"  # env var just for local
    DESTINATION_TABLE = os.environ["DESTINATION_TABLE"]

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

    logging.info(f"[RUN] {START_DATE} -> {END_DATE}, {KEYWORDS}, {COUNTRY}, {LANGUAGE}")
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")

    _con = duckdb.connect()

    ### Checking keywords to process
    kw_dict = utils.get_keyword_ome_hrfp_theme(KEYWORDS, LANGUAGE)

    ### Configuring source DB
    utils.configure_barometre_source(_con, SOURCE_TABLE,
                                     POSTGRES_USER, POSTGRES_PASSWORD,
                                     POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)

    ### start_date: from env, or deduced from destination table (START_DATE global never modified)
    start_date = START_DATE
    if not start_date and CLEAR_DESTINATION_TABLE:
        raise ValueError("Destination table will be cleared. START_DATE required.")
    if not start_date:
        max_start = utils.get_max_start_date_from_destination(_con, POSTGRES_DB, DESTINATION_TABLE)
        if max_start is None:
            raise ValueError("Empty destination: provide START_DATE")
        start_date = max_start
        logging.info(f"Deducted start date from {DESTINATION_TABLE}: {start_date}")
        if not utils.confirm(f"No START_DATE: resuming at {start_date} (last day in table). "
                             f"Continue?", ASSUME_YES):
            raise SystemExit("Cancelled by user")

    if start_date >= END_DATE:
        logging.warning("[END] Deducted start_date >= end_date.")
        return

    if CLEAR_DESTINATION_TABLE and not DRY_RUN:
        if not utils.confirm(f"WARNING: {DESTINATION_TABLE} will be cleared. "
                             f"Continue?", ASSUME_YES):
            raise SystemExit("Cancelled by user")

    ### Configuring s3 connection
    utils._configure_s3(_con, S3_ENDPOINT_URL, S3_REGION, S3_ACCESS_KEY, S3_SECRET_KEY)
    ### Configuring destination table
    utils.configure_barometre_destination(_con, CLEAR_DESTINATION_TABLE, DRY_RUN,
                                          SOURCE_TABLE, DESTINATION_TABLE,
                                          POSTGRES_DB)

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
            keywords_day = utils.read_barometre_keywords(_con, kw_dict,
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
            s3_day_df = utils.read_day_from_s3(_con, day, day_channels, COUNTRY, S3_BUCKET_NAME)
        except Exception:
            logging.exception(f"[S3] {day}: failed to read S3")
            stopped_day = day
            break

        try:
            # 3. Extracting thematic chunks and neighbour chunks
            logging.info("[EXTRACT_ADJACENT_CHUNKS] Starting selection of chunks to keep (thematic + adjacent)")
            day_to_ingest_df = utils.extract_adjacent_chunks_day(keywords_day, s3_day_df, GAP,
                                                                 COUNTRY, CHANNEL_TITLES)
            n_thematic_day = int(day_to_ingest_df["thematic_chunk"].sum())
            n_adjacent_day = len(day_to_ingest_df) - n_thematic_day
            logging.info(
                f"[{DESTINATION_TABLE}] {day}: {len(day_to_ingest_df)} chunks selected for ingestion into destination "
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
                    INSERT INTO {POSTGRES_DB}.{DESTINATION_TABLE} BY NAME
                    SELECT *, now() AT TIME ZONE 'Europe/Paris' AS created_at
                    FROM day_to_ingest_df
                    ON CONFLICT (this_id) DO NOTHING
                """)

                # checks if consistent table (if extra, probably previous runs with other kw)
                n_extra = _con.execute(f"""
                    SELECT count(*) FROM {POSTGRES_DB}.{DESTINATION_TABLE}
                    WHERE start >= ? AND start < ?
                      AND this_id NOT IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
                if n_extra:
                    logging.warning(
                        f"[{DESTINATION_TABLE}] {day}: {n_extra} chunks already in table "
                        f"and missing from this run ({n_expected_day} expected), likely ingested with other keywords"
                    )
                else:
                    logging.info(f"[{DESTINATION_TABLE}] {day}: {n_expected_day} chunks ingested. Table consistent.")

                # checks if n_ingest == n_to_ingest
                n_in_table = _con.execute(f"""
                    SELECT count(*) FROM {POSTGRES_DB}.{DESTINATION_TABLE}
                    WHERE start >= ? AND start < ?
                      AND this_id IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
                if n_in_table != n_expected_day:
                    raise RuntimeError(
                        f"{day}: {n_in_table} chunks found in the table after ingestion, "
                        f"{n_expected_day} expected"
                    )
                _con.commit()
                ingested += len(day_to_ingest_df)
            except Exception:
                _con.rollback()
                raise

        except Exception:
            logging.exception(f"[{DESTINATION_TABLE}] {day}: failed")
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
            f"so the run did NOT reach end_date). Fix the cause, then rerun without START_DATE "
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