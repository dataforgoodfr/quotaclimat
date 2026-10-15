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

import os
from datetime import date
from urllib.parse import quote

LANGUAGES = {"france": "french", "belgium": "french", "belgium_flanders": "dutch"}

REQUIRED_ENV = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
    "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_ENDPOINT_URL", "S3_BUCKET_NAME", "S3_REGION",
    "KEYWORDS", "DESTINATION_TABLE",
]


def load_configuration() -> dict:
    """Read environment variables (credentials and parameters) and return them in a dict.
    Raises if a required one is missing or invalid."""

    ### CHECK REQUIRED VARIABLES BEFORE USING ANY
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

    ### DATES
    start_date, end_date = None, date.today()
    try:
        if os.getenv("START_DATE"):
            start_date = date.fromisoformat(os.environ["START_DATE"])
        if os.getenv("END_DATE"):
            end_date = date.fromisoformat(os.environ["END_DATE"])
    except ValueError as e:
        raise ValueError(f"START_DATE and END_DATE must be YYYY-MM-DD: {e}")
    if start_date and end_date <= start_date:
        raise ValueError("END_DATE must be after START_DATE")

    ### KEYWORDS
    keywords = [k.strip() for k in os.environ["KEYWORDS"].split(",") if k.strip()]
    if not keywords:
        raise ValueError("KEYWORDS is set but contains no keyword")

    ### COUNTRY
    country = (os.getenv("COUNTRY") or "france").lower()
    if country not in LANGUAGES:
        raise ValueError(f"COUNTRY must be one of {list(LANGUAGES)}, got: {country!r}")

    ### S3 CREDENTIALS (if they look like file paths, read the file content)
    s3_access_key = os.environ["S3_ACCESS_KEY"]
    s3_secret_key = os.environ["S3_SECRET_KEY"]
    if s3_access_key.startswith("/"):
        with open(s3_access_key, "r") as f:
            s3_access_key = f.read().strip()
    if s3_secret_key.startswith("/"):
        with open(s3_secret_key, "r") as f:
            s3_secret_key = f.read().strip()

    true_values = ("1", "true", "yes", "y", "oui")

    return {
        ### PARAMETERS
        "START_DATE": start_date,
        "END_DATE": end_date,
        "KEYWORDS": keywords,
        "COUNTRY": country,
        "LANGUAGE": LANGUAGES[country],
        "CLEAR_DESTINATION_TABLE": os.getenv("CLEAR_DESTINATION_TABLE", "").strip().lower() in true_values,
        "ASSUME_YES": os.getenv("ASSUME_YES", "").strip().lower() in true_values,
        "DRY_RUN": os.getenv("DRY_RUN", "").strip().lower() in true_values,

        ### TABLES
        "SOURCE_TABLE": os.getenv("SOURCE_TABLE") or "public.keywords",  # env var just for local
        "DESTINATION_TABLE": os.environ["DESTINATION_TABLE"],

        ### BAROMETRE CREDENTIALS (single database: source and destination)
        "POSTGRES_USER": os.environ["POSTGRES_USER"],
        "POSTGRES_PASSWORD": quote(os.environ["POSTGRES_PASSWORD"], safe=""),
        "POSTGRES_HOST": os.environ["POSTGRES_HOST"],
        "POSTGRES_PORT": os.environ["POSTGRES_PORT"],
        "POSTGRES_DB": os.environ["POSTGRES_DB"],

        ### S3 CREDENTIALS
        "S3_ACCESS_KEY": s3_access_key,
        "S3_SECRET_KEY": s3_secret_key,
        "S3_ENDPOINT_URL": os.environ["S3_ENDPOINT_URL"],
        "S3_BUCKET_NAME": os.environ["S3_BUCKET_NAME"],
        "S3_REGION": os.environ["S3_REGION"],
    }  

def run(cfg: dict) -> None:

    logging.info(f"[RUN] {cfg['START_DATE']} -> {cfg['END_DATE']}, {cfg['KEYWORDS']}, {cfg['COUNTRY']}, {cfg['LANGUAGE']}")
    logging.warning(f"[RUN] DRY_RUN={cfg['DRY_RUN']}")

    _con = duckdb.connect()

    ### Checking keywords to process
    kw_dict = utils.get_keyword_ome_hrfp_theme(cfg["KEYWORDS"], cfg["LANGUAGE"])

    ### Configuring source DB
    utils.configure_barometre_source(_con, cfg["SOURCE_TABLE"],
                                     cfg["POSTGRES_USER"], cfg["POSTGRES_PASSWORD"],
                                     cfg["POSTGRES_HOST"], cfg["POSTGRES_PORT"], cfg["POSTGRES_DB"])

    ### start_date: from config, or deduced from destination table (never modify cfg)
    start_date = cfg["START_DATE"]
    if not start_date and cfg["CLEAR_DESTINATION_TABLE"]:
        raise ValueError("Destination table will be cleared. START_DATE required.")
    if not start_date:
        max_start = utils.get_max_start_date_from_destination(_con, cfg["POSTGRES_DB"], cfg["DESTINATION_TABLE"])
        if max_start is None:
            raise ValueError("Empty destination: provide START_DATE")
        start_date = max_start
        logging.info(f"Deducted start date from {cfg['DESTINATION_TABLE']}: {start_date}")
        if not utils.confirm(f"No START_DATE: resuming at {start_date} (last day in table). "
                             f"Continue?", cfg["ASSUME_YES"]):
            raise SystemExit("Cancelled by user")

    if start_date >= cfg["END_DATE"]:
        logging.warning("[END] Deducted start_date >= end_date.")
        return

    if cfg["CLEAR_DESTINATION_TABLE"] and not cfg["DRY_RUN"]:
        if not utils.confirm(f"WARNING: {cfg['DESTINATION_TABLE']} will be cleared. "
                             f"Continue?", cfg["ASSUME_YES"]):
            raise SystemExit("Cancelled by user")

    ### Configuring s3 connection
    utils._configure_s3(_con, cfg["S3_ENDPOINT_URL"], cfg["S3_REGION"],
                        cfg["S3_ACCESS_KEY"], cfg["S3_SECRET_KEY"])
    ### Configuring destination table
    utils.configure_barometre_destination(_con, cfg["CLEAR_DESTINATION_TABLE"], cfg["DRY_RUN"],
                                          cfg["SOURCE_TABLE"], cfg["DESTINATION_TABLE"], 
                                          cfg["POSTGRES_DB"])

    counter_to_ingest = 0
    ingested = 0
    stopped_day = None

    ### For each day:
    ### 1. reading source table keywords
    ### 2. fetching all needed days and channels on s3
    ### 3. extracting only thematic chunks and neighbour chunks (+/- 2 minutes)
    ### 4. ingesting the result in destination table
    day = start_date
    while day < cfg["END_DATE"]:
        # 1. Reading keywords
        try:
            logging.info(f"[RUN] ------ Processing day {day} ------")
            logging.info(f"[{cfg['SOURCE_TABLE']}] {day}: reading source table")
            keywords_day = utils.read_barometre_keywords(_con, kw_dict,
                                                         day, day + timedelta(days=1),
                                                         cfg["COUNTRY"], cfg["SOURCE_TABLE"], cfg["POSTGRES_DB"])
        except Exception:
            logging.exception(f"[{cfg['SOURCE_TABLE']}] {day}: failed")
            stopped_day = day
            break

        if keywords_day.empty:
            logging.info(f"[{cfg['SOURCE_TABLE']}] {day}: no chunk found")
            day += timedelta(days=1)
            continue
        n_core = int(keywords_day["thematic_core_chunk"].sum())
        logging.info(f"[{cfg['SOURCE_TABLE']}] {day}: {len(keywords_day)} chunks found, {n_core} thematic core chunks")

        # 2. Fetching data in s3, only for channels with at least one core chunk
        s3_day_df = None
        if n_core:
            logging.info(f"[S3] {day}: Fetching s3 data on days and channels from {n_core} core chunks")
            day_channels = (keywords_day.loc[keywords_day["thematic_core_chunk"], "channel_name"]
                            .unique().tolist())
            try:
                s3_day_df = utils.read_day_from_s3(_con, day, day_channels, cfg["COUNTRY"], cfg["S3_BUCKET_NAME"])
            except Exception:
                logging.exception(f"[S3] {day}: failed to read S3")
                stopped_day = day
                break

        try:
            # 3. Extracting core chunks, neighbour chunks and non-core chunks
            logging.info("[EXTRACT_ADJACENT_CHUNKS] Starting selection of chunks to keep (core + adjacent_to_core + others)")
            day_to_ingest_df = utils.extract_adjacent_chunks_day(keywords_day, s3_day_df, GAP, 
                                                                 cfg["COUNTRY"], CHANNEL_TITLES)
            n_core_day = int(day_to_ingest_df["thematic_core_chunk"].sum())
            n_adjacent_day = int(day_to_ingest_df["adjacent_to_thematic_core"].sum())
            n_other_day = len(day_to_ingest_df) - n_core_day - n_adjacent_day
            logging.info(
                f"[{cfg['DESTINATION_TABLE']}] {day}: {len(day_to_ingest_df)} chunks selected for ingestion into destination "
                f"({n_core_day} core, {n_adjacent_day} adjacent, {n_other_day} other)"
            )
            counter_to_ingest += len(day_to_ingest_df)
            n_expected_day = day_to_ingest_df["this_id"].nunique()

            if cfg["DRY_RUN"]:
                day += timedelta(days=1)
                continue

            # 4. Ingesting into destination table
            _con.begin()
            try:
                _con.execute(f"""
                    INSERT INTO {cfg['POSTGRES_DB']}.{cfg['DESTINATION_TABLE']} BY NAME
                    SELECT *, now() AT TIME ZONE 'Europe/Paris' AS created_at
                    FROM day_to_ingest_df
                    ON CONFLICT (this_id) DO NOTHING
                """)

                # checks if consistent table (if extra, probably previous runs with other kw)
                n_extra = _con.execute(f"""
                    SELECT count(*) FROM {cfg['POSTGRES_DB']}.{cfg['DESTINATION_TABLE']}
                    WHERE start >= ? AND start < ?
                      AND this_id NOT IN (SELECT this_id FROM day_to_ingest_df)
                """, [day, day + timedelta(days=1)]).fetchone()[0]
                if n_extra:
                    logging.warning(
                        f"[{cfg['DESTINATION_TABLE']}] {day}: {n_extra} chunks already in table "
                        f"and missing from this run ({n_expected_day} expected), likely ingested with other keywords"
                    )
                else:
                    logging.info(f"[{cfg['DESTINATION_TABLE']}] {day}: {n_expected_day} chunks ingested. Table consistent.")

                # checks if n_ingest == n_to_ingest
                n_in_table = _con.execute(f"""
                    SELECT count(*) FROM {cfg['POSTGRES_DB']}.{cfg['DESTINATION_TABLE']}
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
            logging.exception(f"[{cfg['DESTINATION_TABLE']}] {day}: failed")
            stopped_day = day
            break

        day += timedelta(days=1)

    if cfg["DRY_RUN"]:
        logging.info(f"[END] DRY_RUN: {counter_to_ingest} chunks would have been ingested")
    else:
        logging.info(f"[END] {ingested} chunks inserted or ignored (already present rows are ignored)")
    if stopped_day:
        logging.error(
            f"[END] RUN STOPPED at day {stopped_day} (end_date requested: {cfg['END_DATE']}, "
            f"so the run did NOT reach end_date). Fix the cause, then rerun without START_DATE "
            f"to resume from the last ingested day."
        )
        raise RuntimeError(f"Run stopped at {stopped_day}, end_date {cfg['END_DATE']} not reached")

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler()],
        force=True
    )
    try:
        run(load_configuration())
    except Exception as e:
        logging.error(f"[RUN] ERROR: {e}")
        raise