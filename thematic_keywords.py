import logging
import os
from pathlib import Path
import urllib.parse
from dotenv import load_dotenv
import duckdb
from datetime import date, timedelta
import pandas as pd
import numpy as np
import argparse


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import (
    channel_titles_france,
)

### BAROMETRE CREDENTIALS (une seule base : source et destination)

PG_USER = os.getenv("BAROMETRE_PG_USER")
PG_PASSWORD = os.getenv("BAROMETRE_PG_PASSWORD")
PG_HOST = os.getenv("BAROMETRE_PG_HOST")
PG_PORT = os.getenv("BAROMETRE_PG_PORT")
PG_DATABASE = os.getenv("BAROMETRE_PG_DATABASE")
DESTINATION_TABLE=os.getenv("DESTINATION_TABLE")
SOURCE_TABLE=os.getenv("SOURCE_TABLE")
DB_ALIAS = 'barometre'
SOURCE_ALIAS = DB_ALIAS
DESTINATION_ALIAS = DB_ALIAS

### S3 CREDENTIALS
REGION = "fr-par"
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BUCKET_NAME = "mediatree"


### GLOBALS
DRY_RUN = os.getenv("DRY_RUN", "").lower() in ("1", "true", "yes")
GAP = "2 minutes 30 seconds"
ALL_CHANNELS_FR=channel_titles_france


def get_keyword_ome_hrfp_theme(keywords_to_look_for: list, language: str) -> dict:
    keywords_lower = {m.lower() for m in keywords_to_look_for}
    try:
        logging.info("[OME DICTIONNARY] Starting research on keywords in OME dictionnary")
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
            logging.info("[OME DICTIONNARY] Keywords not found in OME dictionnary : %s", not_found)
        else:
            logging.info("[OME DICTIONNARY] All keywords found in OME dictionnar")
        if keywords_hrfp_theme:
            logging.info("[OME DICTIONNARY] Starting research on keywords on : %s", list(keywords_hrfp_theme.keys()))

    except Exception as e:
        logging.error("[OME DICTIONNARY] ❌ ERROR READING OME DICTIONNARY :")
        logging.error(e)
        raise e
    if not keywords_hrfp_theme:
        raise ValueError("No keywords found in OME dictionnary")
    
    return keywords_hrfp_theme



def _barometre_dsn() -> str:
    return f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"


def configure_barometre_source(con: duckdb.DuckDBPyConnection):
    """Attache la base Barometre (une seule fois : sert à la source et à la destination)."""
    try:
        # Installer et charger l'extension Postgres pour DuckDB
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{_barometre_dsn()}' AS {DB_ALIAS} (TYPE POSTGRES);")
        logging.info("[BAROMETRE_KEYWORDS] ✅ CONNECTION OK")
        
    except Exception as e:
        logging.error("[BAROMETRE_KEYWORDS] ❌ CONNECTION ERROR :")
        logging.error(e)
        raise e
        


def configure_barometre_destination(con: duckdb.DuckDBPyConnection) -> None:
    """La base est déjà attachée par configure_barometre_source : on prépare seulement la table."""
    logging.info("[BAROMETRE_DESTINATION] CONFIGURATION")
    try:
        logging.info(f"[BAROMETRE_DESTINATION] Using {PG_HOST}:{PG_PORT}/{PG_DATABASE} as {PG_USER}")
        if DESTINATION_TABLE == SOURCE_TABLE:
            raise ValueError(
                f"DESTINATION_TABLE == SOURCE_TABLE ({DESTINATION_TABLE}) : "
                "la table source serait vidée par le DELETE"
            )
        if not DRY_RUN:
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {DESTINATION_ALIAS}.{DESTINATION_TABLE} (
                    this_id VARCHAR NOT NULL,
                    channel_name VARCHAR NOT NULL,
                    channel_title VARCHAR NOT NULL,
                    start TIMESTAMP NOT NULL,
                    plaintext VARCHAR NOT NULL,
                    core_chunk BOOLEAN NOT NULL,                
                    PRIMARY KEY (this_id)
                )
            """)
            logging.info("[BAROMETRE_DESTINATION] TABLE CREATED OR ALREADY EXISTS")
            con.execute(
                f"DELETE FROM {DESTINATION_ALIAS}.{DESTINATION_TABLE} "
            )
            logging.info(f"[BAROMETRE_DESTINATION] TABLE {DESTINATION_TABLE} CLEARED")
    except Exception as e:
        logging.error("❌ ERROR CONNECTING TO BAROMETRE DESTINATION :")
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
            SELECT k.id, k.channel_name, k.start,
                unnest(from_json(k.keywords_with_timestamp::JSON,
                        '[{{"keyword":"VARCHAR","theme":"VARCHAR"}}]')) AS kwt
            FROM {SOURCE_ALIAS}.public.{SOURCE_TABLE} k
            WHERE k.start >= '{start_date}' AND k.start < '{end_date}'
                AND k.country = '{country}'
        )
        SELECT DISTINCT e.id, e.channel_name, e.start--, e.kwt.keyword, e.kwt.theme
        FROM exploded e
        WHERE e.kwt.keyword IN ({kewords_sql})
            -- keeping only direct keywords -> either no hrfp (no "_indirectes" suffix), or granted hrfp with suffix deleted
            -- a keyword can have several themes, we keep the chunk if at least one is direct
            AND NOT ends_with(e.kwt.theme, '_indirectes')
        ORDER BY e.start, e.channel_name
    """).df()
        logging.info(f"[BAROMETRE_KEYWORDS] {out.shape[0]} core chunks found in {SOURCE_TABLE} table")
        return out
    except Exception as e:
        logging.error("[BAROMETRE_KEYWORDS] ❌ ERROR READING KEYWORDS :")
        logging.error(e)
        raise e



# SOURCE: GET S3 CREDENTIALS
def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    logging.info("[S3] CONFIGURATION")
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            SET s3_region='{REGION}';
            SET s3_endpoint='s3.{REGION}.scw.cloud';
            SET s3_access_key_id='{ACCESS_KEY}';
            SET s3_secret_access_key='{SECRET_KEY}';
            SET s3_url_style='path';
        """)
        logging.info("[S3] ✅ CONFIGURATION OK")

    except Exception as e:
        logging.error("[S3] ❌ CONFIGURATION ERROR :")
        logging.error(e)
        raise e
    
    
# SOURCE - READ FROM S3 : ONE DAY AT A TIME
def read_day_from_s3(con: duckdb.DuckDBPyConnection, day: date, channels: list[str]) -> pd.DataFrame:
    """Read parquet files for a specific day and channels : channel_name, start, plaintext."""
    
    globs = ", ".join(
        f"'s3://{BUCKET_NAME}/year={day.year}/month={day.month}/day={day.day}"
        f"/channel={ch}/*.parquet'"
        for ch in channels
    )
    try:
        files = [r[0] for r in con.sql(f"SELECT file FROM glob([{globs}])").fetchall()]
        if not files:
            raise FileNotFoundError(f"No parquet found on S3 for {day}")
        # Chaînes sans aucun fichier ce jour-là
        missing = [ch for ch in channels if not any(f"/channel={ch}/" in f for f in files)]
        if missing:
            logging.warning(f"[S3] No S3 data for {day} : {', '.join(missing)}")

        file_list = ", ".join(f"'{f}'" for f in files)
        df = con.sql(
            f"SELECT channel_name, start, plaintext FROM read_parquet([{file_list}], union_by_name=true)"
        ).df()

        # l'API mediatree utilise sud-radio ou sudradio : on uniformise avant de dédoublonner
        df["channel_name"] = df["channel_name"].replace("sudradio", "sud-radio")
        if df.empty:
            logging.warning(f"[S3] No S3 chunks for {day}")
            raise FileNotFoundError(f"No S3 chunks for {day}")
        else:
            logging.info(f"[S3] {day} : {len(df)} chunks read from S3")
            
    except Exception as e:
        logging.error(f"[S3] ❌ ERROR READING S3 FOR {day} : {e}")
        raise e 
        
    return df.drop_duplicates(subset=["channel_name", "start"], keep="last").reset_index(drop=True)





def extract_adjacent_chunks_day(core_chunks: pd.DataFrame, s3_day_df: pd.DataFrame, gap: str) -> pd.DataFrame:
    try:
        gap = pd.Timedelta(gap).to_timedelta64()
        keys = ["start", "channel_name"]
        core_keys = core_chunks[keys].drop_duplicates().assign(core_chunk=True)

        parts = []
        for channel, s3_chan in s3_day_df.groupby("channel_name"):
            core_starts = core_keys.loc[core_keys["channel_name"] == channel, "start"].to_numpy()
            s3_starts = s3_chan["start"].to_numpy()

            # écart de chaque chunk S3 avec chaque core de la chaîne : matrice (n_s3 x n_core)
            ecart = np.abs(s3_starts[:, None] - core_starts[None, :])

            # on garde le chunk s'il est à moins de gap d'au moins un core (le core lui-même, écart 0, est inclus)
            parts.append(s3_chan[(ecart <= gap).any(axis=1)])

        out = pd.concat(parts, ignore_index=True)

        # core_chunk = True si la clé est dans core_chunks, False si simple voisin
        out = out.merge(core_keys, on=keys, how="left")
        out["core_chunk"] = out["core_chunk"].eq(True)
        
        # id = clé primaire : chaîne + start en UTC (sans ambiguïté lors des changements d'heure)
        out["this_id"] = (out["channel_name"] + "_"
                    + out["start"].dt.tz_convert("UTC").dt.strftime("%Y%m%d%H%M%S"))
        out["channel_title"] = out["channel_name"].map(channel_titles_france)

        cols = ["this_id", "channel_name","channel_title", "start", "plaintext", "core_chunk"]
        return out.sort_values(keys)[cols].reset_index(drop=True)
    
    except Exception as e:
        logging.error(f"[EXTRACT_ADJACENT_CHUNKS] ❌ ERROR EXTRACTING ADJACENT CHUNKS : {e}")
        raise e
    


def run(start_date: date, end_date: date,
        keywords_to_look_for: list,
        country: str, language: str) -> None:
    logging.info("------------------------------")
    logging.info(f"[RUN] {start_date} -> {end_date}, {keywords_to_look_for}, {country}, {language}")
    logging.warning(f"[RUN] DRY_RUN={DRY_RUN}")

    _con = duckdb.connect()

    # Les erreurs de ces étapes sont loggées dans les fonctions et remontent à __main__
    kw_dict = get_keyword_ome_hrfp_theme(keywords_to_look_for, language)
    configure_barometre_source(_con)
    core_df = read_barometre_keywords(_con, kw_dict, start_date, end_date, country)
    if core_df.empty:
        logging.warning("[SOURCE_TABLE] No core chunk found, nothing to do")
        return

    _configure_s3(_con)
    configure_barometre_destination(_con)

    table = f"{DESTINATION_ALIAS}.{DESTINATION_TABLE}"
    counter_to_ingest=0
    ingested = 0
    skipped_days, failed_days = [], []

    for day, core_day in core_df.groupby(core_df["start"].dt.date):
        day_channels = core_day["channel_name"].unique().tolist()
        logging.info(f"Processing day {day}")
        logging.info(f"[S3] {day} : processing {len(core_day)} core chunks")

        try:
            s3_day_df = read_day_from_s3(_con, day, day_channels)
        except FileNotFoundError as exc:
            logging.warning(f"[S3] {day} skipped : {exc}")
            skipped_days.append(day)
            continue

        try:
            day_df = extract_adjacent_chunks_day(core_day, s3_day_df, GAP)
            logging.info(f"[DESTINATION_TABLE] {day} : {len(day_df)} chunks (core + adjacents) to ingest into {table}")
            counter_to_ingest+=len(day_df)

            if DRY_RUN:
                continue

            _con.execute(f"""
                INSERT INTO {table} BY NAME
                SELECT * FROM day_df
                ON CONFLICT (this_id) DO UPDATE SET
                    channel_name = EXCLUDED.channel_name,
                    channel_title = EXCLUDED.channel_title,
                    start = EXCLUDED.start,
                    plaintext = EXCLUDED.plaintext,
                    core_chunk = EXCLUDED.core_chunk
            """)
            ingested += len(day_df)
            logging.info(f"[DESTINATION_TABLE] {day} : {len(day_df)} chunks ingested")
        except Exception:
            logging.exception(f"[DESTINATION_TABLE] {day} : failed")
            failed_days.append(day)
    if DRY_RUN:
        logging.info(f"[END] DRY_RUN : {counter_to_ingest} chunks would have been ingested, "
                     f"{len(skipped_days)} days skipped, {len(failed_days)} days failed")
    else:
        logging.info(f"[END] Done: {ingested} chunks ingested, "
                 f"{len(skipped_days)} days skipped, {len(failed_days)} days failed")
    if failed_days:
        raise RuntimeError(f"Failed days: {failed_days}")
    
        
        
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="1. Extract keywords from the source table, "
                    "2. Retrieve adjacent chunks from S3, "
                    "3. Insert into destination table."
    )
    parser.add_argument("--start_date", type=date.fromisoformat, required=True,
                        help="Start date (included), YYYY-MM-DD")
    parser.add_argument("--end_date", type=date.fromisoformat, required=True,
                        help="End date (excluded), YYYY-MM-DD")
    parser.add_argument("--keywords", type=str, nargs="+", required=True,
                        help="Keywords to look for")
    parser.add_argument("--country", type=str, default="france", help="Country name")
    parser.add_argument("--language", type=str, default="french", help="Language name")
    args = parser.parse_args()
    if args.end_date <= args.start_date:
        parser.error("--end_date doit être postérieure à --start_date")
    return args


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler("extraction.log"), logging.StreamHandler()],
        force=True,
    )
    args = parse_args()
    try:
        run(start_date=args.start_date, end_date=args.end_date, keywords_to_look_for=args.keywords,
            country=args.country, language=args.language)
    except Exception as e:
        logging.error(f"[RUN] ERREUR : {e}")
        raise