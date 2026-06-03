import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import (
    channel_titles_france,
)
from rrs.dictionary.upsert_subjects import subject_id as make_subject_id
from rrs.schemas.models import DictionaryEntry

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

ALL_CHANNELS = list(channel_titles_france.keys())

# Module-level connection so relations stay valid across calls in interactive sessions.
_con = duckdb.connect()

CLIMATE_SUBJECT_NAME = "climate"


def get_secret_docker(secret_name: str) -> str:
    value = os.environ.get(secret_name, "")
    if value and os.path.exists(value):
        with open(value, "r") as f:
            return f.read().strip()
    return value


ACCESS_KEY = get_secret_docker("BUCKET")
SECRET_KEY = get_secret_docker("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"


_REGEX_SPECIAL = frozenset(r"\.^$*+?()[]{}|")


def _escape(term: str) -> str:
    # Escape regex special chars; SQL-escape ' as '' (not \' which breaks the SQL string literal)
    result = []
    for c in term.lower():
        if c in _REGEX_SPECIAL:
            result.append("\\" + c)
        elif c == "'":
            result.append("''")
        else:
            result.append(c)
    return "".join(result)


def _build_alternation(terms: list[str]) -> str:
    # Longest terms first so the regex engine prefers them
    by_length = sorted(terms, key=len, reverse=True)
    return "(" + "|".join(_escape(t) for t in by_length) + ")"


def _get_engine():
    host = os.getenv("RRS_PG_HOST", "localhost")
    port = os.getenv("RRS_PG_PORT", "5432")
    database = os.getenv("RRS_PG_DATABASE", "rrs_db")
    user = os.getenv("RRS_PG_USER", "user")
    password = os.getenv("RRS_PG_PASSWORD", "password")
    return create_engine(
        f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    )


def get_keywords_by_subject(
    exclude_subject_name: str = CLIMATE_SUBJECT_NAME,
) -> dict[str, tuple[list[str], list[str]]]:
    """Return {subject_id: (all_keywords, high_risk_keywords)} for all subjects except the excluded one."""
    climate_id = make_subject_id(exclude_subject_name)
    Session = sessionmaker(bind=_get_engine())

    with Session() as session:
        entries = (
            session.query(DictionaryEntry)
            .filter(DictionaryEntry.subject_id != climate_id)
            .all()
        )

    all_kws: dict[str, list[str]] = {}
    high_risk_kws: dict[str, list[str]] = {}
    for entry in entries:
        if not entry.keyword:
            continue
        all_kws.setdefault(entry.subject_id, []).append(entry.keyword)
        if entry.high_risk_false_positive:
            high_risk_kws.setdefault(entry.subject_id, []).append(entry.keyword)

    keywords_by_subject = {
        sid: (all_kws[sid], high_risk_kws.get(sid, []))
        for sid in all_kws
    }
    logging.info(
        f"Loaded keywords for {len(keywords_by_subject)} subject(s) "
        f"(excluding '{exclude_subject_name}')."
    )
    return keywords_by_subject


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{REGION}';
        SET s3_endpoint='s3.{REGION}.scw.cloud';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_url_style='path';
    """)


def _s3_glob_for_day(year: int, month: int, day: int, channel: str) -> str:
    return f"s3://{BUCKET_NAME}/year={year}/month={month}/day={day}/channel={channel}/*.parquet"


def read_from_s3(
    start_date: date,
    end_date: Optional[date] = None,
    channels: Optional[list[str]] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> duckdb.DuckDBPyRelation:
    """Read parquet data from S3 for one or more channels over a date (or date range).

    Defaults to all France channels when channels is None.
    """
    if con is None:
        con = _con
    if channels is None:
        channels = ALL_CHANNELS

    _configure_s3(con)

    if end_date is None:
        end_date = start_date

    globs = []
    current = start_date
    while current <= end_date:
        for channel in channels:
            globs.append(
                _s3_glob_for_day(current.year, current.month, current.day, channel)
            )
        current += timedelta(days=1)

    if not globs:
        raise ValueError("Date range produced no S3 paths.")

    all_globs_sql = ", ".join(f"'{g}'" for g in globs)
    existing_files = con.sql(f"SELECT file FROM glob([{all_globs_sql}])").fetchall()
    existing_files = [row[0] for row in existing_files]

    missing = len(globs) - len(
        {f.rsplit("/", 1)[0] + "/*" for f in existing_files} & set(globs)
    )
    if missing:
        logging.warning(
            f"{missing} channel/day combination(s) had no parquet files on S3 and were skipped."
        )

    if not existing_files:
        raise FileNotFoundError(
            f"No parquet files found on S3 for the requested channels and date range "
            f"({start_date} – {end_date}, {len(channels)} channel(s))."
        )

    file_list = ", ".join(f"'{f}'" for f in existing_files)
    query = f"SELECT * FROM read_parquet([{file_list}], hive_partitioning=true, union_by_name=true)"
    logging.info(
        f"Reading {len(existing_files)} parquet file(s) across {len(channels)} channel(s)."
    )
    return con.sql(query)


def _build_day_query(keywords_by_subject: dict[str, tuple[list[str], list[str]]]) -> str:
    """Build a UNION ALL query that detects keywords for every subject against 'source'.

    Each subject entry is (all_keywords, high_risk_keywords). The query adds:
      - n_keywords_found     : total matched keywords
      - n_hrfp_found    : matched keywords flagged high_risk_false_positive
    """
    union_parts = []
    for subject_id, (kws, high_risk_kws) in keywords_by_subject.items():
        if not kws:
            continue
        kw_alt = _build_alternation(kws)
        if high_risk_kws:
            hr_alt = _build_alternation(high_risk_kws)
            hr_expr = f"len(regexp_extract_all(lower(plaintext), '(?i){hr_alt}'))"
        else:
            hr_expr = "0"
        union_parts.append(f"""
            with detections as (
                SELECT
                    '{subject_id}' AS subject_id,
                    * EXCLUDE srt,
                    regexp_extract_all(lower(plaintext), '(?i){kw_alt}') AS keywords_found,
                    len(regexp_extract_all(lower(plaintext), '(?i){kw_alt}')) AS n_keywords_found,
                    {hr_expr} AS n_hrfp_found
                FROM source
                WHERE len(regexp_extract_all(lower(plaintext), '(?i){kw_alt}')) > 0
            )
            SELECT 
                *
            FROM detections
            WHERE n_keywords_found > 2 * n_hrfp_found
            -- where n_hrfp_found=0
        """)
    if not union_parts:
        raise ValueError("No keywords to search for any subject.")
    return " UNION ALL ".join(union_parts)


def detect_keywords(
    start_date: date,
    end_date: Optional[date] = None,
    channels: Optional[list[str]] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield (day, DataFrame) for each day in the date range.

    Fetches keywords once from the DB, then processes one day at a time from S3
    so peak memory stays bounded to a single day's data. Each DataFrame contains:
      - subject_id      : identifier of the matched subject
      - keywords_found  : list of matched keywords from that subject
      - n_keywords_found: count of matched keywords
    Only rows with at least one keyword match are included.
    """
    if con is None:
        con = _con

    keywords_by_subject = get_keywords_by_subject()
    if not keywords_by_subject:
        raise ValueError("No keyword subjects found (excluding climate).")

    query = _build_day_query(keywords_by_subject)

    if end_date is None:
        end_date = start_date

    current = start_date
    while current <= end_date:
        logging.info(f"Processing {current} ({len(keywords_by_subject)} subject(s)).")
        try:
            source = read_from_s3(
                start_date=current, end_date=current, channels=channels, con=con
            )
        except FileNotFoundError as exc:
            logging.warning(f"Skipping {current}: {exc}")
            current += timedelta(days=1)
            continue

        con.register("source", source)
        df = con.sql(query).df()
        logging.info(f"  {current}: {len(df)} match(es).")
        yield current, df

        current += timedelta(days=1)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Detect dictionary keywords (all subjects except climate) in mediatree parquet data."
    )
    parser.add_argument(
        "--channel",
        nargs="*",
        help="Channel(s) to process (default: all France channels)",
    )
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument(
        "--end-date", help="End date YYYY-MM-DD (inclusive, defaults to start_date)"
    )
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else None
    channels = args.channel or None

    frames: list[pd.DataFrame] = []
    for day, df in detect_keywords(start_date=start, end_date=end, channels=channels):
        frames.append(df)

    if not frames:
        logging.warning("No matches found for the requested date range.")
    else:
        result = pd.concat(frames, ignore_index=True)
        logging.info(f"Total matches: {len(result)}")

        end_label = end or start
        out_path = (
            Path(__file__).parent / "data" / f"keywords_{start}_{end_label}_half_hrfp.xlsx"
        )
        out_path_csv = (
            Path(__file__).parent / "data" / f"keywords_{start}_{end_label}_half_hrfp.csv"
        )
        for col in result.select_dtypes(include=["datetimetz"]).columns:
            result[col] = result[col].dt.tz_localize(None)
        result.to_excel(out_path, index=False)
        result.to_csv(out_path_csv, index=False)
        logging.info(f"Written {len(result)} row(s) to {out_path}")
