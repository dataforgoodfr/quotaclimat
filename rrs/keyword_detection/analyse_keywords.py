import os
import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import duckdb

# Module-level connection so relations stay valid across calls in interactive sessions.
_con = duckdb.connect()

from rrs.dictionary.dictionary import (
    correlate_with,
    keywords,
)
from quotaclimat.data_processing.mediatree.i8n.france.channel_titles import (
    channel_titles_france,
)

ALL_CHANNELS = list(channel_titles_france.keys())


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

# Characters to search around a keyword match for a correlate (~20 words)
PROXIMITY_CHARS = 150


_REGEX_SPECIAL = frozenset(r'\.^$*+?()[]{}|')


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
            globs.append(_s3_glob_for_day(current.year, current.month, current.day, channel))
        current += timedelta(days=1)

    if not globs:
        raise ValueError("Date range produced no S3 paths.")

    # Resolve which globs actually have files on S3 before passing to read_parquet,
    # so missing channel/date combinations are skipped with a warning instead of
    # raising an IOException.
    all_globs_sql = ", ".join(f"'{g}'" for g in globs)
    existing_files = con.sql(f"SELECT file FROM glob([{all_globs_sql}])").fetchall()
    existing_files = [row[0] for row in existing_files]

    missing = len(globs) - len({f.rsplit("/", 1)[0] + "/*" for f in existing_files} & set(globs))
    if missing:
        logging.warning(f"{missing} channel/day combination(s) had no parquet files on S3 and were skipped.")

    if not existing_files:
        raise FileNotFoundError(
            f"No parquet files found on S3 for the requested channels and date range "
            f"({start_date} – {end_date}, {len(channels)} channel(s))."
        )

    file_list = ", ".join(f"'{f}'" for f in existing_files)
    query = f"SELECT * FROM read_parquet([{file_list}], hive_partitioning=true, union_by_name=true)"
    logging.info(f"Reading {len(existing_files)} parquet file(s) across {len(channels)} channel(s).")
    return con.sql(query)


def detect_keywords(
    start_date: date,
    end_date: Optional[date] = None,
    channels: Optional[list[str]] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> duckdb.DuckDBPyRelation:
    """Detect insecurity keywords in plaintext and check for nearby correlates.

    Defaults to all France channels when channels is None.

    For each row that contains at least one keyword, the result includes:
      - keywords_found      : list of matched keywords
      - correlates_found    : list of matched correlate_with terms (only when a keyword is present)
      - has_nearby_correlate: true when a correlate appears within ~20 words of a keyword
    """
    if con is None:
        con = _con

    source = read_from_s3(start_date=start_date, end_date=end_date, channels=channels, con=con)
    con.register("source", source)

    kw_alt = _build_alternation(keywords)
    corr_alt = _build_alternation(correlate_with)

    # Proximity pattern: correlate ... keyword  OR  keyword ... correlate
    proximity = (
        f"(?i){corr_alt}.{{0,{PROXIMITY_CHARS}}}{kw_alt}"
        f"|(?i){kw_alt}.{{0,{PROXIMITY_CHARS}}}{corr_alt}"
    )

    query = f"""
        WITH keyword_matches AS (
            SELECT
                *,
                regexp_extract_all(lower(plaintext), '(?i){kw_alt}') AS keywords_found
            FROM source
        )
        SELECT
            * EXCLUDE (keywords_found),
            keywords_found,
            len(keywords_found) AS n_keywords_found,
            CASE
                WHEN len(keywords_found) > 0
                    THEN regexp_extract_all(lower(plaintext), '(?i){corr_alt}')
                ELSE []
            END AS correlates_found,
            CASE
                WHEN len(keywords_found) > 0
                    THEN len(regexp_extract_all(lower(plaintext), '(?i){corr_alt}'))
                ELSE 0
            END AS n_correlates_found,
            CASE
                WHEN len(keywords_found) > 0
                    THEN regexp_matches(lower(plaintext), '{proximity}')
                ELSE false
            END AS has_nearby_correlate
        FROM keyword_matches
        WHERE len(keywords_found) > 0
            -- AND regexp_matches(lower(plaintext), '{proximity}')
    """
    logging.info("Running keyword + correlate detection query.")
    return con.sql(query)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description="Detect insecurity keywords and correlates in mediatree parquet data."
    )
    parser.add_argument("--channel", nargs="*", help="Channel(s) to process (default: all France channels)")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", help="End date YYYY-MM-DD (inclusive, defaults to start_date)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date) if args.end_date else None
    channels = args.channel or None  # None triggers the ALL_CHANNELS default

    result = detect_keywords(start_date=start, end_date=end, channels=channels)
    result.show()

    end_label = end or start
    out_path = (
        Path(__file__).parent
        / "data"
        / f"insecurity_{start}_{end_label}_no_corr.xlsx"
    )
    df = result.df()
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)
    df.to_excel(out_path, index=False)
    logging.info(f"Written {len(df)} row(s) to {out_path}")
