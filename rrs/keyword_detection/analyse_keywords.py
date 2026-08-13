import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import quote

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
from rrs.utils.mediatree import get_url_mediatree
from rrs.utils.generate_id import get_consistent_hash

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
    user = quote(os.getenv("RRS_PG_USER", "user"), safe="")
    password = quote(os.getenv("RRS_PG_PASSWORD", "password"), safe="")
    return create_engine(
        f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
    )


def _rrs_dsn() -> str:
    user = quote(os.getenv("RRS_PG_USER", "user"), safe="")
    password = quote(os.getenv("RRS_PG_PASSWORD", "password"), safe="")
    host = os.getenv("RRS_PG_HOST", "localhost")
    port = os.getenv("RRS_PG_PORT", "5432")
    database = os.getenv("RRS_PG_DATABASE", "rrs_db")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _s3_uri(start, channel_name: str) -> str:
    return (
        f"s3://{BUCKET_NAME}"
        f"/year={start.year}/month={start.month}/day={start.day}"
        f"/channel={channel_name}/"
    )


def save_segments_to_db(df: pd.DataFrame) -> None:
    """Upsert a detection DataFrame into the RRS segments table."""
    if df.empty:
        return
    segments = df.copy()
    segments["segment_id"] = (
        segments["start"].astype(str) + segments["channel_name"]
    ).apply(get_consistent_hash)
    segments["n_keywords"] = segments["n_keywords_found"]
    segments["keywords"] = segments["keywords_found"]
    segments["s3_uri"] = segments.apply(
        lambda r: _s3_uri(r["start"], r["channel_name"]), axis=1
    )
    segments["url_mediatree"] = segments.apply(
        lambda r: get_url_mediatree(r["start"], r["channel_name"]), axis=1
    )
    if "channel_program" not in segments.columns:
        segments["channel_program"] = None

    batch = segments[
        [
            "segment_id",
            "subject_id",
            "start",
            "s3_uri",
            "n_keywords",
            "channel_name",
            "channel_title",
            "channel_program",
            "keywords",
            "url_mediatree",
        ]
    ]

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{_rrs_dsn()}' AS rrs (TYPE POSTGRES);")
    con.register("segments_batch", batch)
    con.execute("""
        INSERT INTO rrs.segments (
            segment_id, subject_id, start, s3_uri, n_keywords,
            channel_name, channel_title, channel_program, keywords,
            url_mediatree, created_at, updated_at
        )
        SELECT
            segment_id, subject_id, start, s3_uri, n_keywords,
            channel_name, channel_title, channel_program, keywords,
            url_mediatree,
            now() AT TIME ZONE 'utc',
            now() AT TIME ZONE 'utc'
        FROM segments_batch
        ON CONFLICT (segment_id, subject_id) DO UPDATE SET
            start           = EXCLUDED.start,
            s3_uri          = EXCLUDED.s3_uri,
            n_keywords      = EXCLUDED.n_keywords,
            channel_name    = EXCLUDED.channel_name,
            channel_title   = EXCLUDED.channel_title,
            channel_program = EXCLUDED.channel_program,
            keywords        = EXCLUDED.keywords,
            url_mediatree   = EXCLUDED.url_mediatree,
            updated_at      = now() AT TIME ZONE 'utc'
    """)
    con.close()
    logging.info(f"  {len(batch)} segment(s) upserted into DB.")


NON_VALIDATED_KEYWORD_THRESHOLD = int(os.environ.get("NON_VALIDATED_KEYWORD_THRESHOLD", "5"))


def get_keywords_by_subject(
    exclude_subject_name: str = CLIMATE_SUBJECT_NAME,
    only_subject_name: Optional[str] = None,
) -> dict[str, tuple[list[str], list[str], list[str]]]:
    """Return {subject_id: (validated_keywords, high_risk_keywords, non_validated_keywords)}.

    If only_subject_name is given, restrict to that single subject. Otherwise return
    all subjects except the excluded one.
    """
    Session = sessionmaker(bind=_get_engine())

    with Session() as session:
        if only_subject_name:
            only_id = make_subject_id(only_subject_name)
            entries = (
                session.query(DictionaryEntry)
                .filter(DictionaryEntry.subject_id == only_id)
                .all()
            )
        else:
            climate_id = make_subject_id(exclude_subject_name)
            entries = (
                session.query(DictionaryEntry)
                .filter(DictionaryEntry.subject_id != climate_id)
                .all()
            )

    validated_kws: dict[str, list[str]] = {}
    high_risk_kws: dict[str, list[str]] = {}
    non_validated_kws: dict[str, list[str]] = {}
    for entry in entries:
        if not entry.keyword:
            continue
        if entry.validated is False:
            non_validated_kws.setdefault(entry.subject_id, []).append(entry.keyword)
            continue
        if not entry.high_risk_false_positive:
            validated_kws.setdefault(entry.subject_id, []).append(entry.keyword)
        else:
            high_risk_kws.setdefault(entry.subject_id, []).append(entry.keyword)

    subject_ids = set(validated_kws) | set(non_validated_kws)
    keywords_by_subject = {
        sid: (
            validated_kws.get(sid, []),
            high_risk_kws.get(sid, []),
            non_validated_kws.get(sid, []),
        )
        for sid in subject_ids
    }
    if only_subject_name:
        logging.info(f"Loaded keywords for subject '{only_subject_name}' only.")
    else:
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


def _build_day_query(
    keywords_by_subject: dict[str, tuple[list[str], list[str], list[str]]]
) -> str:
    """Build a UNION ALL query that detects keywords for every subject against 'source'.

    Each subject entry is (validated_keywords, high_risk_keywords, non_validated_keywords).
    The query adds:
      - n_keywords_found     : total matched validated (non-high-risk) keywords
      - n_hrfp_found         : matched keywords flagged high_risk_false_positive
      - n_non_validated_found: matched keywords not yet validated

    Non-validated keywords are normally excluded from keywords_found/n_keywords_found
    and never make a segment match on their own, UNLESS a segment contains at least
    NON_VALIDATED_KEYWORD_THRESHOLD of them — in that case the segment is included
    and its non-validated matches are folded into keywords_found/n_keywords_found too.
    """
    union_parts = []
    for subject_id, (kws, high_risk_kws, non_validated_kws) in keywords_by_subject.items():
        if not kws and not non_validated_kws:
            continue
        if kws:
            kw_alt = _build_alternation(kws)
            kw_match_expr = f"regexp_extract_all(lower(plaintext), '(?i){kw_alt}')"
        else:
            kw_match_expr = "[]::VARCHAR[]"
        if high_risk_kws:
            hr_alt = _build_alternation(high_risk_kws)
            hr_expr = f"len(regexp_extract_all(lower(plaintext), '(?i){hr_alt}'))"
        else:
            hr_expr = "0"
        if non_validated_kws:
            nv_alt = _build_alternation(non_validated_kws)
            nv_match_expr = f"regexp_extract_all(lower(plaintext), '(?i){nv_alt}')"
        else:
            nv_match_expr = "[]::VARCHAR[]"
        nv_count_expr = f"len(list_distinct({nv_match_expr}))"
        keywords_found_expr = (
            f"CASE WHEN {nv_count_expr} >= {NON_VALIDATED_KEYWORD_THRESHOLD} "
            f"THEN list_concat({kw_match_expr}, {nv_match_expr}) "
            f"ELSE {kw_match_expr} END"
        )
        union_parts.append(f"""
            with detections as (
                SELECT
                    '{subject_id}' AS subject_id,
                    * EXCLUDE srt,
                    {keywords_found_expr} AS keywords_found,
                    len({keywords_found_expr}) AS n_keywords_found,
                    {hr_expr} AS n_hrfp_found,
                    {nv_count_expr} AS n_non_validated_found
                FROM source
                WHERE len({kw_match_expr}) > 0 OR {nv_count_expr} >= {NON_VALIDATED_KEYWORD_THRESHOLD}
            )
            SELECT
                *
            FROM detections
            -- WHERE n_keywords_found > 2 * n_hrfp_found
            where n_hrfp_found=0
        """)
    if not union_parts:
        raise ValueError("No keywords to search for any subject.")
    return " UNION ALL ".join(union_parts)


def detect_keywords(
    start_date: date,
    end_date: Optional[date] = None,
    channels: Optional[list[str]] = None,
    con: Optional[duckdb.DuckDBPyConnection] = None,
    subject: Optional[str] = None,
) -> Iterator[tuple[date, pd.DataFrame]]:
    """Yield (day, DataFrame) for each day in the date range.

    Fetches keywords once from the DB, then processes one day at a time from S3
    so peak memory stays bounded to a single day's data. Each DataFrame contains:
      - subject_id           : identifier of the matched subject
      - keywords_found       : list of matched keywords from that subject. Includes
                               non-validated keyword matches only when there are at
                               least NON_VALIDATED_KEYWORD_THRESHOLD of them.
      - n_keywords_found     : count of matched keywords in keywords_found
      - n_non_validated_found: count of matched non-validated keywords
    Rows are included if they have at least one validated keyword match, or at
    least NON_VALIDATED_KEYWORD_THRESHOLD non-validated keyword matches.

    If subject is given, only that subject's keywords are searched for.
    """
    if con is None:
        con = _con

    keywords_by_subject = get_keywords_by_subject(only_subject_name=subject)
    if not keywords_by_subject:
        if subject:
            raise ValueError(f"No keywords found for subject '{subject}'.")
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
    parser.add_argument(
        "--subject",
        default=os.environ.get("SUBJECT"),
        help="Only analyse this subject (default: all subjects except climate, env: SUBJECT)",
    )
    parser.add_argument(
        "--days-prior",
        type=int,
        default=int(os.environ.get("DAYS_PRIOR", "1")),
        help="Number of days before end-date to use as start-date (default: 1, env: DAYS_PRIOR)",
    )
    parser.add_argument(
        "--start-date",
        default=os.environ.get("START_DATE"),
        help="Start date YYYY-MM-DD — overrides --days-prior if set (env: START_DATE)",
    )
    parser.add_argument(
        "--end-date",
        default=os.environ.get("END_DATE"),
        help="End date YYYY-MM-DD inclusive (default: today, env: END_DATE)",
    )
    args = parser.parse_args()

    end = date.fromisoformat(args.end_date) if args.end_date else date.today()
    start = (
        date.fromisoformat(args.start_date)
        if args.start_date
        else end - timedelta(days=args.days_prior)
    )
    channels = args.channel or None

    total = 0
    for day, df in detect_keywords(
        start_date=start, end_date=end, channels=channels, subject=args.subject
    ):
        save_segments_to_db(df)
        total += len(df)

    if total == 0:
        logging.warning("No matches found for the requested date range.")
    else:
        logging.info(f"Done. {total} segment(s) upserted in total.")
