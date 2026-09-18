"""
Filter already-detected climate/theme keyword segments down to the ones
that also match a dictionary subject (e.g. environmental_health), then
upsert them into the RRS segments table.

Unlike rrs.keyword_detection.analyse_keywords, which re-scans raw
transcripts on S3 with a regex built from the dictionary, this script
reuses the keywords already detected by the climate pipeline — loaded the
same way as rrs.keyword_detection.import_segments, from the
keywords_with_timestamp column of the quotaclimat PostgreSQL DB — and just
filters them against the target dictionary's keyword list.

A segment is kept only if it contains at least one already-detected
keyword that matches a validated, non-high-risk keyword of the target
dictionary, and none that matches a high-risk-false-positive one. A match
is either an exact (lowercased) string match, or the two keywords sharing
the same set of lemmas (e.g. "pesticides" vs "pesticide"), using the same
spaCy lemmatizer as the main detection pipeline.

Usage:
    poetry run python -m rrs.keyword_detection.filter_keywords
    poetry run python -m rrs.keyword_detection.filter_keywords --subject environmental_health --start-date 2024-01-01 --end-date 2024-03-31
"""

import argparse
import contextlib
import json
import logging
import os
import re
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Optional
from urllib.parse import quote

import duckdb
import psycopg
from dotenv import load_dotenv
from quotaclimat.data_processing.mediatree.detect_keywords import (
    LANGUAGE_CODES,
    get_lemmas,
)
from rrs.dictionary.subjects import subjects
from rrs.dictionary.upsert_subjects import subject_id as make_subject_id
from rrs.utils.mediatree import get_url_mediatree

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION = "fr-par"

DEFAULT_SUBJECT_NAME = "environmental_health"
DEFAULT_LANG = LANGUAGE_CODES["french"]


@lru_cache(maxsize=None)
def _lemma_set(text: str, lang: str = DEFAULT_LANG) -> frozenset:
    """Lemmatize a keyword/phrase into an order-independent set of lemmas."""
    return frozenset(get_lemmas(text.lower(), lang))


_DSN_PASSWORD_RE = re.compile(r"(postgresql://[^:]+:)[^@]+(@)")


def _redact_dsn(msg: str) -> str:
    return _DSN_PASSWORD_RE.sub(r"\1***\2", msg)


@contextlib.contextmanager
def _masked_db_errors():
    try:
        yield
    except Exception as exc:
        raise type(exc)(_redact_dsn(str(exc))) from None


def _dsn(host, port, db, user, password) -> str:
    return f"postgresql://{quote(user, safe='')}:{quote(password, safe='')}@{host}:{port}/{db}"


def source_dsn() -> str:
    return _dsn(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        db=os.getenv("POSTGRES_DB", "barometre"),
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
    )


def rrs_dsn() -> str:
    return _dsn(
        host=os.getenv("RRS_PG_HOST", "localhost"),
        port=os.getenv("RRS_PG_PORT", "5432"),
        db=os.getenv("RRS_PG_DATABASE", "rrs_db"),
        user=os.getenv("RRS_PG_USER", "user"),
        password=os.getenv("RRS_PG_PASSWORD", "password"),
    )


def _s3_uri(start, channel_name: str) -> str:
    return (
        f"s3://{BUCKET_NAME}"
        f"/year={start.year}/month={start.month}/day={start.day}"
        f"/channel={channel_name}/"
    )


def _rrs_conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'supersecret')}"
    )


def _extract_keywords(raw) -> list:
    """Pull the list of keyword strings out of a keywords_with_timestamp JSON value."""
    if raw is None:
        return []
    items = json.loads(raw) if isinstance(raw, str) else raw
    return [item["keyword"] for item in items]


def _index_keywords(keywords: list) -> dict:
    """Build a lookup for a keyword list: exact lowercased strings, plus lemma sets."""
    return {
        "strings": {kw.lower() for kw in keywords},
        "lemmas": {_lemma_set(kw) for kw in keywords},
    }


def get_dictionary_keywords(subject_name: str) -> tuple[dict, dict]:
    """Return (validated_index, high_risk_index) for subject_name.

    Each index has a "strings" set (lowercased keywords) and a "lemmas" set
    (frozenset-of-lemmas per keyword), so a detected keyword can be matched
    either verbatim or by sharing the same set of lemmas.
    """
    if subject_name not in subjects:
        raise ValueError(
            f"Subject {subject_name!r} not found in rrs/dictionary/subjects.py"
        )
    entries = subjects[subject_name]["keywords"]
    validated_kws = [
        entry["keyword"]
        for entry in entries
        if entry.get("validated", True) and not entry.get("high_risk_false_positive")
    ]
    high_risk_kws = [
        entry["keyword"]
        for entry in entries
        if entry.get("validated", True) and entry.get("high_risk_false_positive")
    ]
    return _index_keywords(validated_kws), _index_keywords(high_risk_kws)


def _keyword_matches(kw: str, index: dict) -> bool:
    low = kw.lower()
    if low in index["strings"]:
        return True
    lemmas = _lemma_set(low)
    return bool(lemmas) and lemmas in index["lemmas"]


def _matching_keywords(found_keywords: list, validated_index: dict) -> list:
    seen = set()
    matches = []
    for kw in found_keywords:
        low = kw.lower()
        if low in seen:
            continue
        if _keyword_matches(kw, validated_index):
            seen.add(low)
            matches.append(kw)
    return matches


def _has_high_risk_match(found_keywords: list, high_risk_index: dict) -> bool:
    return any(_keyword_matches(kw, high_risk_index) for kw in found_keywords)


def _get_max_segment_date(sid: str) -> Optional[date]:
    """Return the calendar day of the most recent segment for this subject in the RRS DB, or None."""
    with psycopg.connect(_rrs_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(start::date) FROM segments WHERE subject_id = %s", (sid,)
            )
            row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def filter_keywords(
    subject_name: str = DEFAULT_SUBJECT_NAME,
    start_date: date = None,
    end_date: date = None,
) -> None:
    validated, high_risk = get_dictionary_keywords(subject_name)
    if not validated["strings"]:
        raise ValueError(f"No validated keywords found for subject {subject_name!r}.")

    sid = make_subject_id(subject_name)

    if start_date is None:
        logging.info("No start date provided — querying RRS DB for max segment date...")
        start_date = _get_max_segment_date(sid)
        end_date = datetime.now(tz=timezone.utc)
        logging.info(f"  Auto range: {start_date} → {end_date}")

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    with _masked_db_errors():
        con.execute(f"ATTACH '{source_dsn()}' AS barometre (TYPE POSTGRES, READ_ONLY);")

    date_filter = ""
    params = {}
    if start_date is not None:
        date_filter += " AND start >= $start_date"
        params["start_date"] = start_date
    if end_date is not None:
        date_filter += " AND start < $end_date"
        params["end_date"] = end_date

    logging.info("Fetching rows from quotaclimat keywords table...")
    df = con.execute(
        f"""
        SELECT
            id,
            start,
            channel_name,
            channel_title,
            channel_program,
            keywords_with_timestamp
        FROM barometre.keywords
        WHERE number_of_keywords_climat > 0
        AND country='france'
        {date_filter}
        """,
        params if params else {},
    ).df()

    logging.info(f"  {len(df)} row(s) fetched.")
    if df.empty:
        logging.info("Nothing to filter.")
        return

    df["keywords_found"] = df["keywords_with_timestamp"].apply(_extract_keywords)
    df = df[~df["keywords_found"].apply(lambda kws: _has_high_risk_match(kws, high_risk))]
    df["keywords"] = df["keywords_found"].apply(lambda kws: _matching_keywords(kws, validated))
    df = df[df["keywords"].apply(len) > 0]

    logging.info(f"  {len(df)} segment(s) matched the {subject_name!r} dictionary.")
    if df.empty:
        logging.info("Nothing to upsert.")
        return

    df["segment_id"] = df["id"]
    df["subject_id"] = sid
    df["s3_uri"] = df.apply(lambda r: _s3_uri(r["start"], r["channel_name"]), axis=1)
    df["n_keywords"] = df["keywords"].apply(len)
    df["url_mediatree"] = df.apply(
        lambda r: get_url_mediatree(r["start"], r["channel_name"]), axis=1
    )

    segments = df[
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

    con.register("segments_batch", segments)
    with _masked_db_errors():
        con.execute(f"ATTACH '{rrs_dsn()}' AS rrs (TYPE POSTGRES);")

    con.execute("""
        INSERT INTO rrs.segments (segment_id, subject_id, start, s3_uri, n_keywords, channel_name, channel_title, channel_program, keywords, url_mediatree, created_at, updated_at)
        SELECT segment_id, subject_id, start, s3_uri, n_keywords, channel_name, channel_title, channel_program, keywords, url_mediatree, now() AT TIME ZONE 'utc', now() AT TIME ZONE 'utc'
        FROM segments_batch
        ON CONFLICT (segment_id, subject_id) DO UPDATE SET
            start            = EXCLUDED.start,
            s3_uri           = EXCLUDED.s3_uri,
            n_keywords       = EXCLUDED.n_keywords,
            channel_name     = EXCLUDED.channel_name,
            channel_title    = EXCLUDED.channel_title,
            channel_program  = EXCLUDED.channel_program,
            keywords         = EXCLUDED.keywords,
            url_mediatree    = EXCLUDED.url_mediatree,
            created_at       = CASE WHEN segments.created_at IS NULL THEN now() AT TIME ZONE 'utc' ELSE segments.created_at END,
            updated_at       = now() AT TIME ZONE 'utc'
    """)

    logging.info(f"  {len(segments)} segment(s) upserted (subject: {subject_name!r}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Filter already-detected keyword segments down to a dictionary subject and import into RRS DB."
    )
    parser.add_argument(
        "--subject",
        default=os.getenv("SUBJECT", DEFAULT_SUBJECT_NAME),
        help=f"Dictionary subject to filter for (default: {DEFAULT_SUBJECT_NAME}, env: SUBJECT)",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("START_DATE"),
        help="Filter rows with start >= this date.",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("END_DATE"),
        help="Filter rows with start < this date.",
    )
    args = parser.parse_args()

    filter_keywords(
        subject_name=args.subject,
        start_date=date.fromisoformat(args.start_date) if args.start_date else None,
        end_date=date.fromisoformat(args.end_date) if args.end_date else None,
    )
