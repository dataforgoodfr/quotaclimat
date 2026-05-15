"""
Import climate keyword segments from the quotaclimat keywords table into the
RRS segments table.

Reads from the quotaclimat PostgreSQL DB (POSTGRES_* env vars), transforms
the rows, and upserts them into the RRS PostgreSQL DB (RRS_PG_* env vars).

Usage:
    poetry run python -m rrs.keyword_detection.import_segments
    poetry run python -m rrs.keyword_detection.import_segments --start-date 2024-01-01 --end-date 2024-03-31
"""

import argparse
import logging
import os
from datetime import date

import duckdb
from dotenv import load_dotenv

from rrs.dictionary.subjects import subjects
from rrs.dictionary.upsert_subjects import subject_id as make_subject_id
from rrs.utils.generate_id import get_consistent_hash

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION = "fr-par"

SUBJECT_NAME = "climate"


def _dsn(host, port, db, user, password) -> str:
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


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


def import_segments(start_date: date = None, end_date: date = None) -> None:
    if SUBJECT_NAME not in subjects:
        raise ValueError(f"Subject {SUBJECT_NAME!r} not found in rrs/dictionary/subjects.py")

    sid = make_subject_id(SUBJECT_NAME)

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{source_dsn()}' AS qc (TYPE POSTGRES, READ_ONLY);")

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
            number_of_keywords_climat
        FROM qc.keywords
        WHERE number_of_keywords_climat > 0
        {date_filter}
        """,
        list(params.values()) if params else [],
    ).df()

    logging.info(f"  {len(df)} row(s) fetched.")
    if df.empty:
        logging.info("Nothing to upsert.")
        return

    df["segment_id"] = df["id"].apply(get_consistent_hash)
    df["subject_id"] = sid
    df["s3_uri"] = df.apply(lambda r: _s3_uri(r["start"], r["channel_name"]), axis=1)
    df["n_keywords"] = df["number_of_keywords_climat"]

    segments = df[["segment_id", "subject_id", "start", "s3_uri", "n_keywords", "channel_name", "channel_title"]]

    con.register("segments_batch", segments)
    con.execute(f"ATTACH '{rrs_dsn()}' AS rrs (TYPE POSTGRES);")

    con.execute("""
        INSERT INTO rrs.segments (segment_id, subject_id, start, s3_uri, n_keywords, channel_name, channel_title)
        SELECT segment_id, subject_id, start, s3_uri, n_keywords, channel_name, channel_title
        FROM segments_batch
        ON CONFLICT (segment_id) DO UPDATE SET
            subject_id    = EXCLUDED.subject_id,
            start         = EXCLUDED.start,
            s3_uri        = EXCLUDED.s3_uri,
            n_keywords    = EXCLUDED.n_keywords,
            channel_name  = EXCLUDED.channel_name,
            channel_title = EXCLUDED.channel_title
    """)

    logging.info(f"  {len(segments)} segment(s) upserted (subject: {SUBJECT_NAME!r}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import climate segments from quotaclimat DB into RRS DB.")
    parser.add_argument("--start-date", metavar="YYYY-MM-DD", help="Import rows with start >= this date.")
    parser.add_argument("--end-date", metavar="YYYY-MM-DD", help="Import rows with start < this date.")
    args = parser.parse_args()

    import_segments(
        start_date=date.fromisoformat(args.start_date) if args.start_date else None,
        end_date=date.fromisoformat(args.end_date) if args.end_date else None,
    )
