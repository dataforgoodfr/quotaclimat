"""
Import cases from the analytics.task_global_completion table into the RRS cases table.

Reads from the quotaclimat PostgreSQL DB (POSTGRES_* env vars) and upserts into
the RRS PostgreSQL DB (RRS_PG_* env vars).

Usage:
    poetry run python -m rrs.keyword_detection.import_cases
    poetry run python -m rrs.keyword_detection.import_cases --start-date 2024-01-01 --end-date 2024-03-31
"""

import argparse
import contextlib
import logging
import os
import re
from datetime import date, timedelta
from typing import Optional
from urllib.parse import quote

import duckdb
import psycopg
from dotenv import load_dotenv
from quotaclimat.data_processing.mediatree.i8n.country import FRANCE
from rrs.utils.generate_id import get_consistent_hash

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

SUBJECT_NAME = "climate"

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


def _rrs_conninfo() -> str:
    return (
        f"host={os.getenv('RRS_PG_HOST', 'localhost')} "
        f"port={os.getenv('RRS_PG_PORT', 5432)} "
        f"dbname={os.getenv('RRS_PG_DATABASE', 'rrs_db')} "
        f"user={os.getenv('RRS_PG_USER', 'user')} "
        f"password={os.getenv('RRS_PG_PASSWORD', 'supersecret')}"
    )


def _get_auto_date_range() -> tuple[Optional[date], Optional[date]]:
    """Return (max cases.start date, max segments.start date) from the RRS DB."""
    with psycopg.connect(_rrs_conninfo()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(start::date) FROM cases")
            max_cases: Optional[date] = cur.fetchone()[0]
            cur.execute("SELECT MAX(start::date) FROM segments")
            max_segments: Optional[date] = cur.fetchone()[0]
    return max_cases, max_segments


def get_url_labelstudio(task_id, tab_id=121):
    return (
        "https://barometre7kfudatm-labelstudio.functions.fnc.fr-par.scw.cloud/"
        f"projects/6/data?tab={tab_id}&task={task_id}"
    )


def import_cases(start_date: date = None, end_date: date = None) -> None:
    if start_date is None:
        logging.info("No start date provided — querying RRS DB for date range...")
        max_cases, max_segments = _get_auto_date_range()
        if max_segments is None:
            logging.info("No segments found in RRS DB — nothing to import.")
            return
        start_date = max_cases
        # end_date filter is exclusive, so add one day to include max_segments date
        end_date = max_segments + timedelta(days=1)
        logging.info(f"  Auto range: {start_date} → {max_segments} (end_date exclusive: {end_date})")

    subject_id = get_consistent_hash(SUBJECT_NAME)

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    with _masked_db_errors():
        con.execute(f"ATTACH '{source_dsn()}' AS barometre (TYPE POSTGRES, READ_ONLY);")

    date_filter = ""
    params = {}
    if start_date is not None:
        date_filter += " AND data_item_start >= $start_date"
        params["start_date"] = start_date
    if end_date is not None:
        date_filter += " AND data_item_start < $end_date"
        params["end_date"] = end_date

    logging.info("Fetching rows from analytics.task_global_completion...")
    df = con.execute(
        f"""
        SELECT
            task_aggregate_id,
            task_id,
            data_item_id,
            data_item_start,
            data_item_model_result,
            data_item_model_reason,
            data_item_plaintext_whisper
        FROM barometre.analytics.task_global_completion
        WHERE country = 'france'
        AND data_item_channel in ({", ".join([f"'{c}'" for c in FRANCE.channels])})
        {date_filter}
        """,
        params if params else {},
    ).df()

    logging.info(f"  {len(df)} row(s) fetched.")
    if df.empty:
        logging.info("Nothing to upsert.")
        return

    df["case_id"] = df["task_aggregate_id"]
    df["segment_id"] = df["data_item_id"]
    df["subject_id"] = subject_id
    df["start"] = df["data_item_start"]
    df["model_score"] = df["data_item_model_result"]
    df["model_reason"] = df["data_item_model_reason"]
    df["text"] = df["data_item_plaintext_whisper"]
    df["url_labelstudio"] = df.apply(lambda r: get_url_labelstudio(r["task_id"]), axis=1)

    cases = df[
        [
            "case_id",
            "segment_id",
            "subject_id",
            "start",
            "model_score",
            "model_reason",
            "text",
            "url_labelstudio"
        ]
    ]

    con.register("cases_batch", cases)
    with _masked_db_errors():
        con.execute(f"ATTACH '{rrs_dsn()}' AS rrs (TYPE POSTGRES);")

    con.execute("""
        INSERT INTO rrs.cases (case_id, segment_id, subject_id, start, model_score, model_reason, text, created_at, updated_at)
        SELECT case_id, segment_id, subject_id, start, model_score, model_reason, text, now() AT TIME ZONE 'utc', now() AT TIME ZONE 'utc'
        FROM cases_batch
        ON CONFLICT (case_id, segment_id, subject_id) DO UPDATE SET
            start           = EXCLUDED.start,
            model_score     = EXCLUDED.model_score,
            model_reason    = EXCLUDED.model_reason,
            text            = EXCLUDED.text,
            url_labelstudio = EXCLUDED.url_labelstudio,
            created_at      = CASE WHEN cases.created_at IS NULL THEN now() AT TIME ZONE 'utc' ELSE cases.created_at END,
            updated_at      = now() AT TIME ZONE 'utc'
    """)

    logging.info(f"  {len(cases)} case(s) upserted (subject: {SUBJECT_NAME!r}).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import cases from analytics.task_global_completion into RRS DB."
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("START_DATE"),
        help="Import rows with data_item_start >= this date.",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("END_DATE"),
        help="Import rows with data_item_start < this date.",
    )
    args = parser.parse_args()

    import_cases(
        start_date=date.fromisoformat(args.start_date) if args.start_date else None,
        end_date=date.fromisoformat(args.end_date) if args.end_date else None,
    )
