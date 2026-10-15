"""
Export segments for a dictionary subject from the RRS DB, optionally
filtered down to an exact keyword count and/or a date range.

Equivalent to:
    SELECT * FROM segments s
    INNER JOIN subjects sb ON s.subject_id = sb.subject_id
    WHERE sb.name = :subject
    AND s.n_keywords = :n_keywords
    AND s.start >= :start_date
    AND s.start < :end_date

Usage:
    poetry run python -m rrs.keyword_detection.export_segments
    poetry run python -m rrs.keyword_detection.export_segments --subject environmental_health --n-keywords 4
    poetry run python -m rrs.keyword_detection.export_segments --subject environmental_health --start-date 2024-01-01 --end-date 2024-03-31
    poetry run python -m rrs.keyword_detection.export_segments --subject environmental_health --output ./export.csv
"""

import argparse
import contextlib
import logging
import os
import re
from datetime import date
from typing import Optional
from urllib.parse import quote

import duckdb
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

DEFAULT_SUBJECT_NAME = "environmental_health"


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


def rrs_dsn() -> str:
    return _dsn(
        host=os.getenv("RRS_PG_HOST", "localhost"),
        port=os.getenv("RRS_PG_PORT", "5432"),
        db=os.getenv("RRS_PG_DATABASE", "rrs_db"),
        user=os.getenv("RRS_PG_USER", "user"),
        password=os.getenv("RRS_PG_PASSWORD", "password"),
    )


def export_segments(
    subject_name: str = DEFAULT_SUBJECT_NAME,
    n_keywords: Optional[int] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    output_path: Optional[str] = None,
):
    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    with _masked_db_errors():
        con.execute(f"ATTACH '{rrs_dsn()}' AS rrs (TYPE POSTGRES, READ_ONLY);")

    extra_filters = ""
    params = {"subject_name": subject_name}
    if n_keywords is not None:
        extra_filters += " AND s.n_keywords = $n_keywords"
        params["n_keywords"] = n_keywords
    if start_date is not None:
        extra_filters += " AND s.start >= $start_date"
        params["start_date"] = start_date
    if end_date is not None:
        extra_filters += " AND s.start < $end_date"
        params["end_date"] = end_date

    logging.info(
        f"Fetching segments for subject={subject_name!r}"
        + (f", n_keywords={n_keywords}" if n_keywords is not None else "")
        + (f", start_date={start_date}" if start_date is not None else "")
        + (f", end_date={end_date}" if end_date is not None else "")
        + "..."
    )
    df = con.execute(
        f"""
        SELECT s.*
        FROM rrs.segments s
        INNER JOIN rrs.subjects sb ON s.subject_id = sb.subject_id
        WHERE sb.name = $subject_name
        {extra_filters}
        """,
        params,
    ).df()

    logging.info(f"  {len(df)} segment(s) fetched.")

    if output_path:
        df.to_csv(output_path, index=False)
        logging.info(f"  Saved to {output_path}")

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export segments for a dictionary subject from the RRS DB."
    )
    parser.add_argument(
        "--subject",
        default=os.getenv("SUBJECT", DEFAULT_SUBJECT_NAME),
        help=f"Subject name to filter for (default: {DEFAULT_SUBJECT_NAME}, env: SUBJECT)",
    )
    parser.add_argument(
        "--n-keywords",
        type=int,
        default=os.getenv("N_KEYWORDS"),
        help="Filter segments with exactly this number of keywords (env: N_KEYWORDS)",
    )
    parser.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("START_DATE"),
        help="Filter segments with start >= this date (env: START_DATE)",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        default=os.getenv("END_DATE"),
        help="Filter segments with start < this date (env: END_DATE)",
    )
    parser.add_argument(
        "--output",
        default=os.getenv("OUTPUT_PATH", "./output/segments_export.csv"),
        help="Path to save results as CSV (env: OUTPUT_PATH)",
    )
    args = parser.parse_args()

    export_segments(
        subject_name=args.subject,
        n_keywords=int(args.n_keywords) if args.n_keywords is not None else None,
        start_date=date.fromisoformat(args.start_date) if args.start_date else None,
        end_date=date.fromisoformat(args.end_date) if args.end_date else None,
        output_path=args.output,
    )
