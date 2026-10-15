"""
Export segments for a dictionary subject from the RRS DB, optionally
filtered down to an exact keyword count and/or a date range. The segment
text is recovered from the "plaintext" column of the parquet file at its
s3_uri on S3, matched by start timestamp.

Equivalent to:
    SELECT * FROM segments s
    INNER JOIN subjects sb ON s.subject_id = sb.subject_id
    WHERE sb.name = :subject
    AND s.n_keywords = :n_keywords
    AND s.start >= :start_date
    AND s.start < :end_date

Output columns: id, channel_name, datetime, plaintext, url
(segment_id, channel_name, start, recovered text, url_mediatree).

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
import pandas as pd
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO)

DEFAULT_SUBJECT_NAME = "environmental_health"
REGION = "fr-par"


def get_secret_docker(secret_name: str) -> str:
    value = os.environ.get(secret_name, "")
    if value and os.path.exists(value):
        with open(value, "r") as f:
            return f.read().strip()
    return value


ACCESS_KEY = get_secret_docker("BUCKET")
SECRET_KEY = get_secret_docker("BUCKET_SECRET")


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


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{REGION}';
        SET s3_endpoint='s3.{REGION}.scw.cloud';
        SET s3_access_key_id='{ACCESS_KEY}';
        SET s3_secret_access_key='{SECRET_KEY}';
        SET s3_url_style='path';
    """)


def _fetch_plaintexts(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> pd.Series:
    """Recover the "plaintext" column of each segment from its s3_uri parquet file,
    matched by start timestamp (as UTC, to sidestep tz representation mismatches)."""
    plaintext = pd.Series([None] * len(df), index=df.index, dtype=object)
    match_key = pd.to_datetime(df["start"], utc=True)

    for s3_uri, group in df.groupby("s3_uri"):
        glob = f"{s3_uri}*.parquet"
        try:
            text_df = con.sql(
                f"SELECT start, plaintext FROM read_parquet('{glob}', hive_partitioning=true, union_by_name=true)"
            ).df()
        except duckdb.Error as exc:
            logging.warning(f"Could not read parquet at {s3_uri}: {exc}")
            continue

        text_df["start"] = pd.to_datetime(text_df["start"], utc=True)
        lookup = text_df.drop_duplicates(subset="start").set_index("start")["plaintext"]
        plaintext.loc[group.index] = match_key.loc[group.index].map(lookup)

    return plaintext


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
    if df.empty:
        if output_path:
            df.to_csv(output_path, index=False)
            logging.info(f"  Saved to {output_path}")
        return df

    logging.info("Recovering plaintext from S3...")
    _configure_s3(con)
    df["plaintext"] = _fetch_plaintexts(con, df)
    logging.info(f"  {df['plaintext'].notna().sum()}/{len(df)} segment(s) matched a plaintext.")

    out = df.rename(
        columns={
            "segment_id": "id",
            "start": "datetime",
            "url_mediatree": "url",
        }
    )[["id", "channel_name", "datetime", "plaintext", "url"]]

    if output_path:
        out.to_csv(output_path, index=False)
        logging.info(f"  Saved to {output_path}")

    return out


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
