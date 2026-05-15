"""
Upsert keyword records from the quotaclimat keyword dictionary into the
dictionary table, tagged with the "climate" subject.

Keyword IDs are derived deterministically from (keyword, subject_id) so
running this script multiple times is safe (idempotent).

Usage:
    poetry run python -m rrs.dictionary.upsert_dictionary

Environment variables (defaults match rrs/.env.dist):
    RRS_PG_HOST, RRS_PG_PORT, RRS_PG_DATABASE, RRS_PG_USER, RRS_PG_PASSWORD
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from rrs.dictionary.subjects import subjects
from rrs.dictionary.upsert_subjects import subject_id as make_subject_id
from rrs.schemas.models import DictionaryEntry
from rrs.utils.generate_id import get_consistent_hash

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

SUBJECT_NAME = "climate"


def keyword_id(keyword: str, subject_id: str) -> str:
    return get_consistent_hash(f"{keyword}:{subject_id}")


def get_engine():
    host = os.getenv("RRS_PG_HOST", "localhost")
    port = os.getenv("RRS_PG_PORT", "5432")
    database = os.getenv("RRS_PG_DATABASE", "rrs_db")
    user = os.getenv("RRS_PG_USER", "user")
    password = os.getenv("RRS_PG_PASSWORD", "password")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def extract_rows(subject_id: str) -> list[dict]:
    """Flatten THEME_KEYWORDS into upsertable rows, deduplicating by keyword."""
    seen: set[str] = set()
    rows = []
    for entries in THEME_KEYWORDS.values():
        for entry in entries:
            kw = entry["keyword"]
            if entry.get("language") != "french":
                continue
            if kw in seen:
                continue
            seen.add(kw)
            rows.append({
                "keyword_id": keyword_id(kw, subject_id),
                "subject_id": subject_id,
                "keyword": kw,
                "high_risk_false_positive": entry.get("high_risk_of_false_positive"),
            })
    return rows


def upsert_dictionary() -> None:
    if SUBJECT_NAME not in subjects:
        raise ValueError(
            f"Subject {SUBJECT_NAME!r} not found in rrs/dictionary/subjects.py"
        )

    sid = make_subject_id(SUBJECT_NAME)
    rows = extract_rows(sid)

    engine = get_engine()
    Session = sessionmaker(bind=engine)

    batch_size = 500
    upserted = 0
    with Session() as session:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            stmt = (
                insert(DictionaryEntry)
                .values(batch)
                .on_conflict_do_update(
                    index_elements=["keyword_id"],
                    set_={
                        "subject_id": insert(DictionaryEntry).excluded.subject_id,
                        "keyword": insert(DictionaryEntry).excluded.keyword,
                        "high_risk_false_positive": insert(DictionaryEntry).excluded.high_risk_false_positive,
                    },
                )
            )
            session.execute(stmt)
            upserted += len(batch)
        session.commit()

    print(f"{upserted} keyword(s) upserted (subject: {SUBJECT_NAME!r}, subject_id: {sid!r}).")


if __name__ == "__main__":
    upsert_dictionary()
