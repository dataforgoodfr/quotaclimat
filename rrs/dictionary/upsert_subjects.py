"""
Upsert subjects from rrs/dictionary/subjects.py into the subjects table.

Subject IDs are derived deterministically from the subject name:
  - lowercased
  - non-alphanumeric characters replaced with underscores
  - consecutive/leading/trailing underscores collapsed and stripped

Running this script multiple times is safe (idempotent).

Usage:
    poetry run python -m rrs.scripts.upsert_subjects

Environment variables (defaults match rrs/.env.dist):
    RRS_PG_HOST, RRS_PG_PORT, RRS_PG_DATABASE, RRS_PG_USER, RRS_PG_PASSWORD
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from rrs.dictionary.subjects import subjects
from rrs.schemas.models import Subject
from rrs.utils.generate_id import get_consistent_hash

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


def subject_id(name: str) -> str:
    return get_consistent_hash(name)


def get_engine():
    host = os.getenv("RRS_PG_HOST", "localhost")
    port = os.getenv("RRS_PG_PORT", "5432")
    database = os.getenv("RRS_PG_DATABASE", "rrs_db")
    user = os.getenv("RRS_PG_USER", "user")
    password = os.getenv("RRS_PG_PASSWORD", "password")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")


def upsert_subjects() -> None:
    engine = get_engine()
    Session = sessionmaker(bind=engine)

    rows = [{"subject_id": subject_id(name), "name": name} for name in subjects]

    with Session() as session:
        stmt = (
            insert(Subject)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["subject_id"],
                set_={"name": insert(Subject).excluded.name},
            )
        )
        session.execute(stmt)
        session.commit()

    for row in rows:
        print(f"  upserted: {row['subject_id']!r}  ({row['name']!r})")
    print(f"\n{len(rows)} subject(s) upserted.")


if __name__ == "__main__":
    upsert_subjects()
