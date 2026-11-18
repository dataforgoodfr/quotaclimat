import os
from collections.abc import Iterable, Iterator
from datetime import datetime
from itertools import islice

from sqlalchemy import Executable, Select, and_, exists

from postgres.database_connection import connect_to_db
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.tools.prediction_status import (
    MANUAL_METHOD,
)

engine = connect_to_db(use_custom_json_serializer=True)

# The ads every stage handles: not soft deleted (s04_consolidation), and broadcast since
# CLASSIFICATION_START_DATE (at least one occurrence on or after it).
CLASSIFICATION_START_DATE = datetime.fromisoformat(
    os.environ.get("CLASSIFICATION_START_DATE") or "2026-01-01"
)
IN_POOL = and_(
    Ad.deleted_at.is_(None),
    exists().where(
        Ad_Occurrence.ad_id == Ad.id,
        Ad_Occurrence.deleted_at.is_(None),
        Ad_Occurrence.occurrence_date >= CLASSIFICATION_START_DATE,
    ),
)
# Ads corrected by hand (s04_consolidation/e02_manual_corrections): no stage rewrites them.
NOT_CORRECTED_BY_HAND = Ad.prediction_method.is_distinct_from(MANUAL_METHOD)


def load_row(statement: Select) -> dict:
    """The one row a select returns."""
    with engine.connect() as conn:
        return conn.execute(statement).one()._asdict()


def save(statement: Executable | None) -> None:
    """Runs the statement storing one result, in its own transaction. None saves nothing."""
    if statement is not None:
        with engine.begin() as conn:
            conn.execute(statement)


def stream_rows(statement: Select, chunk_size: int = 1000) -> Iterator[dict]:
    """The rows of a select, fetched chunk_size at a time, so a large table never sits in memory."""
    with engine.connect() as conn:
        options = conn.execution_options(yield_per=chunk_size)
        yield from options.execute(statement).mappings()


def save_in_chunks(
    statement: Executable, rows: Iterable[dict], chunk_size: int = 1000
) -> int:
    """Runs statement with each row's parameters, one transaction per chunk_size rows.
    Returns the number of rows."""
    rows = iter(rows)
    saved = 0
    while chunk := list(islice(rows, chunk_size)):
        with engine.begin() as conn:
            conn.execute(statement, chunk)
        saved += len(chunk)
    return saved
