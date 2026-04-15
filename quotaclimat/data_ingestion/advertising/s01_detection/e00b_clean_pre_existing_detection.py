import logging
from datetime import datetime, timezone

from sqlalchemy import and_, or_, update

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad_Occurrence

from .e00_partition_window import Segment

logger = logging.getLogger(__name__)


def clean_pre_existing_detections(segments: list[Segment]) -> int:
    """Soft-delete all Ad_Occurrence rows whose occurrence_date falls within
    any of the given segments (matched by channel and time window).

    Returns the number of rows soft-deleted.
    """
    if not segments:
        return 0

    conditions = [
        and_(
            Ad_Occurrence.channel_name == segment.channel,
            Ad_Occurrence.occurrence_date >= segment.start_date,
            Ad_Occurrence.occurrence_date < segment.end_date,
        )
        for segment in segments
    ]

    now = datetime.now(tz=timezone.utc)

    session = get_db_session()
    try:
        result = session.execute(
            update(Ad_Occurrence)
            .where(
                and_(
                    Ad_Occurrence.deleted_at.is_(None),
                    or_(*conditions),
                )
            )
            .values(deleted_at=now)
        )
        session.commit()
        count = result.rowcount
        logger.info(
            f"Soft-deleted {count} Ad_Occurrence rows across {len(segments)} segments."
        )
        return count
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
