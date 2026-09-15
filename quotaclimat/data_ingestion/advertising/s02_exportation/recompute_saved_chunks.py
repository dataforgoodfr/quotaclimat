import asyncio
import logging
import os
from datetime import datetime, timedelta

from sentry_sdk.crons import monitor
from sqlalchemy import select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

from ..tools.fingerprints import fingerprinter

logger = logging.getLogger(__name__)

# The history of audio/video saving process is necessary to process them accordingly
MARGIN_ON_MEDIA_EXPORT_HISTORY = [
    (datetime(2025, 4, 1), timedelta(seconds=1)),
    (datetime(2026, 4, 27), timedelta(seconds=0.1)),
]

CLEAN_OTHER_CHUNKS = os.environ.get("CLEAN_OTHER_CHUNKS", False)
CURSOR_BATCH_SIZE = os.environ.get("CURSOR_BATCH_SIZE", 500)


async def run():
    fingerprint_hash = fingerprinter.params_hash()

    with get_db_session() as read_session, get_db_session() as write_session:
        for ads in read_session.scalars(
            select(Ad).execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
            for ad in ads:
                existing_chunk_entry = next(
                    filter(
                        lambda chunk: chunk.get("hash") == fingerprint_hash, ad.chunks
                    )
                )
                if existing_chunk_entry:
                    if CLEAN_OTHER_CHUNKS and len(ad.chunks) > 1:
                        ad.chunks = [existing_chunk_entry]
                        write_session.add(ad)
                else:
                    new_chunk_entry = None
                    if CLEAN_OTHER_CHUNKS or len(ad.chunks) == 0:
                        ad.chunks = [new_chunk_entry]
                        write_session.add(ad)
                    else:
                        ad.chunks.append(ad)
                        write_session.add(ad)

            write_session.commit()


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        asyncio.run(run())
