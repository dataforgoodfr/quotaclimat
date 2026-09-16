import asyncio
import logging
import os
import tempfile
from datetime import datetime, timedelta

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import select, update

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s01_detection.processor import chunk_creator
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.run import (
    get_s3_filesystem,
)
from quotaclimat.data_ingestion.advertising.tools.fingerprints import fingerprinter
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)

# The history of audio/video saving process is necessary to process them accordingly
MARGIN_ON_MEDIA_EXPORT_HISTORY = [
    (datetime(2025, 4, 1), timedelta(seconds=1)),
    (datetime(2026, 4, 27), timedelta(seconds=0.1)),
]

CLEAN_OTHER_CHUNKS = os.environ.get("CLEAN_OTHER_CHUNKS", False)
CURSOR_BATCH_SIZE = os.environ.get("CURSOR_BATCH_SIZE", 500)


def _get_margin_from_detection_date(d: datetime) -> timedelta:
    """Depending on the downloading processs, media and thus detection may vary in quality.
    We do change the margins during extraction process depending on this quality.
    This function helps find back what margin did we apply when the Ad was extracted."""
    for start, value in reversed(MARGIN_ON_MEDIA_EXPORT_HISTORY):
        if d > start:
            return value


async def _download_audio_file(
    fs: s3fs.S3FileSystem, ad_id: str, dest_dir: str
) -> str | None:
    s3_key = ad_media_s3_key(ad_id, "mp3")
    audio_file_path = os.path.join(dest_dir, ad_id + ".mp3")

    try:
        await fs._get_file(s3_key, audio_file_path)
        return audio_file_path
    except Exception as e:
        logger.error(f"Error downloading {s3_key} from S3: {e}")
        return None


async def run():
    fingerprint_hash = fingerprinter.params_hash()
    fs = get_s3_filesystem()

    counters = {
        "ads_seen": 0,
        "already_had_chunk": 0,
        "cleaned_other_chunks": 0,
        "download_failed": 0,
        "chunk_computed": 0,
    }

    with get_db_session() as read_session, get_db_session() as write_session:
        for ads in read_session.scalars(
            select(Ad).execution_options(yield_per=CURSOR_BATCH_SIZE)
        ).partitions():
            for ad in ads:
                counters["ads_seen"] += 1
                existing_chunk_entry = next(
                    filter(
                        lambda chunk: chunk.get("hash") == fingerprint_hash, ad.chunks
                    ),
                    None,
                )
                if existing_chunk_entry:
                    counters["already_had_chunk"] += 1
                    if CLEAN_OTHER_CHUNKS and len(ad.chunks) > 1:
                        counters["cleaned_other_chunks"] += 1
                        write_session.execute(
                            update(Ad)
                            .where(Ad.id == ad.id)
                            .values(chunks=[existing_chunk_entry])
                        )
                else:
                    with tempfile.TemporaryDirectory() as dest_dir:
                        audio_file_path = await _download_audio_file(
                            fs, ad.id, dest_dir
                        )

                        if audio_file_path is None:
                            counters["download_failed"] += 1
                        else:
                            fingerprints = chunk_creator.run_on_audio_file(
                                audio_file_path=audio_file_path,
                                offset=(
                                    _get_margin_from_detection_date(
                                        ad.first_detection_date
                                    ).total_seconds()
                                ),
                                duration=ad.duration_sec,
                            )
                            new_chunk_entry = Ad.generate_chunk_dict(
                                fingerprint_hash, fingerprints
                            )
                            counters["chunk_computed"] += 1

                            if CLEAN_OTHER_CHUNKS or len(ad.chunks) == 0:
                                write_session.execute(
                                    update(Ad)
                                    .where(Ad.id == ad.id)
                                    .values(chunks=[new_chunk_entry])
                                )
                            else:
                                write_session.execute(
                                    update(Ad)
                                    .where(Ad.id == ad.id)
                                    .values(chunks=[*ad.chunks, new_chunk_entry])
                                )

            write_session.commit()

    logger.info(f"Recompute saved chunks finished: {counters}")


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        asyncio.run(run())
