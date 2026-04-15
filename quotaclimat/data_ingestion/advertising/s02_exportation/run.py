import asyncio
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import func, select
from tqdm import tqdm

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.tools.mediatree import (
    MediatreeAPI,
)
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
AD_S3_PREFIX = "ads"

MARGIN_ON_MEDIA_EXPORT = timedelta(seconds=1)

PAGE_SIZE = 100
MAX_CONCURRENT_EXPORTS = 10


def get_s3_filesystem() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT_URL, "region_name": REGION},
    )


async def ad_folder_exists_in_s3(ad_id: str, fs: s3fs.S3FileSystem) -> bool:
    path = f"{BUCKET_NAME}/{AD_S3_PREFIX}/{ad_id}"
    try:
        return await fs._exists(path)
    except Exception as e:
        logger.error(f"Error checking S3 for ad {ad_id}: {e}")
        return False


def _base_ads_query(since_date: datetime):
    return (
        select(Ad, Ad_Occurrence)
        .join(Ad_Occurrence, Ad_Occurrence.ad_id == Ad.id)
        .where(Ad.first_detection_date >= since_date)
        .distinct(Ad.id)
    )


def count_ads_since(session, since_date: datetime) -> int:
    result = session.execute(
        select(func.count()).select_from(_base_ads_query(since_date).subquery())
    )
    return result.scalar()


def iter_ads_pages(session, since_date: datetime, page_size: int):
    """Yield pages of (Ad, Ad_Occurrence) using a server-side cursor."""
    result = session.execute(
        _base_ads_query(since_date),
        execution_options={"stream_results": True},
    )
    yield from result.yield_per(page_size).partitions()


async def _export_ad(
    ad: Ad, occurrence: Ad_Occurrence, api: MediatreeAPI, fs: s3fs.S3FileSystem
):
    """Export an ad to S3 based on one of its occurrences.
    Streams the media file directly to S3 to avoid writing it locally.
    """
    occurence_start_date = occurrence.occurrence_date
    occurence_end_date = occurence_start_date + timedelta(seconds=ad.duration_sec)
    channel = occurrence.channel_name

    from_date = occurence_start_date - MARGIN_ON_MEDIA_EXPORT
    to_date = occurence_end_date + MARGIN_ON_MEDIA_EXPORT

    for media_format in ("mp3", "mp4"):
        buf = io.BytesIO()
        await api.stream_export(channel, from_date, to_date, media_format, buf)

        s3_key = f"{BUCKET_NAME}/{AD_S3_PREFIX}/{ad.id}/raw.{media_format}"
        await fs._pipe_file(s3_key, buf.getvalue(), StorageClass="ONEZONE_IA")
        logger.debug(
            f"Uploaded s3://{BUCKET_NAME}/{AD_S3_PREFIX}/{ad.id}/raw.{media_format}"
        )


async def _process_ad(
    ad: Ad,
    occurrence: Ad_Occurrence,
    api: MediatreeAPI,
    fs: s3fs.S3FileSystem,
) -> str:
    """Check if an ad already exists in S3, and export it if not.
    Returns 'cached' if already in S3, 'uploaded' otherwise.
    """
    if await ad_folder_exists_in_s3(ad.id, fs):
        logger.debug(f"Ad {ad.id} already in S3, skipping")
        return "cached"

    logger.debug(f"Processing ad {ad.id} (channel={occurrence.channel_name})")
    await _export_ad(ad, occurrence, api, fs)
    return "uploaded"


async def run(since_date: datetime):
    session = get_db_session()
    fs = get_s3_filesystem()

    try:
        total = count_ads_since(session, since_date)
        logger.info(f"Found {total} ads since {since_date}")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)
        counts = {"cached": 0, "uploaded": 0, "error": 0}
        progress = tqdm(total=total, desc="Exporting ads")

        async with MediatreeAPI(max_concurrent_requests=MAX_CONCURRENT_EXPORTS) as api:
            for page in iter_ads_pages(session, since_date, PAGE_SIZE):

                async def _limited_process(ad, occurrence):
                    async with semaphore:
                        try:
                            result = await _process_ad(ad, occurrence, api, fs)
                            counts[result] += 1
                        except Exception:
                            logger.error(f"Failed to export ad {ad.id}: {result}")
                            counts["error"] += 1
                        finally:
                            progress.update(1)
                            progress.set_postfix(counts)

                tasks = [_limited_process(ad, occurrence) for ad, occurrence in page]

                await asyncio.gather(*tasks, return_exceptions=True)

        progress.close()
    finally:
        session.close()


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-detection"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        since_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
        asyncio.run(run(since_date))
