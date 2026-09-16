import asyncio
import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import func, select
from sqlalchemy.orm import aliased

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
    ad_prefix_in_bucket,
)
from quotaclimat.data_ingestion.advertising.tools.interactive_tqdm import (
    interactive_tqdm,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    cleanup_day_media_parts,
    download_media_parts,
    extract_segment,
    required_part_starts,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    get_s3_filesystem as get_mediatree_s3_filesystem,
)
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
AD_S3_PREFIX = "ads"

MARGIN_ON_MEDIA_EXPORT = timedelta(
    seconds=float(os.environ.get("MARGIN_ON_MEDIA_EXPORT", 0.1))
)

MIN_BYTES_PER_SECOND_VIDEO = (
    10_000  # ~80 kbps; below this threshold the mp4 is likely corrupted
)

PAGE_SIZE = 100
MAX_CONCURRENT_EXPORTS = 10

LOCAL_CACHE_DIR = "./.cache/mediatree_export"


def get_s3_filesystem() -> s3fs.S3FileSystem:
    # Both the mediatree source bucket and the advertising destination bucket live on
    # the same endpoint/credentials; the bucket name is already part of every S3 path
    # used below, so one filesystem instance can read from one and write to the other.
    return get_mediatree_s3_filesystem()


async def ad_folder_exists_in_s3(ad_id: str, fs: s3fs.S3FileSystem) -> bool:
    path = ad_prefix_in_bucket(ad_id)
    try:
        return await fs._exists(path)
    except Exception as e:
        logger.error(f"Error checking S3 for ad {ad_id}: {e}")
        return False


async def get_raw_mp4_size_in_s3(ad_id: str, fs: s3fs.S3FileSystem) -> int | None:
    path = ad_media_s3_key(ad_id, "mp4")
    try:
        info = await fs._info(path)
        return info.get("size")
    except Exception:
        return None


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


def _grouped_ads_query(since_date: datetime):
    """Same rows as `_base_ads_query`, but read back out of its DISTINCT ON result so
    they can be ordered by channel/date -- Postgres requires DISTINCT ON's own ORDER BY
    to start with the distinct-on expression (Ad.id here), so this order has to be
    applied on top of it rather than combined into the same query.
    """
    inner = _base_ads_query(since_date).subquery()
    ad_alias = aliased(Ad, inner)
    occurrence_alias = aliased(Ad_Occurrence, inner)
    return select(ad_alias, occurrence_alias).order_by(
        occurrence_alias.channel_name, occurrence_alias.occurrence_date
    )


def iter_ads_by_channel_day(session, since_date: datetime, page_size: int):
    """Stream (Ad, Ad_Occurrence) rows from Postgres via a server-side cursor, ordered
    by channel and occurrence date, yielding one (channel, day) group -- where day is
    the occurrence's UTC calendar date, matching how mediatree lays out its S3 bucket --
    at a time as the sorted stream progresses. Only one group is ever held in memory,
    rather than every ad since `since_date`.
    """
    result = session.execute(
        _grouped_ads_query(since_date),
        execution_options={"stream_results": True},
    )

    current_key = None
    current_group: list[tuple[Ad, Ad_Occurrence]] = []

    for ad, occurrence in result.yield_per(page_size):
        key = (occurrence.channel_name, occurrence.occurrence_date.date())
        if current_key is not None and key != current_key:
            yield current_key, current_group
            current_group = []
        current_key = key
        current_group.append((ad, occurrence))

    if current_group:
        yield current_key, current_group


def _ad_export_window(ad: Ad, occurrence: Ad_Occurrence) -> tuple[datetime, datetime]:
    occurrence_start = occurrence.occurrence_date.replace(tzinfo=ZoneInfo("UTC"))
    occurrence_end = occurrence_start + timedelta(seconds=ad.duration_sec)

    from_date = occurrence_start - MARGIN_ON_MEDIA_EXPORT
    to_date = occurrence_end + MARGIN_ON_MEDIA_EXPORT
    return from_date, to_date


async def _export_ad(
    ad: Ad,
    occurrence: Ad_Occurrence,
    from_date: datetime,
    to_date: datetime,
    parts: dict[datetime, dict[str, str]],
    local_dir: str,
    fs: s3fs.S3FileSystem,
):
    """Extract an ad's segment out of the downloaded mediatree parts and upload it to
    S3, for both the mp3 and mp4 formats.
    """
    for media_format in ("mp3", "mp4"):
        local_path = os.path.join(local_dir, f"{ad.id}.{media_format}")
        found = await extract_segment(
            parts, from_date, to_date, media_format, local_path
        )
        if not found:
            raise RuntimeError(
                f"Missing mediatree parts in bucket for ad {ad.id} "
                f"(channel={occurrence.channel_name}, format={media_format})"
            )

        s3_key = ad_media_s3_key(ad.id, media_format)
        try:
            await fs._put_file(local_path, s3_key, StorageClass="ONEZONE_IA")
            logger.debug(f"Uploaded s3://{s3_key}")
        finally:
            os.remove(local_path)


async def _ad_needs_export(ad: Ad, fs: s3fs.S3FileSystem) -> bool:
    """True if the ad is missing from S3, or present with a suspiciously small mp4."""
    if not await ad_folder_exists_in_s3(ad.id, fs):
        return True

    mp4_size = await get_raw_mp4_size_in_s3(ad.id, fs)
    min_expected_size = (ad.duration_sec + 2) * MIN_BYTES_PER_SECOND_VIDEO
    return mp4_size is not None and mp4_size < min_expected_size


async def run(since_date: datetime):
    session = get_db_session()
    fs = get_s3_filesystem()

    missing_ads = []

    try:
        total = count_ads_since(session, since_date)
        logger.info(f"Found {total} ads since {since_date}")

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)
        progress = interactive_tqdm(total=total, desc="Exporting ads")

        for (channel, day), ads in iter_ads_by_channel_day(
            session, since_date, PAGE_SIZE
        ):

            async def _check(ad, occurrence):
                async with semaphore:
                    return (ad, occurrence, await _ad_needs_export(ad, fs))

            checked = await asyncio.gather(
                *(_check(ad, occurrence) for ad, occurrence in ads)
            )

            # (ad, occurrence, from_date, to_date) for ads that still need exporting.
            needs_export = []
            for ad, occurrence, needs in checked:
                if needs:
                    from_date, to_date = _ad_export_window(ad, occurrence)
                    needs_export.append((ad, occurrence, from_date, to_date))
                else:
                    progress.count("cached")
                    progress.update(1)

            if not needs_export:
                continue

            # Only fetch the 2-minutes archives actually covering these ads' segments,
            # not the whole day -- a day can hold ~720 parts while most days only have
            # a handful of ads to export.
            part_starts = required_part_starts(
                [(from_date, to_date) for _, _, from_date, to_date in needs_export]
            )

            local_dir = os.path.join(LOCAL_CACHE_DIR, channel, day.isoformat())
            try:
                try:
                    parts = await download_media_parts(
                        fs,
                        channel,
                        part_starts,
                        local_dir,
                        max_concurrent_downloads=MAX_CONCURRENT_EXPORTS,
                        disable_progress=True,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to download mediatree archives for {channel}/{day}: {e}"
                    )
                    for ad, _, _, _ in needs_export:
                        missing_ads.append(ad.id)
                        progress.count("error")
                        progress.update(1)
                    continue

                async def _limited_export(ad, occurrence, from_date, to_date):
                    async with semaphore:
                        try:
                            await _export_ad(
                                ad, occurrence, from_date, to_date, parts, local_dir, fs
                            )
                            progress.count("uploaded")
                        except Exception as e:
                            logger.error(f"Failed to export ad {ad.id}: {e}")
                            missing_ads.append(ad.id)
                            progress.count("error")
                        finally:
                            progress.update(1)

                await asyncio.gather(
                    *(
                        _limited_export(ad, occurrence, from_date, to_date)
                        for ad, occurrence, from_date, to_date in needs_export
                    ),
                    return_exceptions=True,
                )
            finally:
                cleanup_day_media_parts(local_dir)

        progress.close()
    finally:
        logger.info(f"Finished, here are the {len(missing_ads)} missing ads")
        logger.info(",".join(missing_ads))
        session.close()


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        str_start_date = os.environ.get("START_DATE")
        start_date = datetime.fromisoformat(str_start_date).replace(
            tzinfo=ZoneInfo("Europe/Paris")
        )

        asyncio.run(run(start_date))
