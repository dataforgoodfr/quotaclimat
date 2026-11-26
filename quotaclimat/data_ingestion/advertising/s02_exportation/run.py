import asyncio
import logging
import math
import os
import shutil
import tempfile
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import s3fs
from sentry_sdk.crons import monitor
from sqlalchemy import BigInteger, Date, cast, exists, func, or_, select, update
from sqlalchemy.orm import aliased

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Merge, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
    ad_prefix_in_bucket,
    get_margin_from_detection_date,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.merged_ads import (
    mp4_to_mp3,
    plan_merged_ad,
    render_merged_ad,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.run import (
    ScreenshotResult,
    save_results,
    screenshot_ad_from_video,
)
from quotaclimat.data_ingestion.advertising.tools.ad_merge import (
    MERGE_STATUS_ACTIVE,
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

# Mosaic of the frames of every exported ad (s05_enrichment/e02_screenshots), made from the local
# raw.mp4 before it is deleted: no download, and the margin is the one just applied.
SCREENSHOT_ON_EXPORT = os.environ.get("SCREENSHOT_ON_EXPORT", "true").lower() in (
    "1",
    "true",
    "yes",
)

# false (default): the ads that are not merged ads. true: only the merged ads of an active merge
# (advertising.ad_merge, s04_consolidation): cut from mediatree like the others when the archives of
# their most recent occurrence are still there, rebuilt from the videos of their source ads otherwise
# (see merged_ads.py).
EXPORT_MERGED_ADS = os.environ.get("EXPORT_MERGED_ADS", "false").lower() in (
    "1",
    "true",
    "yes",
)

MIN_BYTES_PER_SECOND_VIDEO = (
    10_000  # ~80 kbps; below this threshold the mp4 is likely corrupted
)


def _env_int(name: str) -> int | None:
    value = os.environ.get(name)
    return int(value) if value else None


def _job_cpus() -> int:
    """CPUs of the Scaleway job (SCW_SLS_CPU, in mVCPU), those of the machine otherwise."""
    mvcpu = os.environ.get("SCW_SLS_CPU")
    if mvcpu:
        return max(1, math.floor(int(mvcpu) / 1000))
    return os.cpu_count() or 1


PAGE_SIZE = 100
# Ads cut/rebuilt (ffmpeg), uploaded and screenshotted at once. The encoding is most of the CPU of the
# run: more exports than CPUs only made them wait for each other and filled the memory of the job.
MAX_CONCURRENT_EXPORTS = _env_int("MAX_CONCURRENT_EXPORTS") or _job_cpus()
# Ads checked in the bucket at once (one HEAD request each): apart from the exports, so that the
# checks of a group don't wait behind the ffmpeg of the other groups.
MAX_CONCURRENT_CHECKS = 50
# How many (channel, day) groups to check/download/export at once. Groups write to
# their own local_dir and don't share state, so this overlaps one group's downloads
# with another's ffmpeg extraction/upload instead of running the pipeline in lockstep.
MAX_CONCURRENT_GROUPS = 3
# S3 downloads of mediatree archives at once, per group.
MAX_CONCURRENT_DOWNLOADS_PER_GROUP = 6

# The run stops starting new (channel, day) groups after this time, finishes the started ones and
# ends normally, before the timeout of the Scaleway job (6 h): every group is saved as soon as it is
# done, and the next run goes on with the ads left. A group takes up to ~30 min on the busiest days.
MAX_RUN_DURATION = timedelta(minutes=_env_int("MAX_RUN_DURATION_MIN") or 300)
# Stops starting new groups once this many ads were taken (unset: no limit).
MAX_ADS = _env_int("MAX_ADS")

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


def _min_mp4_bytes(duration_sec):
    """Below this size the mp4 is likely corrupted (works on a column or a float)."""
    return (duration_sec + 2) * MIN_BYTES_PER_SECOND_VIDEO


def margin_sec_at(export_date: datetime) -> float | None:
    """Margin the exportation applied to a video saved at export_date (MARGIN_ON_MEDIA_EXPORT_HISTORY of
    ad_bucket.py, which follows the changes of MARGIN_ON_MEDIA_EXPORT); None before the history."""
    if export_date.tzinfo is not None:
        export_date = export_date.astimezone(timezone.utc).replace(tzinfo=None)
    margin = get_margin_from_detection_date(export_date)
    return margin.total_seconds() if margin is not None else None


def media_export_info(
    mp4_bytes: int | None,
    margin_sec: float | None,
    exported_at: datetime | None = None,
    method: str | None = None,
) -> dict:
    """Value of advertising.ad.media_export (see the model)."""
    exported_at = exported_at or datetime.now(timezone.utc)
    info = {
        "exported_at": exported_at.isoformat(),
        "mp4_bytes": mp4_bytes,
        "margin_sec": margin_sec,
    }
    if method is not None:
        info["method"] = method
    return info


def _base_ads_query(
    start_date: datetime | None,
    end_date: datetime | None,
    merged_ads: bool = False,
):
    base_query = (
        select(Ad, Ad_Occurrence)
        .join(Ad_Occurrence, Ad_Occurrence.ad_id == Ad.id)
        .where(Ad.deleted_at.is_(None))
    )
    if merged_ads:
        base_query = base_query.where(
            exists(
                select(1)
                .where(Ad_Merge.merged_ad_id == Ad.id)
                .where(Ad_Merge.status == MERGE_STATUS_ACTIVE)
            )
        )
    else:
        # a merged ad, whatever the status of its merge, is left to EXPORT_MERGED_ADS=true
        base_query = base_query.where(
            ~exists(select(1).where(Ad_Merge.merged_ad_id == Ad.id))
        )

    # Only the ads left to export: not known as exported (media_export NULL, checked in the bucket), or
    # exported with a too small mp4. An ad exported without a known size is trusted.
    mp4_bytes = Ad.media_export["mp4_bytes"].astext.cast(BigInteger)
    base_query = base_query.where(
        or_(
            Ad.media_export.is_(None),
            mp4_bytes < _min_mp4_bytes(Ad.duration_sec),
        )
    )

    if start_date:
        base_query = base_query.where(Ad.first_detection_date >= start_date)
    if end_date:
        base_query = base_query.where(Ad.first_detection_date < end_date)

    # DISTINCT ON keeps the first row of each Ad.id in ORDER BY order, so this makes it
    # deterministically keep the ad's most recent occurrence rather than an arbitrary one.
    return base_query.distinct(Ad.id).order_by(
        Ad.id, Ad_Occurrence.occurrence_date.desc()
    )


def count_ads_since(
    session,
    start_date: datetime | None,
    end_date: datetime | None,
    merged_ads: bool = False,
) -> int:
    result = session.execute(
        select(func.count()).select_from(
            _base_ads_query(start_date, end_date, merged_ads).subquery()
        )
    )
    return result.scalar()


def _grouped_ads_query(
    start_date: datetime | None,
    end_date: datetime | None,
    merged_ads: bool = False,
):
    """Same rows as `_base_ads_query`, but read back out of its DISTINCT ON result so
    they can be ordered by day/channel -- Postgres requires DISTINCT ON's own ORDER BY
    to start with the distinct-on expression (Ad.id here), so this order has to be
    applied on top of it rather than combined into the same query.

    Day first: a run stopped by MAX_RUN_DURATION_MIN or MAX_ADS has caught up the oldest
    days of every channel, and the next run goes on with the following days. The day is the
    UTC calendar date of the occurrence, the key of the groups (iter_ads_by_channel_day).
    """
    inner = _base_ads_query(start_date, end_date, merged_ads).subquery()
    ad_alias = aliased(Ad, inner)
    occurrence_alias = aliased(Ad_Occurrence, inner)
    return select(ad_alias, occurrence_alias).order_by(
        cast(occurrence_alias.occurrence_date, Date),
        occurrence_alias.channel_name,
        occurrence_alias.occurrence_date,
    )


def iter_ads_by_channel_day(
    session,
    start_date: datetime | None,
    end_date: datetime | None,
    page_size: int,
    merged_ads: bool = False,
):
    """Stream (Ad, Ad_Occurrence) rows from Postgres via a server-side cursor, ordered
    by occurrence day and channel, yielding one (channel, day) group -- where day is
    the occurrence's UTC calendar date, matching how mediatree lays out its S3 bucket --
    at a time as the sorted stream progresses. Only one group is ever held in memory,
    rather than every ad inside the start and end dates.
    """
    result = session.execute(
        _grouped_ads_query(start_date, end_date, merged_ads),
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
    media_sizes: dict | None = None,
) -> ScreenshotResult | None:
    """Extract an ad's segment out of the downloaded mediatree parts and upload it to
    S3, for both the mp3 and mp4 formats.

    With SCREENSHOT_ON_EXPORT, also makes the mosaic of the ad from the mp4 and returns its
    result (an `error` result when only the mosaic failed: the export itself succeeded).
    With media_sizes, fills it with the size of each uploaded file ({"mp4": bytes, ...}).
    """
    screenshot = None
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
            if media_sizes is not None:
                media_sizes[media_format] = os.path.getsize(local_path)
            await fs._put_file(local_path, s3_key, StorageClass="ONEZONE_IA")
            logger.debug(f"Uploaded s3://{s3_key}")
            if media_format == "mp4" and SCREENSHOT_ON_EXPORT:
                screenshot = await _screenshot_exported_ad(
                    ad, local_path, local_dir, fs
                )
        finally:
            os.remove(local_path)
    return screenshot


async def _rebuild_merged_ad(
    ad: Ad,
    plan_merged,
    local_dir: str,
    fs: s3fs.S3FileSystem,
    media_sizes: dict | None = None,
) -> ScreenshotResult | None:
    """Rebuild the mp4 of a merged ad from the videos of its source ads (merged_ads.py), with the
    usual export margin, make its mp3 from it, and upload both like _export_ad.

    plan_merged(ad_id) reads the plan in the database (synchronous: no other task uses its session
    meanwhile). Raises RebuildError when the video can't be rebuilt.
    """
    plan = plan_merged(ad.id)
    work_dir = tempfile.mkdtemp(dir=local_dir, prefix=f"rebuild_{ad.id}_")
    try:
        mp4_path = os.path.join(work_dir, f"{ad.id}.mp4")
        mp3_path = os.path.join(work_dir, f"{ad.id}.mp3")
        rebuilt = await render_merged_ad(
            fs,
            plan,
            mp4_path,
            work_dir,
            MARGIN_ON_MEDIA_EXPORT.total_seconds(),
        )
        await mp4_to_mp3(mp4_path, mp3_path)
        for media_format, local_path in (("mp3", mp3_path), ("mp4", mp4_path)):
            if media_sizes is not None:
                media_sizes[media_format] = os.path.getsize(local_path)
            await fs._put_file(
                local_path,
                ad_media_s3_key(ad.id, media_format),
                StorageClass="ONEZONE_IA",
            )
        logger.info(
            f"Rebuilt merged ad {ad.id} from its sources ({plan.layout} layout"
            + (f", occurrence {plan.occurrence_id}" if plan.occurrence_id else "")
            + (f", source merged ads rebuilt too: {rebuilt}" if rebuilt else "")
            + ")"
        )
        if SCREENSHOT_ON_EXPORT:
            return await _screenshot_exported_ad(ad, mp4_path, work_dir, fs)
        return None
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def _screenshot_exported_ad(
    ad: Ad,
    video_path: str,
    local_dir: str,
    fs: s3fs.S3FileSystem,
) -> ScreenshotResult:
    # The video starts the margin before the occurrence (see _ad_export_window)
    margin_sec = MARGIN_ON_MEDIA_EXPORT.total_seconds()
    work_dir = tempfile.mkdtemp(dir=local_dir, prefix=f"screenshot_{ad.id}_")
    try:
        return await screenshot_ad_from_video(
            fs, ad.id, ad.duration_sec, video_path, margin_sec, work_dir
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


async def _check_in_bucket(ad: Ad, fs: s3fs.S3FileSystem) -> tuple[bool, dict | None]:
    """(needs export, media_export found in the bucket).

    An ad with a media_export was only selected because its mp4 is too small: exported again without
    a request. Otherwise its raw.mp4 is looked up (one request): missing or too small, the ad is
    exported; present, its media_export is made from it (no method: not known) so that the next runs
    no longer check it. A folder without raw.mp4 is left as before (not exported again) and checked
    again at the next run.
    """
    if ad.media_export is not None:
        return True, None
    try:
        info = await fs._info(ad_media_s3_key(ad.id, "mp4"))
    except FileNotFoundError:
        return not await ad_folder_exists_in_s3(ad.id, fs), None
    except Exception as e:
        logger.error(f"Error checking S3 for ad {ad.id}: {e}")
        return True, None

    size = info.get("size")
    if size is not None and size < _min_mp4_bytes(ad.duration_sec):
        return True, None
    exported_at = info.get("LastModified")
    return False, media_export_info(
        size,
        margin_sec_at(exported_at) if exported_at else None,
        exported_at=exported_at,
    )


async def _process_group(
    channel: str,
    day,
    ads: list,
    fs: s3fs.S3FileSystem,
    semaphore: asyncio.Semaphore,
    progress,
    missing_ads: list,
    reset_no_data_ads: list,
    screenshots: list,
    plan_merged=None,
    exported: dict | None = None,
    check_semaphore: asyncio.Semaphore | None = None,
) -> None:
    """Check, download and export every ad in one (channel, day) group. Groups are
    independent of each other (own local_dir, own downloaded parts), so this is meant
    to be run concurrently across groups by the caller.

    With plan_merged (EXPORT_MERGED_ADS), an ad whose mediatree archives are missing is
    rebuilt from the videos of its source ads instead (see _rebuild_merged_ad).
    `exported` gets the media_export of every ad exported or found in the bucket ({ad id: value}),
    for the caller to save. The checks take `check_semaphore` (`semaphore` when not given).
    """
    group_start = time.monotonic()
    if exported is None:
        exported = {}
    check_semaphore = check_semaphore or semaphore

    async def _check(ad, occurrence):
        async with check_semaphore:
            return (ad, occurrence, *await _check_in_bucket(ad, fs))

    checked = await asyncio.gather(*(_check(ad, occurrence) for ad, occurrence in ads))
    check_elapsed = time.monotonic() - group_start

    # (ad, occurrence, from_date, to_date) for ads that still need exporting.
    needs_export = []
    for ad, occurrence, needs, found in checked:
        if found is not None:
            exported[ad.id] = found
        if needs:
            from_date, to_date = _ad_export_window(ad, occurrence)
            needs_export.append((ad, occurrence, from_date, to_date))
        else:
            if ad.fragment_type == "no_data":
                # The media is already in the bucket, so whatever previously marked
                # this ad "no_data" no longer reflects the bucket's actual state.
                reset_no_data_ads.append(ad.id)
            progress.count("cached")
            progress.update(1)

    if not needs_export:
        logger.info(
            f"[{channel}/{day}] {len(ads)} ads all cached, checked in "
            f"{check_elapsed:.1f}s"
        )
        return

    # Only fetch the 2-minutes archives actually covering these ads' segments, not the
    # whole day -- a day can hold ~720 parts while most days only have a handful of ads
    # to export.
    part_starts = required_part_starts(
        [(from_date, to_date) for _, _, from_date, to_date in needs_export]
    )

    local_dir = os.path.join(LOCAL_CACHE_DIR, channel, day.isoformat())
    try:
        download_start = time.monotonic()
        try:
            parts = await download_media_parts(
                fs,
                channel,
                part_starts,
                local_dir,
                max_concurrent_downloads=MAX_CONCURRENT_DOWNLOADS_PER_GROUP,
                disable_progress=True,
            )
        except Exception as e:
            logger.error(
                f"Failed to download mediatree archives for {channel}/{day}: {e}"
            )
            if plan_merged is None:
                for ad, _, _, _ in needs_export:
                    missing_ads.append(ad.id)
                    progress.count("error")
                    progress.update(1)
                return
            # merged ads: every one is rebuilt from its sources below
            parts = {}
        download_elapsed = time.monotonic() - download_start

        async def _limited_export(ad, occurrence, from_date, to_date):
            async with semaphore:
                try:
                    sizes = {}
                    try:
                        screenshot = await _export_ad(
                            ad,
                            occurrence,
                            from_date,
                            to_date,
                            parts,
                            local_dir,
                            fs,
                            media_sizes=sizes,
                        )
                        method = "mediatree_cut"
                        progress.count("uploaded")
                    except Exception as e:
                        if plan_merged is None:
                            raise
                        logger.info(
                            f"Merged ad {ad.id} not cut from mediatree ({e}), "
                            "rebuilding it from its sources"
                        )
                        sizes = {}
                        screenshot = await _rebuild_merged_ad(
                            ad, plan_merged, local_dir, fs, media_sizes=sizes
                        )
                        method = "merged_rebuild"
                        progress.count("rebuilt")
                    exported[ad.id] = media_export_info(
                        sizes.get("mp4"),
                        MARGIN_ON_MEDIA_EXPORT.total_seconds(),
                        method=method,
                    )
                    if screenshot is not None:
                        screenshots.append(screenshot)
                    if ad.fragment_type == "no_data":
                        # The ad was previously marked "no_data" because its media
                        # wasn't in the bucket yet; now that the export succeeded,
                        # clear it so downstream classification picks it back up.
                        reset_no_data_ads.append(ad.id)
                except Exception as e:
                    logger.error(f"Failed to export ad {ad.id}: {e}")
                    missing_ads.append(ad.id)
                    progress.count("error")
                finally:
                    progress.update(1)

        export_start = time.monotonic()
        await asyncio.gather(
            *(
                _limited_export(ad, occurrence, from_date, to_date)
                for ad, occurrence, from_date, to_date in needs_export
            ),
            return_exceptions=True,
        )
        export_elapsed = time.monotonic() - export_start

        logger.info(
            f"[{channel}/{day}] {len(ads)} ads, {len(needs_export)} exported "
            f"({len(part_starts)} parts): checked in {check_elapsed:.1f}s, "
            f"downloaded in {download_elapsed:.1f}s, exported in "
            f"{export_elapsed:.1f}s"
        )
    finally:
        cleanup_day_media_parts(local_dir)


def _save_exports(session, exported: dict, reset_no_data_ids: list) -> None:
    """Saves a group as soon as it is done, so that a run stopped by its timeout keeps its work: the
    media_export of the ads exported or found in the bucket, and the no_data ads that now have their
    media. A failure is only logged: the next run checks these ads in the bucket again."""
    if not exported and not reset_no_data_ids:
        return
    try:
        if exported:
            session.execute(
                update(Ad),
                [
                    {"id": ad_id, "media_export": info}
                    for ad_id, info in exported.items()
                ],
            )
        if reset_no_data_ids:
            # fragment_type is NOT NULL, so fall back to "advertising" -- same default
            # sector_and_brand/e1_classify_ad uses -- just to clear it out of "no_data" and let it be
            # picked back up by the s03_classification stages.
            session.execute(
                update(Ad)
                .where(Ad.id.in_(reset_no_data_ids))
                .values(fragment_type="advertising")
            )
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to save the exports of {len(exported)} ads: {e}")


def _save_screenshots(session, screenshots: list[ScreenshotResult]) -> None:
    """Rows of advertising.ad_screenshot, upserted: a new export replaces the mosaic of the previous
    one. A failure is only logged: the screenshots job (s05_enrichment/e02_screenshots) catches up
    the ads without a `done` row."""
    if not screenshots:
        return
    try:
        save_results(session, screenshots)
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to save {len(screenshots)} ad screenshots: {e}")


async def run(start_date: datetime | None, end_date: datetime | None):
    session = get_db_session()
    # Reads the plans of the merged ads to rebuild (EXPORT_MERGED_ADS): neither the streaming cursor
    # of `session` nor the commits of `write_session` must get in the way.
    plan_session = get_db_session() if EXPORT_MERGED_ADS else None
    # Another session to save the screenshots of each group: a commit on `session` would close
    # the streaming cursor of iter_ads_by_channel_day.
    write_session = get_db_session()
    fs = get_s3_filesystem()

    missing_ads = []
    reset_no_data_ads = []
    screenshot_statuses = []
    saved_exports = 0

    try:
        total = count_ads_since(session, start_date, end_date, EXPORT_MERGED_ADS)
        logger.info(
            f"Found {total} {'merged ' if EXPORT_MERGED_ADS else ''}ads "
            f"from {start_date} to {end_date}"
        )
        plan_merged = (
            (lambda ad_id: plan_merged_ad(plan_session, ad_id))
            if EXPORT_MERGED_ADS
            else None
        )

        semaphore = asyncio.Semaphore(MAX_CONCURRENT_EXPORTS)
        check_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        group_semaphore = asyncio.Semaphore(MAX_CONCURRENT_GROUPS)
        progress = interactive_tqdm(total=total, desc="Exporting ads")
        run_start = time.monotonic()
        deadline = run_start + MAX_RUN_DURATION.total_seconds()
        logger.info(
            f"{MAX_CONCURRENT_EXPORTS} exports at once, new groups started for "
            f"{MAX_RUN_DURATION}" + (f", up to {MAX_ADS} ads" if MAX_ADS else "")
        )

        async def _bounded_group(channel, day, ads):
            nonlocal saved_exports
            screenshots, exported, reset_no_data = [], {}, []
            try:
                await _process_group(
                    channel,
                    day,
                    ads,
                    fs,
                    semaphore,
                    progress,
                    missing_ads,
                    reset_no_data,
                    screenshots,
                    plan_merged,
                    exported,
                    check_semaphore,
                )
            finally:
                _save_screenshots(write_session, screenshots)
                _save_exports(write_session, exported, reset_no_data)
                saved_exports += len(exported)
                reset_no_data_ads.extend(reset_no_data)
                screenshot_statuses.extend(s.status for s in screenshots)
                group_semaphore.release()

        tasks = []
        taken = 0
        for (channel, day), ads in iter_ads_by_channel_day(
            session, start_date, end_date, PAGE_SIZE, EXPORT_MERGED_ADS
        ):
            # Acquired here rather than inside _bounded_group, so this loop -- and the
            # streaming DB cursor behind iter_ads_by_channel_day -- pauses until a slot
            # frees up, instead of eagerly materializing every group's ads in memory.
            await group_semaphore.acquire()
            if time.monotonic() > deadline or (MAX_ADS and taken >= MAX_ADS):
                group_semaphore.release()
                logger.warning(
                    f"Run stopped before {channel}/{day} after {taken} ads "
                    f"(MAX_RUN_DURATION_MIN or MAX_ADS reached): the next run goes on "
                    "with the ads left"
                )
                break
            taken += len(ads)
            tasks.append(asyncio.create_task(_bounded_group(channel, day, ads)))

        await asyncio.gather(*tasks)

        logger.info(f"media_export saved for {saved_exports} ads")
        if reset_no_data_ads:
            logger.info(
                f"Reset fragment_type for {len(reset_no_data_ads)} "
                "previously no_data ads:"
            )
            logger.info(",".join(reset_no_data_ads))

        progress.close()
        logger.info(f"Total export run time: {time.monotonic() - run_start:.1f}s")
    finally:
        if SCREENSHOT_ON_EXPORT:
            logger.info(
                f"Ad screenshots: {screenshot_statuses.count('done')} done, "
                f"{screenshot_statuses.count('error')} errors"
            )
        logger.info(f"Finished, here are the {len(missing_ads)} missing ads")
        logger.info(",".join(missing_ads))
        write_session.close()
        if plan_session is not None:
            plan_session.close()
        session.close()


if __name__ == "__main__":
    with monitor(
        monitor_slug="advertising-exportation"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        str_start_date = os.environ.get("START_DATE")
        start_date = (
            datetime.fromisoformat(str_start_date).replace(
                tzinfo=ZoneInfo("Europe/Paris")
            )
            if str_start_date
            else None
        )

        str_end_date = os.environ.get("END_DATE")
        end_date = (
            datetime.fromisoformat(str_end_date).replace(
                tzinfo=ZoneInfo("Europe/Paris")
            )
            if str_end_date
            else None
        )

        asyncio.run(run(start_date=start_date, end_date=end_date))
