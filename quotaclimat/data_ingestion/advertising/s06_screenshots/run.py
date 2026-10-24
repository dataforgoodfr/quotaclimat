import asyncio
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime
from zoneinfo import ZoneInfo

import s3fs
from sqlalchemy import exists, func, select
from sqlalchemy.dialects.postgresql import insert

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, Ad_Screenshot
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    ad_media_s3_key,
)
from quotaclimat.data_ingestion.advertising.s02_exportation.run import (
    MARGIN_ON_MEDIA_EXPORT,
)
from quotaclimat.data_ingestion.advertising.s06_screenshots.frames import (
    build_mosaic,
    extract_frame,
    frame_offsets,
    frame_positions_in_parts,
)
from quotaclimat.data_ingestion.advertising.tools.interactive_tqdm import (
    interactive_tqdm,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    cleanup_day_media_parts,
    download_media_parts,
    get_s3_filesystem,
    list_available_part_starts,
)
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = os.environ.get("ADVERTISING_BUCKET_REGION", "fr-par")
SCREENSHOT_S3_PREFIX = "screenshots"
# "public-read" lets Metabase show the image from its URL (Display as image); "private" keeps it
# reachable only with the bucket credentials.
SCREENSHOT_ACL = os.environ.get("SCREENSHOT_ACL", "public-read")

# Same statuses as analytics.publicites: the ads whose metadata is shown in Metabase.
CLASSIFIED_PREDICTION_STATUSES = (
    "dict_miss",
    "dict_tier1",
    "dict_tier2",
    "dict_tier2_no_kw",
    "dict_tier3",
    "dict_tier3_no_kw",
    "subcat_done",
)

MAX_CONCURRENT_JOBS = 10
BATCH_SIZE = 50
LOCAL_CACHE_DIR = "./.cache/screenshots"


@dataclass
class Filters:
    start_date: datetime | None = None
    end_date: datetime | None = None
    channel: str | None = None
    ad_ids: list[str] = field(default_factory=list)
    # None: every ad, whatever its classification
    prediction_statuses: tuple[str, ...] | None = CLASSIFIED_PREDICTION_STATUSES
    # occurrences per ad, picked at random but always the same ones (0: every occurrence)
    max_occurrences_per_ad: int = 3
    limit: int | None = None
    # also redo the targets already marked missing_source (errors are always retried)
    retry_missing: bool = False


@dataclass
class ScreenshotResult:
    target_type: str
    target_id: str
    ad_id: str
    status: str
    source: str
    image_url: str | None = None
    frame_offsets_sec: list[float] | None = None
    nb_frames: int | None = None
    details: dict | None = None


def screenshot_s3_key(target_type: str, target_id: str) -> str:
    folder = "ads" if target_type == "ad" else "occurrences"
    return f"{BUCKET_NAME}/{SCREENSHOT_S3_PREFIX}/{folder}/{target_id}.jpg"


def screenshot_public_url(target_type: str, target_id: str) -> str:
    key = screenshot_s3_key(target_type, target_id).removeprefix(f"{BUCKET_NAME}/")
    return f"https://{BUCKET_NAME}.s3.{REGION}.scw.cloud/{key}"


def _not_already_done(target_type: str, target_id_column, retry_missing: bool):
    skipped_statuses = ["done"] if retry_missing else ["done", "missing_source"]
    return ~exists().where(
        Ad_Screenshot.target_type == target_type,
        Ad_Screenshot.target_id == target_id_column,
        Ad_Screenshot.status.in_(skipped_statuses),
    )


def _ad_filters(filters: Filters) -> list:
    conditions = []
    if filters.ad_ids:
        conditions.append(Ad.id.in_(filters.ad_ids))
    if filters.prediction_statuses is not None:
        conditions.append(Ad.prediction_status.in_(filters.prediction_statuses))
    return conditions


def ads_query(filters: Filters):
    query = select(Ad.id, Ad.duration_sec).where(
        *_ad_filters(filters),
        _not_already_done("ad", Ad.id, filters.retry_missing),
    )
    if filters.start_date:
        query = query.where(Ad.first_detection_date >= filters.start_date)
    if filters.end_date:
        query = query.where(Ad.first_detection_date < filters.end_date)
    if filters.channel:
        query = query.where(
            exists().where(
                Ad_Occurrence.ad_id == Ad.id,
                Ad_Occurrence.channel_name == filters.channel,
            )
        )
    return query.order_by(Ad.first_detection_date).limit(filters.limit)


def occurrences_query(filters: Filters):
    """Occurrences to screenshot, ordered by channel and date so that the occurrences sharing
    the same mediatree archives are processed together.

    The sample of each ad is taken before leaving out the occurrences already done (md5 of the id:
    random but stable), so a new run completes the same sample rather than drawing a new one.
    """
    ranked = select(
        Ad_Occurrence.id,
        Ad_Occurrence.ad_id,
        Ad_Occurrence.channel_name,
        Ad_Occurrence.occurrence_date,
        func.row_number()
        .over(partition_by=Ad_Occurrence.ad_id, order_by=func.md5(Ad_Occurrence.id))
        .label("rank_in_ad"),
    ).where(Ad_Occurrence.deleted_at.is_(None))
    if filters.start_date:
        ranked = ranked.where(Ad_Occurrence.occurrence_date >= filters.start_date)
    if filters.end_date:
        ranked = ranked.where(Ad_Occurrence.occurrence_date < filters.end_date)
    if filters.channel:
        ranked = ranked.where(Ad_Occurrence.channel_name == filters.channel)
    ranked = ranked.subquery()

    query = (
        select(
            ranked.c.id,
            ranked.c.ad_id,
            ranked.c.channel_name,
            ranked.c.occurrence_date,
            Ad.duration_sec,
        )
        .join(Ad, Ad.id == ranked.c.ad_id)
        .where(
            *_ad_filters(filters),
            _not_already_done("occurrence", ranked.c.id, filters.retry_missing),
        )
    )
    if filters.max_occurrences_per_ad:
        query = query.where(ranked.c.rank_in_ad <= filters.max_occurrences_per_ad)
    return query.order_by(ranked.c.channel_name, ranked.c.occurrence_date).limit(
        filters.limit
    )


def save_results(session, results: list[ScreenshotResult]) -> None:
    if not results:
        return
    rows = [r.__dict__ for r in results]
    stmt = insert(Ad_Screenshot).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["target_type", "target_id"],
        set_={
            "ad_id": stmt.excluded.ad_id,
            "status": stmt.excluded.status,
            "source": stmt.excluded.source,
            "image_url": stmt.excluded.image_url,
            "frame_offsets_sec": stmt.excluded.frame_offsets_sec,
            "nb_frames": stmt.excluded.nb_frames,
            "details": stmt.excluded.details,
            "updated_at": func.now(),
        },
    )
    session.execute(stmt)
    session.commit()


async def _upload_mosaic(
    fs: s3fs.S3FileSystem, local_path: str, target_type: str, target_id: str
) -> str:
    await fs._put_file(
        local_path,
        screenshot_s3_key(target_type, target_id),
        ACL=SCREENSHOT_ACL,
        ContentType="image/jpeg",
    )
    return screenshot_public_url(target_type, target_id)


async def _mosaic_from_frames(
    fs: s3fs.S3FileSystem,
    frames: list[tuple[str, float] | None],
    work_dir: str,
    target_type: str,
    target_id: str,
) -> tuple[str, int]:
    """Extract each (video path, offset) frame, tile them and upload the mosaic.

    Returns the image url and the number of frames found (a frame outside its video, or without a
    video, is a black tile).
    """
    frame_paths = []
    for i, frame in enumerate(frames):
        frame_path = os.path.join(work_dir, f"frame_{i:03d}.jpg")
        if frame is not None and await extract_frame(frame[0], frame[1], frame_path):
            frame_paths.append(frame_path)
        else:
            frame_paths.append(None)

    nb_frames = sum(p is not None for p in frame_paths)
    if nb_frames == 0:
        raise RuntimeError("no frame could be extracted from the video")

    mosaic_path = os.path.join(work_dir, "mosaic.jpg")
    await build_mosaic(frame_paths, os.path.join(work_dir, "tiles"), mosaic_path)
    return await _upload_mosaic(fs, mosaic_path, target_type, target_id), nb_frames


async def screenshot_ad(
    fs: s3fs.S3FileSystem, ad_id: str, duration_sec: float
) -> ScreenshotResult:
    """Mosaic of the ad, from the video saved by the exportation (s02): ads/<ad_id>/raw.mp4."""
    offsets = frame_offsets(duration_sec)
    result = ScreenshotResult(
        target_type="ad",
        target_id=ad_id,
        ad_id=ad_id,
        status="done",
        source="ad_export",
        frame_offsets_sec=offsets,
    )
    work_dir = tempfile.mkdtemp(dir=LOCAL_CACHE_DIR)
    try:
        video_path = os.path.join(work_dir, "raw.mp4")
        try:
            await fs._get_file(ad_media_s3_key(ad_id, "mp4"), video_path)
        except FileNotFoundError:
            result.status = "missing_source"
            result.details = {"missing": ad_media_s3_key(ad_id, "mp4")}
            return result

        # raw.mp4 starts MARGIN_ON_MEDIA_EXPORT before the occurrence it was cut from
        margin = MARGIN_ON_MEDIA_EXPORT.total_seconds()
        frames = [(video_path, margin + offset) for offset in offsets]
        result.image_url, result.nb_frames = await _mosaic_from_frames(
            fs, frames, work_dir, "ad", ad_id
        )
    except Exception as e:
        logger.error(f"Failed to screenshot ad {ad_id}: {e}")
        result.status = "error"
        result.details = {"error": str(e)[:2000]}
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return result


async def _screenshot_occurrence(
    fs: s3fs.S3FileSystem,
    occurrence,
    positions: list[tuple[datetime, float]],
    offsets: list[float],
    parts: dict[datetime, dict[str, str]],
) -> ScreenshotResult:
    result = ScreenshotResult(
        target_type="occurrence",
        target_id=occurrence.id,
        ad_id=occurrence.ad_id,
        status="done",
        source="mediatree_archive",
        frame_offsets_sec=offsets,
    )
    work_dir = tempfile.mkdtemp(dir=LOCAL_CACHE_DIR)
    try:
        frames = [
            (parts[part_start]["mp4"], offset_in_part) if part_start in parts else None
            for part_start, offset_in_part in positions
        ]
        result.image_url, result.nb_frames = await _mosaic_from_frames(
            fs, frames, work_dir, "occurrence", occurrence.id
        )
    except Exception as e:
        logger.error(f"Failed to screenshot occurrence {occurrence.id}: {e}")
        result.status = "error"
        result.details = {"error": str(e)[:2000]}
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    return result


async def screenshot_occurrence_group(
    fs: s3fs.S3FileSystem,
    channel: str,
    occurrences: list,
    semaphore: asyncio.Semaphore,
) -> list[ScreenshotResult]:
    """Mosaics of the occurrences of one channel and day, from the 2-minutes mediatree archives.

    The archives are first listed: an occurrence whose frames fall in an archive missing from the
    bucket is marked missing_source without downloading anything. The archives of the other
    occurrences are downloaded once for the whole group.
    """
    results = []
    planned = []
    available_by_day: dict[date, set[datetime]] = {}
    for occurrence in occurrences:
        occurrence_start = occurrence.occurrence_date.replace(tzinfo=ZoneInfo("UTC"))
        offsets = frame_offsets(occurrence.duration_sec)
        positions = frame_positions_in_parts(occurrence_start, offsets)

        needed_parts = {part_start for part_start, _ in positions}
        for day in {p.date() for p in needed_parts} - available_by_day.keys():
            available_by_day[day] = await list_available_part_starts(fs, channel, day)
        missing_parts = sorted(
            p for p in needed_parts if p not in available_by_day[p.date()]
        )

        if missing_parts:
            results.append(
                ScreenshotResult(
                    target_type="occurrence",
                    target_id=occurrence.id,
                    ad_id=occurrence.ad_id,
                    status="missing_source",
                    source="mediatree_archive",
                    frame_offsets_sec=offsets,
                    details={"missing_parts": [p.isoformat() for p in missing_parts]},
                )
            )
        else:
            planned.append((occurrence, positions, offsets))

    if not planned:
        return results

    part_starts = {p for _, positions, _ in planned for p, _ in positions}
    local_dir = tempfile.mkdtemp(dir=LOCAL_CACHE_DIR, prefix=f"{channel}_")
    try:
        parts = await download_media_parts(
            fs,
            channel,
            part_starts,
            local_dir,
            formats=("mp4",),
            max_concurrent_downloads=MAX_CONCURRENT_JOBS,
            disable_progress=True,
        )

        async def _limited(occurrence, positions, offsets):
            async with semaphore:
                return await _screenshot_occurrence(
                    fs, occurrence, positions, offsets, parts
                )

        results += await asyncio.gather(
            *(_limited(*p) for p in planned), return_exceptions=False
        )
    finally:
        cleanup_day_media_parts(local_dir)
    return results


def _group_by_channel_day(occurrences: list) -> dict[tuple[str, date], list]:
    groups: dict[tuple[str, date], list] = {}
    for occurrence in occurrences:
        key = (occurrence.channel_name, occurrence.occurrence_date.date())
        groups.setdefault(key, []).append(occurrence)
    return groups


def _log_counts(label: str, results: list[ScreenshotResult]) -> None:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.status] = counts.get(r.status, 0) + 1
    logger.info(f"{label}: {counts}")


async def run_ads(session, fs: s3fs.S3FileSystem, filters: Filters) -> None:
    ads = session.execute(ads_query(filters)).all()
    logger.info(f"{len(ads)} ads to screenshot")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    progress = interactive_tqdm(total=len(ads), desc="Screenshots of ads")
    all_results = []

    async def _limited(ad_id, duration_sec):
        async with semaphore:
            result = await screenshot_ad(fs, ad_id, duration_sec)
            progress.count(result.status)
            progress.update(1)
            return result

    try:
        for i in range(0, len(ads), BATCH_SIZE):
            batch = ads[i : i + BATCH_SIZE]
            results = await asyncio.gather(*(_limited(*ad) for ad in batch))
            save_results(session, results)
            all_results += results
    finally:
        progress.close()
    _log_counts("Ads", all_results)


async def run_occurrences(session, fs: s3fs.S3FileSystem, filters: Filters) -> None:
    occurrences = session.execute(occurrences_query(filters)).all()
    groups = _group_by_channel_day(occurrences)
    logger.info(
        f"{len(occurrences)} occurrences to screenshot in {len(groups)} channel-days"
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    progress = interactive_tqdm(
        total=len(occurrences), desc="Screenshots of occurrences"
    )
    all_results = []
    try:
        for (channel, _), group in groups.items():
            results = await screenshot_occurrence_group(fs, channel, group, semaphore)
            save_results(session, results)
            for r in results:
                progress.count(r.status)
            progress.update(len(results))
            all_results += results
    finally:
        progress.close()
    _log_counts("Occurrences", all_results)


async def run(filters: Filters, targets: set[str]) -> None:
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    session = get_db_session()
    # The mediatree bucket and the advertising bucket share the endpoint and credentials
    fs = get_s3_filesystem()
    try:
        if "ads" in targets:
            await run_ads(session, fs, filters)
        if "occurrences" in targets:
            await run_occurrences(session, fs, filters)
    finally:
        session.close()


def _parse_date(name: str) -> datetime | None:
    value = os.environ.get(name)
    if not value:
        return None
    return datetime.fromisoformat(value).replace(tzinfo=ZoneInfo("Europe/Paris"))


def filters_from_env() -> Filters:
    statuses = os.environ.get("PREDICTION_STATUSES", "classified")
    if statuses == "all":
        prediction_statuses = None
    elif statuses == "classified":
        prediction_statuses = CLASSIFIED_PREDICTION_STATUSES
    else:
        prediction_statuses = tuple(s.strip() for s in statuses.split(","))

    limit = os.environ.get("LIMIT")
    return Filters(
        start_date=_parse_date("START_DATE"),
        end_date=_parse_date("END_DATE"),
        channel=os.environ.get("CHANNEL") or None,
        ad_ids=[
            a.strip() for a in os.environ.get("AD_IDS", "").split(",") if a.strip()
        ],
        prediction_statuses=prediction_statuses,
        max_occurrences_per_ad=int(os.environ.get("MAX_OCCURRENCES_PER_AD", 3)),
        limit=int(limit) if limit else None,
        retry_missing=os.environ.get("RETRY_MISSING", "false").lower() == "true",
    )


if __name__ == "__main__":
    getLogger()
    sentry_init()

    targets = {
        t.strip() for t in os.environ.get("TARGETS", "ads,occurrences").split(",")
    }
    asyncio.run(run(filters_from_env(), targets))
