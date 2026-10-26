import asyncio
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from zoneinfo import ZoneInfo

import s3fs
from sqlalchemy import exists, func, select
from sqlalchemy.dialects.postgresql import insert

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence, Ad_Screenshot
from quotaclimat.data_ingestion.advertising.s02_exportation.ad_bucket import (
    MARGIN_ON_MEDIA_EXPORT_HISTORY,
    ad_media_s3_key,
    get_margin_from_detection_date,
)
from quotaclimat.data_ingestion.advertising.s05_enrichment.e02_screenshots.frames import (
    build_mosaic,
    extract_frame,
    frame_offsets,
    frame_positions_in_parts,
    video_duration,
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
# Occurrences are processed by batches of ads, the most broadcast first: within a batch, the occurrences
# sharing the same mediatree archives are still processed together.
ADS_PER_OCCURRENCE_BATCH = int(os.environ.get("ADS_PER_OCCURRENCE_BATCH", 20))
LOCAL_CACHE_DIR = "./.cache/screenshots"


@dataclass
class Filters:
    start_date: datetime | None = None
    end_date: datetime | None = None
    channel: str | None = None
    ad_ids: list[str] = field(default_factory=list)
    # None: every ad, whatever its classification
    prediction_statuses: tuple[str, ...] | None = CLASSIFIED_PREDICTION_STATUSES
    limit: int | None = None
    # only the occurrences broadcast from this date (the ads keep start_date / end_date on their detection)
    occurrences_from: datetime | None = None
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


def _occurrence_counts(filters: Filters):
    """Number of occurrences of each ad (on the channel and from occurrences_from when set): the
    priority of the ads, the most broadcast first."""
    query = select(Ad_Occurrence.ad_id, func.count().label("nb_occurrences")).where(
        Ad_Occurrence.deleted_at.is_(None)
    )
    if filters.occurrences_from:
        query = query.where(Ad_Occurrence.occurrence_date >= filters.occurrences_from)
    if filters.channel:
        query = query.where(Ad_Occurrence.channel_name == filters.channel)
    return query.group_by(Ad_Occurrence.ad_id).subquery()


def ads_query(filters: Filters):
    """Ads to screenshot, the most broadcast first. With a channel or occurrences_from, only the ads
    broadcast there."""
    counts = _occurrence_counts(filters)
    query = select(Ad.id, Ad.duration_sec, Ad.first_detection_date).join(
        counts,
        counts.c.ad_id == Ad.id,
        isouter=not (filters.channel or filters.occurrences_from),
    )
    query = query.where(
        *_ad_filters(filters),
        _not_already_done("ad", Ad.id, filters.retry_missing),
    )
    if filters.start_date:
        query = query.where(Ad.first_detection_date >= filters.start_date)
    if filters.end_date:
        query = query.where(Ad.first_detection_date < filters.end_date)
    return query.order_by(counts.c.nb_occurrences.desc().nulls_last(), Ad.id).limit(
        filters.limit
    )


def ranked_ad_ids_query(filters: Filters):
    """Ids of the ads having occurrences, the most broadcast first: the order of the occurrence
    batches."""
    counts = _occurrence_counts(filters)
    return (
        select(Ad.id)
        .join(counts, counts.c.ad_id == Ad.id)
        .where(*_ad_filters(filters))
        .order_by(counts.c.nb_occurrences.desc(), Ad.id)
    )


def occurrences_query(filters: Filters):
    """Occurrences to screenshot, ordered by channel and date so that the occurrences sharing
    the same mediatree archives are processed together.
    """
    query = (
        select(
            Ad_Occurrence.id,
            Ad_Occurrence.ad_id,
            Ad_Occurrence.channel_name,
            Ad_Occurrence.occurrence_date,
            Ad.duration_sec,
        )
        .join(Ad, Ad.id == Ad_Occurrence.ad_id)
        .where(
            Ad_Occurrence.deleted_at.is_(None),
            *_ad_filters(filters),
            _not_already_done("occurrence", Ad_Occurrence.id, filters.retry_missing),
        )
    )
    if filters.start_date:
        query = query.where(Ad_Occurrence.occurrence_date >= filters.start_date)
    if filters.end_date:
        query = query.where(Ad_Occurrence.occurrence_date < filters.end_date)
    if filters.occurrences_from:
        query = query.where(Ad_Occurrence.occurrence_date >= filters.occurrences_from)
    if filters.channel:
        query = query.where(Ad_Occurrence.channel_name == filters.channel)
    return query.order_by(
        Ad_Occurrence.channel_name, Ad_Occurrence.occurrence_date
    ).limit(filters.limit)


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


def export_margin_sec(
    ad_duration_sec: float,
    first_detection_date: datetime,
    video_duration_sec: float | None,
) -> float:
    """Margin the exportation (s02) put before (and after) the ad in raw.mp4, in seconds.

    It changed over time (MARGIN_ON_MEDIA_EXPORT_HISTORY, 1s then 0.1s). The detection date gives the
    margin of the first export, but an ad exported later (merged ad, mp4 exported again) has the
    margin of its export: when the video duration is known, the margin of the history closest to
    (video duration - ad duration) / 2 is used, the detection date otherwise.
    """
    known_margins = {m.total_seconds() for _, m in MARGIN_ON_MEDIA_EXPORT_HISTORY}
    if video_duration_sec is not None:
        measured = (video_duration_sec - ad_duration_sec) / 2
        return min(known_margins, key=lambda m: abs(m - measured))
    margin = get_margin_from_detection_date(first_detection_date)
    return margin.total_seconds() if margin is not None else 0.0


async def screenshot_ad(
    fs: s3fs.S3FileSystem,
    ad_id: str,
    duration_sec: float,
    first_detection_date: datetime,
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

        # raw.mp4 starts a margin before the occurrence it was cut from
        margin = export_margin_sec(
            duration_sec, first_detection_date, await video_duration(video_path)
        )
        frames = [(video_path, margin + offset) for offset in offsets]
        result.image_url, result.nb_frames = await _mosaic_from_frames(
            fs, frames, work_dir, "ad", ad_id
        )
        result.details = {"margin_sec": margin}
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


def iter_occurrences_by_channel_day(session, query, page_size: int = 1000):
    """Stream the occurrences of the query (ordered by channel and date) from Postgres with a
    server-side cursor, one (channel, UTC day) group at a time: only one group is held in memory.
    The UTC day matches how mediatree lays out its bucket.
    """
    result = session.execute(query, execution_options={"stream_results": True})
    current_key = None
    current_group = []
    for occurrence in result.yield_per(page_size):
        key = (occurrence.channel_name, occurrence.occurrence_date.date())
        if current_key is not None and key != current_key:
            yield current_key, current_group
            current_group = []
        current_key = key
        current_group.append(occurrence)
    if current_group:
        yield current_key, current_group


def _log_counts(label: str, statuses: list[str]) -> None:
    counts: dict[str, int] = {}
    for status in statuses:
        counts[status] = counts.get(status, 0) + 1
    logger.info(f"{label}: {counts}")


async def run_ads(session, fs: s3fs.S3FileSystem, filters: Filters) -> None:
    ads = session.execute(ads_query(filters)).all()
    logger.info(f"{len(ads)} ads to screenshot")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    progress = interactive_tqdm(total=len(ads), desc="Screenshots of ads")
    all_results = []

    async def _limited(ad_id, duration_sec, first_detection_date):
        async with semaphore:
            result = await screenshot_ad(fs, ad_id, duration_sec, first_detection_date)
            progress.count(result.status)
            progress.update(1)
            return result

    try:
        for i in range(0, len(ads), BATCH_SIZE):
            batch = ads[i : i + BATCH_SIZE]
            results = await asyncio.gather(*(_limited(*ad) for ad in batch))
            save_results(session, results)
            all_results += [r.status for r in results]
    finally:
        progress.close()
    _log_counts("Ads", all_results)


async def run_occurrences(
    read_session, write_session, fs: s3fs.S3FileSystem, filters: Filters
) -> None:
    """Screenshot the occurrences by batches of ADS_PER_OCCURRENCE_BATCH ads, the most broadcast
    first, so that their checks are complete first. Within a batch, one (channel, day) group at a time.
    """
    total = write_session.execute(
        select(func.count()).select_from(occurrences_query(filters).subquery())
    ).scalar()
    if filters.limit is not None:
        total = min(total, filters.limit)
    ranked_ad_ids = write_session.execute(ranked_ad_ids_query(filters)).scalars().all()
    logger.info(
        f"{total} occurrences to screenshot, {len(ranked_ad_ids)} ads by batches of "
        f"{ADS_PER_OCCURRENCE_BATCH}"
    )

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    progress = interactive_tqdm(total=total, desc="Screenshots of occurrences")
    all_results = []
    try:
        for i in range(0, len(ranked_ad_ids), ADS_PER_OCCURRENCE_BATCH):
            remaining = (
                None if filters.limit is None else filters.limit - len(all_results)
            )
            if remaining is not None and remaining <= 0:
                break
            batch_filters = replace(
                filters,
                ad_ids=ranked_ad_ids[i : i + ADS_PER_OCCURRENCE_BATCH],
                limit=remaining,
            )
            groups = iter_occurrences_by_channel_day(
                read_session, occurrences_query(batch_filters)
            )
            for (channel, _), group in groups:
                results = await screenshot_occurrence_group(
                    fs, channel, group, semaphore
                )
                # written with another session: a commit would close the streaming cursor
                save_results(write_session, results)
                for r in results:
                    progress.count(r.status)
                progress.update(len(results))
                all_results += [r.status for r in results]
    finally:
        progress.close()
    _log_counts("Occurrences", all_results)


async def run(filters: Filters, targets: set[str]) -> None:
    os.makedirs(LOCAL_CACHE_DIR, exist_ok=True)
    # The mediatree bucket and the advertising bucket share the endpoint and credentials
    fs = get_s3_filesystem()
    with get_db_session() as read_session, get_db_session() as write_session:
        if "ads" in targets:
            await run_ads(write_session, fs, filters)
        if "occurrences" in targets:
            await run_occurrences(read_session, write_session, fs, filters)


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
        limit=int(limit) if limit else None,
        occurrences_from=_parse_date("OCCURRENCES_FROM"),
        retry_missing=os.environ.get("RETRY_MISSING", "false").lower() == "true",
    )


if __name__ == "__main__":
    getLogger()
    sentry_init()

    targets = {
        t.strip() for t in os.environ.get("TARGETS", "ads,occurrences").split(",")
    }
    asyncio.run(run(filters_from_env(), targets))
