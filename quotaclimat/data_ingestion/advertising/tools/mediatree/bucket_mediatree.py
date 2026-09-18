import asyncio
import logging
import os
import shutil
import tarfile
import tempfile
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import s3fs
from dotenv import load_dotenv

from ..interactive_tqdm import interactive_tqdm
from . import overlay_correction

logger = logging.getLogger(__name__)

load_dotenv()

ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("MEDIATREE_BUCKET_NAME", "mediatree-videos-prod")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
AD_S3_PREFIX = "ads"

MEDIATREE_CHANNEL_MAPPING = {"fr3-idf": "france3"}


def get_s3_filesystem() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT_URL, "region_name": REGION},
    )


def _get_s3_folder(channel: str, day: date) -> str:
    # /mediatree-videos-prod/output/franceinfotv/2026/09/07/
    mediatree_channel = MEDIATREE_CHANNEL_MAPPING.get(channel, channel)
    return f"/{BUCKET_NAME}/output/{mediatree_channel}/{day.strftime('%Y/%m/%d')}"


def _get_s3_file_basename(
    channel: str, start_date: datetime, end_date: datetime
) -> str:
    # franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.tar
    start_date_utc = start_date.astimezone(tz=ZoneInfo("UTC"))
    end_date_utc = end_date.astimezone(tz=ZoneInfo("UTC"))
    mediatree_channel = MEDIATREE_CHANNEL_MAPPING.get(channel, channel)
    return f"{mediatree_channel}_{start_date_utc.strftime('%Y-%m-%dT%H-%M-%SZ')}_{end_date_utc.strftime('%Y-%m-%dT%H-%M-%SZ')}"


def _get_s3_archive_key_for_part(
    channel: str, start_date: datetime, end_date: datetime
) -> str:
    # /mediatree-videos-prod/output/franceinfotv/2026/09/07/franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.tar

    folder = _get_s3_folder(channel, start_date.date())
    basename = _get_s3_file_basename(channel, start_date, end_date)
    return f"{folder}/{basename}.tar"


def get_datetime_from_basename(basename: str) -> datetime:
    # franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.tar
    # Extract the start and end datetime from the basename
    (channel, start_str, end_str) = basename.split("_")
    start_dt = datetime.strptime(start_str, "%Y-%m-%dT%H-%M-%SZ").replace(
        tzinfo=ZoneInfo("UTC")
    )
    end_dt = datetime.strptime(end_str, "%Y-%m-%dT%H-%M-%SZ").replace(
        tzinfo=ZoneInfo("UTC")
    )
    return start_dt, end_dt


def _floor_to_2_minutes(dt: datetime) -> datetime:
    """Round down to the start of the 2-minutes interval containing dt."""
    return dt.replace(minute=dt.minute - dt.minute % 2, second=0, microsecond=0)


def _split_segment_into_parts(
    start_date: datetime, end_date: datetime
) -> list[tuple[datetime, datetime]]:
    """Return a list of start and end dates for each 2-minutes file that covers the given segment.

    Start and end are aligned to the enclosing 2-minutes interval, so the segment is never
    truncated: e.g. 00:05 -> 00:04, and an end date already at 00:06 stays at 00:06.
    """
    aligned_start = _floor_to_2_minutes(start_date)

    part_start_dates = []
    current_start_dt = aligned_start
    while current_start_dt < end_date:
        part_start_dates.append(current_start_dt)
        current_start_dt += timedelta(minutes=2)

    return [(start, start + timedelta(minutes=2)) for start in part_start_dates]


def _extract_audio_from_archive(
    archive_path: str, dest_dir: str, audio_name: str
) -> str:
    # Each tar also contains per-part thumbnail jpgs and an mp4; extract only the mp3
    # rather than everything, so callers sharing one dest_dir across many tars don't
    # get flooded with unrelated files.
    with tarfile.open(archive_path) as tar:
        tar.extract(audio_name, dest_dir, filter="data")

    return os.path.join(dest_dir, audio_name)


async def _download_part(fs: s3fs.S3FileSystem, s3_key: str, dest_dir: str) -> str:
    basename = os.path.basename(s3_key)
    archive_path = os.path.join(dest_dir, basename)
    try:
        await fs._get_file(s3_key, archive_path)
    except Exception as e:
        logger.error(f"Error downloading {s3_key} from S3: {e}")
        raise

    extract_dir = os.path.join(dest_dir, os.path.splitext(basename)[0])
    audio_name = f"{os.path.splitext(basename)[0]}.mp3"
    os.makedirs(extract_dir, exist_ok=True)
    part_path = await asyncio.to_thread(
        _extract_audio_from_archive, archive_path, extract_dir, audio_name
    )
    return part_path


async def _download_and_extract_audio(
    fs: s3fs.S3FileSystem, s3_key: str, archive_dir: str, audio_dir: str
) -> str:
    basename = os.path.basename(s3_key)
    archive_path = os.path.join(archive_dir, basename)
    try:
        await fs._get_file(s3_key, archive_path)
    except Exception as e:
        logger.error(f"Error downloading {s3_key} from S3: {e}")
        raise

    audio_name = f"{os.path.splitext(basename)[0]}.mp3"
    try:
        return await asyncio.to_thread(
            _extract_audio_from_archive, archive_path, audio_dir, audio_name
        )
    finally:
        os.remove(archive_path)


async def download_days_audio_parts(
    fs: s3fs.S3FileSystem,
    channel: str,
    days: list[date],
    dest_dir: str,
    max_concurrent_downloads: int = 10,
) -> list[str]:
    """Download every 2-minutes tar archive for `channel` on each of `days` (UTC calendar
    dates, matching how mediatree lays out its S3 bucket) and extract each one's mp3 into
    `dest_dir`. Only the extracted mp3s are kept; the tar archives themselves are
    discarded once extracted. Parts whose mp3 is already present in `dest_dir` are left
    untouched rather than re-downloaded.

    Returns the mp3 path of every part (both pre-existing and newly downloaded), sorted
    chronologically.
    """
    keys_per_day = await asyncio.gather(
        *(fs._ls(_get_s3_folder(channel, day)) for day in days)
    )
    s3_keys = sorted(
        key for keys in keys_per_day for key in keys if key.endswith(".tar")
    )

    os.makedirs(dest_dir, exist_ok=True)
    inflight = asyncio.Semaphore(max_concurrent_downloads)

    progress = interactive_tqdm(
        total=len(s3_keys),
        desc="Downloading mediatree parts",
        unit="file",
    )

    async def _bounded_download(s3_key: str, archive_dir: str) -> str:
        audio_name = f"{os.path.splitext(os.path.basename(s3_key))[0]}.mp3"
        audio_path = os.path.join(dest_dir, audio_name)
        try:
            if os.path.isfile(audio_path):
                progress.count("cached")
                return audio_path

            async with inflight:
                result = await _download_and_extract_audio(
                    fs, s3_key, archive_dir, dest_dir
                )
                progress.count("downloaded")
                return result
        finally:
            progress.update(1)

    try:
        with tempfile.TemporaryDirectory(dir=dest_dir) as archive_dir:
            result = await asyncio.gather(
                *(_bounded_download(s3_key, archive_dir) for s3_key in s3_keys)
            )
            return result
    finally:
        progress.close()


async def _merge_audio_parts(
    part_paths: list[str],
    output_path: str,
    trim_start: timedelta,
    trim_duration: timedelta,
) -> None:
    """Losslessly concatenate audio parts (in order) into a single file.

    Uses ffmpeg's concat demuxer rather than raw byte concatenation, which can
    introduce glitches/gaps at part boundaries for compressed formats like mp3.
    Every part but the first has its leading encoder-delay frame, plus any detected
    duplicate audio, skipped via the demuxer's `inpoint` directive for gapless joins
    (see `overlay_correction`).

    `trim_start`/`trim_duration` cut the concatenated audio down to the originally
    requested segment, since the parts cover the enclosing 2-minutes intervals and
    can therefore extend before/after the requested start/end dates.
    """
    overlaps = await asyncio.gather(
        *(
            overlay_correction.detect_overlap(prev_path, next_path)
            for prev_path, next_path in zip(part_paths, part_paths[1:])
        )
    )

    total_overlap = sum(overlaps, timedelta())
    if total_overlap > timedelta():
        logger.warning(
            "Removed %.3fs of duplicate audio across %d part boundaries in %s; "
            "the merged output will fall short of the requested %.3fs by that amount.",
            total_overlap.total_seconds(),
            sum(1 for o in overlaps if o > timedelta()),
            output_path,
            trim_duration.total_seconds(),
        )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as filelist:
        for i, part_path in enumerate(part_paths):
            filelist.write(f"file '{os.path.abspath(part_path)}'\n")
            if i > 0:
                inpoint = overlay_correction.ENCODER_DELAY + overlaps[i - 1]
                filelist.write(f"inpoint {inpoint.total_seconds()}\n")
        filelist_path = filelist.name

    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            filelist_path,
            "-ss",
            str(trim_start.total_seconds()),
            "-t",
            str(trim_duration.total_seconds()),
            "-c",
            "copy",
            output_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"ffmpeg failed to merge audio parts into {output_path}: "
                f"{stderr.decode(errors='replace')}"
            )
    finally:
        os.remove(filelist_path)


async def _merge_video_parts(
    part_paths: list[str],
    output_path: str,
    trim_start: timedelta,
    trim_duration: timedelta,
) -> None:
    """Concatenate video parts (in order) and trim down to the requested segment.

    Re-encodes rather than stream-copying so the trim lands on the exact requested
    boundaries instead of the nearest keyframe; export volumes are low enough that the
    extra CPU cost doesn't matter.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as filelist:
        for part_path in part_paths:
            filelist.write(f"file '{os.path.abspath(part_path)}'\n")
        filelist_path = filelist.name

    try:
        process = await asyncio.create_subprocess_exec(
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            filelist_path,
            "-ss",
            str(trim_start.total_seconds()),
            "-t",
            str(trim_duration.total_seconds()),
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            output_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(
                f"ffmpeg failed to merge video parts into {output_path}: "
                f"{stderr.decode(errors='replace')}"
            )
    finally:
        os.remove(filelist_path)


EXPORT_MEDIA_FORMATS = ("mp3", "mp4")


def _extract_media_from_archive(
    archive_path: str, dest_dir: str, basename: str, formats: tuple[str, ...]
) -> dict[str, str]:
    paths = {}
    with tarfile.open(archive_path) as tar:
        for media_format in formats:
            member_name = f"{basename}.{media_format}"
            tar.extract(member_name, dest_dir, filter="data")
            paths[media_format] = os.path.join(dest_dir, member_name)
    return paths


async def _download_and_extract_media(
    fs: s3fs.S3FileSystem,
    s3_key: str,
    archive_dir: str,
    dest_dir: str,
    formats: tuple[str, ...],
) -> dict[str, str]:
    basename = os.path.splitext(os.path.basename(s3_key))[0]
    archive_path = os.path.join(archive_dir, os.path.basename(s3_key))
    try:
        await fs._get_file(s3_key, archive_path)
    except Exception as e:
        logger.error(f"Error downloading {s3_key} from S3: {e}")
        raise

    try:
        return await asyncio.to_thread(
            _extract_media_from_archive, archive_path, dest_dir, basename, formats
        )
    finally:
        os.remove(archive_path)


def required_part_starts(
    segments: list[tuple[datetime, datetime]],
) -> set[datetime]:
    """Return the set of 2-minutes part start datetimes covering every (start, end)
    segment, for use with `download_media_parts`.
    """
    starts: set[datetime] = set()
    for start_date, end_date in segments:
        starts.update(
            window_start
            for window_start, _ in _split_segment_into_parts(start_date, end_date)
        )
    return starts


async def download_media_parts(
    fs: s3fs.S3FileSystem,
    channel: str,
    part_starts: set[datetime],
    dest_dir: str,
    formats: tuple[str, ...] = EXPORT_MEDIA_FORMATS,
    max_concurrent_downloads: int = 10,
    disable_progress: bool = False,
) -> dict[datetime, dict[str, str]]:
    """Download and extract exactly the given 2-minutes tar archives for `channel`
    (each identified by its UTC start datetime, 2-minutes aligned) into `dest_dir`,
    rather than every archive for a whole day -- see `required_part_starts` to compute
    this set from a list of ad segments, so only the parts actually needed are fetched.

    This is meant to back a short-lived local cache: call `cleanup_day_media_parts`
    once every segment needing these parts has been extracted and uploaded, rather than
    keeping the files around.

    Returns a mapping from each part's UTC start datetime to its {format: path} dict.
    """
    os.makedirs(dest_dir, exist_ok=True)
    inflight = asyncio.Semaphore(max_concurrent_downloads)

    sorted_starts = sorted(part_starts)
    progress = interactive_tqdm(
        total=len(sorted_starts),
        desc=f"Downloading {channel} parts",
        unit="file",
        disable=disable_progress,
    )

    parts: dict[datetime, dict[str, str]] = {}

    async def _bounded_download(part_start: datetime, archive_dir: str) -> None:
        s3_key = _get_s3_archive_key_for_part(
            channel, part_start, part_start + timedelta(minutes=2)
        )
        try:
            async with inflight:
                try:
                    parts[part_start] = await _download_and_extract_media(
                        fs, s3_key, archive_dir, dest_dir, formats
                    )
                    progress.count("downloaded")
                except Exception as e:
                    # Some parts are legitimately absent from mediatree's bucket (e.g.
                    # short/low-priority archives that were never purchased). Leaving
                    # this part out of `parts` rather than failing the whole batch lets
                    # every other part still download, and ads that don't need this
                    # specific part still get exported -- extract_segment already
                    # treats a missing part as "can't export this ad" on its own.
                    logger.warning(f"Skipping missing/unreadable part {s3_key}: {e}")
                    progress.count("missing")
        finally:
            progress.update(1)

    try:
        with tempfile.TemporaryDirectory(dir=dest_dir) as archive_dir:
            await asyncio.gather(
                *(_bounded_download(s, archive_dir) for s in sorted_starts)
            )
    finally:
        progress.close()

    return parts


def cleanup_day_media_parts(dest_dir: str) -> None:
    """Delete every file downloaded/extracted by `download_media_parts` for a group."""
    shutil.rmtree(dest_dir, ignore_errors=True)


async def extract_segment(
    parts: dict[datetime, dict[str, str]],
    start_date: datetime,
    end_date: datetime,
    media_format: str,
    output_path: str,
) -> bool:
    """Extract [start_date, end_date) for `media_format` out of the downloaded parts
    (see `download_media_parts`), writing the merged/trimmed result to `output_path`.

    Returns False (and writes nothing) if any 2-minutes part covering the segment is
    missing from `parts` -- e.g. it wasn't included in the requested part_starts, or a
    part failed to download.
    """
    windows = _split_segment_into_parts(start_date, end_date)
    part_paths = []
    for window_start, _ in windows:
        part = parts.get(window_start)
        if part is None or media_format not in part:
            return False
        part_paths.append(part[media_format])

    trim_start = start_date - windows[0][0]
    trim_duration = end_date - start_date

    if media_format == "mp3":
        await _merge_audio_parts(part_paths, output_path, trim_start, trim_duration)
    elif media_format == "mp4":
        await _merge_video_parts(part_paths, output_path, trim_start, trim_duration)
    else:
        raise ValueError(f"Unsupported media format: {media_format}")

    return True
