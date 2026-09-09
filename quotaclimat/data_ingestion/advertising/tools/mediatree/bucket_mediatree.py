import asyncio
import logging
import os
import tarfile
import tempfile
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import s3fs
from dotenv import load_dotenv

from . import overlay_correction

logger = logging.getLogger(__name__)

load_dotenv()

ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("MEDIATREE_BUCKET_NAME", "mediatree-videos-prod")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
AD_S3_PREFIX = "ads"


def get_s3_filesystem() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=ACCESS_KEY,
        secret=SECRET_KEY,
        client_kwargs={"endpoint_url": ENDPOINT_URL, "region_name": REGION},
    )


def _get_s3_archive_key_for_part(
    channel: str, start_date: datetime, end_date: datetime
) -> str:
    # /mediatree-videos-prod/output/franceinfotv/2026/09/07/franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.tar

    start_date_utc = start_date.astimezone(tz=ZoneInfo("UTC"))
    end_date_utc = end_date.astimezone(tz=ZoneInfo("UTC"))

    return f"/{BUCKET_NAME}/output/{channel}/{start_date_utc.strftime('%Y/%m/%d')}/{channel}_{start_date_utc.strftime('%Y-%m-%dT%H-%M-%SZ')}_{end_date_utc.strftime('%Y-%m-%dT%H-%M-%SZ')}.tar"


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


def _get_s3_day_prefix(channel: str, day: date) -> str:
    # /mediatree-videos-prod/output/franceinfotv/2026/09/07/
    return f"/{BUCKET_NAME}/output/{channel}/{day.strftime('%Y/%m/%d')}"


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
        *(fs._ls(_get_s3_day_prefix(channel, day)) for day in days)
    )
    s3_keys = sorted(
        key for keys in keys_per_day for key in keys if key.endswith(".tar")
    )

    os.makedirs(dest_dir, exist_ok=True)
    inflight = asyncio.Semaphore(max_concurrent_downloads)

    async def _bounded_download(s3_key: str, archive_dir: str) -> str:
        audio_name = f"{os.path.splitext(os.path.basename(s3_key))[0]}.mp3"
        audio_path = os.path.join(dest_dir, audio_name)
        if os.path.isfile(audio_path):
            return audio_path

        async with inflight:
            return await _download_and_extract_audio(fs, s3_key, archive_dir, dest_dir)

    with tempfile.TemporaryDirectory(dir=dest_dir) as archive_dir:
        return await asyncio.gather(
            *(_bounded_download(s3_key, archive_dir) for s3_key in s3_keys)
        )


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


async def download_mediatree_audio(
    fs: s3fs.S3FileSystem,
    file_path: str,
    channel: str,
    start_date: datetime,
    end_date: datetime,
) -> None:
    """Download a segment's audio parts from S3 and merge them into a single file.

    `s3_keys` must be ordered chronologically: parts are downloaded concurrently
    but merged back in the given order. Uses the local cache if the merged file
    already exists.
    """

    parts = [
        (
            channel,
            part_start_date,
            part_end_date,
        )
        for part_start_date, part_end_date in _split_segment_into_parts(
            start_date, end_date
        )
    ]

    s3_archive_keys = [_get_s3_archive_key_for_part(*part) for part in parts]

    trim_start = start_date - parts[0][1]
    trim_duration = end_date - start_date

    with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmp_dir:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        part_paths = await asyncio.gather(
            *(_download_part(fs, s3_key, tmp_dir) for s3_key in s3_archive_keys)
        )
        await _merge_audio_parts(part_paths, file_path, trim_start, trim_duration)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    CHANNEL = "franceinfotv"
    START_DATE = "2026-09-07T04:37:35Z"
    END_DATE = "2026-09-07T05:05:03Z"

    asyncio.run(
        download_mediatree_audio(
            get_s3_filesystem(),
            f"{CHANNEL}_{START_DATE}_{END_DATE}.mp3",
            CHANNEL,
            datetime.fromisoformat(START_DATE),
            datetime.fromisoformat(END_DATE),
        )
    )
