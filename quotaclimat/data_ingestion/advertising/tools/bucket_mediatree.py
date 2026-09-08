import asyncio
import logging
import os
import tarfile
import tempfile
from datetime import datetime, timedelta

import s3fs
from dotenv import load_dotenv

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

    return f"/{BUCKET_NAME}/output/{channel}/{start_date.strftime('%Y/%m/%d')}/{channel}_{start_date.strftime('%Y-%m-%dT%H-%M-%SZ')}_{end_date.strftime('%Y-%m-%dT%H-%M-%SZ')}.tar"


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
    with tarfile.open(archive_path) as tar:
        tar.extractall(dest_dir, filter="data")

    return os.path.join(dest_dir, audio_name)


async def _download_part(fs: s3fs.S3FileSystem, s3_key: str, dest_dir: str) -> str:
    basename = os.path.basename(s3_key)
    archive_path = os.path.join(dest_dir, basename)
    await fs._get_file(s3_key, archive_path)

    extract_dir = os.path.join(dest_dir, os.path.splitext(basename)[0])
    audio_name = f"{os.path.splitext(basename)[0]}.mp3"
    os.makedirs(extract_dir, exist_ok=True)
    part_path = await asyncio.to_thread(
        _extract_audio_from_archive, archive_path, extract_dir, audio_name
    )
    return part_path


async def _merge_audio_parts(
    part_paths: list[str],
    output_path: str,
    trim_start: timedelta,
    trim_duration: timedelta,
) -> None:
    """Losslessly concatenate audio parts (in order) into a single file.

    Uses ffmpeg's concat demuxer rather than raw byte concatenation, which can
    introduce glitches/gaps at part boundaries for compressed formats like mp3.

    `trim_start`/`trim_duration` cut the concatenated audio down to the originally
    requested segment, since the parts cover the enclosing 2-minutes intervals and
    can therefore extend before/after the requested start/end dates.
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


async def download_audio(
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
        part_paths = await asyncio.gather(
            *(_download_part(fs, s3_key, tmp_dir) for s3_key in s3_archive_keys)
        )
        await _merge_audio_parts(part_paths, file_path, trim_start, trim_duration)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    logger.info(ACCESS_KEY)

    CHANNEL = "franceinfotv"
    START_DATE = "2026-09-07T04:37:35Z"
    END_DATE = "2026-09-07T04:45:03Z"

    asyncio.run(
        download_audio(
            get_s3_filesystem(),
            f"{CHANNEL}_{START_DATE}_{END_DATE}.mp3",
            CHANNEL,
            datetime.fromisoformat(START_DATE),
            datetime.fromisoformat(END_DATE),
        )
    )
