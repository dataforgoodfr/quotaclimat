import asyncio
import logging
import os
import tarfile
import tempfile
from datetime import datetime, timedelta

import numpy as np
import s3fs
from dotenv import load_dotenv
from scipy import signal

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


# Each part is encoded independently and carries a leading LAME encoder-delay/priming
# frame (~1 audio frame, empirically 0.024s) before its real content starts. Naively
# concatenating parts leaves that frame in at every internal boundary, producing a
# small audible glitch. Skipping it at the start of every part but the first gives
# gapless concatenation.
_ENCODER_DELAY = timedelta(seconds=0.024)

# Consecutive parts sometimes (not consistently) share close to a second of duplicate
# audio at their boundary, presumably from how mediatree exports each 2-minutes window.
# We detect it by cross-correlating a short window near the join rather than assuming a
# fixed size, since it varies per boundary and is sometimes absent entirely.
_SAMPLE_RATE = 48_000
_OVERLAP_SEARCH_WINDOW = timedelta(seconds=3)
_OVERLAP_MATCH_WINDOW_SAMPLES = 8_000
_OVERLAP_CORRELATION_THRESHOLD = 0.6


async def _decode_pcm(
    path: str,
    *,
    seek: timedelta | None = None,
    seek_from_end: timedelta | None = None,
    duration: timedelta,
) -> np.ndarray:
    """Decode `duration` of mono PCM audio from `path` into a numpy array of samples."""
    seek_args = (
        ["-sseof", f"-{seek_from_end.total_seconds()}"]
        if seek_from_end is not None
        else ["-ss", str(seek.total_seconds())]
        if seek is not None
        else []
    )
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        *seek_args,
        "-i",
        path,
        "-t",
        str(duration.total_seconds()),
        "-ac",
        "1",
        "-ar",
        str(_SAMPLE_RATE),
        "-f",
        "s16le",
        "-",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed to decode {path}: {stderr.decode(errors='replace')}"
        )
    return np.frombuffer(stdout, dtype="<i2").astype(np.float64)


def _find_best_correlation(
    haystack: np.ndarray, needle: np.ndarray
) -> tuple[int, float]:
    """Return the (start_index, correlation) of the best match of `needle` within `haystack`,
    using normalized cross-correlation so the result is comparable across audio segments.
    """
    needle = needle - needle.mean()
    haystack = haystack - haystack.mean()
    needle_norm = np.linalg.norm(needle) + 1e-9

    numerator = signal.correlate(haystack, needle, mode="valid")

    cumulative_energy = np.concatenate(([0.0], np.cumsum(haystack**2)))
    window_energy = cumulative_energy[len(needle) :] - cumulative_energy[: -len(needle)]
    window_norm = np.sqrt(window_energy) + 1e-9

    correlation = numerator / (needle_norm * window_norm)
    best_start = int(np.argmax(correlation))
    return best_start, float(correlation[best_start])


async def _detect_overlap(prev_path: str, next_path: str) -> timedelta:
    """Detect how much of `next_path`'s start duplicates `prev_path`'s end, by decoding a
    short window on each side of the boundary and cross-correlating them. Returns zero
    when no confident match is found, so real content near a non-overlapping boundary is
    never trimmed.
    """
    tail, head = await asyncio.gather(
        _decode_pcm(
            prev_path,
            seek_from_end=_OVERLAP_SEARCH_WINDOW,
            duration=_OVERLAP_SEARCH_WINDOW,
        ),
        _decode_pcm(next_path, seek=_ENCODER_DELAY, duration=_OVERLAP_SEARCH_WINDOW),
    )

    needle = head[:_OVERLAP_MATCH_WINDOW_SAMPLES]
    if len(needle) < _OVERLAP_MATCH_WINDOW_SAMPLES or len(tail) <= len(needle):
        return timedelta(0)

    start, correlation = await asyncio.to_thread(_find_best_correlation, tail, needle)
    if correlation < _OVERLAP_CORRELATION_THRESHOLD:
        return timedelta(0)

    overlap = timedelta(seconds=(len(tail) - start) / _SAMPLE_RATE)
    logger.debug(
        "Detected %.3fs overlap between %s and %s (correlation=%.2f)",
        overlap.total_seconds(),
        prev_path,
        next_path,
        correlation,
    )
    return overlap


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
    (see `_ENCODER_DELAY` and `_detect_overlap`).

    `trim_start`/`trim_duration` cut the concatenated audio down to the originally
    requested segment, since the parts cover the enclosing 2-minutes intervals and
    can therefore extend before/after the requested start/end dates.
    """
    overlaps = await asyncio.gather(
        *(
            _detect_overlap(prev_path, next_path)
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
                inpoint = _ENCODER_DELAY + overlaps[i - 1]
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

    CHANNEL = "franceinfotv"
    START_DATE = "2026-09-07T04:37:35Z"
    END_DATE = "2026-09-07T05:05:03Z"

    asyncio.run(
        download_audio(
            get_s3_filesystem(),
            f"{CHANNEL}_{START_DATE}_{END_DATE}.mp3",
            CHANNEL,
            datetime.fromisoformat(START_DATE),
            datetime.fromisoformat(END_DATE),
        )
    )
