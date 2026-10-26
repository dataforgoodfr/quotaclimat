import asyncio
import os
from datetime import datetime, timedelta

NB_FRAMES = int(os.environ.get("SCREENSHOT_NB_FRAMES", 6))
MOSAIC_COLUMNS = int(os.environ.get("SCREENSHOT_MOSAIC_COLUMNS", 3))
FRAME_WIDTH = 320
FRAME_HEIGHT = 180


def frame_offsets(duration_sec: float, nb_frames: int = NB_FRAMES) -> list[float]:
    """Offsets of the frames from the start of the ad, in the middle of nb_frames equal slices.

    Depends only on the ad duration, so an ad and each of its occurrences get frames at the same
    positions: tile i of an occurrence's mosaic compares with tile i of its ad's.
    """
    return [round((i + 0.5) * duration_sec / nb_frames, 3) for i in range(nb_frames)]


def floor_to_2_minutes(dt: datetime) -> datetime:
    """Start of the 2-minutes mediatree archive containing dt."""
    return dt.replace(minute=dt.minute - dt.minute % 2, second=0, microsecond=0)


def frame_positions_in_parts(
    occurrence_start: datetime, offsets: list[float]
) -> list[tuple[datetime, float]]:
    """For each frame, the start of the 2-minutes archive holding it and the offset in this archive."""
    positions = []
    for offset in offsets:
        frame_time = occurrence_start + timedelta(seconds=offset)
        part_start = floor_to_2_minutes(frame_time)
        positions.append((part_start, (frame_time - part_start).total_seconds()))
    return positions


async def video_duration(video_path: str) -> float | None:
    """Duration of the video container in seconds, None when ffprobe can't read it."""
    process = await asyncio.create_subprocess_exec(
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        video_path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await process.communicate()
    try:
        return float(stdout.decode().strip())
    except ValueError:
        return None


async def _run_ffmpeg(*args: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-loglevel",
        "error",
        "-y",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {stderr.decode(errors='replace')}")


# Every frame is scaled into the same box (black bands on other aspect ratios), as the tile filter
# needs frames of the same size. Full range pixels: recent ffmpeg versions refuse to encode a jpg
# from limited range ones ("Non full-range YUV is non-standard").
_JPEG_PIXEL_FORMAT = "format=yuvj420p"
_SCALE_AND_PAD = (
    f"scale={FRAME_WIDTH}:{FRAME_HEIGHT}:force_original_aspect_ratio=decrease,"
    f"pad={FRAME_WIDTH}:{FRAME_HEIGHT}:(ow-iw)/2:(oh-ih)/2,{_JPEG_PIXEL_FORMAT}"
)


async def extract_frame(video_path: str, offset_sec: float, output_path: str) -> bool:
    """Write the frame at offset_sec of the video to output_path (jpg).

    Returns False when the video has no frame there (ffmpeg then succeeds without writing anything,
    e.g. an offset past the end of the video).
    """
    await _run_ffmpeg(
        "-ss",
        f"{max(offset_sec, 0):.3f}",
        "-i",
        video_path,
        "-frames:v",
        "1",
        "-vf",
        _SCALE_AND_PAD,
        "-q:v",
        "4",
        output_path,
    )
    return os.path.isfile(output_path)


async def _black_frame(output_path: str) -> None:
    await _run_ffmpeg(
        "-f",
        "lavfi",
        "-i",
        f"color=c=black:s={FRAME_WIDTH}x{FRAME_HEIGHT},{_JPEG_PIXEL_FORMAT}",
        "-frames:v",
        "1",
        output_path,
    )


async def build_mosaic(
    frame_paths: list[str | None],
    work_dir: str,
    output_path: str,
    columns: int = MOSAIC_COLUMNS,
) -> None:
    """Tile the frames, in order, into one jpg. A missing frame (None) is a black tile, so the
    tiles stay at the same place as in the other mosaics.
    """
    os.makedirs(work_dir, exist_ok=True)
    for i, frame_path in enumerate(frame_paths):
        tile_path = os.path.join(work_dir, f"tile_{i:03d}.jpg")
        if frame_path is None:
            await _black_frame(tile_path)
        elif frame_path != tile_path:
            os.replace(frame_path, tile_path)

    rows = -(-len(frame_paths) // columns)
    await _run_ffmpeg(
        "-start_number",
        "0",
        "-i",
        os.path.join(work_dir, "tile_%03d.jpg"),
        "-vf",
        f"tile={columns}x{rows}:padding=4:margin=4,{_JPEG_PIXEL_FORMAT}",
        "-frames:v",
        "1",
        "-q:v",
        "4",
        output_path,
    )
