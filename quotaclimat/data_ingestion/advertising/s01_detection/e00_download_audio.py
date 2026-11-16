import dataclasses
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

from ..tools.mediatree.bucket_mediatree import (
    archive_audio_path,
    archive_part,
    download_audio_archives,
    get_s3_filesystem,
    list_days_archives,
)
from ..tools.segments import Segment
from .e01_check_partition_cover import expected_parts_for_week
from .e02_split_in_chunks import ChunkCreatorJob

logger = logging.getLogger(__name__)

AUDIO_DIR = "./.cache/mediatree"

# The job's disk is small (1 GB): the week is downloaded, split in chunks and deleted
# by batches of 2-minutes files (a mp3 is ~2 MB, the tar it comes from, with its
# video, much more: at most MAX_CONCURRENT_DOWNLOADS tars are on disk at once).
DOWNLOAD_BATCH_SIZE = 100
MAX_CONCURRENT_DOWNLOADS = 5


@dataclass
class WeekAudio:
    jobs: list[ChunkCreatorJob]
    # Local mp3 path -> S3 key of the archive it is extracted from
    s3_keys: dict[str, str] = field(default_factory=dict)


async def list_audio_jobs(channel: str, start_date: datetime) -> WeekAudio:
    """One job per 2-minutes file of the week to analyse, without downloading anything.

    Only the files of the shows of the program grid (with their margins) are
    analysed: what mediatree sent outside the program is skipped, what is missing is
    reported by check_partition_cover. The audio of the jobs is downloaded later, batch
    by batch, by `download_jobs_audio`.
    """
    expected_parts = expected_parts_for_week(channel, start_date.date().isoformat())
    if not expected_parts:
        logger.warning(
            f"No show in the program grid for {channel} the week of {start_date.date()}"
        )
        return WeekAudio(jobs=[])

    s3_keys = await list_days_archives(
        fs=get_s3_filesystem(),
        channel=channel,
        # Mediatree files are in the folder of the UTC day they start on
        days=sorted({part_start.date() for part_start, _ in expected_parts}),
        only_parts=expected_parts,
    )

    audio_segments = []
    for key in s3_keys:
        part_start, part_end = archive_part(key)
        audio_segments.append(
            (
                Segment(start_date=part_start, end_date=part_end, channel=channel),
                archive_audio_path(key, AUDIO_DIR),
            )
        )

    chunks_creator_jobs: list[ChunkCreatorJob] = []

    for i, (segment, audio_file_path) in enumerate(audio_segments):
        is_previous_contiguous = (
            i > 0 and audio_segments[i - 1][0].end_date == segment.start_date
        )
        next_contiguous = (
            audio_segments[i + 1]
            if (
                i < len(audio_segments) - 1
                and audio_segments[i + 1][0].start_date == segment.end_date
            )
            else None
        )
        chunks_creator_jobs.append(
            ChunkCreatorJob(
                segment=segment,
                audio_file_path=audio_file_path,
                has_previous_segment=is_previous_contiguous,
                next_audio_file_path=next_contiguous[1] if next_contiguous else None,
            )
        )

    logger.info(
        f"{len(chunks_creator_jobs)} audio files to analyse for {channel} the week of {start_date.date()}"
    )
    return WeekAudio(
        jobs=chunks_creator_jobs,
        s3_keys={path: key for key, (_, path) in zip(s3_keys, audio_segments)},
    )


async def download_jobs_audio(
    week_audio: WeekAudio, jobs: list[ChunkCreatorJob]
) -> tuple[list[ChunkCreatorJob], list[str]]:
    """Download the audio of these jobs and of the file following each of them.

    Returns the jobs that can run (a job whose own file failed to download is dropped,
    one whose next file failed runs without it) and the local files to delete once
    they ran.
    """
    paths = sorted(
        {
            path
            for job in jobs
            for path in (job.audio_file_path, job.next_audio_file_path)
            if path is not None
        }
    )
    downloaded = await download_audio_archives(
        fs=get_s3_filesystem(),
        s3_keys=[week_audio.s3_keys[path] for path in paths],
        dest_dir=AUDIO_DIR,
        max_concurrent_downloads=MAX_CONCURRENT_DOWNLOADS,
        disable_progress=True,
        skip_failures=True,
    )

    ready = []
    for job in jobs:
        if not os.path.isfile(job.audio_file_path):
            continue
        if job.next_audio_file_path and not os.path.isfile(job.next_audio_file_path):
            job = dataclasses.replace(job, next_audio_file_path=None)
        ready.append(job)
    return ready, downloaded


def delete_audio(paths: list[str]) -> None:
    for path in paths:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
