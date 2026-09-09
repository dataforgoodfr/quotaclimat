import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from ..tools.mediatree.bucket_mediatree import (
    download_days_audio_parts,
    get_s3_filesystem,
)
from ..tools.segments import Segment
from .e02_create_chunks import ChunkCreatorJob

logger = logging.getLogger(__name__)


async def download_all_audio_parts(
    channel, start_date: datetime, end_date: datetime
) -> list[ChunkCreatorJob]:
    audio_files = await download_days_audio_parts(
        fs=get_s3_filesystem(),
        channel=channel,
        # list all days between start_date and end_date
        days=[
            start_date.date() + timedelta(days=i)
            for i in range((end_date.date() - start_date.date()).days)
        ],
        dest_dir="./.cache/mediatree",
        max_concurrent_downloads=10,
    )
    logger.info(
        f"Downloaded {len(audio_files)} audio files for channel {channel} between {start_date} and {end_date}"
    )

    # audio file names look like: franceinfotv_2026-09-07T04-36-00Z_2026-09-07T04-38-00Z.mp3
    filename_dt_format = "%Y-%m-%dT%H-%M-%SZ"
    audio_segments = [
        (
            Segment(
                start_date=datetime.strptime(
                    f.split("/")[-1].split(".")[0].split("_")[1],
                    filename_dt_format,
                ).replace(tzinfo=ZoneInfo("UTC")),
                end_date=datetime.strptime(
                    f.split("/")[-1].split(".")[0].split("_")[2],
                    filename_dt_format,
                ).replace(tzinfo=ZoneInfo("UTC")),
                channel=channel,
            ),
            f,
        )
        for f in audio_files
    ]

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

    return chunks_creator_jobs
