from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from quotaclimat.data_ingestion.advertising_detection.e00_download_audio_files.partition_window import (
    DownloadTask,
)
from quotaclimat.data_ingestion.advertising_detection.processor import (
    ProcessingTask,
    processor,
)


async def mock_download_audio(task: DownloadTask) -> ProcessingTask:
    item = 1 if task.start_date.minute == 0 else 2
    return ProcessingTask(
        audio_file_path=f"test/advertising_detection/assets/tf1_{item}.mp3",
        start_date=task.start_date,
        end_date=task.end_date,
        channel=task.channel,
        download_was_cached=True,
    )


@pytest.mark.asyncio
@patch(
    "quotaclimat.data_ingestion.advertising_detection.processor.download_audio",
    new=mock_download_audio,
)
async def test_extract_segments_run_successfully():
    channel = "tf1"
    start_date = "2025-05-05"

    partition = [
        DownloadTask(
            start_date=datetime(2025, 5, 5, 12, 00, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 1, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
        DownloadTask(
            start_date=datetime(2025, 5, 5, 12, 3, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 4, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
    ]

    await processor(
        channel=channel,
        start_date=start_date,
        partition=list(partition),
    )
