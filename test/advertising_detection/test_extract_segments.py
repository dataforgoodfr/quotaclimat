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
        download_task=task,
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

    groups = await processor(
        operation_name="test",
        channel=channel,
        start_date=start_date,
        partition=list(partition),
    )

    ads = [group for group in groups if group.count >= 2]
    assert len(ads) == 1

    assert len(ads[0].occurrences) == 2

    assert ads[0].occurrences[0].duration_sec >= 20
    assert ads[0].occurrences[0].duration_sec <= 21
    assert ads[0].occurrences[1].duration_sec >= 20
    assert ads[0].occurrences[1].duration_sec <= 21

    start_date_1 = datetime.fromtimestamp(ads[0].occurrences[0].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    start_date_2 = datetime.fromtimestamp(ads[0].occurrences[1].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    assert start_date_1 >= partition[0].start_date
    assert start_date_1 <= partition[0].end_date
    assert start_date_2 >= partition[1].start_date
    assert start_date_2 <= partition[1].end_date
