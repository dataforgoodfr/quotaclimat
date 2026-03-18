from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from quotaclimat.data_ingestion.advertising_detection.e00_partition_window import (
    Segment,
)
from quotaclimat.data_ingestion.advertising_detection.processor import (
    processor,
)


async def mock_download_audio(task: Segment) -> tuple[str, bool]:
    item = 1 if task.start_date.minute == 0 else 2
    return (
        f"test/advertising_detection/assets/tf1_{item}.mp3",
        task.start_date.minute == 0,
    )


@pytest.mark.asyncio
@patch(
    "quotaclimat.data_ingestion.advertising_detection.e01_download_audio.download_audio",
    new=mock_download_audio,
)
async def test_extract_chunks_run_successfully():
    channel = "tf1"
    start_date = "2025-05-05"

    segments = [
        Segment(
            start_date=datetime(2025, 5, 5, 12, 00, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 1, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
        Segment(
            start_date=datetime(2025, 5, 5, 12, 3, tzinfo=ZoneInfo("Europe/Paris")),
            end_date=datetime(2025, 5, 5, 12, 4, tzinfo=ZoneInfo("Europe/Paris")),
            channel=channel,
        ),
    ]

    groups = await processor(
        operation_name="test",
        channel=channel,
        start_date=start_date,
        segments=segments,
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
    assert start_date_1 >= segments[0].start_date
    assert start_date_1 <= segments[0].end_date
    assert start_date_2 >= segments[1].start_date
    assert start_date_2 <= segments[1].end_date
