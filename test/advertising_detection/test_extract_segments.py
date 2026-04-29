from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from postgres.schemas.advertising.models import AdvertisingBase
from postgres.schemas.models import (
    connect_to_db,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
    Segment,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import (
    processor,
)


async def mock_download_audio(api, task: Segment) -> tuple[str, bool]:
    item = 1 if task.start_date.minute == 0 else 2
    return (
        f"test/advertising_detection/assets/tf1_{item}.mp3",
        task.start_date.minute == 0,
    )


@pytest.mark.asyncio
@patch(
    "quotaclimat.data_ingestion.advertising.s01_detection.e01_download_audio.download_audio",
    new=mock_download_audio,
)
async def test_extract_fragments_run_successfully():
    # This should be put in pytest configuration
    conn = connect_to_db()
    AdvertisingBase.metadata.drop_all(conn)
    AdvertisingBase.metadata.create_all(conn, checkfirst=True)

    channel = "test-channel"

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

    fragments = await processor(
        channel=channel,
        operation_name="test_extract_fragments_run_successfully",
        segments=segments,
        report_folder=None,
    )

    maybe_ads = [f for f in fragments if f.classification == "new_ad"]
    assert len(maybe_ads) == 2
    assert maybe_ads[0].group_id == maybe_ads[1].group_id

    assert maybe_ads[0].end_sec - maybe_ads[0].start_sec >= 20
    assert maybe_ads[0].end_sec - maybe_ads[0].start_sec <= 21
    assert maybe_ads[1].end_sec - maybe_ads[1].start_sec >= 20
    assert maybe_ads[1].end_sec - maybe_ads[1].start_sec <= 21

    start_date_1 = datetime.fromtimestamp(maybe_ads[0].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    start_date_2 = datetime.fromtimestamp(maybe_ads[1].start_sec).astimezone(
        ZoneInfo("Europe/Paris")
    )
    assert start_date_1 >= segments[0].start_date
    assert start_date_1 <= segments[0].end_date
    assert start_date_2 >= segments[1].start_date
    assert start_date_2 <= segments[1].end_date
