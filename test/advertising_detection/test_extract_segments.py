import logging
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from postgres.schemas.advertising.models import AdvertisingBase
from postgres.schemas.models import (
    connect_to_db,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e02_split_in_chunks import (
    ChunkCreatorJob,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import (
    processor,
)
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@patch(
    "quotaclimat.data_ingestion.advertising.s01_detection.processor.download_all_audio_parts",
)
async def test_extract_fragments_run_successfully(mocked_download_all_audio_parts):
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

    mocked_download_all_audio_parts.return_value = [
        ChunkCreatorJob(
            segment=segments[0],
            audio_file_path="test/advertising_detection/assets/tf1_1.mp3",
        ),
        ChunkCreatorJob(
            segment=segments[1],
            audio_file_path="test/advertising_detection/assets/tf1_2.mp3",
        ),
    ]

    fragments = await processor(
        channel=channel,
        start_date=datetime(2025, 5, 5, 12, 00, tzinfo=ZoneInfo("Europe/Paris")),
        end_date=datetime(2025, 5, 5, 12, 4, tzinfo=ZoneInfo("Europe/Paris")),
        operation_name="test_extract_fragments_run_successfully",
        report_folder=None,
    )

    maybe_ads = [f for f in fragments if f.classification == "new_ad"]
    assert len(maybe_ads) == 2
    assert maybe_ads[0].group_id == maybe_ads[1].group_id

    # It was 20, it may depends on the splitting algo, it needs to be checked again
    AD_0_DURATION = maybe_ads[0].end_sec - maybe_ads[0].start_sec
    AD_1_DURATION = maybe_ads[1].end_sec - maybe_ads[1].start_sec
    assert abs(AD_0_DURATION - AD_1_DURATION) < 1, (
        "The two ads does not have the same duration"
    )
    assert AD_0_DURATION > 10, (
        "The detected ad does not seem to be a long enough segment"
    )

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
