import pytest

from quotaclimat.data_ingestion.advertising_detection.processor import processor


@pytest.mark.asyncio
async def test_extract_segments_run_successfully():
    channel = "tf1"
    start_date = "2025-05-05"

    partition = [
        # DownloadTask(
        #     start_date="2025-05-05T00:00:00",
        #     end_date="2025-05-05T00:01:00",
        #     channel=channel,
        # ),
        # DownloadTask(
        #     start_date="2025-05-05T00:03:00",
        #     end_date="2025-05-05T00:04:00",
        #     channel=channel,
        # ),
    ]

    await processor(
        channel=channel,
        start_date=start_date,
        partition=list(partition),
    )
