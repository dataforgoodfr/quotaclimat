import asyncio
import logging
import os
from datetime import timedelta

from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
    partition_week_program,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    channel = os.environ.get("CHANNEL")
    start_date = os.environ.get("START_DATE")

    partition = partition_week_program(
        channel=channel,
        start_date=start_date,
        margin=timedelta(minutes=15),
    )

    asyncio.run(
        processor(
            operation_name=f"{channel}-week-{start_date}",
            report_folder=f"year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/channel={channel}",
            segments=partition,
        )
    )
