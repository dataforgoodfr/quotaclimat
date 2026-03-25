import asyncio
import logging
from datetime import timedelta

from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
    partition_week_program,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor
from quotaclimat.data_ingestion.advertising.s01_detection.tools.testimony_data.extract import (
    get_testimony_data,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    channel = "tf1"
    TESTIMONY_CHANNEL = "TF1"
    start_date = "2025-05-05"

    partition = partition_week_program(
        channel=channel,
        start_date=start_date,
        margin=timedelta(minutes=15),
    )

    annotations = get_testimony_data(
        channel=TESTIMONY_CHANNEL,
        from_date=partition[0].start_date,
        to_date=partition[-1].end_date,
        source_file="export.csv",
    )

    asyncio.run(
        processor(
            operation_name=f"{channel}-full_week-{start_date}",
            segments=partition,
            annotations=annotations,
        )
    )
