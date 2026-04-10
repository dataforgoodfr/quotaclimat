import asyncio
import logging
import math
import os
from datetime import timedelta

from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
    partition_week_program,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e00b_clean_pre_existing_detection import (
    clean_pre_existing_detections,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor
from quotaclimat.data_ingestion.advertising.s01_detection.tools.testimony_data.extract import (
    get_testimony_data,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    channel = os.environ.get("CHANNEL")
    start_date = os.environ.get("START_DATE")

    num_workers = max(
        1,
        int(os.environ.get("AUDIO_WORKERS") or "0")
        or math.floor(
            int(os.environ.get("SCW_SLS_CPU") or "0") / 1000  # Scaleway mVCPU value
        ),
    )

    logger.info(
        f"Start processing of 1 week of {channel} starting from {start_date} on {num_workers} cpu"
    )

    # Annotations, for local run
    testimony_channel = os.environ.get("TESTIMONY_CHANNEL")
    testimony_file = os.environ.get("TESTIMONY_FILE", "export.csv")

    partition = partition_week_program(
        channel=channel,
        start_date=start_date,
        margin=timedelta(minutes=15),
    )

    if testimony_channel:
        annotations = get_testimony_data(
            channel=testimony_channel,
            from_date=partition[0].start_date,
            to_date=partition[-1].end_date,
            source_file=testimony_file,
        )
    else:
        annotations = None

    clean_pre_existing_detections(partition)

    asyncio.run(
        processor(
            operation_name=f"{channel}-week-{start_date}",
            report_folder=f"year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/channel={channel}",
            segments=partition,
            annotations=annotations,
            num_workers=num_workers,
        )
    )
