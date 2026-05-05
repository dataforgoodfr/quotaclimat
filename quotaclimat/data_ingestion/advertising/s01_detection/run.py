import asyncio
import logging
import math
import os
from datetime import date, datetime, timedelta

from sentry_sdk.crons import monitor
from sqlalchemy import desc, select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.e00_partition_window import (
    partition_week_program,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor
from quotaclimat.data_ingestion.advertising.s01_detection.tools.scheduled_rolling_channels import (
    get_scheduled_rolling_channel,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.testimony_data.extract import (
    get_testimony_data,
)
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


METABASE_RESULT_QUESTION = os.environ.get("METABASE_RESULT_QUESTION")


def _get_next_start_date_from_db(channel: str) -> date | None:
    with get_db_session() as session:
        last_occurrence = session.scalars(
            select(Ad_Occurrence)
            .where(Ad_Occurrence.channel_name == channel)
            .order_by(desc(Ad_Occurrence.occurrence_date))
            .limit(1)
        ).first()

    if last_occurrence is None:
        return None

    last_occurence_date = last_occurrence.occurrence_date.date()

    next_date = last_occurence_date + timedelta(days=1)
    return next_date.isoformat()


def _scaleway_cpu_numbers() -> int:
    mvcpu = os.environ.get("SCW_SLS_CPU")  # Scaleway mVCPU value

    if not mvcpu:
        return 0

    cpu = int(mvcpu) / 1000
    return math.floor(cpu)


def _main():
    with monitor(
        monitor_slug="advertising-detection"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        channel = os.environ.get("CHANNEL")
        if not channel:
            channel = get_scheduled_rolling_channel()
        if not channel:
            rolling_channels_var = os.environ.get("ROLLING_CHANNELS")
            if rolling_channels_var is not None:
                # This feature allow rotating on channels by specifying ROLLING_CHANNELS=bfmtv,arte
                # By specifying a cron that run x times every two hour ("*/y * * * *" with y=120/x), the rotation will execute each channel one time every two hours (the maximum duratio nof the cron)
                # If it is not easy to split the two hours exactly (for instance 3 channels), just leave blank channels it will just raise an error (but make sure to put the correct number of channels)
                rolling_channels = rolling_channels_var.split(",")
                max_hour_duration = 2
                if len(rolling_channels) > 0:
                    now = datetime.now()
                    total_minutes = (now.hour % max_hour_duration) * 60 + now.minute
                    rolling_index = (
                        total_minutes
                        * len(rolling_channels)
                        // (60 * max_hour_duration)
                    )
                    channel = rolling_channels[rolling_index]

        if channel is None:
            logger.warning("No channel provided, exited")
            return

        start_date = os.environ.get("START_DATE")
        if not start_date:
            start_date = _get_next_start_date_from_db(channel)
        if not start_date:
            start_date = os.environ.get("MINIMAL_START_DATE")
        assert start_date is not None, "Need start_date to run the detection process"

        num_workers = max(
            1,
            (
                int(os.environ.get("AUDIO_WORKERS") or "0")
                or (_scaleway_cpu_numbers() - 2)
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

        asyncio.run(
            processor(
                channel=channel,
                operation_name=f"week-{start_date}",
                report_folder=f"year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/channel={channel}",
                segments=partition,
                annotations=annotations,
                num_workers=num_workers,
            )
        )

        if METABASE_RESULT_QUESTION:
            matabase_result_url = METABASE_RESULT_QUESTION.format(
                START_DATE=start_date,
                CHANNEL_NAME=channel,
            )
            logger.info(f"Check results in metabase: {matabase_result_url}")


if __name__ == "__main__":
    _main()
