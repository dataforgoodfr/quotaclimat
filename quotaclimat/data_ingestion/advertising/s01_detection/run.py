import asyncio
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone

from sentry_sdk.crons import monitor
from sqlalchemy import desc, select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.e01_check_partition_cover import (
    check_bucket_cover,
    tz_paris,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor
from quotaclimat.data_ingestion.advertising.s01_detection.tools.scheduled_rolling_channels import (
    get_scheduled_rolling_channel,
)
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


METABASE_RESULT_QUESTION = os.environ.get("METABASE_RESULT_QUESTION")


def _get_next_start_date_from_db(channel: str) -> str | None:
    """Monday (Paris) of the week following the last week processed for this channel."""
    with get_db_session() as session:
        last_occurrence = session.scalars(
            select(Ad_Occurrence)
            .where(Ad_Occurrence.channel_name == channel)
            .order_by(desc(Ad_Occurrence.occurrence_date))
            .limit(1)
        ).first()

    if last_occurrence is None:
        return None

    # occurrence_date is a naive UTC datetime (datetime.fromtimestamp in a UTC container).
    # The last occurrences of a week can fall on the Monday after it (a Sunday show
    # crossing midnight, plus its margin): going back 12 hours puts them in their week.
    last_occurrence_paris = last_occurrence.occurrence_date.replace(
        tzinfo=timezone.utc
    ).astimezone(tz_paris) - timedelta(hours=12)
    last_monday = last_occurrence_paris.date() - timedelta(
        days=last_occurrence_paris.weekday()
    )
    return (last_monday + timedelta(days=7)).isoformat()


def _scaleway_cpu_numbers() -> int:
    mvcpu = os.environ.get("SCW_SLS_CPU")  # Scaleway mVCPU value

    if not mvcpu:
        return 0

    cpu = int(mvcpu) / 1000
    return math.floor(cpu)


def _run_cover_check():
    # COVER_CHECK_ONLY=true only lists mediatree's bucket and reports, for every channel
    # (or the ones of COVER_CHECK_CHANNELS=tf1,france2), which files of the week are missing
    getLogger()
    start_date = os.environ.get("START_DATE")
    assert start_date, "Need START_DATE (a Monday) to check the bucket cover"
    channels = [
        c.strip()
        for c in os.environ.get("COVER_CHECK_CHANNELS", "").split(",")
        if c.strip()
    ]
    print(asyncio.run(check_bucket_cover(start_date, channels or None)))


def _process_week(channel: str, start_date: str):
    num_workers = max(
        1,
        (int(os.environ.get("AUDIO_WORKERS") or "0") or (_scaleway_cpu_numbers() - 2)),
    )

    logger.info(
        f"Start processing of 1 week of {channel} starting from {start_date} on {num_workers} cpu"
    )

    start_datetime = datetime.fromisoformat(start_date)
    end_datetime = start_datetime + timedelta(days=7)

    asyncio.run(
        processor(
            channel=channel,
            start_date=start_datetime,
            end_date=end_datetime,
            operation_name=f"week-{start_date}",
            report_folder=f"year={start_date[:4]}/month={start_date[5:7]}/day={start_date[8:10]}/channel={channel}",
            annotations=None,
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
    if os.environ.get("COVER_CHECK_ONLY", "").lower() in ("1", "true", "yes"):
        _run_cover_check()
        sys.exit(0)

    with monitor(
        monitor_slug="advertising-detection"
    ):  # https://docs.sentry.io/platforms/python/crons/
        getLogger()
        sentry_init()

        channel = os.environ.get("CHANNEL")
        if not channel:
            channel = get_scheduled_rolling_channel()
        if not channel:
            # This feature allow rotating on channels by specifying ROLLING_CHANNELS=bfmtv,arte
            # By specifying a cron that run x times every two hour ("*/y * * * *" with y=120/x), the rotation will execute each channel one time every two hours (the maximum duratio nof the cron)
            # If it is not easy to split the two hours exactly (for instance 3 channels), just leave blank channels it will just raise an error (but make sure to put the correct number of channels)
            rolling_channels = [
                c.strip() for c in os.environ.get("ROLLING_CHANNELS", "").split(",")
            ]
            max_hour_duration = 2
            if any(rolling_channels):
                now = datetime.now()
                total_minutes = (now.hour % max_hour_duration) * 60 + now.minute
                rolling_index = (
                    total_minutes * len(rolling_channels) // (60 * max_hour_duration)
                )
                channel = rolling_channels[rolling_index]
        assert channel, (
            "Need a channel to run the detection process: "
            "CHANNEL, SCHEDULED_ROLLING_CHANNELS or ROLLING_CHANNELS"
        )

        start_date = os.environ.get("START_DATE")
        if not start_date:
            start_date = _get_next_start_date_from_db(channel)
        if not start_date:
            start_date = os.environ.get("MINIMAL_START_DATE")
        assert start_date, (
            "Need a start date to run the detection process: START_DATE, "
            "or MINIMAL_START_DATE for a channel without any occurrence in the database"
        )

        start_monday = date.fromisoformat(start_date)
        assert start_monday.weekday() == 0, f"start_date {start_date} is not a Monday"
        if start_monday > datetime.now(tz=tz_paris).date() - timedelta(days=7):
            # Catching up is over for this channel: the week is not finished yet
            logger.info(
                f"Week of {start_date} not finished yet for {channel}, nothing to process"
            )
        else:
            _process_week(channel, start_date)
