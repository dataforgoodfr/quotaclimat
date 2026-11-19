import asyncio
import logging
import math
import multiprocessing
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta

from sentry_sdk.crons import monitor
from sqlalchemy import func, select

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.e00_download_audio import (
    AUDIO_DIR,
)
from quotaclimat.data_ingestion.advertising.s01_detection.e01_check_partition_cover import (
    check_bucket_cover,
    tz_paris,
)
from quotaclimat.data_ingestion.advertising.s01_detection.processor import processor
from quotaclimat.data_ingestion.advertising.s01_detection.tools.cache import (
    GLOBAL_CACHE_FOLDER,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.memory import (
    memory_summary,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.rolling_channels import (
    channel_statuses,
    format_summary,
    is_week_finished,
    parse_channels,
    plan_week,
)
from quotaclimat.utils.logger import getLogger, set_log_prefix
from quotaclimat.utils.sentry import sentry_init

logger = logging.getLogger(__name__)


METABASE_RESULT_QUESTION = os.environ.get("METABASE_RESULT_QUESTION")


def _get_last_occurrences_from_db(channels: list[str]) -> dict[str, datetime]:
    """Date (naive UTC) of the last occurrence of each channel that has some."""
    with get_db_session() as session:
        rows = session.execute(
            select(Ad_Occurrence.channel_name, func.max(Ad_Occurrence.occurrence_date))
            .where(Ad_Occurrence.channel_name.in_(channels))
            .group_by(Ad_Occurrence.channel_name)
        ).all()
    return {channel: last_occurrence for channel, last_occurrence in rows}


def _clean_local_files():
    # The job's disk cannot hold the chunks of several channels: between two channels,
    # drop the caches and the audio files a failed channel may have left
    for folder in {GLOBAL_CACHE_FOLDER, AUDIO_DIR}:
        shutil.rmtree(folder, ignore_errors=True)


def _run_in_subprocess(target, *args) -> bool:
    """Run target(*args) in a new Python process (spawn), True if it succeeded.

    Python does not give back to the system the memory of the objects it freed: after a
    channel, the main process keeps the memory of the biggest week processed so far, and
    the worker processes forked from it inherit it. A new process per channel starts
    each channel from an empty memory. The child logs its own errors.
    """
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join()
    if process.exitcode != 0:
        # Negative: killed by this signal (-9: SIGKILL, from the system when out of memory)
        logger.error(f"The process of {args} ended with exit code {process.exitcode}")
    return process.exitcode == 0


def _process_week_in_subprocess(channel: str, start_date: str):
    # Entry point of the process of one channel: logs and Sentry are set up again
    getLogger(prefix=channel)
    sentry_init()
    try:
        _process_week(channel, start_date)
    except Exception:
        logger.exception(f"Detection failed for {channel} on the week of {start_date}")
        sys.exit(1)


def _format_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    return f"{minutes // 60}h{minutes % 60:02d}"


def _run_detection():
    # One run processes one week of every channel that needs it, one by one, then stops:
    # - channels: CHANNEL (a single one) or ROLLING_CHANNELS=tf1,france2,arte
    # - week: START_DATE, or the oldest "Monday after the last occurrence" of these channels
    # - only the channels not processed yet on this week are run
    channels = parse_channels(
        os.environ.get("CHANNEL") or os.environ.get("ROLLING_CHANNELS")
    )
    assert channels, (
        "Need channels to run the detection process: CHANNEL or ROLLING_CHANNELS "
        "(SCHEDULED_ROLLING_CHANNELS is not read anymore)"
    )

    forced_start_date = os.environ.get("START_DATE")
    minimal_start_date = os.environ.get("MINIMAL_START_DATE")

    statuses = channel_statuses(
        channels,
        _get_last_occurrences_from_db(channels),
        date.fromisoformat(minimal_start_date) if minimal_start_date else None,
    )
    week, channels_to_run = plan_week(
        statuses,
        date.fromisoformat(forced_start_date) if forced_start_date else None,
    )
    logger.info(format_summary(statuses, week, channels_to_run))

    assert week.weekday() == 0, f"start_date {week} is not a Monday"
    if not is_week_finished(week, datetime.now(tz=tz_paris).date()):
        # Catching up is over: the week is not finished yet
        logger.info(f"Week of {week} not finished yet, nothing to process")
        return

    results = []
    for index, channel in enumerate(channels_to_run):
        logger.info(
            f"Channel {index + 1}/{len(channels_to_run)}: {channel}, week of {week}"
        )
        started = time.monotonic()
        # Every log line of this channel (worker processes included) starts with it
        set_log_prefix(channel)
        try:
            if len(channels_to_run) > 1:
                # Each channel in its own process, to start it from an empty memory
                succeeded = _run_in_subprocess(
                    _process_week_in_subprocess, channel, week.isoformat()
                )
            else:
                _process_week(channel, week.isoformat())
                succeeded = True
        except Exception:
            logger.exception(f"Detection failed for {channel} on the week of {week}")
            succeeded = False
        finally:
            set_log_prefix(None)
        # A failed channel does not stop the others, the job fails at the end
        status = "done" if succeeded else "FAILED"
        results.append((channel, status, time.monotonic() - started))
        if len(channels_to_run) > 1:
            _clean_local_files()
        logger.info(f"After {channel}, memory: {memory_summary()}")

    logger.info(
        "\n".join(
            [f"Result of the week of {week}:"]
            + [
                f"  {channel}: {status} in {_format_duration(duration)}"
                for channel, status, duration in results
            ]
        )
    )
    failed = [channel for channel, status, _ in results if status == "FAILED"]
    if failed:
        raise RuntimeError(
            f"Detection failed for {', '.join(failed)} on the week of {week}"
        )


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
    # Worker processes of the CPU bound steps (audio processing, comparison of the
    # fingerprints): one per CPU but one, left to the main process (downloads, database)
    num_workers = max(
        1,
        (int(os.environ.get("AUDIO_WORKERS") or "0") or (_scaleway_cpu_numbers() - 1)),
    )

    logger.info(
        f"Start processing of 1 week of {channel} starting from {start_date} on {num_workers} worker processes"
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

        _run_detection()
