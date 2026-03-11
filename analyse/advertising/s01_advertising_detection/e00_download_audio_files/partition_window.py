from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .download_partition import DownloadTask

tz_paris = ZoneInfo("Europe/Paris")


def _all_intervals_between(
    start_date: datetime, end_date: datetime, interval: timedelta
):
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + interval, end_date)
        yield (current_start, current_end)
        current_start = current_end


def partition_week(
    start_date: str,  # Start of the analyzed week, format iso 2026-12-31
    channel: str,
) -> list[DownloadTask]:
    week_start_date = datetime.fromisoformat(start_date).replace(tzinfo=tz_paris)
    return (
        DownloadTask(
            start_sec=start_sec.timestamp(),
            end_sec=end_sec.timestamp(),
            channel=channel,
        )
        for start_sec, end_sec in _all_intervals_between(
            week_start_date, week_start_date + timedelta(days=7), timedelta(minutes=30)
        )
    )
