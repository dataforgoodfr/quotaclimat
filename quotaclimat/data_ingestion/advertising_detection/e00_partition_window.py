from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

tz_paris = ZoneInfo("Europe/Paris")


@dataclass
class Segment:
    start_date: datetime
    end_date: datetime
    channel: str

    @property
    def identifier(self) -> str:
        return f"{self.channel}_{self.start_date.strftime('%Y-%m-%d_%H-%M-%S')}"


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
) -> list[Segment]:
    week_start_date = datetime.fromisoformat(start_date).replace(tzinfo=tz_paris)
    return (
        Segment(
            start_date=segment_start_date,
            end_date=segment_end_date,
            channel=channel,
        )
        for segment_start_date, segment_end_date in _all_intervals_between(
            week_start_date, week_start_date + timedelta(days=7), timedelta(minutes=30)
        )
    )
