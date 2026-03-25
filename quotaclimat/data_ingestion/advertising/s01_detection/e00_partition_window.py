from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quotaclimat.data_ingestion.advertising.s01_detection.tools.program import (
    Show,
    extend_program_by,
    get_channel_program,
)

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


def _all_intervals_for_program(
    program: list[Show], week_start_date: datetime, interval: timedelta
):
    for show in program:
        start_date, end_date = show.for_week(week_start_date)

        for segment_start_date, segment_end_date in _all_intervals_between(
            start_date, end_date, interval
        ):
            yield (segment_start_date, segment_end_date)


def partition_week(
    start_date: str,  # Start of the analyzed week, format iso 2026-12-31
    channel: str,
) -> list[Segment]:
    week_start_date = datetime.fromisoformat(start_date).replace(tzinfo=tz_paris)
    return [
        Segment(
            start_date=segment_start_date,
            end_date=segment_end_date,
            channel=channel,
        )
        for segment_start_date, segment_end_date in _all_intervals_between(
            week_start_date, week_start_date + timedelta(days=7), timedelta(minutes=30)
        )
    ]


def partition_week_program(
    start_date: str,  # Start of the analyzed week, format iso 2026-12-31
    channel: str,
    margin: timedelta,
) -> list[Segment]:
    week_start_date = datetime.fromisoformat(start_date).replace(tzinfo=tz_paris)

    # Ensures week_start_date is a Monday
    if week_start_date.weekday() != 0:
        raise ValueError("start_date must be a Monday")

    program = extend_program_by(get_channel_program(channel), margin)

    return [
        Segment(
            start_date=segment_start_date,
            end_date=segment_end_date,
            channel=channel,
        )
        for segment_start_date, segment_end_date in _all_intervals_for_program(
            program, week_start_date, timedelta(minutes=30)
        )
    ]


if __name__ == "__main__":
    channel = "tf1"
    start_date = "2025-05-05"
    margin = timedelta(minutes=30)

    print(
        partition_week_program(
            channel=channel,
            start_date=start_date,
            margin=margin,
        )
    )
