from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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

    if week_start_date > datetime.now(tz=tz_paris) - timedelta(days=7):
        raise ValueError("start_date must be at least 7 days in the past")

    program = extend_program_by(get_channel_program(channel), margin)

    return [
        Segment(
            start_date=segment_start_date,
            end_date=segment_end_date,
            channel=channel,
        )
        for segment_start_date, segment_end_date in _all_intervals_for_program(
            program, week_start_date, timedelta(minutes=10)
        )
    ]


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ceil_to_multiple(dt: datetime, rounding_drift: timedelta) -> datetime:
    remainder = (dt - _EPOCH) % rounding_drift
    if remainder == timedelta():
        return dt
    return dt + (rounding_drift - remainder)


def add_rounding_drift(
    segments: list[Segment], rounding_drift: timedelta
) -> list[Segment]:
    """This function ensures all segments start and stop at times that are multiples of the rounding_drift, to match provider file format.
    If a segment start or end time is not aligned, it will be adjusted to the next multiple of rounding_drift.
    For instance with a rounding_drift=timedelta(minutes=2), a segment starting at 10:01:30 will be adjusted to start at 10:02:00, and a segment ending at 10:03:45 will be adjusted to end at 10:04:00.
    """
    return [
        Segment(
            start_date=_ceil_to_multiple(segment.start_date, rounding_drift),
            end_date=_ceil_to_multiple(segment.end_date, rounding_drift),
            channel=segment.channel,
        )
        for segment in segments
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
