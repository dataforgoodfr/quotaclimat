import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from quotaclimat.data_ingestion.advertising.s01_detection.tools.program import (
    Show,
    extend_program_by,
    get_channel_program,
)
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

logger = logging.getLogger(__name__)

tz_paris = ZoneInfo("Europe/Paris")


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


def _merge_segments(segments: list[Segment]) -> list[Segment]:
    # This function merge consecutive shows in the same channel into a single show.
    # For example, if there are two shows on TF1 on Monday from 20:00 to 21:00 and from 21:00 to 22:00, they would be merged into a single show from 20:00 to 22:00.
    if not segments:
        return []

    # Sort shows by start time
    segments.sort(key=lambda segment: segment.start_date)
    merged_segments = [segments[0]]
    for segment in segments[1:]:
        last_segment = merged_segments[-1]
        if (
            segment.channel == last_segment.channel
            and segment.start_date <= last_segment.end_date
        ):
            # Merge segments by extending the end time of the last segment
            merged_segments[-1] = Segment(
                channel=last_segment.channel,
                start_date=last_segment.start_date,
                end_date=max(last_segment.end_date, segment.end_date),
            )
        else:
            merged_segments.append(segment)

    return merged_segments


def _partition_week(
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


def _partition_week_program(
    start_date: str,  # Start of the analyzed week, format iso 2026-12-31
    channel: str,
    margin: timedelta,
    segment_size: timedelta = timedelta(minutes=10),
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
            program, week_start_date, segment_size
        )
    ]


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ceil_to_multiple(dt: datetime, rounding_drift: timedelta) -> datetime:
    remainder = (dt - _EPOCH) % rounding_drift
    if remainder == timedelta():
        return dt
    return dt + (rounding_drift - remainder)


def _add_rounding_drift(
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


def check_partition_cover(
    segments: list[Segment], start_date: str, channel: str
) -> None | str:
    expected_segments = _partition_week_program(
        channel=channel,
        start_date=start_date,
        margin=timedelta(minutes=15),
        segment_size=timedelta(minutes=2),
    )
    # This is specific to the mediatree sent files into our bucket: they drift asked interval in order to match their two minutes file format.
    expected_segments = _add_rounding_drift(
        expected_segments, rounding_drift=timedelta(minutes=2)
    )

    expected_bounds = {*expected_segments}
    actual_bounds = {*segments}

    missing = sorted(expected_bounds - actual_bounds)
    unexpected = sorted(actual_bounds - expected_bounds)

    if unexpected:
        unexpected_windows = _merge_segments(unexpected)
        unexpected_windows_str = "\n".join(
            [
                f"Unexpected segment on weekday={s.start_date.weekday()} from {s.start_date.astimezone(tz_paris).strftime('%H:%M')} to {s.end_date.astimezone(tz_paris).strftime('%H:%M')}"
                for s in unexpected_windows
            ]
        )
        logger.info(
            f"{len(unexpected)} downloaded segment(s) not expected by the program "
            f"for channel={channel} start_date={start_date}: {unexpected_windows_str}"
        )
    if missing:
        missing_windows = _merge_segments(missing)
        missing_windows_str = "\n".join(
            [
                f"Missing segment on weekday={s.start_date.weekday()} from {s.start_date.astimezone(tz_paris).strftime('%H:%M')} to {s.end_date.astimezone(tz_paris).strftime('%H:%M')}"
                for s in missing_windows
            ]
        )
        logger.warning(
            f"{len(missing)} expected segment(s) missing from downloaded segments "
            f"for channel={channel} start_date={start_date}: {missing_windows_str}"
        )

        return missing_windows_str


if __name__ == "__main__":
    channel = "tf1"
    start_date = "2025-05-05"
    margin = timedelta(minutes=30)

    print(
        _partition_week_program(
            channel=channel,
            start_date=start_date,
            margin=margin,
        )
    )
