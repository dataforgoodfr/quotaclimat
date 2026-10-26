import asyncio
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from quotaclimat.data_ingestion.advertising.s01_detection.tools.program import (
    Show,
    get_channel_program,
)
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    get_s3_filesystem,
    list_channels,
    list_days_parts,
)
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

logger = logging.getLogger(__name__)

tz_paris = ZoneInfo("Europe/Paris")

# Mediatree sends 2-minutes files, aligned on even minutes
PART_DURATION = timedelta(minutes=2)

# Margins mediatree records around each show of the program
MARGIN_BEFORE = timedelta(
    minutes=int(os.environ.get("PROGRAM_MARGIN_BEFORE_MINUTES") or 15)
)
MARGIN_AFTER = timedelta(
    minutes=int(os.environ.get("PROGRAM_MARGIN_AFTER_MINUTES") or 15)
)

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

Part = tuple[datetime, datetime]  # UTC start and end of a 2-minutes file


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _ceil_to_multiple(dt: datetime, rounding_drift: timedelta) -> datetime:
    remainder = (dt - _EPOCH) % rounding_drift
    if remainder == timedelta():
        return dt
    return dt + (rounding_drift - remainder)


def _parts_between(start_date: datetime, end_date: datetime) -> list[Part]:
    """2-minutes parts mediatree sends for a requested interval.
    Mediatree drifts the asked interval to match its two minutes file format: both bounds
    are moved to the next multiple of 2 minutes (10:01:30 -> 10:02, 10:03:45 -> 10:04).
    """
    current = _ceil_to_multiple(start_date.astimezone(timezone.utc), PART_DURATION)
    end = _ceil_to_multiple(end_date.astimezone(timezone.utc), PART_DURATION)
    parts = []
    while current < end:
        parts.append((current, current + PART_DURATION))
        current += PART_DURATION
    return parts


def _merge_parts(parts: list[Part]) -> list[Part]:
    # Merge contiguous parts into windows, e.g. 10:00-10:02 and 10:02-10:04 into 10:00-10:04
    windows: list[Part] = []
    for start, end in sorted(parts):
        if windows and start <= windows[-1][1]:
            windows[-1] = (windows[-1][0], max(windows[-1][1], end))
        else:
            windows.append((start, end))
    return windows


def _minutes(parts_count: int) -> int:
    return parts_count * int(PART_DURATION.total_seconds()) // 60


def _format_duration(minutes: int) -> str:
    return f"{minutes // 60}h{minutes % 60:02d}"


def _format_range(values: list[int]) -> str:
    low, high = min(values), max(values)
    return f"{low} min" if low == high else f"{low} to {high} min"


def _format_days(days: set[int]) -> str:
    """{0, 1, 2, 3, 4} -> "mon-fri", {0, 2, 3} -> "mon, wed-thu", all days -> "every day"."""
    if days == set(range(7)):
        return "every day"
    runs: list[list[int]] = []
    for day in sorted(days):
        if runs and day == runs[-1][-1] + 1:
            runs[-1].append(day)
        else:
            runs.append([day])
    return ", ".join(
        WEEKDAYS[run[0]] if len(run) == 1 else f"{WEEKDAYS[run[0]]}-{WEEKDAYS[run[-1]]}"
        for run in runs
    )


def _hhmm(dt: datetime) -> str:
    return dt.astimezone(tz_paris).strftime("%H:%M")


@dataclass
class ShowCover:
    show: Show
    start_date: datetime  # Paris time, without margins
    end_date: datetime
    expected_parts: list[Part]  # With margins
    missing_parts: list[Part]

    @property
    def weekday(self) -> int:
        return self.start_date.weekday()

    @property
    def slot(self) -> str:
        return f"{_hhmm(self.start_date)}-{_hhmm(self.end_date)}"

    @property
    def missing_show_parts(self) -> list[Part]:
        # Missing parts overlapping the show itself, margins excluded
        return [
            (start, end)
            for start, end in self.missing_parts
            if start < self.end_date and end > self.start_date
        ]

    @property
    def show_parts_count(self) -> int:
        return sum(
            1
            for start, end in self.expected_parts
            if start < self.end_date and end > self.start_date
        )

    @property
    def status(self) -> str:
        if not self.missing_parts:
            return "complete"
        if len(self.missing_show_parts) == 0:
            return "margins"
        if len(self.missing_show_parts) == self.show_parts_count:
            return "absent"
        return "incomplete"


@dataclass
class CoverReport:
    channel: str
    week_start_date: datetime
    shows: list[ShowCover]
    expected_parts: list[Part]
    missing_parts: list[Part]
    unexpected_parts: list[Part]

    def count_shows(self, status: str) -> int:
        return sum(1 for show in self.shows if show.status == status)

    @property
    def missing_pct(self) -> float:
        if not self.expected_parts:
            return 0.0
        return 100 * len(self.missing_parts) / len(self.expected_parts)

    def _missing_slots_lines(self) -> list[str]:
        # Group the missing windows by time of day, so that a slot missing every day is written once
        days_by_slot: dict[tuple[str, str], set[int]] = defaultdict(set)
        for start, end in _merge_parts(self.missing_parts):
            days_by_slot[(_hhmm(start), _hhmm(end))].add(
                start.astimezone(tz_paris).weekday()
            )
        return [
            f"    {_format_days(days):<22} {start}-{end}"
            for (start, end), days in sorted(days_by_slot.items())
        ]

    def _shows_lines(self) -> list[str]:
        airing_days: dict[tuple[str, str], set[int]] = defaultdict(set)
        by_status: dict[tuple[str, str, str], list[ShowCover]] = defaultdict(list)
        for show in self.shows:
            key = (show.show.program_name or "(unnamed show)", show.slot)
            airing_days[key].add(show.weekday)
            if show.status != "complete":
                by_status[(show.status, *key)].append(show)

        status_order = {"absent": 0, "incomplete": 1, "margins": 2}
        lines = []
        for status, name, slot in sorted(
            by_status, key=lambda k: (status_order[k[0]], k[2], k[1])
        ):
            shows = by_status[(status, name, slot)]
            days = {show.weekday for show in shows}
            days_str = _format_days(days)
            if days == airing_days[(name, slot)] and len(days) > 1:
                days_str += " (every airing day)"
            margins_missing = [
                _minutes(len(s.missing_parts) - len(s.missing_show_parts))
                for s in shows
            ]
            if status == "absent":
                detail = "show entirely missing"
            elif status == "incomplete":
                detail = f"{_format_range([_minutes(len(s.missing_show_parts)) for s in shows])} of the show missing"
            else:
                detail = "show complete, only margins missing"
            if status == "incomplete" and max(margins_missing) > 0:
                detail += f", plus {_format_range(margins_missing)} of margins"
            elif status == "margins":
                detail += f" ({_format_range(margins_missing)})"
            lines.append(
                f"    {status.upper():<11} {name} {slot}, {days_str}: {detail}"
            )
        return lines

    def summary(self) -> str:
        week = self.week_start_date.date().isoformat()
        header = (
            f"{self.channel} - week of {week} (Paris time, margins "
            f"-{_minutes_of(MARGIN_BEFORE)}/+{_minutes_of(MARGIN_AFTER)} min)"
        )
        if not self.shows:
            return f"{header}\n  No show in program_metadata.json for this channel and week"

        lines = [
            header,
            f"  {len(self.expected_parts)} expected 2-min files "
            f"({_format_duration(_minutes(len(self.expected_parts)))}), "
            f"{len(self.missing_parts)} missing "
            f"({_format_duration(_minutes(len(self.missing_parts)))}, {self.missing_pct:.1f} %)",
        ]
        if not self.missing_parts:
            lines.append("  All expected files received")
        else:
            lines.append("  Shows not fully received:")
            lines += self._shows_lines()
            lines.append("  Missing time slots:")
            lines += self._missing_slots_lines()
        if self.unexpected_parts:
            lines.append(
                f"  {len(self.unexpected_parts)} received files outside the program "
                f"({_format_duration(_minutes(len(self.unexpected_parts)))})"
            )
        return "\n".join(lines)


def _minutes_of(delta: timedelta) -> int:
    return int(delta.total_seconds() // 60)


def _week_start_date(start_date: str) -> datetime:
    week_start_date = datetime.fromisoformat(start_date).replace(tzinfo=tz_paris)

    # Ensures week_start_date is a Monday
    if week_start_date.weekday() != 0:
        raise ValueError("start_date must be a Monday")

    if week_start_date > datetime.now(tz=tz_paris) - timedelta(days=7):
        raise ValueError("start_date must be at least 7 days in the past")

    return week_start_date


def compute_cover(
    segments: list[Segment],
    start_date: str,  # Start of the analyzed week, format iso 2026-12-31
    channel: str,
) -> CoverReport:
    week_start_date = _week_start_date(start_date)
    week_end_date = week_start_date + timedelta(days=7)

    actual = {
        (s.start_date.astimezone(timezone.utc), s.end_date.astimezone(timezone.utc))
        for s in segments
    }

    shows = []
    for show in get_channel_program(channel, week_start_date):
        start, end = show.for_week(week_start_date)
        expected = _parts_between(start - MARGIN_BEFORE, end + MARGIN_AFTER)
        shows.append(
            ShowCover(
                show=show,
                start_date=start,
                end_date=end,
                expected_parts=expected,
                missing_parts=[part for part in expected if part not in actual],
            )
        )
    shows.sort(key=lambda s: s.start_date)

    expected = {part for show in shows for part in show.expected_parts}
    # Files of the previous or next week downloaded with the same UTC days are not unexpected
    in_week = {
        part
        for part in actual
        if week_start_date <= part[0] and part[1] <= week_end_date
    }

    return CoverReport(
        channel=channel,
        week_start_date=week_start_date,
        shows=shows,
        expected_parts=sorted(expected),
        missing_parts=sorted(expected - actual),
        unexpected_parts=sorted(in_week - expected),
    )


def check_partition_cover(
    segments: list[Segment], start_date: str, channel: str
) -> None | str:
    report = compute_cover(segments=segments, start_date=start_date, channel=channel)

    if report.unexpected_parts:
        logger.info(
            f"{len(report.unexpected_parts)} downloaded segment(s) not expected by the program "
            f"for channel={channel} start_date={start_date}"
        )
    if report.missing_parts:
        summary = report.summary()
        logger.warning(
            f"{len(report.missing_parts)} expected segment(s) missing from downloaded segments:\n{summary}"
        )
        return summary


async def check_bucket_cover(start_date: str, channels: list[str] | None = None) -> str:
    """Check, for each channel, which files of the week are missing in mediatree's bucket.
    Only lists the bucket, nothing is downloaded. Without channels, checks every channel of the bucket.
    """
    week_start_date = _week_start_date(start_date)
    fs = get_s3_filesystem()
    if not channels:
        channels = await list_channels(fs)
        logger.info(f"Channels found in the bucket: {', '.join(channels)}")

    # Mediatree folders are UTC days: the week in Paris time with its margins spans 9 of them
    days = [week_start_date.date() + timedelta(days=i) for i in range(-1, 8)]

    reports: list[CoverReport] = []
    errors: dict[str, str] = {}
    for channel in channels:
        try:
            parts = await list_days_parts(fs, channel, days)
            report = compute_cover(
                segments=[
                    Segment(start_date=start, end_date=end, channel=channel)
                    for start, end in parts
                ],
                start_date=start_date,
                channel=channel,
            )
            logger.info(
                f"{channel}: {len(report.missing_parts)}/{len(report.expected_parts)} expected files missing"
            )
            reports.append(report)
        except Exception as e:
            logger.exception(f"Cover check failed for channel={channel}")
            errors[channel] = str(e)

    lines = [
        f"Mediatree bucket cover for the week of {start_date}",
        "",
        f"  {'channel':<20} {'expected':>9} {'missing':>8} {'%':>6} {'absent':>7} {'incompl.':>9} {'margins':>8}",
    ]
    for report in reports:
        if not report.shows:
            lines.append(f"  {report.channel:<20} no program for this week")
            continue
        lines.append(
            f"  {report.channel:<20} "
            f"{_format_duration(_minutes(len(report.expected_parts))):>9} "
            f"{_format_duration(_minutes(len(report.missing_parts))):>8} "
            f"{report.missing_pct:>5.1f}% "
            f"{report.count_shows('absent'):>7} "
            f"{report.count_shows('incomplete'):>9} "
            f"{report.count_shows('margins'):>8}"
        )
    for channel, error in errors.items():
        lines.append(f"  {channel:<20} ERROR {error}")
    lines.append(
        "  (absent / incompl. / margins: number of show airings entirely missing, "
        "partly missing, complete but with missing margins)"
    )

    for report in reports:
        if report.shows and report.missing_parts:
            lines += ["", report.summary()]

    return "\n".join(lines)


if __name__ == "__main__":
    print(asyncio.run(check_bucket_cover("2026-10-12", ["tf1"])))
