import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

tz_paris = ZoneInfo("Europe/Paris")

# Mediatree delivers a day about 24 hours after it ends, and the last show of a Sunday
# (plus its margin) can end on Monday morning: by default a week is processed from the
# Tuesday after it, 6 a.m. Paris time (Monday midnight + 30 hours).
DATA_DELAY = timedelta(hours=int(os.environ.get("MEDIATREE_DATA_DELAY_HOURS") or 30))


@dataclass
class ChannelStatus:
    channel: str
    # Naive UTC datetime of the last occurrence in the database, None without any occurrence
    last_occurrence: datetime | None
    # Monday of the next week to process, None when it cannot be known
    next_start_date: date | None


def parse_channels(channels_str: str | None) -> list[str]:
    """ "tf1, france2,,tf1" -> ["tf1", "france2"] (order kept, blanks and duplicates dropped)"""
    channels = []
    for channel in (channels_str or "").split(","):
        channel = channel.strip()
        if channel and channel not in channels:
            channels.append(channel)
    return channels


def next_monday_after(last_occurrence: datetime) -> date:
    """Monday (Paris) of the week following the week of this occurrence."""
    # occurrence_date is a naive UTC datetime (datetime.fromtimestamp in a UTC container).
    # The last occurrences of a week can fall on the Monday after it (a Sunday show
    # crossing midnight, plus its margin): going back 12 hours puts them in their week.
    last_occurrence_paris = last_occurrence.replace(tzinfo=timezone.utc).astimezone(
        tz_paris
    ) - timedelta(hours=12)
    last_monday = last_occurrence_paris.date() - timedelta(
        days=last_occurrence_paris.weekday()
    )
    return last_monday + timedelta(days=7)


def channel_statuses(
    channels: list[str],
    last_occurrences: dict[str, datetime],
    minimal_start_date: date | None,
) -> list[ChannelStatus]:
    """A channel without any occurrence starts from minimal_start_date."""
    statuses = []
    for channel in channels:
        last_occurrence = last_occurrences.get(channel)
        statuses.append(
            ChannelStatus(
                channel=channel,
                last_occurrence=last_occurrence,
                next_start_date=(
                    next_monday_after(last_occurrence)
                    if last_occurrence
                    else minimal_start_date
                ),
            )
        )
    return statuses


def plan_week(
    statuses: list[ChannelStatus], forced_start_date: date | None = None
) -> tuple[date, list[str]]:
    """The week to process and the channels to run on it, in the order of the list.

    The week is the oldest "next week to process" of the channels, and the channels are
    the ones that have not been processed on this week yet. With a forced start date,
    every channel runs on it.
    """
    if forced_start_date:
        return forced_start_date, [status.channel for status in statuses]

    missing = [status.channel for status in statuses if not status.next_start_date]
    assert not missing, (
        f"No occurrence in the database for {', '.join(missing)}: "
        "need START_DATE or MINIMAL_START_DATE"
    )
    week = min(status.next_start_date for status in statuses)
    return week, [
        status.channel for status in statuses if status.next_start_date == week
    ]


def week_available_from(week: date) -> datetime:
    """Paris time from which the whole week starting on Monday `week` is in mediatree's
    bucket: the end of the week (the Monday after, midnight) plus DATA_DELAY."""
    end_of_week = datetime.combine(week + timedelta(days=7), time(), tzinfo=tz_paris)
    return end_of_week + DATA_DELAY


def is_week_finished(week: date, now_paris: datetime) -> bool:
    """True when the week is over and its last day has been delivered by mediatree."""
    return now_paris >= week_available_from(week)


def format_summary(
    statuses: list[ChannelStatus], week: date, channels_to_run: list[str]
) -> str:
    width = max(len(status.channel) for status in statuses)
    lines = ["State of the channels (last occurrence in UTC, next week to process):"]
    for status in statuses:
        last = (
            status.last_occurrence.strftime("%Y-%m-%d %H:%M")
            if status.last_occurrence
            else "none"
        )
        next_week = (
            status.next_start_date.isoformat() if status.next_start_date else "unknown"
        )
        run = "  <- to run" if status.channel in channels_to_run else ""
        lines.append(
            f"  {status.channel:<{width}}  last occurrence: {last:<16}  next week: {next_week}{run}"
        )
    lines.append(
        f"Week to process: {week.isoformat()} for {len(channels_to_run)} channel(s): "
        f"{', '.join(channels_to_run)}"
    )
    return "\n".join(lines)
