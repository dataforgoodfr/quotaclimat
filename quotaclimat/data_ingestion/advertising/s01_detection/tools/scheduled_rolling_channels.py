import logging
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)

# Expected cron format: "*/INTERVAL HOUR_START-HOUR_END * * WEEKDAY"
# or "MINUTE HOUR_START-HOUR_END * * WEEKDAY" (WEEKDAY can be *)
CRON_PATTERN = re.compile(r"^(?:\*/(\d+)|(\d+))\s+(\d+)-(\d+)\s+\*\s+\*\s+(\d+|\*)$")


def get_scheduled_rolling_channel():
    # These var envs allow the job to run alternatively on all the channels:
    # - SCHEDULED_ROLLING_CHANNELS: the list of channels separated by a ","
    # - SCHEDULED_ROLLING_CHANNELS_CRON: the cron expression(s) placed on the job, separated by a ";"
    #   when the job has several schedules (e.g. "15 8-19 * * *;15 20-23 * * *")
    # - SCHEDULED_ROLLING_CHANNELS_TIMEZONE: the time zone of the job's cron schedules (default Europe/Paris)
    #
    # Behavior:
    # - If the CRON starts with a "-", this feature is desactivated
    # - Each CRON should have one of these exact shapes (the full cron syntax is not implemented):
    #   "*/30 18-23 * * 1": every 30 minutes from 18:00 to 23:30, on Mondays
    #   "55 7-18 * * *": every hour at minute 55 from 7:55 to 18:55, every day
    #   (the weekday can be a number or "*")
    # - The cron hours are read in TIMEZONE, not in the container's time zone (UTC)
    # - When the process is running, it looks for the cron that triggered it, and counts the number of its runs
    #   already done this day: the n-th run of each cron runs the n-th channel (each cron goes through the whole list)
    # - If the list is too short, this feature is desactivated

    channels_str = os.environ.get("SCHEDULED_ROLLING_CHANNELS")
    crons_str = os.environ.get("SCHEDULED_ROLLING_CHANNELS_CRON")
    if not channels_str or not crons_str or crons_str.strip().startswith("-"):
        return

    if ":" in channels_str:
        logger.warning(
            "SCHEDULED_ROLLING_CHANNELS should only list the channels, the cron is in "
            f"SCHEDULED_ROLLING_CHANNELS_CRON: {channels_str}"
        )
        return

    channels = [c.strip() for c in channels_str.split(",")]

    timezone_name = (
        os.environ.get("SCHEDULED_ROLLING_CHANNELS_TIMEZONE") or "Europe/Paris"
    )
    try:
        tz = ZoneInfo(timezone_name.strip())
    except (ZoneInfoNotFoundError, ValueError):
        logger.warning(
            f"SCHEDULED_ROLLING_CHANNELS_TIMEZONE is an unknown time zone: {timezone_name}"
        )
        return

    now = datetime.now(tz=tz)

    for cron in crons_str.split(";"):
        if not cron.strip():
            continue
        channel = _channel_of_cron(cron, channels, now)
        if channel:
            return channel


def _channel_of_cron(cron, channels, now):
    match = CRON_PATTERN.match(cron.strip())
    if not match:
        logger.warning(
            f"SCHEDULED_ROLLING_CHANNELS_CRON: the cron did not match the expected shapes: {cron}"
        )
        return

    if match.group(1) is not None:
        interval, first_minute = int(match.group(1)), 0
    else:
        interval, first_minute = 60, int(match.group(2))
    hour_start = int(match.group(3))
    hour_end = int(match.group(4))
    weekday = match.group(
        5
    )  # cron: 0=Sunday … 6=Saturday, 7=Sunday; iso: 1=Monday … 7=Sunday

    # % 7 maps both cron 0 and cron 7 to 0 (Sunday), and isoweekday() 7 to 0 as well
    if weekday != "*" and now.isoweekday() % 7 != int(weekday) % 7:
        return

    minutes_since_start = (
        now.hour * 60 + now.minute - (hour_start * 60 + first_minute) + 1
    )  # Plus one in case the cron starts just before the official time (already observed)
    if minutes_since_start < 0 or now.hour > hour_end:
        return
    iteration = minutes_since_start // interval

    if iteration >= len(channels):
        return

    return channels[iteration]
