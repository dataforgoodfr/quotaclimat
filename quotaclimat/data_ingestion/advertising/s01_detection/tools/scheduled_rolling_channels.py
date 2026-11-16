import logging
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def get_scheduled_rolling_channel():
    # This var env allows the job to run alternatively on all the channels.
    # This var should be in the format:
    # CRON:CHANNELS
    # where CRON is the value of the cron expression placed on the job
    # and CHANNELS is the list of channels separated by a ","
    #
    # Behavior:
    # - If the CRON starts with a "-", this feature is desactivated
    # - The CRON should have one of these exact shapes (the full cron syntax is not implemented):
    #   "*/30 18-23 * * 1": every 30 minutes from 18:00 to 23:30, on Mondays
    #   "55 7-18 * * *": every hour at minute 55 from 7:55 to 18:55, every day
    #   (the weekday can be a number or "*")
    # - The cron hours are read in Europe/Paris time (the time zone of the job's cron schedule),
    #   not in the container's time zone (UTC). SCHEDULED_ROLLING_CHANNELS_TIMEZONE can change it.
    # - When the process is running, it tries to find if it was triggered by the cron, and count the number of iterations already happens this day
    # - This number is used in the CHANNELS list to pick the channel. If the list is to short, this feature is desactivated

    src = os.environ.get("SCHEDULED_ROLLING_CHANNELS")
    if src is None:
        return

    cron, channels_str = src.split(":")
    if not cron or not channels_str or cron.startswith("-"):
        return

    channels = [c.strip() for c in channels_str.split(",")]

    # Expected cron format: "*/INTERVAL HOUR_START-HOUR_END * * WEEKDAY"
    # or "MINUTE HOUR_START-HOUR_END * * WEEKDAY" (WEEKDAY can be *)
    match = re.match(
        r"^(?:\*/(\d+)|(\d+))\s+(\d+)-(\d+)\s+\*\s+\*\s+(\d+|\*)$", cron.strip()
    )
    if not match:
        logger.warning(
            f"It looks like a SCHEDULED_ROLLING_CHANNELS but the cron part did not match: {cron}"
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

    now = datetime.now(
        tz=ZoneInfo(os.environ.get("SCHEDULED_ROLLING_CHANNELS_TIMEZONE", "Europe/Paris"))
    )

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
