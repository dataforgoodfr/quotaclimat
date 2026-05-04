import logging
import os
import re
from datetime import datetime

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
    # - Currently the CRON should be in the exact format of this example "*/30 18-23 * * 1"
    # (numbers can be different but not their places and number, because we did not implement full behavior of the cron)
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
    match = re.match(r"^\*/(\d+)\s+(\d+)-(\d+)\s+\*\s+\*\s+(\d+)$", cron.strip())
    if not match:
        logger.warning(
            f"It looks like a SCHEDULED_ROLLING_CHANNELS but the cron part did not match: {cron}"
        )
        return

    interval = int(match.group(1))
    hour_start = int(match.group(2))
    hour_end = int(match.group(3))
    weekday = int(match.group(4))  # cron: 0=Sunday … 6=Saturday, 7=Sunday; iso: 1=Monday … 7=Sunday

    now = datetime.now()

    # % 7 maps both cron 0 and cron 7 to 0 (Sunday), and isoweekday() 7 to 0 as well
    if now.isoweekday() % 7 != weekday % 7:
        return

    if not (hour_start <= now.hour <= hour_end):
        return

    minutes_since_start = (
        (now.hour - hour_start) * 60 + now.minute + 1
    )  # Plus one in case the cron starts just before the official time (already observed)
    iteration = minutes_since_start // interval

    if iteration >= len(channels):
        return

    return channels[iteration]
