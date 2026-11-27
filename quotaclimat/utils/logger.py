import logging
import os
import sys

# Text put at the start of every log line (e.g. the channel being processed), see set_log_prefix
_prefix = ""


def set_log_prefix(prefix: str | None):
    """Start every log line with `prefix` (e.g. the channel), or with nothing if None/"".

    Worker processes forked afterwards (ProcessPoolExecutor on Linux) keep the prefix.
    """
    global _prefix
    _prefix = f"{prefix} | " if prefix else ""


def _env_flag(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.lower() in ("1", "true", "yes")


class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    light_blue = "\x1b[36m"
    # No time by default: Scaleway Cockpit and Kestra already show it in front of each line.
    # The level stays: Cockpit reads it from the line (a line without it shows as UNK).
    # LOG_TIME=true puts the time back (to run a script locally).
    LOG_FORMAT = "%(levelname)s - %(filename)s:%(lineno)d | %(message)s"
    LOG_FORMAT_WITH_TIME = "%(asctime)s - " + LOG_FORMAT

    COLORS = {
        logging.DEBUG: grey,
        logging.INFO: light_blue,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: bold_red,
    }

    def __init__(self, with_time: bool | None = None, with_color: bool | None = None):
        super().__init__()
        if with_time is None:
            with_time = _env_flag("LOG_TIME", False)
        if with_color is None:
            # Colors only in a terminal by default. In Cockpit (Grafana), the level of a line is the
            # first level word found between word boundaries: "\x1b[36mINFO" hides INFO behind the
            # color code ("m" and "I" are both word characters), so a line showed as UNK, or as ERROR
            # when its message contained "error" (e.g. "error=23" in a progress line).
            with_color = _env_flag("LOG_COLOR", sys.stdout.isatty())
        log_fmt = self.LOG_FORMAT_WITH_TIME if with_time else self.LOG_FORMAT
        self.formatters = {
            level: logging.Formatter(
                color + "%(log_prefix)s" + log_fmt + self.reset
                if with_color
                else "%(log_prefix)s" + log_fmt
            )
            for level, color in self.COLORS.items()
        }

    def format(self, record):
        record.log_prefix = _prefix
        formatter = self.formatters.get(record.levelno, self.formatters[logging.INFO])
        return formatter.format(record)


def getLogger(prefix: str | None = None):
    # create logger with 'spam_application'
    logger = logging.getLogger()
    logger.setLevel(level=os.getenv('LOGLEVEL', 'INFO').upper())
    # create console handler with a higher log level
    if (logger.hasHandlers()):
        logger.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)
    if prefix is not None:
        set_log_prefix(prefix)

    return logger
