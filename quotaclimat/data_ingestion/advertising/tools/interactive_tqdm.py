import logging
import os
import sys

from tqdm import tqdm

logger = logging.getLogger(__name__)

# When running in a production stack where logs are collected, tqdm's cursor
# movement codes (\r, ANSI escapes) make all updates appear on a single line.
# Set TQDM_LOG_MODE=1 to replace progress bars with plain logger.info() lines.
_LOG_MODE = os.environ.get("TQDM_LOG_MODE", "0") == "1" or not sys.stdout.isatty()

_LOG_INTERVAL = 50


class _InteractiveTqdm(tqdm):
    """A tqdm progress bar that falls back to periodic logger.info() lines when stdout
    isn't a tty (or TQDM_LOG_MODE=1), instead of a disabled, silent bar.

    Same call signature as tqdm; tqdm doesn't track progress internally while disabled,
    so this keeps its own counter to log every `_LOG_INTERVAL` updates.
    """

    def __init__(self, *args, **kwargs):
        # tqdm's __init__ returns early when disabled, without setting self.desc/self.total,
        # so those are captured here rather than read back off self later. Mirrors tqdm's own
        # fallback of inferring total from a sized iterable when not given explicitly.
        self._log_desc = kwargs.get("desc") or "Progress"
        total = kwargs.get("total")
        if total is None and args and hasattr(args[0], "__len__"):
            total = len(args[0])
        self._log_total = total
        self._completed = 0

        kwargs.setdefault("disable", _LOG_MODE)
        super().__init__(*args, **kwargs)

    def update(self, n=1):
        result = super().update(n)
        if _LOG_MODE:
            self._completed += n
            if self._completed % _LOG_INTERVAL == 0 or self._completed == self._log_total:
                logger.info(
                    "%s: %d/%s", self._log_desc, self._completed, self._log_total or "?"
                )
        return result

    def __iter__(self):
        # tqdm's own __iter__ has a fast path that skips calling self.update() entirely
        # while disabled (for performance), which would silently skip our log-mode
        # fallback for `for x in interactive_tqdm(iterable)` usage. Only take that
        # detour while disabled; an enabled (interactive) bar keeps tqdm's normal,
        # throttled __iter__.
        if self.disable:
            for obj in self.iterable:
                yield obj
                self.update(1)
            return
        yield from super().__iter__()


interactive_tqdm = _InteractiveTqdm
