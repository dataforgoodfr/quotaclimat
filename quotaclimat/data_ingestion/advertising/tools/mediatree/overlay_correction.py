import asyncio
import logging
from datetime import timedelta

import numpy as np
from scipy import signal

logger = logging.getLogger(__name__)

# Each part is encoded independently and carries a leading LAME encoder-delay/priming
# frame (~1 audio frame, empirically 0.024s) before its real content starts. Naively
# concatenating parts leaves that frame in at every internal boundary, producing a
# small audible glitch. Skipping it at the start of every part but the first gives
# gapless concatenation.
ENCODER_DELAY = timedelta(seconds=0.024)

# Consecutive parts sometimes (not consistently) share close to a second of duplicate
# audio at their boundary, presumably from how mediatree exports each 2-minutes window.
# We detect it by cross-correlating a short window near the join rather than assuming a
# fixed size, since it varies per boundary and is sometimes absent entirely.
SAMPLE_RATE = 48_000
_OVERLAP_SEARCH_WINDOW = timedelta(seconds=3)
_OVERLAP_MATCH_WINDOW_SAMPLES = 8_000
_OVERLAP_CORRELATION_THRESHOLD = 0.6


async def _decode_pcm(
    path: str,
    *,
    seek: timedelta | None = None,
    seek_from_end: timedelta | None = None,
    duration: timedelta,
) -> np.ndarray:
    """Decode `duration` of mono PCM audio from `path` into a numpy array of samples."""
    seek_args = (
        ["-sseof", f"-{seek_from_end.total_seconds()}"]
        if seek_from_end is not None
        else ["-ss", str(seek.total_seconds())]
        if seek is not None
        else []
    )
    process = await asyncio.create_subprocess_exec(
        "ffmpeg",
        "-v",
        "error",
        *seek_args,
        "-i",
        path,
        "-t",
        str(duration.total_seconds()),
        "-ac",
        "1",
        "-ar",
        str(SAMPLE_RATE),
        "-f",
        "s16le",
        "-",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"ffmpeg failed to decode {path}: {stderr.decode(errors='replace')}"
        )
    return np.frombuffer(stdout, dtype="<i2").astype(np.float64)


def _find_best_correlation(
    haystack: np.ndarray, needle: np.ndarray
) -> tuple[int, float]:
    """Return the (start_index, correlation) of the best match of `needle` within `haystack`,
    using normalized cross-correlation so the result is comparable across audio segments.
    """
    needle = needle - needle.mean()
    haystack = haystack - haystack.mean()
    needle_norm = np.linalg.norm(needle) + 1e-9

    numerator = signal.correlate(haystack, needle, mode="valid")

    cumulative_energy = np.concatenate(([0.0], np.cumsum(haystack**2)))
    window_energy = cumulative_energy[len(needle) :] - cumulative_energy[: -len(needle)]
    window_norm = np.sqrt(window_energy) + 1e-9

    correlation = numerator / (needle_norm * window_norm)
    best_start = int(np.argmax(correlation))
    return best_start, float(correlation[best_start])


async def detect_overlap(prev_path: str, next_path: str) -> timedelta:
    """Detect how much of `next_path`'s start duplicates `prev_path`'s end, by decoding a
    short window on each side of the boundary and cross-correlating them. Returns zero
    when no confident match is found, so real content near a non-overlapping boundary is
    never trimmed.
    """
    tail, head = await asyncio.gather(
        _decode_pcm(
            prev_path,
            seek_from_end=_OVERLAP_SEARCH_WINDOW,
            duration=_OVERLAP_SEARCH_WINDOW,
        ),
        _decode_pcm(next_path, seek=ENCODER_DELAY, duration=_OVERLAP_SEARCH_WINDOW),
    )

    needle = head[:_OVERLAP_MATCH_WINDOW_SAMPLES]
    if len(needle) < _OVERLAP_MATCH_WINDOW_SAMPLES or len(tail) <= len(needle):
        return timedelta(0)

    start, correlation = await asyncio.to_thread(_find_best_correlation, tail, needle)
    if correlation < _OVERLAP_CORRELATION_THRESHOLD:
        return timedelta(0)

    overlap = timedelta(seconds=(len(tail) - start) / SAMPLE_RATE)
    logger.debug(
        "Detected %.3fs overlap between %s and %s (correlation=%.2f)",
        overlap.total_seconds(),
        prev_path,
        next_path,
        correlation,
    )
    return overlap
