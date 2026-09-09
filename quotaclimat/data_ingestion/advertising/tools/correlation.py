import numpy as np
from scipy import signal


def find_best_correlation(haystack: np.ndarray, needle: np.ndarray) -> tuple[int, float]:
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
