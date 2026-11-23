"""Merging the text read on each frame of an ad into one transcript."""

import unicodedata
from difflib import SequenceMatcher

from thefuzz import fuzz

from quotaclimat.data_ingestion.advertising.tools.normalize import (
    normalize,
)

MIN_BLOCK = 10  # characters two windows onto a scrolling line must share
MAX_EDGE = 35  # how far from the edges of the two readings that shared block may sit
SAME_RATIO = 90  # fuzzy similarity above which two lines read the same


def transcript(frame_texts: list[str]) -> str:
    """One transcript from each frame's text, in time order, in the order lines appeared."""
    lines = [
        " ".join(line.split())
        for text in frame_texts
        for line in text.splitlines()
        if line.strip()
    ]
    return "\n".join(dedupe(merge_windows(lines)))


def merge_windows(lines: list[str]) -> list[str]:
    """Chain readings that are windows onto one scrolling line.

    Each second shows a different slice of a scrolling sentence, garbled at both edges
    where the text is cut off, so neither containment nor prefix/suffix matching joins
    them. What they share is a long middle block, which has to sit near the end of the
    chain and near the start of the new reading: that is what a window sliding forward
    looks like, and it keeps unrelated lines apart.
    """
    chains: list[str] = []
    for line in lines:
        best = None  # (chain index, matching block)
        for i, chain in enumerate(chains):
            m = SequenceMatcher(
                None, chain.lower(), line.lower(), autojunk=False
            ).find_longest_match(0, len(chain), 0, len(line))
            tail = len(chain) - (m.a + m.size)  # chain text past the block
            added = len(line) - (m.b + m.size)  # text the reading brings in
            window = m.size >= MIN_BLOCK and tail <= MAX_EDGE and m.b <= MAX_EDGE
            # The reading must move the line forward or repeat it whole, or two lines
            # merely sharing an ending would lose the second one's start.
            moves = added > 0 or m.size == len(line)
            if window and moves and (best is None or m.size > best[1].size):
                best = (i, m)
        if best is None:
            chains.append(line)
        else:
            i, m = best
            chains[i] = chains[i][: m.a] + line[m.b :]
    return chains


def dedupe(lines: list[str]) -> list[str]:
    """Drop lines that read the same as an earlier one, keeping the longer reading.

    The same slogan comes back in several shots; a partial or slightly garbled reading
    counts as the same.
    """
    kept: list[str] = []
    for line in lines:
        norm = clean_text(line)
        if not norm:
            continue
        for i, other in enumerate(kept):
            other_norm = clean_text(other)
            same = fuzz.ratio(norm, other_norm) >= SAME_RATIO
            contained = (
                min(len(norm), len(other_norm)) >= 8
                and fuzz.partial_ratio(norm, other_norm) >= SAME_RATIO
            )
            if same or contained:
                if len(norm) > len(other_norm):
                    kept[i] = line
                break
        else:
            kept.append(line)
    return kept


def clean_text(text: str) -> str:
    # NFKD before normalising, as OCR may return compatibility forms (ﬁ, full-width letters and punctuation).
    text = normalize(unicodedata.normalize("NFKD", text), keep_punctuation=True)
    return text.strip(" .,;:!?-'\"«»()[]")
