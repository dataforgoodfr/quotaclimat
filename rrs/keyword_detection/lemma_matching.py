"""
Lemma-based keyword matching on raw transcript text.

Same notion of a match as rrs.keyword_detection.filter_keywords: a keyword
and a span of text match when they share the same set of lemmas (order-
independent, spaCy lemmatizer of the main detection pipeline), so
"pesticides" matches "pesticide" and "sécurité" no longer matches "sécu".
Whole lemmas only: a keyword can never match inside another word.
"""

from functools import lru_cache
from typing import Iterable

from quotaclimat.data_processing.mediatree.detect_keywords import (
    get_lemmas,
    get_pipeline,
)
from rrs.keyword_detection.filter_keywords import DEFAULT_LANG


@lru_cache(maxsize=None)
def _keyword_lemmas(keyword: str) -> tuple:
    return tuple(get_lemmas(keyword.lower(), DEFAULT_LANG))


def lemmatize_texts(texts: Iterable[str]) -> list[list[str]]:
    """Lemmas of each text, in order, punctuation dropped (as get_lemmas does)."""
    nlp = get_pipeline(DEFAULT_LANG)
    docs = nlp.pipe((t.lower() if t else "" for t in texts), batch_size=256)
    return [[tok.lemma_.lower() for tok in doc if not tok.is_punct] for doc in docs]


class LemmaMatcher:
    """Finds dictionary keywords in a lemmatized text.

    A keyword of n lemmas matches any window of n consecutive text lemmas with
    the same set of lemmas. Longest keywords are tried first and matches do not
    overlap, like the regex alternation this replaces. Returns one entry per
    occurrence, using the dictionary spelling of the keyword.
    """

    def __init__(self, keywords: Iterable[str]):
        self._index: dict[tuple, str] = {}
        for kw in keywords:
            lemmas = _keyword_lemmas(kw)
            if lemmas:
                self._index.setdefault((len(lemmas), frozenset(lemmas)), kw)
        self._sizes = sorted({n for n, _ in self._index}, reverse=True)

    def find(self, lemmas: list[str]) -> list[str]:
        found = []
        i = 0
        while i < len(lemmas):
            for n in self._sizes:
                if i + n > len(lemmas):
                    continue
                kw = self._index.get((n, frozenset(lemmas[i : i + n])))
                if kw is not None:
                    found.append(kw)
                    i += n
                    break
            else:
                i += 1
        return found
