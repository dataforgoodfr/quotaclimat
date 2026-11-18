"""One spelling per brand, for the brands e1_classify_ad read that the dictionary didn't already spell.

Brands are compared as keys ("Coca-Cola" -> "cocacola"). For each key, in order:
  1. the dictionary's spelling, when the key is close enough to a dictionary brand, or
     when that brand is on screen (OCR) and the key is recognisably it;
  2. otherwise the spelling of a brand already seen with a close key and a shared
     sector, so that Orange the telecom company never merges with an orange juice;
  3. reviewers' verdicts from the shared sheet force pairs together (merge) or apart
     (split), see ``brand_overrides``.

Each run writes three audits for review: the merges (those a verdict could undo), the
near-misses (pairs kept apart that end up with different spellings), and the brands close
to a dictionary entry that weren't merged (likely dictionary gaps).

RECANONICALISE=1 redoes every brand not spelled by the dictionary, so that new verdicts
and threshold changes apply to past ads too. SHEET_REVIEW=0 runs without the sheet.

Run with ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.e5_canonicalise_brand``.
"""

import bisect
import logging
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from sqlalchemy import bindparam, func, select, update
from thefuzz import fuzz

from postgres.schemas.advertising.models import Ad
from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    brand_overrides,
    dictionary,
)
from quotaclimat.data_ingestion.advertising.s03_classification.utils import (
    database,
    stage_version_stamp,
)
from quotaclimat.data_ingestion.advertising.tools.brand_dictionary import (
    brand_key,
)
from quotaclimat.data_ingestion.advertising.tools.normalize import (
    normalize,
)
from quotaclimat.utils.logger import getLogger

STAGE = "canonicalise_brand"
AUDIT_DIR = Path("data")
MERGE_COLUMNS = [
    "brand_canonical",
    "brand_variant",
    "pass",
    "score",
    "sectors",
    "variant_n_ads",
    "canonical_n_ads",
]
NEAR_MISS_COLUMNS = [
    "variant",
    "near_canonical",
    "score",
    "threshold",
    "reason",
    "variant_sectors",
    "near_canonical_sectors",
    "variant_n_ads",
    "near_canonical_n_ads",
]
DICT_GAP_COLUMNS = [
    "brand_variant",
    "dict_candidate",
    "score",
    "verdict",
    "variant_n_ads",
]

# Between brands: a short name needs a closer match, as one letter matters more.
FUZZY_THRESHOLD = 88
SHORT_THRESHOLD = 92
SHORT_LEN = 5
NEAR_MISS_MARGIN = 8

DICT_THRESHOLD = 92
DICT_MAX_LENGTH_DIFF = 2

# The screen settles it: when a dictionary brand is on screen and the guess is recognisably that name, they agree.
# Low score because only few brands appear on screen ('Marc Repaire' is 70 against 'Marque Repère').
DICT_OCR_THRESHOLD = 70
DICT_OCR_MAX_LENGTH_DIFF = 4
# A short name on screen ('Tena') is easy to land near by accident ('technal'), so it
# needs this stricter score; it is also the floor for listing dictionary gaps.
DICT_NEAR_MIN = 86

LEADING_NOISE = {"le", "la", "les", "l", "de", "du", "des", "st", "saint"}

# Dictionary outcomes for which e3_dict_match already wrote the dictionary's spelling.
BRAND_FROM_DICT = ("dict_tier1", "dict_tier2", "dict_tier2_no_kw", "dict_tier3")

# The dictionary's spelling of each brand key, sorted so that ties always go the same way.
DICT_SPELLINGS = dict(
    sorted(
        {
            key: rules[0].brand
            for tier in (dictionary.TIER3, dictionary.TIER2, dictionary.TIER1)
            for key, rules in tier.items()
        }.items()
    )
)


def select_ads(recanonicalise: bool) -> list[dict]:
    stmt = (
        select(Ad.id, Ad.prediction, Ad.predicted_sector, Ad.ocr_transcript)
        .where(Ad.prediction_status.is_not(None))
        .where(func.json_typeof(Ad.prediction) == "array")
        .where(database.IN_POOL)
        .order_by(Ad.id)
    )
    if not recanonicalise:
        stmt = stmt.where(Ad.predicted_brand.is_(None))
    ads = []
    with database.engine.connect() as conn:
        for row in conn.execute(stmt):
            dict_method = stage_version_stamp.find_entry(
                row.prediction, "dict_match"
            ).get("method")
            if dict_method in BRAND_FROM_DICT:
                continue  # the dictionary's spelling always stands
            ad_type = stage_version_stamp.find_response(row.prediction, "ad_type")
            brand = normalize(ad_type.get("brand_name"))
            if brand:
                ads.append(
                    {
                        "id": row.id,
                        "brand": brand,
                        "sector": row.predicted_sector,
                        "ocr_transcript": row.ocr_transcript,
                    }
                )
    return ads


def ocr_spellings(text: str) -> set[str]:
    words = re.findall(r"\w+", text)
    return {
        brand_key(" ".join(words[i : i + n]))
        for n in (1, 2, 3)
        for i in range(len(words) - n + 1)
    }


def _nearest_dict(key: str) -> tuple[str, int] | None:
    if key in DICT_SPELLINGS:
        return DICT_SPELLINGS[key], 100
    close = [
        k for k in DICT_SPELLINGS if abs(len(key) - len(k)) <= DICT_MAX_LENGTH_DIFF
    ]
    if not close:
        return None
    best = max(close, key=lambda k: (fuzz.ratio(key, k), -abs(len(key) - len(k))))
    return DICT_SPELLINGS[best], fuzz.ratio(key, best)


def _dict_spelling(
    key: str,
    nearest: tuple[str, int] | None,
    on_screen: set[str],
    blocklist: set[frozenset[str]],
) -> tuple[str, int, str] | None:
    if nearest and nearest[1] >= DICT_THRESHOLD:
        return nearest[0], nearest[1], "dict_override"
    candidates = [
        (fuzz.ratio(key, k), k)
        for k in on_screen & DICT_SPELLINGS.keys()
        if abs(len(key) - len(k)) <= DICT_OCR_MAX_LENGTH_DIFF
        and frozenset((key, k)) not in blocklist
    ]
    accepted = [
        (score, k)
        for score, k in candidates
        if score >= (DICT_NEAR_MIN if len(k) <= SHORT_LEN else DICT_OCR_THRESHOLD)
    ]
    if not accepted:
        return None
    score, k = max(accepted)
    return DICT_SPELLINGS[k], score, "dict_ocr"


def _readable(spellings: list[str], n_ads: Counter) -> str:
    """Among spacing variants of a brand: the most frequent, spaced, without article."""
    return max(
        spellings,
        key=lambda s: (
            n_ads[s],
            " " in s,
            not any(s.startswith(word + " ") for word in LEADING_NOISE),
        ),
    )


def _cluster(
    order: list[str],
    sectors: dict[str, set[str]],
    key_n_ads: dict[str, int],
    blocklist: set[frozenset[str]],
    reviewed: set[frozenset[str]],
) -> tuple[dict[str, str], list[tuple]]:
    """The key each key merges into (itself when none), and the near-misses.

    A key merges into the closest key already kept that shares a sector with it, or is
    kept itself. Near-misses are those just below the threshold, or blocked only because
    the sectors differ, which reviewers haven't judged yet.
    """
    canonical: dict[str, str] = {}
    kept: list[str] = []  # sorted, so that equal candidates always go the same way
    near_misses = []
    for key in order:
        threshold = SHORT_THRESHOLD if len(key) <= SHORT_LEN else FUZZY_THRESHOLD
        scored = [
            (fuzz.ratio(key, other), other, bool(sectors[key] & sectors[other]))
            for other in kept
        ]
        eligible = [
            (score, other)
            for score, other, shared in scored
            if score >= threshold
            and shared
            and frozenset((key, other)) not in blocklist
        ]
        if eligible:
            # The closest; ties go to the brand with more ads, then the first sorted.
            canonical[key] = max(eligible, key=lambda e: (e[0], key_n_ads[e[1]]))[1]
            continue
        canonical[key] = key
        bisect.insort(kept, key)

        same = max(((s, o) for s, o, shared in scored if shared), default=None)
        other = max(((s, o) for s, o, shared in scored if not shared), default=None)
        if (
            same
            and threshold - NEAR_MISS_MARGIN <= same[0] < threshold
            and frozenset((key, same[1])) not in reviewed
        ):
            near_misses.append((key, same[1], same[0], threshold, "below_threshold"))
        if (
            other
            and other[0] >= threshold
            and frozenset((key, other[1])) not in reviewed
        ):
            near_misses.append((key, other[1], other[0], threshold, "sector_blocked"))
    return canonical, near_misses


def _join_merged(
    canonical: dict[str, str], order: list[str], allowlist: set[frozenset[str]]
) -> dict[str, str]:
    """The key whose spelling each key takes, once 'merge' verdicts join their groups.

    A group takes the spelling of its first key in ``order``.
    """
    parent = dict(canonical)  # kept keys are their own parent

    def root(key: str) -> str:
        while parent[key] != key:
            key = parent[key]
        return key

    for keys in allowlist:
        a, b = sorted(keys)
        if a in parent and b in parent:
            parent[root(a)] = root(b)
    groups: dict[str, list[str]] = defaultdict(list)
    for key in order:
        groups[root(key)].append(key)
    return {key: group[0] for group in groups.values() for key in group}


def canonicalise(
    ads: list[dict], verdicts: dict[frozenset[str], str]
) -> tuple[dict[str, str], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """The spelling to write for each normalised brand, then the merge, near-miss and
    dictionary-gap audits."""
    blocklist = {keys for keys, verdict in verdicts.items() if verdict == "split"}
    allowlist = {keys for keys, verdict in verdicts.items() if verdict == "merge"}

    # Normalised brands by key, in order of first appearance.
    n_ads = Counter(ad["brand"] for ad in ads)
    variants: dict[str, list[str]] = defaultdict(list)
    for brand in n_ads:
        variants[brand_key(brand)].append(brand)
    key_n_ads = {key: sum(n_ads[b] for b in brands) for key, brands in variants.items()}
    sectors: dict[str, set[str]] = defaultdict(set)
    on_screen: dict[str, set[str]] = defaultdict(set)
    for ad in ads:
        key = brand_key(ad["brand"])
        if ad["sector"]:
            sectors[key].add(ad["sector"])
        if ad["ocr_transcript"]:
            on_screen[key] |= ocr_spellings(ad["ocr_transcript"])

    nearest = {key: _nearest_dict(key) for key in variants}
    from_dict = {
        key: _dict_spelling(key, nearest[key], on_screen[key], blocklist)
        for key in variants
    }
    display = {
        key: from_dict[key][0] if from_dict[key] else _readable(brands, n_ads)
        for key, brands in variants.items()
    }

    # Dictionary spellings first, then the brands with the most ads, so that each brand
    # merges into the most established spelling it is close to.
    order = sorted(variants, key=lambda k: (from_dict[k] is None, -key_n_ads[k]))
    canonical, near_misses = _cluster(
        order, sectors, key_n_ads, blocklist, blocklist | allowlist
    )
    final = _join_merged(canonical, order, allowlist)
    brand_map = {brand: display[final[brand_key(brand)]] for brand in n_ads}

    canonical_n_ads: Counter = Counter()
    for brand, shown in brand_map.items():
        canonical_n_ads[shown] += n_ads[brand]
    merge_rows = []
    for brand, shown in brand_map.items():
        key = brand_key(brand)
        # Left out when no verdict could change the spelling: the same key written
        # differently ("gerble" -> "Gerblé"), or a brand the dictionary spells by its own
        # name or alias ("gerbe" -> "Gerblé").
        if key == brand_key(shown) or DICT_SPELLINGS.get(key) == shown:
            continue
        if canonical[key] != key:
            rule, score = "fuzzy", fuzz.ratio(key, canonical[key])
        elif final[key] != key:
            rule, score = "allowlist", None
        elif from_dict[key]:
            _, score, rule = from_dict[key]
        else:
            rule, score = "spacing", 100
        merge_rows.append(
            {
                "brand_canonical": shown,
                "brand_variant": brand,
                "pass": rule,
                "score": score,
                "sectors": "|".join(sorted(sectors[key])),
                "variant_n_ads": n_ads[brand],
                "canonical_n_ads": canonical_n_ads[shown],
            }
        )

    near_miss_rows = [
        {
            "variant": display[key],
            "near_canonical": display[other],
            "score": score,
            "threshold": threshold,
            "reason": reason,
            "variant_sectors": "|".join(sorted(sectors[key])),
            "near_canonical_sectors": "|".join(sorted(sectors[other])),
            "variant_n_ads": key_n_ads[key],
            "near_canonical_n_ads": key_n_ads[other],
        }
        for key, other, score, threshold, reason in near_misses
        # Left out when both already get the same spelling: a verdict would change nothing.
        if display[final[key]] != display[final[other]]
    ]

    gap_rows = []
    for key in variants:
        if not nearest[key] or not DICT_NEAR_MIN <= nearest[key][1] < DICT_THRESHOLD:
            continue
        candidate, score = nearest[key]
        if display[final[key]] == candidate:
            continue
        seen = on_screen[key]
        gap_rows.append(
            {
                "brand_variant": display[key],
                "dict_candidate": candidate,
                "score": score,
                "verdict": "no_ocr"
                if not seen
                else "screen_backs_variant"
                if key in seen
                else "unconfirmed",
                "variant_n_ads": key_n_ads[key],
            }
        )

    merges = pd.DataFrame(merge_rows, columns=MERGE_COLUMNS).sort_values(
        ["score", "canonical_n_ads"], ascending=[True, False], na_position="last"
    )
    near_misses_audit = pd.DataFrame(near_miss_rows, columns=NEAR_MISS_COLUMNS)
    near_misses_audit = near_misses_audit.sort_values(
        ["score", "variant_n_ads"], ascending=[False, False]
    )
    dict_gaps = pd.DataFrame(gap_rows, columns=DICT_GAP_COLUMNS).sort_values(
        ["verdict", "variant_n_ads"], ascending=[True, False]
    )
    return brand_map, merges, near_misses_audit, dict_gaps


def run(recanonicalise: bool = False, sheet_review: bool = True) -> None:
    ads = select_ads(recanonicalise)
    logging.info("[%s] %d ad(s) with a brand to canonicalise", STAGE, len(ads))
    if not ads:
        return
    verdicts = brand_overrides.load_verdicts() if sheet_review else {}
    logging.info("[%s] %d reviewer verdict(s)", STAGE, len(verdicts))
    brand_map, merges, near_misses, dict_gaps = canonicalise(ads, verdicts)

    with database.engine.begin() as conn:
        conn.execute(
            update(Ad)
            .where(Ad.id == bindparam("b_id"))
            .values(predicted_brand=bindparam("b_brand")),
            [{"b_id": ad["id"], "b_brand": brand_map[ad["brand"]]} for ad in ads],
        )

    AUDIT_DIR.mkdir(exist_ok=True)
    merges.to_csv(AUDIT_DIR / "brand_merges.csv", index=False)
    near_misses.to_csv(AUDIT_DIR / "brand_merge_near_misses.csv", index=False)
    dict_gaps.to_csv(AUDIT_DIR / "brand_dict_gaps.csv", index=False)
    if sheet_review:
        brand_overrides.publish_audits(merges, near_misses, verdicts)
    logging.info(
        "[%s] %d merge(s) to review, %d by the screen; %d near-miss(es); %d dictionary gap(s)",
        STAGE,
        len(merges),
        merges["pass"].eq("dict_ocr").sum(),
        len(near_misses),
        len(dict_gaps),
    )


if __name__ == "__main__":
    getLogger()
    run(
        recanonicalise=os.environ.get("RECANONICALISE") == "1",
        sheet_review=os.environ.get("SHEET_REVIEW", "1") == "1",
    )
