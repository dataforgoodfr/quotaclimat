"""The brand dictionary, i.e. brands whose sector and sub-category are known in advance.

Kept in a Google Sheet and snapshotted locally, like the taxonomy. Stages read the latest
snapshot at import, fetching the sheet when this machine has none yet; to pick up edits
made in the sheet, sync first:
``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data.dictionary``.

One tab per tier:
  1. the brand alone gives the sector and the sub-category;
  2. the brand gives the sector, a product keyword picks the sub-category;
  3. the brand spans several sectors, so only a product keyword decides both.

``alias_marque`` lists other names of the brand, separated by ";" (misheard spellings,
short forms): an ad under an alias is matched as the brand itself.
"""

from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import pandas as pd

from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    sheets,
)
from quotaclimat.data_ingestion.advertising.tools.normalize import (
    normalize,
    nospace,
)
from quotaclimat.utils.logger import getLogger

REFERENCE_DATA = Path(__file__).resolve().parent
FILENAME = "dictionnaire_marques_secteurs.xlsx"
VERSIONS_DIR = REFERENCE_DATA / "dictionary_versions"
SHEET_NAMES = ("marques_type_1", "marques_type_2", "marques_type_3")


class Rule(NamedTuple):
    brand: str  # as written in the dictionary
    sector: str
    subcat: str
    keywords: list[str]  # normalized; none in tier 1


def brand_key(brand: str | None) -> str:
    """The same key for "Coca-Cola", "coca cola" and "cocacola"."""
    return nospace(normalize(brand))


def _build(tabs: dict[str, pd.DataFrame]) -> list[dict[str, list[Rule]]]:
    """For each tier, its rules by brand key, in sheet order, also under the brand's aliases."""
    tiers = []
    owners: dict[str, set[str]] = defaultdict(set)  # alias key -> brand keys
    for sheet in SHEET_NAMES:
        rules: dict[str, list[Rule]] = {}
        for row in tabs[sheet].fillna("").to_dict("records"):
            keywords = row.get("mots_clés_produit", "").split(";")
            rule = Rule(
                row["nom_marque"],
                row["secteur"],
                row["catégorie"],
                [kw for kw in map(normalize, keywords) if kw],
            )
            if key := brand_key(rule.brand):
                rules.setdefault(key, []).append(rule)
                for alias in str(row.get("alias_marque", "")).split(";"):
                    if brand_key(alias) not in ("", key):
                        owners[brand_key(alias)].add(key)
        tiers.append(rules)

    brands = set().union(*tiers)
    conflicts = {
        alias: sorted(keys)
        for alias, keys in owners.items()
        if len(keys) > 1 or alias in brands
    }
    if conflicts:
        raise ValueError(
            f"Aliases that are another brand or the alias of several: {conflicts}"
        )
    for alias, (key,) in owners.items():
        for rules in tiers:
            if key in rules:
                rules[alias] = rules[key]
    return tiers


def sync() -> str:
    """Fetch the sheet and snapshot it if it changed. Returns the version now in use."""
    tabs = sheets.fetch(SHEET_NAMES)
    _build(tabs)  # a renamed or missing column fails here, before it becomes the latest
    return sheets.save_snapshot(tabs, VERSIONS_DIR, FILENAME)


VERSION = sheets.latest(VERSIONS_DIR, FILENAME) or sync()
_tabs = sheets.read_xlsx(VERSIONS_DIR / VERSION / FILENAME, SHEET_NAMES)
TIER1, TIER2, TIER3 = _build(_tabs)


def match(brand: str | None, product: str | None, transcript: str | None) -> dict:
    """What the dictionary says about an ad, as recorded in its dict_match entry.

    ``method`` is dict_tier1/2/3 when the dictionary decides, dict_tier2_no_kw or
    dict_tier3_no_kw when it knows the brand but no keyword decides, else dict_miss.
    """
    key = brand_key(brand)
    if key in TIER1:
        rule = TIER1[key][0]
        return {
            "method": "dict_tier1",
            "matched_brand": rule.brand,
            "sector_code": rule.sector,
            "subcat_code": rule.subcat,
        }

    # Keywords are looked for in what is sold. The transcript only stands in when
    # e1_classify_ad found no product, so that a word said in passing can't decide on its own.
    haystack = normalize(product) or normalize(transcript)

    if key in TIER2:
        rules = TIER2[key]
        found = _keyword_hit(rules, haystack)
        # The brand gives the sector; a keyword can only pick the sub-category.
        known = {
            "matched_brand": rules[0].brand,
            "sector_code": rules[0].sector,
            "haystack": haystack,
        }
        if found:
            keyword, rule = found
            return {
                "method": "dict_tier2",
                **known,
                "subcat_code": rule.subcat,
                "matched_keyword": keyword,
                "rule_keywords": rule.keywords,
            }
        return {
            "method": "dict_tier2_no_kw",
            **known,
            "subcat_code": None,
            "tried_rules": [r.keywords for r in rules],
        }

    if key in TIER3:
        rules = TIER3[key]
        found = _keyword_hit(rules, haystack)
        if found:
            keyword, rule = found
            return {
                "method": "dict_tier3",
                "matched_brand": rule.brand,
                "sector_code": rule.sector,
                "subcat_code": rule.subcat,
                "matched_keyword": keyword,
                "rule_keywords": rule.keywords,
                "haystack": haystack,
            }
        return {
            "method": "dict_tier3_no_kw",
            "matched_brand": min(r.brand for r in rules),
            "sector_code": None,
            "subcat_code": None,
            "tried_rules": [r.keywords for r in rules],
            "haystack": haystack,
        }

    return {"method": "dict_miss", "brand_in": brand}


def _keyword_hit(rules: list[Rule], haystack: str) -> tuple[str, Rule] | None:
    """The most specific keyword of ``rules`` found in ``haystack``, with its rule.

    Keywords match whole words, in the singular or plural; the most specific has the
    most words, then the most characters. None when nothing matches, or when the most
    specific keywords lead to different answers: the sheet's row order must not decide.
    """
    words = haystack.split()
    hits = [
        (kw, rule)
        for rule in rules
        for kw in rule.keywords
        if _contains(words, kw.split())
    ]
    if not hits:
        return None
    best = max(_specificity(kw) for kw, _ in hits)
    top = [(kw, rule) for kw, rule in hits if _specificity(kw) == best]
    if len({(rule.sector, rule.subcat) for _, rule in top}) > 1:
        return None
    return min(top, key=lambda hit: hit[0])


def _specificity(keyword: str) -> tuple[int, int]:
    return len(keyword.split()), len(keyword)


def _contains(words: list[str], phrase: list[str]) -> bool:
    """Whether ``phrase`` appears as consecutive words in ``words``."""
    n = len(phrase)
    return any(
        all(map(_same_word, words[i : i + n], phrase))
        for i in range(len(words) - n + 1)
    )


def _same_word(a: str, b: str) -> bool:
    """Equal, or the plural of one another ("vol"/"vols", "eau"/"eaux", "animal"/"animaux").
    Not for words of one or two letters, as "de"/"des" or "au"/"aux" are different words."""
    if a == b:
        return True
    short, long = sorted((a, b), key=len)
    return len(short) > 2 and (
        long in (short + "s", short + "x")
        or (short.endswith("al") and long == short[:-2] + "aux")
    )


if __name__ == "__main__":
    getLogger()
    sync()
