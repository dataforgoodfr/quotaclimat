"""Rules based approach for determining brand sector and subcat based on the reference data dictionnary

Three tiers
Tier 1 : brand alone determines (sector, sub-cat)
Tier 2: brand determines sector, product keywords pick the sub-cat (if no keywords, only the sector is determined).
Tier 3: brand can be in multiple sectors. Only if there are product keyword matches, the sector and subcat are determined.

If no brand hit, the llm's prediction is used for the sector and the ad moves to the subcat classification stage.
If there are no keywords for tier 2 brands, the dict sector is used but the ad moves to the subcat classification stage.
Tier 3 brands always go to the subcat classification stage, due to the ambiguous nature of these brands.
"""

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import (
    normalize, nospace)

DICT_PATH = Path(
    "quotaclimat/data_ingestion/advertising/s03_classification/reference_data/dictionnaire_marques_secteurs.xlsx"
)


def _brand_key(name: str | None) -> str:
    """Creates brand lookup key, normalised (accent/punct/case stripped) and space-free"""
    return nospace(normalize(name))


@dataclass(slots=True)
class DictMatch:
    method: str  # dict_tier1 | dict_tier2 | dict_tier2_no_kw | dict_tier3
    sector_code: str | None
    subcat_code: str | None
    matched_brand: str
    matched_keyword: str | None = None
    rule_keywords: list[str] | None = None  # the rule's full keyword set, set on hits
    tried_rules: list[
        list[str]
    ] | None = None  # every rule's keywords, set on no_kw cases
    haystack: str | None = None


def _split_keywords(cell: object) -> list[str]:
    if cell is None or (isinstance(cell, float) and pd.isna(cell)):
        return []
    return [normalize(part) for part in str(cell).split(";") if part.strip()]


def _haystack(product: str | None, transcript: str | None) -> str:
    return f"{normalize(product)} {normalize(transcript)}".strip()


class BrandDictionary:
    def __init__(self, path: str | Path = DICT_PATH):
        path = Path(path)
        self.t1 = self._load_tier1(pd.read_excel(path, sheet_name="marques_type_1"))
        self.t2 = self._load_tier2(pd.read_excel(path, sheet_name="marques_type_2"))
        self.t3 = self._load_tier3(pd.read_excel(path, sheet_name="marques_type_3"))
        print(f"t1={type(self.t1)}, t2={type(self.t2)}, t3={type(self.t3)}")
        print(
            f"t1 keys={len(self.t1) if self.t1 else 0}, t2 keys={len(self.t2) if self.t2 else 0}, t3 keys={len(self.t3) if self.t3 else 0}"
        )

    @staticmethod
    def _load_tier1(df: pd.DataFrame) -> dict[str, tuple[str, str, str]]:
        """brand_key: (sector, subcat, brand_raw)"""
        return {
            _brand_key(row["nom_marque"]): (
                row["secteur"],
                row["catégorie"],
                row["nom_marque"],
            )
            for _, row in df.iterrows()
        }

    @staticmethod
    def _load_tier2(df: pd.DataFrame) -> dict[str, dict]:
        """brand_key: {sector, brand_raw, rules: [(keywords, subcat)]}"""
        out: dict[str, dict] = {}
        for _, row in df.iterrows():
            b = _brand_key(row["nom_marque"])
            if b not in out:
                out[b] = {
                    "sector": row["secteur"],
                    "brand_raw": row["nom_marque"],
                    "rules": [],
                }
            out[b]["rules"].append(
                (_split_keywords(row.get("mots_clés_produit")), row["catégorie"])
            )
        return out

    @staticmethod
    def _load_tier3(
        df: pd.DataFrame,
    ) -> dict[str, list[tuple[list[str], str, str, str]]]:
        """brand_key: [(keywords, sector, subcat, brand_raw)]"""
        out: dict[str, list[tuple[list[str], str, str, str]]] = {}
        for _, row in df.iterrows():
            b = _brand_key(row["nom_marque"])
            if b not in out:
                out[b] = []
            out[b].append(
                (
                    _split_keywords(row.get("mots_clés_produit")),
                    row["secteur"],
                    row["catégorie"],
                    row["nom_marque"],
                )
            )
        return out

    @staticmethod
    def _kw_hit(keywords: list[str], haystack: str) -> str | None:
        for kw in keywords:
            if kw and kw in haystack:
                return kw
        return None

    def match(
        self,
        brand_raw: str | None,
        product_raw: str | None = None,
        transcript: str | None = None,
    ) -> DictMatch | None:
        b = _brand_key(brand_raw)
        if not b:
            return None

        if b in self.t1:
            sector, subcat, brand = self.t1[b]
            return DictMatch("dict_tier1", sector, subcat, brand)

        hay = _haystack(product_raw, transcript)

        if b in self.t2:
            entry = self.t2[b]
            for keywords, subcat in entry["rules"]:
                hit = self._kw_hit(keywords, hay)
                if hit is not None:
                    return DictMatch(
                        method="dict_tier2",
                        sector_code=entry["sector"],
                        subcat_code=subcat,
                        matched_brand=entry["brand_raw"],
                        matched_keyword=hit,
                        rule_keywords=keywords,
                        haystack=hay,
                    )
            return DictMatch(
                method="dict_tier2_no_kw",
                sector_code=entry["sector"],
                subcat_code=None,
                matched_brand=entry["brand_raw"],
                tried_rules=[keywords for keywords, _ in entry["rules"]],
                haystack=hay,
            )

        if b in self.t3:
            for keywords, sector, subcat, brand in self.t3[b]:
                hit = self._kw_hit(keywords, hay)
                if hit is not None:
                    return DictMatch(
                        method="dict_tier3",
                        sector_code=sector,
                        subcat_code=subcat,
                        matched_brand=brand,
                        matched_keyword=hit,
                        rule_keywords=keywords,
                        haystack=hay,
                    )
            first_row_brand = self.t3[b][0][3]
            return DictMatch(
                method="dict_tier3_no_kw",
                sector_code=None,
                subcat_code=None,
                matched_brand=first_row_brand,
                tried_rules=[keywords for keywords, _, _, _ in self.t3[b]],
                haystack=hay,
            )

        return None
