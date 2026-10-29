"""Reference data to check the corrections of the ads, read from the database: the dbt seeds downloaded from the
Dictionnaire_marques_secteurs and Inventaires_des_marques Google Sheets (advertising.ref_*, see
my_dbt_project/external_sources.yml) and the brands already predicted.

A missing table (extended perimeter, tests) gives an empty reference: the matching check is skipped.
"""

from dataclasses import dataclass, field

from sqlalchemy import text
from sqlalchemy.orm import Session
from thefuzz import fuzz, process

from quotaclimat.data_ingestion.advertising.s03_classification.dictionary.normalize import (
    normalize,
    nospace,
)

# brands of the dictionary: tiers 1 and 2 give one sector to a brand, tier 3 several
DICTIONARY_TABLES = (
    "ref_ome_marques_type_1",
    "ref_ome_marques_type_2",
    "ref_ome_marques_type_3",
)
SIMILAR_BRAND_SCORE = 85
MAX_SIMILAR_BRANDS = 3


def brand_key(name: str | None) -> str:
    """Key of a brand, the same as the dictionary (s03_classification/dictionary/matcher.py)."""
    return nospace(normalize(name))


@dataclass
class References:
    # sector codes (ref_ome_secteurs), empty when unknown
    sectors: set[str] = field(default_factory=set)
    # category code -> its sector code (ref_ome_categories)
    category_sector: dict[str, str] = field(default_factory=dict)
    # brand key -> sector codes of the brand in the dictionary (its name and its aliases)
    dictionary_sectors: dict[str, set[str]] = field(default_factory=dict)
    # brand key -> one spelling, for every known brand: dictionary, brand inventory, predicted brands
    known_brands: dict[str, str] = field(default_factory=dict)

    def similar_brands(self, name: str) -> list[str]:
        """Known brands with a close spelling, for a brand that is not known."""
        if not self.known_brands:
            return []
        # compared on the keys: a dict of choices is compared on its values, returned with their key
        choices = {spelling: key for key, spelling in self.known_brands.items()}
        matches = process.extract(
            brand_key(name), choices, scorer=fuzz.ratio, limit=MAX_SIMILAR_BRANDS
        )
        return [
            spelling for _, score, spelling in matches if score >= SIMILAR_BRAND_SCORE
        ]


def _table_exists(session: Session, table: str) -> bool:
    return (
        session.execute(
            text("SELECT to_regclass(:name)"), {"name": f"advertising.{table}"}
        ).scalar()
        is not None
    )


def load_references(session: Session) -> References:
    references = References()
    if _table_exists(session, "ref_ome_secteurs"):
        references.sectors = set(
            session.execute(
                text("SELECT sector_code FROM advertising.ref_ome_secteurs")
            ).scalars()
        )
    if _table_exists(session, "ref_ome_categories"):
        references.category_sector = dict(
            session.execute(
                text("SELECT cat_code, sector_code FROM advertising.ref_ome_categories")
            ).all()
        )

    names: list[str] = []
    for table in DICTIONARY_TABLES:
        if not _table_exists(session, table):
            continue
        rows = session.execute(
            text(f'SELECT nom_marque, secteur, alias_marque FROM advertising."{table}"')
        ).all()
        for name, sector, aliases in rows:
            spellings = [name] + [
                alias.strip() for alias in (aliases or "").split(";") if alias.strip()
            ]
            for spelling in spellings:
                key = brand_key(spelling)
                if key and sector:
                    references.dictionary_sectors.setdefault(key, set()).add(sector)
            names.append(name)

    if _table_exists(session, "ref_inventaire_marques"):
        names.extend(
            session.execute(
                text("SELECT marque FROM advertising.ref_inventaire_marques")
            ).scalars()
        )
    names.extend(
        session.execute(
            text(
                "SELECT DISTINCT predicted_brand FROM advertising.ad"
                " WHERE predicted_brand IS NOT NULL"
            )
        ).scalars()
    )
    for name in names:
        key = brand_key(name)
        if key:
            references.known_brands.setdefault(key, name)
    return references
