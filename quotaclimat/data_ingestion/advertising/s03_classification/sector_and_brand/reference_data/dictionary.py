"""The brand dictionary of the classification: its rules (``tools/brand_dictionary.py``), read
from the latest local snapshot of the Google Sheet.

Stages read the latest snapshot at import, fetching the sheet when this machine has none
yet; to pick up edits made in the sheet, sync first:
``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data.dictionary``.
"""

from pathlib import Path

from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    sheets,
)
from quotaclimat.data_ingestion.advertising.tools import brand_dictionary
from quotaclimat.utils.logger import getLogger

REFERENCE_DATA = Path(__file__).resolve().parent
FILENAME = "dictionnaire_marques_secteurs.xlsx"
VERSIONS_DIR = REFERENCE_DATA / "dictionary_versions"


def sync() -> str:
    """Fetch the sheet and snapshot it if it changed. Returns the version now in use."""
    tabs = sheets.fetch(brand_dictionary.SHEET_NAMES)
    # A renamed or missing column fails here, before it becomes the latest.
    brand_dictionary.build(tabs)
    return sheets.save_snapshot(tabs, VERSIONS_DIR, FILENAME)


VERSION = sheets.latest(VERSIONS_DIR, FILENAME) or sync()
TIERS = brand_dictionary.build(
    sheets.read_xlsx(VERSIONS_DIR / VERSION / FILENAME, brand_dictionary.SHEET_NAMES)
)
TIER1, TIER2, TIER3 = TIERS


def match(brand: str | None, product: str | None, transcript: str | None) -> dict:
    """``brand_dictionary.match`` on the rules of the snapshot."""
    return brand_dictionary.match(TIERS, brand, product, transcript)


if __name__ == "__main__":
    getLogger()
    sync()
