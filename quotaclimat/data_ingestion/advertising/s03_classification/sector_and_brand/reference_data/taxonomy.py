"""The sector and sub-category taxonomy, kept in a Google Sheet and snapshotted locally.

Stages read the latest snapshot at import, fetching the sheet only when this machine has
none yet. To pick up edits made in the sheet, sync first: ``python -m quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data.taxonomy``.
"""

from pathlib import Path

import pandas as pd

from quotaclimat.data_ingestion.advertising.s03_classification.sector_and_brand.reference_data import (
    sheets,
)
from quotaclimat.utils.logger import getLogger

REFERENCE_DATA = Path(__file__).resolve().parent
FILENAME = "classification_pub_ome.xlsx"
VERSIONS_DIR = REFERENCE_DATA / "taxonomy_versions"
SECTORS_SHEET, CATEGORIES_SHEET = "secteurs", "catégories"
SHEET_NAMES = (SECTORS_SHEET, CATEGORIES_SHEET)


def _build(tabs: dict[str, pd.DataFrame]) -> tuple[dict, str, dict, dict]:
    """Sectors by code and their list for the e1_classify_ad prompt, then sub-categories by
    code for each sector and their lists for the e4_classify_subcat prompt."""
    sectors = tabs[SECTORS_SHEET].set_index("sector_code").to_dict(orient="index")
    sector_list = "\n".join(
        f"{code} | {s['sector_label_en']} | {s['sector_description']} | {s['example_brands']}"
        for code, s in sectors.items()
    )
    subcategories = {
        sector: group.set_index("cat_code").to_dict(orient="index")
        for sector, group in tabs[CATEGORIES_SHEET].groupby("sector_code")
    }
    subcategory_lists = {
        sector: "\n".join(
            f"{code} | {c['product_category_en']} | {c['description_en']} | {c['brand_examples']}"
            for code, c in subcats.items()
        )
        for sector, subcats in subcategories.items()
    }
    return sectors, sector_list, subcategories, subcategory_lists


def sync() -> str:
    tabs = sheets.fetch(SHEET_NAMES)
    _build(tabs)  # a renamed or missing column fails here, before it becomes the latest
    return sheets.save_snapshot(tabs, VERSIONS_DIR, FILENAME)


VERSION = sheets.latest(VERSIONS_DIR, FILENAME) or sync()
_tabs = sheets.read_xlsx(VERSIONS_DIR / VERSION / FILENAME, SHEET_NAMES)
SECTORS, SECTOR_LIST, SUBCATEGORIES, SUBCATEGORY_LISTS = _build(_tabs)


if __name__ == "__main__":
    getLogger()
    sync()
