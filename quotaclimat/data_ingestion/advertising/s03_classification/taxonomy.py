"""Sector list and per-sector subcategory list, loaded from the reference data excel file"""

from pathlib import Path

import pandas as pd

FILE_PATH = Path(
    "./quotaclimat/data_ingestion/advertising/s03_classification/reference_data/classification_pub_ome.xlsx"
)


df_sectors = pd.read_excel(FILE_PATH, sheet_name="secteurs")
df_sectors.index = df_sectors["sector_code"]
sectors = df_sectors["sector_code"].to_list()
dict_sectors = df_sectors.set_index("sector_code").to_dict(orient="index")


def _sector_line(code: str) -> str | None:
    """Format the sectors as the prompt expects"""
    s = dict_sectors.get(code)
    if not s:
        return None
    return f"{code} | {s['sector_label_en']} | {s['sector_description']} | {s['example_brands']}"


SECTOR_LIST = "\n".join(line for line in (_sector_line(c) for c in sectors) if line)


df_categories = pd.read_excel(FILE_PATH, sheet_name="catégories")
SUBCATEGORIES = {
    group: {
        r["cat_code"]: (
            r["product_category_en"],
            r["product_category_fr"],
            r["description_en"],
            r["brand_examples"],
        )
        for _, r in sub.iterrows()
    }
    for group, sub in df_categories.groupby("sector_code")
}


def get_subcategory_list(sector_code: str | None) -> str | None:
    """Format the sub-category list for a sector as the prompt expects."""
    sub_cats = SUBCATEGORIES.get(sector_code) if sector_code else None
    if not sub_cats:
        return None
    return "\n".join(f"{k} | {v[0]} | {v[2]} | {v[3]}" for k, v in sub_cats.items())
