"""Cross-cutting (macro) categories of the keywords.

The data is in macro_category.json, generated from the Excel dictionaries by
transform_excel_to_json.py (see keyword.py for why it is not a Python literal).
"""

import json
from pathlib import Path

with open(Path(__file__).with_name("macro_category.json"), encoding="utf-8") as _f:
    MACRO_CATEGORIES = json.load(_f)
