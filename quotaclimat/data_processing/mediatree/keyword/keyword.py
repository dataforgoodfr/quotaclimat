"""Keyword dictionary of the OME, by theme.

The data is in keyword.json, generated from the Excel dictionaries by
transform_excel_to_json.py. It is not a Python literal: under a tracer
(coverage, debugger), CPython 3.12 takes about a minute to instrument the code
object of a module of this size.
"""

import json
from pathlib import Path

with open(Path(__file__).with_name("keyword.json"), encoding="utf-8") as _f:
    THEME_KEYWORDS = json.load(_f)
