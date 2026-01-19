import logging
import os

import pandas as pd

from quotaclimat.data_processing.mediatree.keyword.macro_category import \
    MACRO_CATEGORIES


def test_macro_category():
    """ Check that there is no duplicate keyword in the macro category """
    keywords = [category["keyword"] for category in MACRO_CATEGORIES]
    print(len(keywords))
    print(len(set(keywords)))

    assert len(keywords) == len(set(keywords)), "There is a duplicate keyword in the macro category"
