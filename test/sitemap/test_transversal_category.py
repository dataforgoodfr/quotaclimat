import logging
import os

import pandas as pd

from quotaclimat.data_processing.mediatree.keyword.macro_category import (
    MACRO_CATEGORIES,
)


def test_macro_category():
    """ Check that there is no duplicate keyword in the macro category """
    keywords = pd.DataFrame.from_records(MACRO_CATEGORIES)

    assert len(keywords) == len(keywords.drop_duplicates()), "There is a duplicate keyword in the macro category"
