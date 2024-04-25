import pytest
import pandas as pd

from test_utils import get_localhost
from quotaclimat.data_processing.mediatree.utils import *

import logging
from time import strftime,localtime

localhost = get_localhost()

def test_get_yesterday():
    yesterday = get_yesterday()
    yesterday_string = strftime('%Y-%m-%d %H:%M:%S', localtime(yesterday))
    logging.info(f"yesterday_string {yesterday_string}")
    assert '00:00:00' in yesterday_string


def test_get_date_range():
    range = get_date_range(1681214197, 1681646197) # 11 to 16 april --> 6 days

    assert 6  == len(range) # ValueError: the 'dtype' parameter is not supported in the pandas implementation of any()

def test_get_default_date_range():
    # test default
    range = get_date_range(get_yesterday(), None)
    assert len(range) == 1

    # test with function
    (start_date_to_query, end_epoch) = get_start_end_date_env_variable_with_default()
    range = get_date_range(start_date_to_query, end_epoch)
    assert len(range) == 1


def test_is_it_tuesday():
    date = pd.Timestamp("2024-02-13 15:34:28")
    assert is_it_tuesday(date) == True

    date = pd.Timestamp("2024-01-01 15:34:28")
    assert is_it_tuesday(date) == False