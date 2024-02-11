import pytest
import pandas as pd

from utils import get_localhost
from quotaclimat.data_processing.mediatree.utils import get_yesterday, get_date_range, get_start_end_date_env_variable_with_default

import logging
from time import strftime,localtime

localhost = get_localhost()

def test_get_yesterday():
    yesterday = get_yesterday()
    yesterday_string = strftime('%Y-%m-%d %H:%M:%S', localtime(yesterday))
    logging.info(f"yesterday_string {yesterday_string}")
    assert '00:00:00' in yesterday_string


def test_get_date_range():
    range = get_date_range(1681214197, 1681646197)
    expected = pd.DatetimeIndex(['2023-04-11 11:56:37', '2023-04-11 23:56:37',
         '2023-04-12 11:56:37', '2023-04-12 23:56:37',
         '2023-04-13 11:56:37', '2023-04-13 23:56:37',
         '2023-04-14 11:56:37', '2023-04-14 23:56:37',
         '2023-04-15 11:56:37', '2023-04-15 23:56:37',
         '2023-04-16 11:56:37'],
        dtype='datetime64[ns]', freq='12H')
    assert len(expected) == len(range) # ValueError: the 'dtype' parameter is not supported in the pandas implementation of any()

def test_get_default_date_range():
    # test default
    range = get_date_range(get_yesterday(), None)
    assert len(range) == 4

    # test with function
    (start_date_to_query, end_epoch) = get_start_end_date_env_variable_with_default()
    range = get_date_range(start_date_to_query, end_epoch)
    assert len(range) == 4