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

def test_is_it_tuesday():
    date = pd.Timestamp("2024-02-13 15:34:28")
    assert is_it_tuesday(date) == True

    date = pd.Timestamp("2024-01-01 15:34:28")
    assert is_it_tuesday(date) == False

def test_get_end_of_month():
    assert get_end_of_month("2024-04-01") == "2024-04-30"
    assert get_end_of_month("2024-02-01") == "2024-02-29"
    assert get_end_of_month("2024-02-15") == "2024-02-29"



def test_get_start_end_date_env_variable_with_default():
    start_date = 0
    
    assert get_start_end_date_env_variable_with_default(start_date, minus_days=1) == (get_yesterday(), None)

def test_get_start_end_date_env_variable_with_start_date_value():
    start_date = 1734508085
    number_of_previous_days = 7
    start_date_minus_days = start_date - (number_of_previous_days * 24 * 60 * 60)

    assert get_start_end_date_env_variable_with_default(start_date, minus_days=number_of_previous_days) == (int(start_date), start_date_minus_days)

def test_get_start_end_date_with_get_date_range():
    start_date = 1734508085
    number_of_previous_days = 7
    (start,end) = get_start_end_date_env_variable_with_default(start_date, minus_days=number_of_previous_days)

    expected = pd.DatetimeIndex(['2024-12-11', '2024-12-12', '2024-12-13', '2024-12-14', '2024-12-15', '2024-12-16', '2024-12-17', '2024-12-18'],
                        dtype='datetime64[ns]', freq='D')
    
    output = get_date_range(start,end)
    assert len(output) == number_of_previous_days + 1
    pd.testing.assert_index_equal(output, expected)
