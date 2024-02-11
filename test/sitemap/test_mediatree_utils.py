import pytest
import pandas as pd

from utils import get_localhost
from quotaclimat.data_processing.mediatree.utils import get_yesterday, date_to_epoch

import logging
from time import strftime,localtime

localhost = get_localhost()

def test_date_to_epoch():
    epoch = date_to_epoch("2024-01-20 10:11:55")
    assert epoch == 1705741915

def test_get_yesterday():
    yesterday = get_yesterday()
    yesterday_string = strftime('%Y-%m-%d %H:%M:%S', localtime(yesterday))
    logging.info(f"yesterday_string {yesterday_string}")
    assert '00:00:00' in yesterday_string