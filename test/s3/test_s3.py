import pytest
import pandas as pd
from quotaclimat.data_processing.mediatree.s3.api_to_s3 import get_bucket_key, save_to_s3


def test_get_bucket_key():
    friday_6h26 = 1726719981
    date = pd.to_datetime(friday_6h26, unit='s', utc=True)
    channel = "tf1"
    assert get_bucket_key(date, channel) == "year=2024/month=09/day=19/channel=tf1/data.json.gz"


def test_get_bucket_key_first_of_the_month():
    first_december = 1733040125
    date = pd.to_datetime(first_december, unit='s', utc=True)
    channel = "tf1"
    assert get_bucket_key(date, channel) == "year=2024/month=12/day=01/channel=tf1/data.json.gz"