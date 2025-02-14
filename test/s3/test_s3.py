import pytest
import pandas as pd
from quotaclimat.data_processing.mediatree.s3.api_to_s3 import get_bucket_key, get_bucket_key_folder
from quotaclimat.data_processing.mediatree.s3.s3_utils import read_folder_from_s3, transform_raw_keywords
from quotaclimat.data_processing.mediatree.channel_program import *

def test_get_bucket_key():
    friday_6h26 = 1726719981
    date = pd.to_datetime(friday_6h26, unit='s', utc=True)
    channel = "tf1"
    assert get_bucket_key(date, channel) == "year=2024/month=9/day=19/channel=tf1/*.parquet"


def test_get_bucket_key_first_of_the_month():
    first_december = 1733040125
    date = pd.to_datetime(first_december, unit='s', utc=True)
    channel = "tf1"
    assert get_bucket_key(date, channel) == "year=2024/month=12/day=1/channel=tf1/*.parquet"

def test_get_bucket_key_first_of_the_month():
    first_december = 1733040125
    date = pd.to_datetime(first_december, unit='s', utc=True)
    channel = "tf1"
    assert get_bucket_key_folder(date, channel) == "year=2024/month=12/day=1/channel=tf1/"

# TODO need to mock s3 reads
# def test_read_folder_from_s3():
#     first_december = 1733040125
#     date = pd.to_datetime(first_december, unit='s', utc=True)
#     read_folder_from_s3(date=date, channel="tf1", storage_options=None)

#     assert False == True

def test_transform_raw_keywords():
    df= pd.read_parquet(path="test/s3/one-day-one-channel.parquet")
    df_programs = get_programs()
    output = transform_raw_keywords(df, df_programs=df_programs)

    assert len(output) == 31