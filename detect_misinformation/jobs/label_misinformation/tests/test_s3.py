import pytest
import sys
import os

sys.path.append(os.path.abspath("/app"))
import modin.pandas as pd
from app.s3_utils import (
    save_to_s3,
    save_csv,
    save_parquet,
    get_s3_client,
    get_bucket_key_folder,
    save_json,
)
from app.country import FRANCE_COUNTRY
from datetime import datetime, timedelta
import logging


def test_save_csv():
    df: pd.DataFrame = pd.read_parquet(
        "tests/label_misinformation/data/misinformation.parquet"
    )
    date: datetime = datetime.now()
    df["model_reason"] = "test"
    df["plaintext_whisper"] = "mytest"
    df["id"] = "keyword_id"
    output = save_csv(df, channel="itele", date=date, s3_path="test")
    assert output == "s3/misinformation.tsv"


def test_save_json():
    country = FRANCE_COUNTRY
    df: pd.DataFrame = pd.read_parquet(
        "tests/label_misinformation/data/misinformation.parquet"
    )
    date: datetime = datetime.now()
    channel = "itele"
    df["year"] = date.year
    df["month"] = date.month
    df["day"] = date.day
    df["channel"] = channel  # channel_name from mediatree's api
    df["model_reason"] = "test"
    df["plaintext_whisper"] = "mytest"
    df["id"] = "keyword_id"
    df["country"] = country.name
    output = save_json(
        df, channel=channel, date=date, s3_path="test", country=FRANCE_COUNTRY
    )
    assert output == "s3/json"


def test_save_parquet():
    df: pd.DataFrame = pd.read_parquet(
        "tests/label_misinformation/data/misinformation.parquet"
    )
    channel = "itele"
    s3_path = "test_s3_path"
    date: datetime = datetime.now()
    df["year"] = date.year
    df["month"] = date.month
    df["day"] = date.day
    df["channel"] = channel  # channel_name from mediatree's api
    df["model_reason"] = "test"
    df["plaintext_whisper"] = "mytest"
    df["id"] = "keyword_id"
    date: datetime = datetime.now()
    output = save_parquet(df, channel=channel, date=date, s3_path=s3_path)
    assert output == f"s3/parquet/{s3_path}"


# def test_save_to_s3():
#     df: pd.DataFrame = pd.DataFrame([]) #pd.read_parquet("tests/label_misinformation/data/misinformation.parquet")
#     s3_client = get_s3_client()
#     date: datetime = datetime.now()
#     folder_inside_bucket="test"
#     channel= "tf2"

#     save_to_s3(df, channel=channel, date=date, s3_client=s3_client, bucket="climateguard", folder_inside_bucket="test")

#     assert False == True
