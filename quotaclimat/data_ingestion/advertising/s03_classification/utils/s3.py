"""To get presigned URLs"""

import os
from functools import cache

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = os.environ.get("ADVERTISING_BUCKET_REGION", "fr-par")


@cache
def _client():
    return boto3.client(
        "s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=f"https://s3.{REGION}.scw.cloud",
        config=Config(signature_version="s3v4", max_pool_connections=50),
    )


_MISSING = ("404", "NoSuchKey", "NotFound")


def video_url(ad_id: str, ttl: int = 3600) -> str | None:
    key = f"ads/{ad_id}/raw.mp4"
    try:
        _client().head_object(Bucket=BUCKET_NAME, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return _client().generate_presigned_url(
        "get_object", Params={"Bucket": BUCKET_NAME, "Key": key}, ExpiresIn=ttl
    )


def video_bytes(ad_id: str) -> bytes | None:
    key = f"ads/{ad_id}/raw.mp4"
    try:
        return _client().get_object(Bucket=BUCKET_NAME, Key=key)["Body"].read()
    except ClientError as e:
        if e.response["Error"]["Code"] in _MISSING:
            return None
        raise
