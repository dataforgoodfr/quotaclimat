import logging
import os
from typing import Literal

logger = logging.getLogger(__name__)


BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
AD_S3_PREFIX = "ads"


def ad_prefix_in_bucket(ad_id: str):
    return f"{BUCKET_NAME}/{AD_S3_PREFIX}/{ad_id}"


def ad_media_s3_key(ad_id: str, media_format: Literal["mp3" | "mp4"]):
    return f"{ad_prefix_in_bucket(ad_id)}/raw.{media_format}"
