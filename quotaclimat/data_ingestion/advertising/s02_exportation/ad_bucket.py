import logging
import os
from datetime import datetime, timedelta
from typing import Literal

logger = logging.getLogger(__name__)


BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
AD_S3_PREFIX = "ads"


def ad_prefix_in_bucket(ad_id: str):
    return f"{BUCKET_NAME}/{AD_S3_PREFIX}/{ad_id}"


def ad_media_s3_key(ad_id: str, media_format: Literal["mp3", "mp4"]):
    return f"{ad_prefix_in_bucket(ad_id)}/raw.{media_format}"


# The history of audio/video saving process is necessary to process them accordingly
MARGIN_ON_MEDIA_EXPORT_HISTORY = [
    (datetime(2025, 4, 1), timedelta(seconds=1)),
    (datetime(2026, 4, 27), timedelta(seconds=0.1)),
]


def get_margin_from_detection_date(d: datetime) -> timedelta:
    """Depending on the downloading processs, media and thus detection may vary in quality.
    We do change the margins during extraction process depending on this quality.
    This function helps find back what margin did we apply when the Ad was extracted."""
    for start, value in reversed(MARGIN_ON_MEDIA_EXPORT_HISTORY):
        if d > start:
            return value
