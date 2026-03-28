import asyncio
import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

import boto3
from sqlalchemy import select
from tqdm import tqdm

from postgres.database_connection import get_db_session
from postgres.schemas.advertising.models import Ad, Ad_Occurrence
from quotaclimat.data_ingestion.advertising.s01_detection.tools.mediatree import (
    MediatreeAPI,
)

logger = logging.getLogger(__name__)


ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = "fr-par"
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
AD_S3_PREFIX = "ads"


def get_s3_client():
    return boto3.client(
        service_name="s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )


def ad_folder_exists_in_s3(ad_id: str, s3_client) -> bool:
    prefix = f"{AD_S3_PREFIX}/{ad_id}/"
    try:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME, Prefix=prefix, MaxKeys=1
        )
        return "Contents" in response
    except Exception as e:
        logger.error(f"Error checking S3 for ad {ad_id}: {e}")
        return False


def query_ads_since(session, since_date: datetime) -> list[tuple[Ad, Ad_Occurrence]]:
    result = session.execute(
        select(Ad, Ad_Occurrence)
        .join(Ad_Occurrence, Ad_Occurrence.ad_id == Ad.id)
        .where(Ad.first_detection_date >= since_date)
        .distinct(Ad.id)
    )
    return result.all()


async def _export_ad(
    ad: Ad, occurrence: Ad_Occurrence, api: MediatreeAPI, s3_client, tmp_dir: str
):
    from_date = occurrence.occurrence_date
    to_date = from_date + timedelta(seconds=ad.duration_sec)
    channel = occurrence.channel_name

    for media_format in ("mp3", "mp4"):
        local_path = os.path.join(tmp_dir, f"{ad.id}.{media_format}")
        await api.download_export(
            channel, from_date, to_date, media_format, file_path=local_path
        )

        s3_key = f"{AD_S3_PREFIX}/{ad.id}/raw.{media_format}"
        s3_client.upload_file(local_path, BUCKET_NAME, s3_key)
        logger.info(f"Uploaded s3://{BUCKET_NAME}/{s3_key}")

        os.remove(local_path)


async def run(since_date: datetime):
    session = get_db_session()
    s3_client = get_s3_client()

    try:
        ads = query_ads_since(session, since_date)
        logger.info(f"Found {len(ads)} ads since {since_date}")

        async with MediatreeAPI() as api:
            with tempfile.TemporaryDirectory() as tmp_dir:
                for ad, occurrence in tqdm(ads, desc="Exporting ads"):
                    if ad_folder_exists_in_s3(ad.id, s3_client):
                        logger.info(f"Ad {ad.id} already in S3, skipping")
                        continue

                    logger.info(
                        f"Processing ad {ad.id} (channel={occurrence.channel_name})"
                    )
                    try:
                        await _export_ad(ad, occurrence, api, s3_client, tmp_dir)
                    except Exception as e:
                        logger.error(f"Failed to export ad {ad.id}: {e}")
                        raise e
    finally:
        session.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    since_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    asyncio.run(run(since_date))
