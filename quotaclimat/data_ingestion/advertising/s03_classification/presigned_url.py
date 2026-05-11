import os
from dataclasses import dataclass

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from quotaclimat.data_processing.mediatree.s3.s3_utils import get_secret_docker

AD_S3_PREFIX = "ads"


@dataclass
class AdvertisingS3Config:
    bucket_name: str
    prefix: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: str

    @classmethod
    def from_env(cls):
        access_key = get_secret_docker("BUCKET")
        secret_key = get_secret_docker("BUCKET_SECRET")
        if not access_key or not secret_key:
            raise ValueError("S3 credentials missing. Set BUCKET and BUCKET_SECRET.")
        region = os.getenv("ADVERTISING_BUCKET_REGION", "fr-par")
        return cls(
            bucket_name=os.environ["ADVERTISING_BUCKET_NAME"],
            prefix=AD_S3_PREFIX,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=os.getenv("S3_ENDPOINT_URL", f"https://s3.{region}.scw.cloud"),
        )


def get_s3_client(config: AdvertisingS3Config):
    return boto3.client(
        "s3",
        region_name=config.region,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        endpoint_url=config.endpoint_url,
        config=BotoConfig(signature_version="s3v4", max_pool_connections=50),
    )


def build_s3_key(prefix: str, ad_id: str, extension: str) -> str:
    return f"{prefix.rstrip('/')}/{ad_id}/raw.{extension}"


def get_presigned_url_for_ad(
    ad_id: str,
    extension: str = "mp4",
    ttl: int = 3600,
    config: AdvertisingS3Config | None = None,
    s3_client=None,
    check_exists: bool = True,
) -> str:
    if config is None:
        config = AdvertisingS3Config.from_env()
    if s3_client is None:
        s3_client = get_s3_client(config)

    s3_key = build_s3_key(config.prefix, ad_id, extension)

    if check_exists:
        try:
            s3_client.head_object(Bucket=config.bucket_name, Key=s3_key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise FileNotFoundError(
                    f"S3 object not found: s3://{config.bucket_name}/{s3_key}"
                ) from e
            raise

    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": config.bucket_name, "Key": s3_key},
        ExpiresIn=ttl,
    )
