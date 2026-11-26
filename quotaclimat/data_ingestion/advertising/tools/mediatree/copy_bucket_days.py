"""Copy every channel of mediatree's video bucket between two dates into another bucket,
keeping exactly the same keys (output/<channel>/YYYY/MM/DD/<file>), e.g. to fill a test bucket.

Usage:
    poetry run python -m quotaclimat.data_ingestion.advertising.tools.mediatree.copy_bucket_days \
        --start 2026-09-01 --end 2026-09-07 [--channels tf1,france2] [--dry-run]

The dates are the UTC day folders of the bucket, both included.

Env vars:
    BUCKET, BUCKET_SECRET            credentials (read the source; write the destination when
                                     DEST_BUCKET/DEST_BUCKET_SECRET are not set)
    MEDIATREE_BUCKET_NAME            source bucket (default mediatree-videos-prod)
    DEST_BUCKET_NAME                 destination bucket (required, must already exist)
    DEST_BUCKET, DEST_BUCKET_SECRET  optional credentials of the destination, when it is in another
                                     project. The files then go through this machine (download +
                                     upload) instead of a copy made by Scaleway.
    REGION / DEST_REGION             default fr-par
    MAX_CONCURRENT_COPIES            default 16

A file already in the destination with the same size is skipped, so a run can be restarted.
"""

import argparse
import logging
import os
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

ROOT_PREFIX = "output"


def _client(key: str, secret: str, region: str):
    return boto3.client(
        service_name="s3",
        region_name=region,
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        endpoint_url=f"https://s3.{region}.scw.cloud",
        config=Config(
            max_pool_connections=64, retries={"max_attempts": 10, "mode": "adaptive"}
        ),
    )


def _list(client, bucket: str, prefix: str) -> dict[str, int]:
    """Key -> size of every object under `prefix`."""
    objects = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = obj["Size"]
    return objects


def list_channels(client, bucket: str) -> list[str]:
    """Folder names under output/ (mediatree's names, e.g. sudradio)."""
    channels = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{ROOT_PREFIX}/", Delimiter="/"
    ):
        for common in page.get("CommonPrefixes", []):
            channels.append(common["Prefix"].rstrip("/").split("/")[-1])
    return sorted(channels)


def days_between(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def day_prefix(channel: str, day: date) -> str:
    # output/franceinfotv/2026/09/07/
    return f"{ROOT_PREFIX}/{channel}/{day.strftime('%Y/%m/%d')}/"


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--channels", help="comma-separated bucket folder names (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="list what would be copied"
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    if args.end < args.start:
        parser.error("--end is before --start")

    src_bucket = os.environ.get("MEDIATREE_BUCKET_NAME", "mediatree-videos-prod")
    dest_bucket = os.environ.get("DEST_BUCKET_NAME")
    if not dest_bucket:
        parser.error("DEST_BUCKET_NAME is not set")
    if dest_bucket == src_bucket:
        parser.error("the destination is the source bucket")

    region = os.environ.get("REGION", "fr-par")
    src = _client(os.environ["BUCKET"], os.environ["BUCKET_SECRET"], region)
    # Same credentials: Scaleway copies the files itself, nothing goes through this machine
    server_side = not os.environ.get("DEST_BUCKET")
    dest = (
        src
        if server_side
        else _client(
            os.environ["DEST_BUCKET"],
            os.environ["DEST_BUCKET_SECRET"],
            os.environ.get("DEST_REGION", region),
        )
    )

    channels = (
        args.channels.split(",") if args.channels else list_channels(src, src_bucket)
    )
    days = days_between(args.start, args.end)
    logger.info(
        f"{len(channels)} channels, {len(days)} days, {src_bucket} -> {dest_bucket} "
        f"({'server-side copy' if server_side else 'download + upload'})"
    )

    transfer = TransferConfig(multipart_threshold=512 * 1024**2, max_concurrency=4)

    def copy(key: str) -> None:
        if server_side:
            dest.copy(
                {"Bucket": src_bucket, "Key": key},
                dest_bucket,
                key,
                SourceClient=src,
                Config=transfer,
            )
        else:
            with tempfile.TemporaryFile() as tmp:
                src.download_fileobj(src_bucket, key, tmp, Config=transfer)
                tmp.seek(0)
                dest.upload_fileobj(tmp, dest_bucket, key, Config=transfer)

    to_copy, skipped, total_bytes = [], 0, 0
    for channel in channels:
        for day in days:
            prefix = day_prefix(channel, day)
            source_objects = _list(src, src_bucket, prefix)
            if not source_objects:
                continue
            existing = _list(dest, dest_bucket, prefix)
            for key, size in source_objects.items():
                if existing.get(key) == size:
                    skipped += 1
                else:
                    to_copy.append(key)
                    total_bytes += size
        logger.info(f"{channel}: listed, {len(to_copy)} files to copy so far")

    logger.info(
        f"{len(to_copy)} files to copy ({total_bytes / 1024**3:.1f} GiB), "
        f"{skipped} already in the destination"
    )
    if args.dry_run or not to_copy:
        return 0

    failed = []
    max_workers = int(os.environ.get("MAX_CONCURRENT_COPIES", "16"))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(copy, key): key for key in to_copy}
        for done, future in enumerate(as_completed(futures), start=1):
            key = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"{key}: {e}")
                failed.append(key)
            if done % 500 == 0 or done == len(futures):
                logger.info(f"{done}/{len(futures)} copied, {len(failed)} failed")

    if failed:
        logger.error(f"{len(failed)} files failed, run again to retry them")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
