"""Minimal S3 helpers for the RAG pipeline: list/download source PDFs,
upload/download a built index directory.

Scaleway Object Storage (same account/region as the rest of the pipeline),
not duckdb/s3fs — a plain boto3 client is enough for whole-file transfers.

Uses its own EMBEDDINGS_BUCKET* credentials/bucket, deliberately separate
from the BUCKET*/BUCKET_NAME ones run_rag_agent_mistral.py passes to duckdb
to read segment plaintext from the main mediatree bucket — the expert-model
source documents/index live in a different bucket (and may use different
credentials), so the two must not collide when both are set in the same
container (the rag service does both).
"""

import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "fr-par"


def get_secret_docker(secret_name: str) -> str:
    value = os.environ.get(secret_name, "")
    if value and os.path.exists(value):
        with open(value, "r") as f:
            return f.read().strip()
    return value


ACCESS_KEY = get_secret_docker("EMBEDDINGS_BUCKET")
SECRET_KEY = get_secret_docker("EMBEDDINGS_BUCKET_SECRET")
BUCKET_NAME = os.environ.get("EMBEDDINGS_BUCKET_NAME")
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"


def get_s3_client():
    return boto3.client(
        service_name="s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )


def download_prefix(s3_client, bucket: str, prefix: str, local_dir: Path) -> list[Path]:
    """Download every object under prefix into local_dir (flat, by basename). Returns the local paths."""
    local_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            dest = local_dir / Path(key).name
            logging.info(f"  Downloading s3://{bucket}/{key} -> {dest}")
            s3_client.download_file(bucket, key, str(dest))
            downloaded.append(dest)
    return downloaded


def get_object_etags(s3_client, bucket: str, prefix: str) -> dict[str, str]:
    """Return {key: etag} for every object under prefix, without downloading
    anything — cheap enough to call on every run to detect source changes."""
    etags = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            etags[key] = obj["ETag"].strip('"')
    return etags


def read_text(s3_client, bucket: str, key: str) -> str | None:
    """Fetch a single small text object, or None if it doesn't exist."""
    try:
        return s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def upload_folder(s3_client, local_dir: Path, bucket: str, prefix: str) -> list[str]:
    """Upload every file directly under local_dir (non-recursive) to s3://bucket/prefix/<name>."""
    uploaded = []
    for path in sorted(local_dir.iterdir()):
        if not path.is_file():
            continue
        key = f"{prefix.rstrip('/')}/{path.name}"
        logging.info(f"  Uploading {path} -> s3://{bucket}/{key}")
        s3_client.upload_file(str(path), bucket, key)
        uploaded.append(key)
    return uploaded
