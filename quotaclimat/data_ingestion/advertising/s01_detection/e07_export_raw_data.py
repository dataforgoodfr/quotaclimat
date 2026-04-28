import json
import logging
import os
import time
from pathlib import Path
from urllib.parse import quote

import boto3

from .tools.common_objects import Chunk, Fragment
from .tools.visualizer.weekly_viewer import generate_weekly_viewer

logger = logging.getLogger(__name__)

ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("ADVERTISING_BUCKET_NAME")
REGION = os.environ.get("ADVERTISING_BUCKET_REGION", "fr-par")
ENDPOINT_URL = f"https://s3.{REGION}.scw.cloud"
REPORTS_S3_PREFIX = "reports"
RAW_CHUNKS_S3_PREFIX = "raw_chunks"
S3_FOLDER_ACCESS_URL = os.environ.get(
    "S3_FOLDER_ACCESS_URL",
    "https://console.scaleway.com/object-storage/buckets/{REGION}/{BUCKET_NAME}/files/{ENCODED_KEY}/",
)


def _s3_folder_url(s3_key: str) -> str:
    encoded_key = quote(s3_key, safe="/")

    return S3_FOLDER_ACCESS_URL.format(
        REGION=REGION,
        BUCKET_NAME=BUCKET_NAME,
        ENCODED_KEY=encoded_key,
    )


class Timer:
    def __init__(self, timings: dict[str, float], key: str):
        self.timings = timings
        self.key = key

    def __enter__(self):
        self.t0 = time.monotonic()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.timings[self.key] = time.monotonic() - self.t0


class TimingCollector:
    def __init__(self):
        self.timings: dict[str, float] = {}

    def measure(self, key):
        return Timer(self.timings, key)

    @property
    def durations(self) -> list[tuple[str, float]]:
        return self.timings.items()


def get_s3_client():
    return boto3.client(
        service_name="s3",
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )


def upload_to_s3(local_path: Path, s3_key: str, s3_client):
    try:
        s3_client.upload_file(
            str(local_path),
            BUCKET_NAME,
            s3_key,
            {"StorageClass": "ONEZONE_IA"},
        )
        logger.info(f"Uploaded {_s3_folder_url(s3_key)}")
    except Exception as e:
        logger.error(f"Failed to upload {local_path} to S3: {e}")


class Report:
    def __init__(
        self,
        reports_name: str,
        chunk_hash: str,
        params: dict,
        local_path: str,
    ):
        self.params = params
        self.reports_name = reports_name

        self.html_report_path = local_path / f"{self.reports_name}.html"
        self.text_report_path = local_path / f"{self.reports_name}.txt"

    def generate(
        self,
        fragments: list[Fragment],
        timings: TimingCollector,
        annotations: list[dict] | None = None,
    ):
        html_report = generate_weekly_viewer(
            fragments=fragments,
            annotations=annotations,
            params_summary=self.params,
        )

        with open(self.html_report_path, "w", encoding="utf-8") as f:
            f.write(html_report)

        timing_lines = [
            "Timing report for:",
            json.dumps(self.params),
            "",
        ]
        for step, duration in timings.durations:
            timing_lines.append(f"  {step:<30} {duration:>8.2f}s")

        timing_report = "\n".join(timing_lines)

        with open(self.text_report_path, "w", encoding="utf-8") as f:
            f.write(timing_report)

    def save_to_s3(self, report_folder: str):
        s3_client = get_s3_client()
        s3_folder = f"{REPORTS_S3_PREFIX}/{report_folder}"

        s3_client.upload_file(
            str(self.html_report_path),
            BUCKET_NAME,
            f"{s3_folder}/{self.reports_name}.html",
            {"StorageClass": "ONEZONE_IA"},
        )
        s3_client.upload_file(
            str(self.text_report_path),
            BUCKET_NAME,
            f"{s3_folder}/{self.reports_name}.txt",
            {"StorageClass": "ONEZONE_IA"},
        )

        logger.info(f"Reports uploaded in folder: {_s3_folder_url(s3_folder)}")


def export_chunks_to_s3(chunks: list[Chunk], report_folder: str):
    s3_client = get_s3_client()
    s3_path = f"{RAW_CHUNKS_S3_PREFIX}/{report_folder}/chunks.json"
    s3_client.put_object(
        Body=json.dumps([c.to_dict() for c in chunks]),
        Bucket=BUCKET_NAME,
        Key=s3_path,
        StorageClass="GLACIER",
    )
