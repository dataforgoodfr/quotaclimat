"""S3 helper utilities for the Factiva stream exporter."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

import boto3
from botocore.config import Config as BotoConfig

from quotaclimat.data_ingestion.factiva.factiva_to_s3.config import FactivaS3Config


def ensure_directory(path: str) -> Path:
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def build_s3_key(base_prefix: str, timestamp: datetime, filename: str) -> str:
    year = timestamp.year
    month = timestamp.month
    return f"{base_prefix}/articles/year_{year}/month_{month:02d}/{filename}"


@dataclass(slots=True)
class FactivaS3Uploader:
    """Upload JSON files to the configured S3 bucket."""

    config: FactivaS3Config
    _client: Optional[Any] = None

    def __post_init__(self) -> None:
        session_config = BotoConfig(signature_version="s3v4")
        self._client = boto3.client(
            "s3",
            region_name=self.config.region,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            endpoint_url=self.config.endpoint_url,
            config=session_config,
        )

    def upload(self, local_path: Path, destination_key: str) -> None:
        logging.info("Uploading %s to s3://%s/%s", local_path, self.config.bucket_name, destination_key)
        self._client.upload_file(str(local_path), self.config.bucket_name, destination_key)

        if self.config.delete_local_files:
            try:
                local_path.unlink(missing_ok=True)
            except Exception as error:  # pragma: no cover - defensive cleanup
                logging.warning("Unable to delete temporary file %s: %s", local_path, error)

    def upload_many(self, pairs: Iterable[tuple[Path, str]]) -> None:
        for local_path, key in pairs:
            self.upload(local_path, key)


def find_json_files(folder: str, pattern: str = "*.json") -> list[Path]:
    base = Path(folder)
    if not base.exists():
        return []
    return sorted(base.glob(pattern))
