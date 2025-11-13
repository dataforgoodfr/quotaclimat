"""Configuration helpers for the Factiva stream to S3 ingestion job."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _read_value_or_file(value: Optional[str]) -> Optional[str]:
    """Return the value or the content of the file pointed by the value."""
    if not value:
        return None

    path = Path(value)
    if path.exists():
        return path.read_text(encoding="utf-8").strip()

    return value.strip()


def _read_from_env_or_file(env_key: str, file_env_key: Optional[str] = None, fallback_path: Optional[str] = None) -> Optional[str]:
    """Read a secret either from an env var, an env var that points to a file or a fallback file."""
    raw_value = os.getenv(env_key)
    if raw_value:
        # Si la valeur ressemble à un chemin de fichier secret Docker, on la lit comme un fichier
        if raw_value.startswith('/run/secrets/'):
            return _read_value_or_file(raw_value)
        return raw_value.strip()

    if file_env_key:
        file_value = _read_value_or_file(os.getenv(file_env_key))
        if file_value:
            return file_value

    if fallback_path:
        return _read_value_or_file(fallback_path)

    return None


def _read_bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class FactivaStreamConfig:
    """Runtime configuration for fetching Factiva stream messages."""

    user_key: str
    subscription_id: str
    api_host: str
    batch_size: int
    max_empty_pulls: int
    empty_pull_wait: int
    pull_timeout: int
    mock_mode: bool
    mock_source_dir: str
    mock_file_pattern: str
    local_tmp_dir: str
    region: Optional[str]

    @classmethod
    def from_env(cls) -> "FactivaStreamConfig":
        user_key = _read_from_env_or_file(
            "FACTIVA_USERKEY",
            file_env_key="FACTIVA_USERKEY_FILE",
        )
        subscription_id = _read_from_env_or_file(
            "FACTIVA_SUBSCRIPTION_ID",
            file_env_key="FACTIVA_SUBSCRIPTION_ID_FILE",
        )

        if not user_key:
            raise ValueError(
                "Factiva user key is missing. Set FACTIVA_USERKEY or FACTIVA_USERKEY_FILE."
            )
        if not subscription_id:
            raise ValueError(
                "Factiva subscription id is missing. Set FACTIVA_SUBSCRIPTION_ID or FACTIVA_SUBSCRIPTION_ID_FILE."
            )

        api_host = os.getenv("FACTIVA_API_HOST", "https://api.dowjones.com").strip()
        batch_size = int(os.getenv("FACTIVA_BATCH_SIZE", "100"))
        max_empty_pulls = int(os.getenv("FACTIVA_MAX_EMPTY_PULLS", "3"))
        empty_pull_wait = int(os.getenv("FACTIVA_EMPTY_PULL_WAIT", "5"))
        pull_timeout = int(os.getenv("FACTIVA_PULL_TIMEOUT", "30"))

        mock_mode = _read_bool("FACTIVA_MOCK_MODE", default=False)
        mock_source_dir = os.getenv("FACTIVA_MOCK_SOURCE_DIR", "./test/factiva_mock").strip()
        mock_file_pattern = os.getenv("FACTIVA_MOCK_FILE_PATTERN", "*.json").strip()
        local_tmp_dir = os.getenv("FACTIVA_LOCAL_TMP", "/tmp/factiva").strip()
        region = os.getenv("FACTIVA_PUBSUB_REGION")

        return cls(
            user_key=user_key,
            subscription_id=subscription_id,
            api_host=api_host,
            batch_size=batch_size,
            max_empty_pulls=max_empty_pulls,
            empty_pull_wait=empty_pull_wait,
            pull_timeout=pull_timeout,
            mock_mode=mock_mode,
            mock_source_dir=mock_source_dir,
            mock_file_pattern=mock_file_pattern,
            local_tmp_dir=local_tmp_dir,
            region=region.strip() if region else None,
        )


@dataclass(slots=True)
class FactivaS3Config:
    """Configuration for S3 uploads."""

    bucket_name: str
    base_prefix: str
    access_key: str
    secret_key: str
    endpoint_url: str
    region: str
    delete_local_files: bool

    @classmethod
    def from_env(cls) -> "FactivaS3Config":
        bucket_name = os.getenv("FACTIVA_S3_BUCKET", "factiva").strip()
        base_prefix = os.getenv("FACTIVA_S3_PREFIX", "country_france").strip()

        access_key = _read_from_env_or_file(
            "BUCKET",
            file_env_key="BUCKET_FILE",
        )
        secret_key = _read_from_env_or_file(
            "BUCKET_SECRET",
            file_env_key="BUCKET_SECRET_FILE",
        )

        if not access_key or not secret_key:
            raise ValueError(
                "S3 credentials are missing. Set BUCKET/BUCKET_SECRET or BUCKET_FILE/BUCKET_SECRET_FILE."
            )

        endpoint_url = os.getenv("FACTIVA_S3_ENDPOINT", "https://s3.fr-par.scw.cloud").strip()
        region = os.getenv("FACTIVA_S3_REGION", "fr-par").strip()
        delete_local = _read_bool("FACTIVA_DELETE_LOCAL_FILES", default=True)

        return cls(
            bucket_name=bucket_name,
            base_prefix=base_prefix,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
            region=region,
            delete_local_files=delete_local,
        )
