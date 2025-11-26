"""Export Factiva streaming messages to S3 in JSON format."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Sequence

import requests
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from sentry_sdk.crons import monitor

from quotaclimat.data_ingestion.factiva.factiva_to_s3.config import (
    FactivaS3Config,
    FactivaStreamConfig,
)
from quotaclimat.data_ingestion.factiva.factiva_to_s3.s3_utils import (
    FactivaS3Uploader,
    build_s3_key,
    ensure_directory,
    find_json_files,
)
from quotaclimat.data_ingestion.factiva.utils_data_processing.utils_extract import (
    load_json_values,
)
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

MAX_ARTICLES_PER_FILE = 1000
FOLLOWED_SOURCES_PATH = "quotaclimat/data_ingestion/factiva/inputs/followed_sources.json"


@dataclass(slots=True)
class ExportStats:
    processed_messages: int = 0
    processed_records: int = 0
    filtered_records: int = 0
    files_written: int = 0
    empty_pulls: int = 0
    errors: int = 0


class FactivaPubSubClient:
    """Client responsible for pulling messages from Factiva's Pub/Sub subscription."""

    def __init__(self, config: FactivaStreamConfig) -> None:
        self._config = config
        self._client: pubsub_v1.SubscriberClient | None = None
        self._subscription_path: str | None = None
        self._project_id: str | None = None

    def __enter__(self) -> "FactivaPubSubClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:  # pragma: no cover - context manager helper
        self.close()

    def connect(self) -> None:
        streaming_credentials = self._fetch_streaming_credentials()
        credentials = service_account.Credentials.from_service_account_info(streaming_credentials)

        client_options = None
        if self._config.region:
            client_options = {"api_endpoint": f"{self._config.region}-pubsub.googleapis.com:443"}

        self._client = pubsub_v1.SubscriberClient(credentials=credentials, client_options=client_options)
        self._project_id = streaming_credentials["project_id"]
        self._subscription_path = self._client.subscription_path(self._project_id, self._config.subscription_id)
        logging.info("Connected to Factiva subscription")

    def _fetch_streaming_credentials(self) -> dict:
        url = f"{self._config.api_host.rstrip('/')}/sns-accounts/streaming-credentials"
        headers = {"user-key": self._config.user_key}
        logging.debug("Fetching streaming credentials from %s", url)
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 401:
            raise PermissionError("Factiva authentication failed. Check the provided user key.")
        if response.status_code >= 400:
            raise RuntimeError(f"Error fetching streaming credentials ({response.status_code}): {response.text}")

        payload = response.json()
        try:
            credentials_str = payload["data"]["attributes"]["streaming_credentials"]
        except KeyError as error:  # pragma: no cover - defensive branch
            raise KeyError("Malformed streaming credential response") from error

        credentials = json.loads(credentials_str)
        if "project_id" not in credentials:
            raise KeyError("Streaming credentials do not contain a project_id")
        return credentials

    def pull(self) -> Sequence[pubsub_v1.types.ReceivedMessage]:
        if not self._client or not self._subscription_path:
            raise RuntimeError("Pub/Sub client is not connected")

        pull_request = pubsub_v1.types.PullRequest(
            subscription=self._subscription_path,
            max_messages=self._config.batch_size,
        )
        response = self._client.pull(request=pull_request, timeout=self._config.pull_timeout)
        return response.received_messages

    def acknowledge(self, ack_ids: Sequence[str]) -> None:
        if not ack_ids or not self._client or not self._subscription_path:
            return
        acknowledge_request = pubsub_v1.types.AcknowledgeRequest(
            subscription=self._subscription_path,
            ack_ids=list(ack_ids),
        )
        self._client.acknowledge(request=acknowledge_request)

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None


class FactivaStreamExporter:
    """Exports Factiva stream events to S3."""

    def __init__(self, stream_config: FactivaStreamConfig, s3_config: FactivaS3Config) -> None:
        self._stream_config = stream_config
        self._s3_config = s3_config
        self._uploader = FactivaS3Uploader(s3_config)
        self._stats = ExportStats()
        self._file_counter = 0
        self._tmp_dir = ensure_directory(stream_config.local_tmp_dir)
        self._followed_sources = self._load_followed_sources()

    def _load_followed_sources(self) -> set[str]:
        """Load the list of followed source codes from JSON file."""
        try:
            sources = load_json_values(FOLLOWED_SOURCES_PATH)
            logging.info("Loaded %d followed sources for filtering", len(sources))
            return set(sources)
        except Exception as error:
            logging.warning("Could not load followed sources from %s: %s", FOLLOWED_SOURCES_PATH, error)
            return set()

    def _filter_articles_by_source(self, records: List[dict]) -> List[dict]:
        """
        Filter articles based on source_code field.
        If no source_code field exists, keep the article.
        If source_code exists but is not in followed sources, filter it out.
        """
        if not self._followed_sources:
            # If no followed sources loaded, keep all articles
            return records

        filtered = []
        for record in records:
            # Check if source_code exists in attributes
            source_code = record.get("attributes", {}).get("source_code")
            
            if source_code is None:
                # No source_code field, keep the article
                filtered.append(record)
            elif source_code in self._followed_sources:
                # source_code is in followed sources, keep it
                filtered.append(record)
            else:
                # source_code exists but not in followed sources, filter it out
                self._stats.filtered_records += 1
                logging.debug("Filtered out article with source_code: %s", source_code)
        
        return filtered

    async def run(self) -> None:
        if self._stream_config.mock_mode:
            await asyncio.to_thread(self._run_mock_mode)
        else:
            await asyncio.to_thread(self._run_stream_mode)

        self._log_summary()

    def _run_stream_mode(self) -> None:
        logging.info("Starting Factiva stream export job")
        empty_pull_streak = 0
        all_records: List[dict] = []
        
        try:
            with FactivaPubSubClient(self._stream_config) as client:
                while empty_pull_streak < self._stream_config.max_empty_pulls:
                    try:
                        messages = client.pull()
                    except NotFound:
                        raise
                    except GoogleAPICallError as error:
                        self._stats.errors += 1
                        logging.error("Error pulling messages: %s", error)
                        time.sleep(self._stream_config.empty_pull_wait)
                        continue

                    if not messages:
                        empty_pull_streak += 1
                        self._stats.empty_pulls += 1
                        logging.info(
                            "Empty pull %s/%s", empty_pull_streak, self._stream_config.max_empty_pulls
                        )
                        time.sleep(self._stream_config.empty_pull_wait)
                        continue

                    empty_pull_streak = 0
                    ack_ids: list[str] = []

                    for message in messages:
                        ack_ids.append(message.ack_id)
                        converted = self._convert_received_message(message)
                        if converted:
                            all_records.extend(converted)

                    self._stats.processed_messages += len(messages)
                    client.acknowledge(ack_ids)

                logging.info("Reached maximum consecutive empty pulls, stopping stream export.")
                logging.info("Total articles collected: %d", len(all_records))

                # Filter articles by source_code before uploading to S3
                filtered_records = self._filter_articles_by_source(all_records)
                logging.info("Articles after filtering: %d (filtered out: %d)", 
                           len(filtered_records), len(all_records) - len(filtered_records))

                # Write filtered records to JSON files (max 1000 per file)
                if filtered_records:
                    files_created = self._write_json_files(filtered_records)
                    
                    # Upload all files to S3
                    for local_path, timestamp, filename in files_created:
                        destination_key = build_s3_key(self._s3_config.base_prefix, timestamp, filename)
                        self._uploader.upload(local_path, destination_key)
                        self._stats.files_written += 1
                    
                    self._stats.processed_records = len(filtered_records)

        except NotFound as error:
            raise RuntimeError("Factiva subscription not found. Double-check the subscription id.") from error
        except Exception as error:
            self._stats.errors += 1
            logging.error("Fatal error while exporting Factiva stream: %s", error)
            raise

    def _run_mock_mode(self) -> None:
        logging.info("Running Factiva export in mock mode from %s", self._stream_config.mock_source_dir)
        files = find_json_files(self._stream_config.mock_source_dir, self._stream_config.mock_file_pattern)
        if not files:
            logging.warning(
                "No JSON files found in %s (pattern=%s)",
                self._stream_config.mock_source_dir,
                self._stream_config.mock_file_pattern,
            )
            return

        # Accumulate all articles from all mock files
        all_records: List[dict] = []
        
        for source_file in files:
            logging.info("Reading mock file: %s", source_file)
            try:
                with source_file.open("r", encoding="utf-8") as f:
                    mock_data = json.load(f)
                
                # Extract articles from the "data" array
                data_entries = mock_data.get("data", [])
                if not isinstance(data_entries, list):
                    data_entries = [data_entries]
                
                # Add articles to the accumulator
                for entry in data_entries:
                    if isinstance(entry, dict):
                        all_records.append(entry)
                
                logging.info("Loaded %d articles from %s", len(data_entries), source_file.name)
                
            except (json.JSONDecodeError, KeyError) as error:
                logging.error("Error reading mock file %s: %s", source_file, error)
                self._stats.errors += 1
                continue

        logging.info("Total articles loaded from mock files: %d", len(all_records))

        # Filter articles by source_code before uploading to S3
        filtered_records = self._filter_articles_by_source(all_records)
        logging.info("Articles after filtering: %d (filtered out: %d)", 
                   len(filtered_records), len(all_records) - len(filtered_records))

        # Use the same logic as real mode: split into files with max 1000 articles
        if filtered_records:
            files_created = self._write_json_files(filtered_records)
            
            # Upload all files to S3
            for local_path, timestamp, filename in files_created:
                destination_key = build_s3_key(self._s3_config.base_prefix, timestamp, filename)
                self._uploader.upload(local_path, destination_key)
                self._stats.files_written += 1
                logging.info("Uploaded mock file %s to %s", filename, destination_key)
            
            self._stats.processed_records = len(filtered_records)

    def _convert_received_message(self, message: pubsub_v1.types.ReceivedMessage) -> List[dict]:
        """
        Convert a Pub/Sub message to Factiva data format.
        Returns a list of article records from the 'data' array.
        """
        try:
            payload = json.loads(message.message.data)
        except json.JSONDecodeError:
            self._stats.errors += 1
            logging.warning("Skipping message with invalid JSON payload: %s", message.message.data)
            return []

        data_entries = payload.get("data", [])
        if not isinstance(data_entries, list):
            data_entries = [data_entries]

        # Return the data entries directly (Factiva format with id, type, attributes)
        return [entry for entry in data_entries if isinstance(entry, dict)]

    def _write_json_files(self, all_records: List[dict]) -> List[tuple[Path, datetime, str]]:
        """
        Write records to JSON files, max 1000 articles per file.
        Returns list of (local_path, timestamp, filename) tuples.
        """
        if not all_records:
            return []

        files_created = []
        timestamp = datetime.now(tz=timezone.utc)

        # Split records into chunks of MAX_ARTICLES_PER_FILE
        for i in range(0, len(all_records), MAX_ARTICLES_PER_FILE):
            chunk = all_records[i:i + MAX_ARTICLES_PER_FILE]
            
            filename = self._next_filename(timestamp)
            target_path = self._tmp_dir / filename

            # Write in Factiva format: {"data": [...]}
            output_data = {"data": chunk}
            
            with target_path.open("w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            files_created.append((target_path, timestamp, filename))
            logging.info("Created JSON file with %d articles: %s", len(chunk), filename)

        return files_created

    def _next_filename(self, timestamp: datetime) -> str:
        self._file_counter += 1
        return f"{timestamp:%Y_%m_%d_%H_%M_%S}_{self._file_counter}_stream.json"

    def _log_summary(self) -> None:
        logging.info("=" * 80)
        logging.info("FACTIVA STREAM EXPORT SUMMARY")
        logging.info("Messages processed: %s", self._stats.processed_messages)
        logging.info("Records exported  : %s", self._stats.processed_records)
        logging.info("Records filtered  : %s", self._stats.filtered_records)
        logging.info("Files written     : %s", self._stats.files_written)
        logging.info("Empty pulls       : %s", self._stats.empty_pulls)
        logging.info("Errors            : %s", self._stats.errors)
        logging.info("=" * 80)


async def main() -> None:
    getLogger()
    logging.info("Launching Factiva stream to S3 job")
    sentry_init()

    stream_config = FactivaStreamConfig.from_env()
    s3_config = FactivaS3Config.from_env()
    exporter = FactivaStreamExporter(stream_config, s3_config)

    with monitor(monitor_slug="factiva-stream-to-s3"):
        health_task = asyncio.create_task(run_health_check_server())
        try:
            await exporter.run()
        finally:
            health_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover - graceful shutdown
        logging.info("Job interrupted by user")
        sys.exit(1)
    except Exception as error:
        logging.fatal("Factiva stream to S3 job failed: %s", error)
        sys.exit(1)
    sys.exit(0)
