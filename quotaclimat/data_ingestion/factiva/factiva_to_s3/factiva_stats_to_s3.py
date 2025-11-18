"""Export Factiva Time Series statistics to S3 in JSON format."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from sentry_sdk.crons import monitor

from quotaclimat.data_ingestion.factiva.factiva_to_s3.config import FactivaS3Config
from quotaclimat.data_ingestion.factiva.factiva_to_s3.factiva_api_utils import (
    download_time_series_results,
    poll_time_series,
    submit_time_series,
)
from quotaclimat.data_ingestion.factiva.factiva_to_s3.s3_utils import (
    FactivaS3Uploader,
    ensure_directory,
)
from quotaclimat.data_ingestion.factiva.utils_data_processing.detect_keywords import (
    create_combined_regex_pattern,
)
from quotaclimat.data_ingestion.factiva.utils_data_processing.utils_extract import (
    load_json_values,
)
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init


@dataclass(slots=True)
class FactivaStatsConfig:
    """Configuration for Factiva stats extraction."""

    user_key: str
    minimal_word_count: int
    language_code: str
    followed_sources_path: str
    local_tmp_dir: str
    days_before_today: int
    analysis_period_duration: int
    extract_stats: bool
    select_date: bool
    start_date: str | None
    end_date: str | None

    @classmethod
    def from_env(cls) -> "FactivaStatsConfig":
        # Try FACTIVA_USERKEY first (could be direct value or file path)
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key:
            # If it looks like a file path (Docker secret), read the file
            if user_key.startswith('/run/secrets/') or user_key.startswith('/'):
                from pathlib import Path
                user_key = Path(user_key).read_text(encoding="utf-8").strip()
        
        if not user_key:
            raise ValueError(
                "FACTIVA_USERKEY environment variable is required"
            )

        minimal_word_count = int(os.getenv("FACTIVA_MINIMAL_WORD_COUNT", "10"))
        language_code = os.getenv("FACTIVA_LANGUAGE_CODE", "fr")
        followed_sources_path = os.getenv(
            "FACTIVA_FOLLOWED_SOURCES_PATH",
            "quotaclimat/data_ingestion/factiva/inputs/followed_sources.json",
        )
        local_tmp_dir = os.getenv("FACTIVA_LOCAL_TMP", "/tmp/factiva_stats")
        days_before_today = int(os.getenv("FACTIVA_DAYS_BEFORE_TODAY", "7"))
        analysis_period_duration = int(os.getenv("FACTIVA_STATS_ANALYSIS_DURATION", "7"))
        
        # New configuration variables
        extract_stats = os.getenv("EXTRACT_STATS", "true").lower() == "true"
        select_date = os.getenv("SELECT_DATE", "false").lower() == "true"
        start_date = os.getenv("START_DATE") if select_date else None
        end_date = os.getenv("END_DATE") if select_date else None
        
        # Validate custom date configuration
        if select_date and (not start_date or not end_date):
            raise ValueError(
                "When SELECT_DATE is true, both START_DATE and END_DATE must be provided"
            )

        return cls(
            user_key=user_key,
            minimal_word_count=minimal_word_count,
            language_code=language_code,
            followed_sources_path=followed_sources_path,
            local_tmp_dir=local_tmp_dir,
            days_before_today=days_before_today,
            analysis_period_duration=analysis_period_duration,
            extract_stats=extract_stats,
            select_date=select_date,
            start_date=start_date,
            end_date=end_date,
        )


@dataclass(slots=True)
class ExportStats:
    """Statistics for the export job."""

    time_series_records: int = 0
    files_written: int = 0
    errors: int = 0


class FactivaStatsExporter:
    """Exports Factiva Time Series statistics to S3."""

    def __init__(
        self, stats_config: FactivaStatsConfig, s3_config: FactivaS3Config
    ) -> None:
        self._stats_config = stats_config
        self._s3_config = s3_config
        self._uploader = FactivaS3Uploader(s3_config)
        self._stats = ExportStats()
        self._tmp_dir = ensure_directory(stats_config.local_tmp_dir)
        logging.info(f"Temporary directory initialized at: {self._tmp_dir}")

    async def run(self) -> None:
        """Execute the Time Series extraction and upload to S3."""
        await asyncio.to_thread(self._run_extraction)
        self._log_summary()

    def _run_extraction(self) -> None:
        """Main extraction logic."""
        logging.info("Starting Factiva Time Series statistics export job")

        # Step 1: Process any unprocessed IDs from the last month
        self._process_unprocessed_ids()

        # Step 2: Check if we should extract new stats
        if not self._stats_config.extract_stats:
            logging.info("EXTRACT_STATS is False, skipping new stats extraction")
            return

        # Step 3: Calculate date range
        if self._stats_config.select_date:
            # Use custom dates
            start_date_str = self._stats_config.start_date
            end_date_str = self._stats_config.end_date
            logging.info(f"Using custom date range: {start_date_str} to {end_date_str}")
        else:
            # Use default logic
            end_date = datetime.now(tz=timezone.utc) - timedelta(
                days=self._stats_config.days_before_today
            )
            start_date = end_date - timedelta(days=self._stats_config.analysis_period_duration)
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            logging.info(f"Using calculated date range: {start_date_str} to {end_date_str}")

        # Load source codes
        source_codes = self._load_source_codes()
        logging.info(f"Loaded {len(source_codes)} source codes")

        # Step 4: Submit Time Series job
        logging.info("Submitting Time Series job...")
        submit_result = submit_time_series(
            source_codes=source_codes,
            start_date=start_date_str,
            end_date=end_date_str,
            minimal_word_count=self._stats_config.minimal_word_count,
            language_code=self._stats_config.language_code,
            regex_pattern=None,
            frequency="DAY",
            group_dimensions="source_code",
            user_key=self._stats_config.user_key,
        )

        if not submit_result["success"]:
            self._stats.errors += 1
            logging.error(
                f"Failed to submit Time Series job: {submit_result['error']}"
            )
            raise RuntimeError(f"Time Series job submission failed: {submit_result['error']}")

        analytics_id = submit_result["analytics_id"]
        logging.info(f"Time Series job submitted with ID: {analytics_id}")

        # Step 5: Store analytics ID in S3
        timestamp = datetime.now(tz=timezone.utc)
        counter = 1  # Simple counter for this extraction
        id_s3_key = self._store_analytics_id(analytics_id, timestamp, counter)

        # Step 6: Poll until completion
        logging.info("Polling Time Series job...")
        poll_result = poll_time_series(
            analytics_id=analytics_id,
            user_key=self._stats_config.user_key,
        )

        if not poll_result["success"]:
            self._stats.errors += 1
            logging.error(f"Time Series job failed: {poll_result['error']}")
            # Don't mark as processed since it failed
            raise RuntimeError(f"Time Series job failed: {poll_result['error']}")

        download_link = poll_result["download_link"]
        logging.info(f"Time Series job completed! Download link: {download_link}")

        # Step 7: Download results
        logging.info("Downloading Time Series results...")
        results = download_time_series_results(
            download_link=download_link,
            user_key=self._stats_config.user_key,
        )

        logging.info(f"Downloaded {len(results)} time series records")
        self._stats.time_series_records = len(results)

        # Step 8: Save to local file
        filename = self._generate_filename(timestamp, counter)
        local_path = self._tmp_dir / filename

        # Save as JSON Lines format (one object per line)
        with local_path.open("w", encoding="utf-8") as f:
            for record in results:
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")

        logging.info(f"Saved results to {local_path}")

        # Step 9: Upload to S3
        destination_key = self._build_s3_key(timestamp, filename)
        self._uploader.upload(local_path, destination_key)
        self._stats.files_written += 1
        logging.info(f"Uploaded to s3://{self._s3_config.bucket_name}/{destination_key}")

        # Clean up stats data file
        try:
            if local_path.exists():
                local_path.unlink()
                logging.info(f"Cleaned up stats data file: {local_path}")
        except Exception as e:
            logging.warning(f"Could not delete stats data file {local_path}: {e}")

        # Step 10: Mark analytics ID as processed
        self._mark_id_as_processed(id_s3_key, analytics_id)

    def _load_source_codes(self) -> List[str]:
        """Load source codes from the JSON file."""
        return load_json_values(self._stats_config.followed_sources_path)

    def _generate_keyword_regex(self) -> str:
        """Generate regex pattern from THEME_KEYWORDS."""
        keywords_filtered = []
        for lst in THEME_KEYWORDS.values():
            for entry in lst:
                if (
                    not entry.get("high_risk_of_false_positive", True)
                    and entry.get("language") == "french"
                ):
                    keywords_filtered.append(entry.get("keyword"))

        # Remove duplicates
        keywords_filtered = list(set(keywords_filtered))
        logging.info(f"Filtered {len(keywords_filtered)} keywords")

        # Create BigQuery-compatible regex pattern
        keyword_regex = create_combined_regex_pattern(
            keywords_filtered, bigquery_compatible=True
        )
        return keyword_regex

    def _generate_filename(self, timestamp: datetime, counter: int) -> str:
        """Generate filename for the statistics file."""
        return f"{timestamp:%Y_%m_%d_%H_%M_%S}_{counter}_stats.json"

    def _build_s3_key(self, timestamp: datetime, filename: str) -> str:
        """Build S3 key for the statistics file."""
        year = timestamp.year
        month = timestamp.month
        return f"{self._s3_config.base_prefix}/nb_articles/year_{year}/month_{month:02d}/{filename}"

    def _generate_id_filename(self, timestamp: datetime, counter: int, processed: bool = False) -> str:
        """Generate filename for analytics ID tracking."""
        suffix = "_PROCESSED" if processed else ""
        return f"{timestamp:%Y_%m_%d_%H_%M_%S}_{counter}_ts_id{suffix}.txt"

    def _build_id_s3_key(self, timestamp: datetime, filename: str) -> str:
        """Build S3 key for analytics ID tracking file."""
        year = timestamp.year
        month = timestamp.month
        return f"{self._s3_config.base_prefix}/id_extracts/time_series_id/year_{year}/month_{month:02d}/{filename}"

    def _store_analytics_id(self, analytics_id: str, timestamp: datetime, counter: int) -> str:
        """Store analytics ID in S3 and return the S3 key."""
        filename = self._generate_id_filename(timestamp, counter, processed=False)
        local_path = self._tmp_dir / filename
        
        # Ensure the directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)
        logging.info(f"Creating analytics ID file at: {local_path}")
        
        # Write analytics ID to local file
        with local_path.open("w", encoding="utf-8") as f:
            f.write(analytics_id)
        
        # Upload to S3
        s3_key = self._build_id_s3_key(timestamp, filename)
        self._uploader.upload(local_path, s3_key)
        logging.info(f"Stored analytics ID {analytics_id} at s3://{self._s3_config.bucket_name}/{s3_key}")
        
        # Clean up local file
        try:
            if local_path.exists():
                local_path.unlink()
                logging.info(f"Cleaned up local file: {local_path}")
        except Exception as e:
            logging.warning(f"Could not delete local file {local_path}: {e}")
        
        return s3_key

    def _mark_id_as_processed(self, original_s3_key: str, analytics_id: str) -> None:
        """Mark an analytics ID as processed by renaming the S3 file (copy + delete)."""
        try:
            # Generate the new key with _PROCESSED suffix
            if not original_s3_key.endswith("_ts_id.txt"):
                logging.warning(f"Original key doesn't end with _ts_id.txt: {original_s3_key}")
                return
            
            processed_s3_key = original_s3_key.replace("_ts_id.txt", "_ts_id_PROCESSED.txt")
            
            logging.info(f"Renaming S3 file from {original_s3_key} to {processed_s3_key}")
            
            # Step 1: Copy the file to the new name
            self._uploader._client.copy_object(
                Bucket=self._s3_config.bucket_name,
                CopySource={'Bucket': self._s3_config.bucket_name, 'Key': original_s3_key},
                Key=processed_s3_key
            )
            logging.info(f"Copied {original_s3_key} to {processed_s3_key}")
            
            # Step 2: Delete the original file
            self._uploader._client.delete_object(
                Bucket=self._s3_config.bucket_name,
                Key=original_s3_key
            )
            logging.info(f"Deleted original file {original_s3_key}")
            logging.info(f"✓ Successfully marked analytics ID {analytics_id} as PROCESSED")
            
        except Exception as e:
            logging.warning(f"Could not rename S3 file {original_s3_key}: {e}")

    def _find_unprocessed_ids(self) -> List[tuple[str, str]]:
        """Find unprocessed analytics IDs from the last month."""
        from botocore.exceptions import ClientError
        
        unprocessed_ids = []
        
        # Calculate date range for the last month
        now = datetime.now(tz=timezone.utc)
        last_month = now - timedelta(days=30)
        
        logging.info(f"Searching for unprocessed IDs between {last_month.date()} and {now.date()}")
        
        try:
            # List objects in the ID extracts directory for the last month
            prefix = f"{self._s3_config.base_prefix}/id_extracts/time_series_id/"
            logging.info(f"Searching in S3 prefix: {prefix}")
            
            # We need to check multiple months potentially
            months_to_check = set()
            # Add the month from last_month
            months_to_check.add((last_month.year, last_month.month))
            # Add the current month
            months_to_check.add((now.year, now.month))
            
            # If there are months in between, add them too
            current_date = last_month.replace(day=1)
            end_date = now.replace(day=1)
            while current_date <= end_date:
                months_to_check.add((current_date.year, current_date.month))
                # Move to first day of next month
                if current_date.month == 12:
                    current_date = current_date.replace(year=current_date.year + 1, month=1)
                else:
                    current_date = current_date.replace(month=current_date.month + 1)
            
            # Convert to sorted list
            months_to_check = sorted(list(months_to_check))
            logging.info(f"Months to check: {[(y, m) for y, m in months_to_check]}")
            
            for year, month in months_to_check:
                month_prefix = f"{prefix}year_{year}/month_{month:02d}/"
                logging.info(f"Checking month: {year}-{month:02d} (prefix: {month_prefix})")
                
                try:
                    response = self._uploader._client.list_objects_v2(
                        Bucket=self._s3_config.bucket_name,
                        Prefix=month_prefix
                    )
                    
                    if 'Contents' in response:
                        logging.info(f"Found {len(response['Contents'])} objects in {month_prefix}")
                        for obj in response['Contents']:
                            key = obj['Key']
                            filename = key.split("/")[-1]
                            
                            # Check if it's an ID file (ending with _ts_id.txt but NOT _ts_id_PROCESSED.txt)
                            if filename.endswith("_ts_id.txt") and not filename.endswith("_ts_id_PROCESSED.txt"):
                                logging.info(f"Found unprocessed ID file: {filename}")
                                # Download the analytics ID
                                try:
                                    response = self._uploader._client.get_object(
                                        Bucket=self._s3_config.bucket_name,
                                        Key=key
                                    )
                                    analytics_id = response['Body'].read().decode('utf-8').strip()
                                    unprocessed_ids.append((analytics_id, key))
                                    logging.info(f"✓ Added unprocessed analytics ID to recovery list: {analytics_id}")
                                except Exception as e:
                                    logging.warning(f"Could not read analytics ID from {key}: {e}")
                
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchKey':
                        logging.warning(f"Error listing objects in {month_prefix}: {e}")
        
        except Exception as e:
            logging.warning(f"Error finding unprocessed IDs: {e}")
        
        return unprocessed_ids

    def _process_unprocessed_id(self, analytics_id: str, original_s3_key: str) -> bool:
        """Process a single unprocessed analytics ID with max_attempts=1."""
        logging.info(f"Attempting to recover analytics ID: {analytics_id}")
        
        try:
            # Poll with max_attempts=1 (single attempt)
            poll_result = poll_time_series(
                analytics_id=analytics_id,
                user_key=self._stats_config.user_key,
                max_attempts=1,
            )
            
            if not poll_result["success"]:
                logging.warning(f"Failed to recover analytics ID {analytics_id}: {poll_result['error']}")
                return False
            
            download_link = poll_result["download_link"]
            logging.info(f"Successfully recovered analytics ID {analytics_id}, download link: {download_link}")
            
            # Download results
            results = download_time_series_results(
                download_link=download_link,
                user_key=self._stats_config.user_key,
            )
            
            logging.info(f"Downloaded {len(results)} time series records for recovered ID {analytics_id}")
            self._stats.time_series_records += len(results)
            
            # Save to local file
            timestamp = datetime.now(tz=timezone.utc)
            counter = 1  # Simple counter for recovered extraction
            filename = self._generate_filename(timestamp, counter)
            local_path = self._tmp_dir / filename
            
            # Ensure the directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)
            logging.info(f"Creating recovered data file at: {local_path}")
            
            # Save as JSON Lines format
            with local_path.open("w", encoding="utf-8") as f:
                for record in results:
                    json.dump(record, f, ensure_ascii=False)
                    f.write("\n")
            
            # Upload to S3
            destination_key = self._build_s3_key(timestamp, filename)
            self._uploader.upload(local_path, destination_key)
            self._stats.files_written += 1
            logging.info(f"Uploaded recovered data to s3://{self._s3_config.bucket_name}/{destination_key}")
            
            # Clean up local file
            try:
                if local_path.exists():
                    local_path.unlink()
                    logging.info(f"Cleaned up recovered data file: {local_path}")
            except Exception as e:
                logging.warning(f"Could not delete recovered data file {local_path}: {e}")
            
            # Mark the ID as processed
            self._mark_id_as_processed(original_s3_key, analytics_id)
            
            return True
            
        except Exception as e:
            logging.warning(f"Error processing unprocessed ID {analytics_id}: {e}")
            return False

    def _process_unprocessed_ids(self) -> None:
        """Process all unprocessed IDs found in the last month."""
        logging.info("=" * 80)
        logging.info("CHECKING FOR UNPROCESSED ANALYTICS IDs FROM THE LAST MONTH")
        logging.info("=" * 80)
        
        unprocessed_ids = self._find_unprocessed_ids()
        
        if not unprocessed_ids:
            logging.info("No unprocessed analytics IDs found - all IDs have been processed")
            logging.info("=" * 80)
            return
        
        logging.info(f"Found {len(unprocessed_ids)} unprocessed analytics IDs to recover")
        logging.info("=" * 80)
        
        for idx, (analytics_id, original_s3_key) in enumerate(unprocessed_ids, 1):
            logging.info(f"Processing unprocessed ID {idx}/{len(unprocessed_ids)}: {analytics_id}")
            success = self._process_unprocessed_id(analytics_id, original_s3_key)
            if success:
                logging.info(f"✓ Successfully recovered and processed analytics ID: {analytics_id}")
            else:
                logging.warning(f"✗ Failed to recover analytics ID: {analytics_id}")
        
        logging.info("=" * 80)
        logging.info("FINISHED PROCESSING UNPROCESSED IDs")
        logging.info("=" * 80)

    def _log_summary(self) -> None:
        """Log summary statistics."""
        logging.info("=" * 80)
        logging.info("FACTIVA TIME SERIES STATS EXPORT SUMMARY")
        logging.info(f"Time series records: {self._stats.time_series_records}")
        logging.info(f"Files written      : {self._stats.files_written}")
        logging.info(f"Errors             : {self._stats.errors}")
        logging.info("=" * 80)


async def main() -> None:
    """Main entry point."""
    getLogger()
    logging.info("Launching Factiva Time Series stats to S3 job")
    sentry_init()

    stats_config = FactivaStatsConfig.from_env()
    s3_config = FactivaS3Config.from_env()
    exporter = FactivaStatsExporter(stats_config, s3_config)

    with monitor(monitor_slug="factiva-stats-to-s3"):
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
        logging.fatal(f"Factiva stats to S3 job failed: {error}")
        sys.exit(1)
    sys.exit(0)

