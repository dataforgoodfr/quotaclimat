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

        return cls(
            user_key=user_key,
            minimal_word_count=minimal_word_count,
            language_code=language_code,
            followed_sources_path=followed_sources_path,
            local_tmp_dir=local_tmp_dir,
            days_before_today=days_before_today,
            analysis_period_duration=analysis_period_duration,
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

    async def run(self) -> None:
        """Execute the Time Series extraction and upload to S3."""
        await asyncio.to_thread(self._run_extraction)
        self._log_summary()

    def _run_extraction(self) -> None:
        """Main extraction logic."""
        logging.info("Starting Factiva Time Series statistics export job")

        # Calculate date range
        end_date = datetime.now(tz=timezone.utc) - timedelta(
            days=self._stats_config.days_before_today
        )
        start_date = end_date - timedelta(days=self._stats_config.analysis_period_duration)

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        logging.info(f"Date range: {start_date_str} to {end_date_str}")

        # Load source codes
        source_codes = self._load_source_codes()
        logging.info(f"Loaded {len(source_codes)} source codes")

        # Generate regex pattern from keywords
        regex_pattern = self._generate_keyword_regex()
        logging.info(f"Generated regex pattern with {len(regex_pattern)} characters")

        # Step 1: Submit Time Series job
        logging.info("Submitting Time Series job...")
        submit_result = submit_time_series(
            source_codes=source_codes,
            start_date=start_date_str,
            end_date=end_date_str,
            minimal_word_count=self._stats_config.minimal_word_count,
            language_code=self._stats_config.language_code,
            regex_pattern=regex_pattern,
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

        # Step 2: Poll until completion
        logging.info("Polling Time Series job...")
        poll_result = poll_time_series(
            analytics_id=analytics_id,
            user_key=self._stats_config.user_key,
        )

        if not poll_result["success"]:
            self._stats.errors += 1
            logging.error(f"Time Series job failed: {poll_result['error']}")
            raise RuntimeError(f"Time Series job failed: {poll_result['error']}")

        download_link = poll_result["download_link"]
        logging.info(f"Time Series job completed! Download link: {download_link}")

        # Step 3: Download results
        logging.info("Downloading Time Series results...")
        results = download_time_series_results(
            download_link=download_link,
            user_key=self._stats_config.user_key,
        )

        logging.info(f"Downloaded {len(results)} time series records")
        self._stats.time_series_records = len(results)

        # Step 4: Save to local file
        timestamp = datetime.now(tz=timezone.utc)
        counter = 1  # Simple counter for this extraction
        filename = self._generate_filename(timestamp, counter)
        local_path = self._tmp_dir / filename

        # Save as JSON Lines format (one object per line)
        with local_path.open("w", encoding="utf-8") as f:
            for record in results:
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")

        logging.info(f"Saved results to {local_path}")

        # Step 5: Upload to S3
        destination_key = self._build_s3_key(timestamp, filename)
        self._uploader.upload(local_path, destination_key)
        self._stats.files_written += 1
        logging.info(f"Uploaded to s3://{self._s3_config.bucket_name}/{destination_key}")

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

