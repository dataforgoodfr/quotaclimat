"""Process Le Monde FTP data from S3 and load into PostgreSQL.

This script handles Le Monde articles from FTP:
1. Article JSON files → lemonde_ftp_articles table with keyword extraction
2. Statistics JSON files → stats_factiva_articles table
3. Upsert recent articles with keywords from lemonde_ftp_articles → factiva_articles
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

import boto3
from botocore.config import Config as BotoConfig
from dateutil import parser as date_parser
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from postgres.database_connection import connect_to_db
from postgres.schemas.factiva_models import (
    Factiva_Article,
    LeMonde_FTP_Article,
    Stats_Factiva_Article,
)
from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    build_article_text,
    extract_keyword_data_from_article,
)
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init


@dataclass(slots=True)
class S3Config:
    """Configuration for S3 access."""

    bucket_name: str
    region: str
    access_key: str
    secret_key: str
    endpoint_url: Optional[str]
    base_prefix: str

    @classmethod
    def from_env(cls, bucket_env_var: str = "S3_BUCKET_NAME_LEMONDE") -> "S3Config":
        bucket_name = os.getenv(bucket_env_var)
        if not bucket_name:
            raise ValueError(f"{bucket_env_var} environment variable is required")

        region = os.getenv("S3_REGION", "fr-par")
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        base_prefix = os.getenv("S3_BASE_PREFIX", "country_france")

        # Handle S3 credentials - can be direct values or file paths (Docker secrets)
        access_key = os.getenv("S3_ACCESS_KEY")
        secret_key = os.getenv("S3_SECRET_KEY")

        # If they look like file paths, read the file content
        if access_key and access_key.startswith("/"):
            try:
                with open(access_key, "r") as f:
                    access_key = f.read().strip()
            except Exception as e:
                raise ValueError(
                    f"Could not read S3_ACCESS_KEY from file {access_key}: {e}"
                )

        if secret_key and secret_key.startswith("/"):
            try:
                with open(secret_key, "r") as f:
                    secret_key = f.read().strip()
            except Exception as e:
                raise ValueError(
                    f"Could not read S3_SECRET_KEY from file {secret_key}: {e}"
                )

        if not access_key or not secret_key:
            raise ValueError("S3_ACCESS_KEY and S3_SECRET_KEY are required")

        return cls(
            bucket_name=bucket_name,
            region=region,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
            base_prefix=base_prefix,
        )


@dataclass(slots=True)
class ProcessingConfig:
    """Configuration for processing options."""

    process_articles: bool
    process_stats: bool
    upsert_to_factiva: bool
    lookback_days: int
    lemonde_upsert_lookback_hours: int
    start_date_upload: Optional[str]  # Format: YYYY-MM-DD
    end_date_upload: Optional[str]    # Format: YYYY-MM-DD
    local_tmp_dir: str

    @classmethod
    def from_env(cls) -> "ProcessingConfig":
        process_articles = (
            os.getenv("LEMONDE_PROCESS_ARTICLES", "true").lower() == "true"
        )
        process_stats = os.getenv("LEMONDE_PROCESS_STATS", "true").lower() == "true"
        upsert_to_factiva = (
            os.getenv("LEMONDE_UPSERT_TO_FACTIVA", "true").lower() == "true"
        )
        lookback_days = int(os.getenv("LEMONDE_LOOKBACK_DAYS", "30"))
        lemonde_upsert_lookback_hours = int(
            os.getenv("LEMONDE_UPSERT_LOOKBACK_HOURS", "24")
        )
        start_date_upload = os.getenv("LEMONDE_START_DATE_UPLOAD")  # Optional: YYYY-MM-DD
        end_date_upload = os.getenv("LEMONDE_END_DATE_UPLOAD")      # Optional: YYYY-MM-DD
        local_tmp_dir = os.getenv("LOCAL_TMP_DIR", "/tmp/s3_lemonde_to_postgre")

        return cls(
            process_articles=process_articles,
            process_stats=process_stats,
            upsert_to_factiva=upsert_to_factiva,
            lookback_days=lookback_days,
            lemonde_upsert_lookback_hours=lemonde_upsert_lookback_hours,
            start_date_upload=start_date_upload,
            end_date_upload=end_date_upload,
            local_tmp_dir=local_tmp_dir,
        )


@dataclass(slots=True)
class ProcessingStats:
    """Statistics for the processing job."""

    articles_processed: int = 0
    articles_upserted: int = 0
    articles_deleted: int = 0
    stats_processed: int = 0
    stats_upserted: int = 0
    articles_upserted_to_factiva: int = 0
    errors: int = 0


class S3Client:
    """Client for interacting with S3."""

    def __init__(self, config: S3Config):
        self.config = config
        session_config = BotoConfig(signature_version="s3v4")
        self.client = boto3.client(
            "s3",
            region_name=config.region,
            aws_access_key_id=config.access_key,
            aws_secret_access_key=config.secret_key,
            endpoint_url=config.endpoint_url,
            config=session_config,
        )

    def list_files_in_prefix(self, prefix: str) -> List[str]:
        """List all files under a given S3 prefix."""
        logging.info(f"Listing files in s3://{self.config.bucket_name}/{prefix}")

        files = []
        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=self.config.bucket_name, Prefix=prefix
        ):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])

        logging.info(f"Found {len(files)} files in prefix {prefix}")
        return files

    def download_file(self, s3_key: str, local_path: Path) -> None:
        """Download a file from S3 to local path."""
        logging.debug(
            f"Downloading s3://{self.config.bucket_name}/{s3_key} to {local_path}"
        )
        self.client.download_file(self.config.bucket_name, s3_key, str(local_path))

    def rename_file(self, s3_key: str, new_s3_key: str) -> None:
        """Rename a file in S3 by copying and deleting."""
        logging.info(f"Renaming {s3_key} to {new_s3_key}")

        # Copy to new location
        self.client.copy_object(
            Bucket=self.config.bucket_name,
            CopySource={"Bucket": self.config.bucket_name, "Key": s3_key},
            Key=new_s3_key,
        )

        # Delete original
        self.client.delete_object(Bucket=self.config.bucket_name, Key=s3_key)


class LemondeArticleProcessor:
    """Process Le Monde article JSON files from S3."""

    def __init__(self, s3_client: S3Client, config: S3Config, tmp_dir: Path):
        self.s3_client = s3_client
        self.config = config
        self.tmp_dir = tmp_dir
        # Use custom JSON serializer to preserve Unicode characters in JSON columns
        self.engine = connect_to_db(use_custom_json_serializer=True)

    def get_unprocessed_article_files(
        self,
        lookback_days: int,
        start_date_str: Optional[str] = None,
        end_date_str: Optional[str] = None,
    ) -> List[str]:
        """Get list of unprocessed article files.
        
        Args:
            lookback_days: Number of days to look back (used if start_date_str is None)
            start_date_str: Optional start date filter (format: YYYY-MM-DD)
            end_date_str: Optional end date filter (format: YYYY-MM-DD)
        """
        # Determine date range
        if start_date_str and end_date_str:
            # Use provided date range
            try:
                cutoff_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                now = datetime.strptime(end_date_str, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc
                )
                logging.info(
                    f"Using custom date range: {start_date_str} to {end_date_str}"
                )
            except ValueError as e:
                logging.error(
                    f"Invalid date format in LEMONDE_START_DATE_UPLOAD or LEMONDE_END_DATE_UPLOAD: {e}"
                )
                logging.error("Expected format: YYYY-MM-DD. Falling back to lookback_days.")
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
                now = datetime.now(timezone.utc)
        elif start_date_str:
            # Only start date provided, use it until now
            try:
                cutoff_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                now = datetime.now(timezone.utc)
                logging.info(f"Using start date: {start_date_str} to now")
            except ValueError as e:
                logging.error(f"Invalid date format in LEMONDE_START_DATE_UPLOAD: {e}")
                logging.error("Expected format: YYYY-MM-DD. Falling back to lookback_days.")
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
                now = datetime.now(timezone.utc)
        else:
            # Use lookback_days (default behavior)
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
            now = datetime.now(timezone.utc)
            logging.info(f"Using lookback days: {lookback_days}")

        all_files = []

        # Look through year/month folders - only scan months within the date range
        current_date = cutoff_date.replace(day=1)  # Start from first day of cutoff month
        end_date = now.replace(day=1)  # End at first day of end month

        while current_date <= end_date:
            prefix = f"{self.config.base_prefix}/articles/year_{current_date.year}/month_{current_date.month:02d}/"
            files = self.s3_client.list_files_in_prefix(prefix)
            all_files.extend(files)

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        # Filter for stream or snapshot_extract files without PROCESSED in name
        unprocessed = []
        for file_key in all_files:
            filename = file_key.split("/")[-1]

            # Must be a stream or snapshot_extract file and not already processed
            if (
                "_stream.json" in filename or "_snapshot_extract.json" in filename
            ) and "PROCESSED" not in filename:
                # Extract date from filename (format: YYYY_MM_DD_HH_MM_SS_N_stream.json or YYYY_MM_DD_HH_MM_SS_N_snapshot_extract.json)
                try:
                    date_str = "_".join(filename.split("_")[:6])
                    file_date = datetime.strptime(date_str, "%Y_%m_%d_%H_%M_%S")
                    file_date = file_date.replace(tzinfo=timezone.utc)

                    # Check if file is within the date range
                    if cutoff_date <= file_date <= now:
                        unprocessed.append(file_key)
                except (ValueError, IndexError) as e:
                    logging.warning(
                        f"Could not parse date from filename {filename}: {e}"
                    )
                    continue

        logging.info(f"Found {len(unprocessed)} unprocessed article files")
        return unprocessed

    def process_article_file(self, s3_key: str) -> dict:
        """Process a single article JSON file.

        Returns:
            Dictionary with counts: {'upserted': int, 'deleted': int}
        """
        filename = s3_key.split("/")[-1]
        local_path = self.tmp_dir / filename

        try:
            # Download file
            self.s3_client.download_file(s3_key, local_path)

            # Read JSON
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Process data - can contain both articles and bulk events
            articles = data.get("data", [])
            if not isinstance(articles, list):
                articles = [articles]

            logging.info(f"Processing {len(articles)} items from {filename}")

            # Counters for this file
            upserted_count = 0
            deleted_count = 0

            # Process each item
            for item in articles:
                result = self.process_single_article(item)
                if result == "upserted":
                    upserted_count += 1
                elif result == "deleted":
                    deleted_count += 1

            # Rename file in S3 to mark as processed
            if "_stream.json" in filename:
                new_filename = filename.replace("_stream.json", "_stream_PROCESSED.json")
            elif "_snapshot_extract.json" in filename:
                new_filename = filename.replace(
                    "_snapshot_extract.json", "_snapshot_extract_PROCESSED.json"
                )
            else:
                # Fallback: just add _PROCESSED before .json
                new_filename = filename.replace(".json", "_PROCESSED.json")

            new_s3_key = s3_key.replace(filename, new_filename)
            self.s3_client.rename_file(s3_key, new_s3_key)

            # Clean up local file
            local_path.unlink(missing_ok=True)

            return {"upserted": upserted_count, "deleted": deleted_count}

        except Exception as e:
            logging.error(f"Error processing article file {s3_key}: {e}")
            raise

    def process_single_article(self, article_data: dict) -> Optional[str]:
        """Process a single article and upsert/delete into lemonde_ftp_articles.

        Returns:
            'upserted' if article was upserted
            'deleted' if article was deleted
            None if article was skipped or error occurred
        """
        try:
            attributes = article_data.get("attributes", {})

            # Extract article ID (AN)
            an = attributes.get("an")
            if not an:
                logging.warning("Article missing 'an' field, skipping")
                return None

            # Build combined text for keyword extraction
            article_text = build_article_text(article_data)

            # Extract keyword data (counts + lists, HRFP and non-HRFP)
            keyword_data = extract_keyword_data_from_article(article_text)

            # Parse dates
            publication_datetime = self._parse_datetime(
                attributes.get("publication_datetime")
            )
            publication_date = self._parse_datetime(attributes.get("publication_date"))
            modification_datetime = self._parse_datetime(
                attributes.get("modification_datetime")
            )
            modification_date = self._parse_datetime(
                attributes.get("modification_date")
            )
            ingestion_datetime = self._parse_datetime(
                attributes.get("ingestion_datetime")
            )
            availability_datetime = self._parse_datetime(
                attributes.get("availability_datetime")
            )

            # Prepare article data for upsert
            # Use empty string for missing snippet and art fields
            article_dict = {
                "an": an,
                "document_type": attributes.get("document_type"),
                "action": attributes.get("action"),
                "event_type": attributes.get("event_type"),
                "title": attributes.get("title"),
                "body": attributes.get("body"),
                "snippet": attributes.get("snippet", ""),
                "art": attributes.get("art", ""),
                "byline": attributes.get("byline"),
                "credit": attributes.get("credit"),
                "dateline": attributes.get("dateline"),
                "source_code": attributes.get("source_code"),
                "source_name": attributes.get("source_name"),
                "publisher_name": attributes.get("publisher_name"),
                "section": attributes.get("section"),
                "copyright": attributes.get("copyright"),
                "publication_date": publication_date,
                "publication_datetime": publication_datetime,
                "modification_date": modification_date,
                "modification_datetime": modification_datetime,
                "ingestion_datetime": ingestion_datetime,
                "availability_datetime": availability_datetime,
                "language_code": attributes.get("language_code"),
                "region_of_origin": attributes.get("region_of_origin"),
                "word_count": attributes.get("word_count"),
                "company_codes": self._join_codes(attributes.get("company_codes")),
                "company_codes_about": self._join_codes(
                    attributes.get("company_codes_about")
                ),
                "company_codes_association": self._join_codes(
                    attributes.get("company_codes_association")
                ),
                "company_codes_lineage": self._join_codes(
                    attributes.get("company_codes_lineage")
                ),
                "company_codes_occur": self._join_codes(
                    attributes.get("company_codes_occur")
                ),
                "company_codes_relevance": self._join_codes(
                    attributes.get("company_codes_relevance")
                ),
                "subject_codes": self._join_codes(attributes.get("subject_codes")),
                "region_codes": self._join_codes(attributes.get("region_codes")),
                "industry_codes": self._join_codes(attributes.get("industry_codes")),
                "person_codes": self._join_codes(attributes.get("person_codes")),
                "currency_codes": self._join_codes(attributes.get("currency_codes")),
                "market_index_codes": self._join_codes(
                    attributes.get("market_index_codes")
                ),
                "allow_translation": attributes.get("allow_translation"),
                "attrib_code": attributes.get("attrib_code"),
                "authors": attributes.get("authors"),
                "clusters": attributes.get("clusters"),
                "content_type_codes": self._join_codes(
                    attributes.get("content_type_codes")
                ),
                "footprint_company_codes": self._join_codes(
                    attributes.get("footprint_company_codes")
                ),
                "footprint_person_codes": self._join_codes(
                    attributes.get("footprint_person_codes")
                ),
                "industry_classification_benchmark_codes": self._join_codes(
                    attributes.get("industry_classification_benchmark_codes")
                ),
                "newswires_codes": self._join_codes(attributes.get("newswires_codes")),
                "org_type_codes": self._join_codes(attributes.get("org_type_codes")),
                "pub_page": attributes.get("pub_page"),
                "restrictor_codes": self._join_codes(
                    attributes.get("restrictor_codes")
                ),
                "is_deleted": False,
                **keyword_data,  # Add all keyword counts, lists, and aggregated counts
            }

            # Perform UPSERT into lemonde_ftp_articles
            stmt = insert(LeMonde_FTP_Article).values(**article_dict)
            stmt = stmt.on_conflict_do_update(
                index_elements=["an"],
                set_={
                    **article_dict,
                    "updated_at": datetime.now(timezone.utc),
                },
            )

            with self.engine.begin() as conn:
                conn.execute(stmt)

            logging.debug(f"Upserted article {an} to lemonde_ftp_articles")
            return "upserted"

        except Exception as e:
            logging.error(f"Error processing single article: {e}")
            return None

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not date_str:
            return None
        try:
            dt = date_parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception as e:
            logging.warning(f"Could not parse datetime '{date_str}': {e}")
            return None

    def _join_codes(self, codes) -> Optional[str]:
        """Convert code list/dict to comma-separated string."""
        if not codes:
            return None
        if isinstance(codes, list):
            return ",".join(str(c) for c in codes)
        if isinstance(codes, dict):
            return ",".join(str(v) for v in codes.values())
        return str(codes)

    def delete_article(self, an: str) -> bool:
        """Delete an article from lemonde_ftp_articles by its AN."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("DELETE FROM lemonde_ftp_articles WHERE an = :an"), {"an": an}
                )

                if result.rowcount > 0:
                    logging.info(f"Deleted article {an} from lemonde_ftp_articles")
                    return True
                else:
                    logging.debug(
                        f"Article {an} not found in lemonde_ftp_articles (already deleted or never inserted)"
                    )
                    return False

        except Exception as e:
            logging.error(f"Error deleting article {an}: {e}")
            return False


class LemondeStatsProcessor:
    """Process Le Monde statistics JSON files from S3."""

    def __init__(self, s3_client: S3Client, config: S3Config, tmp_dir: Path):
        self.s3_client = s3_client
        self.config = config
        self.tmp_dir = tmp_dir
        self.engine = connect_to_db(use_custom_json_serializer=True)

    def get_unprocessed_stats_files(self, lookback_days: int) -> List[str]:
        """Get list of unprocessed stats files from the last N days."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        now = datetime.now(timezone.utc)

        all_files = []

        # Look through year/month folders - only scan months within the lookback period
        current_date = cutoff_date.replace(day=1)  # Start from first day of cutoff month
        end_date = now.replace(day=1)  # End at first day of current month

        while current_date <= end_date:
            prefix = f"{self.config.base_prefix}/nb_articles/year_{current_date.year}/month_{current_date.month:02d}/"
            files = self.s3_client.list_files_in_prefix(prefix)
            all_files.extend(files)

            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

        # Filter for stats files without PROCESSED in name
        unprocessed = []
        for file_key in all_files:
            filename = file_key.split("/")[-1]

            # Must be a stats file and not already processed
            if "_stats.json" in filename and "PROCESSED" not in filename:
                # Extract date from filename (format: YYYY_MM_DD_HH_MM_SS_N_stats.json)
                try:
                    date_str = "_".join(filename.split("_")[:6])
                    file_date = datetime.strptime(date_str, "%Y_%m_%d_%H_%M_%S")
                    file_date = file_date.replace(tzinfo=timezone.utc)

                    if file_date >= cutoff_date:
                        unprocessed.append(file_key)
                except (ValueError, IndexError) as e:
                    logging.warning(
                        f"Could not parse date from filename {filename}: {e}"
                    )
                    continue

        logging.info(f"Found {len(unprocessed)} unprocessed stats files")
        return unprocessed

    def process_stats_file(self, s3_key: str) -> int:
        """Process a single stats JSON file."""
        filename = s3_key.split("/")[-1]
        local_path = self.tmp_dir / filename

        try:
            # Download file
            self.s3_client.download_file(s3_key, local_path)

            # Read JSON Lines format (one object per line)
            stats_records = []
            with open(local_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        stats_records.append(json.loads(line))

            logging.info(f"Processing {len(stats_records)} stats records from {filename}")

            # Process each stats record
            upserted_count = 0
            for record in stats_records:
                if self.process_single_stats_record(record):
                    upserted_count += 1

            # Rename file in S3 to mark as processed
            new_filename = filename.replace("_stats.json", "_stats_PROCESSED.json")
            new_s3_key = s3_key.replace(filename, new_filename)
            self.s3_client.rename_file(s3_key, new_s3_key)

            # Clean up local file
            local_path.unlink(missing_ok=True)

            return upserted_count

        except Exception as e:
            logging.error(f"Error processing stats file {s3_key}: {e}")
            raise

    def process_single_stats_record(self, record: dict) -> bool:
        """Process a single stats record and upsert into stats_factiva_articles."""
        try:
            source_code = record.get("source_code")
            publication_datetime_str = record.get("publication_datetime")
            count_str = record.get("count")

            if not source_code or not publication_datetime_str or count_str is None:
                logging.warning(f"Stats record missing required fields: {record}")
                return False

            # Convert count from string to int
            try:
                count = int(count_str)
            except (ValueError, TypeError) as e:
                logging.warning(f"Could not convert count '{count_str}' to int: {e}")
                return False

            # Parse publication_datetime
            publication_datetime = self._parse_datetime(publication_datetime_str)
            if not publication_datetime:
                logging.warning(
                    f"Could not parse publication_datetime: {publication_datetime_str}"
                )
                return False

            # Prepare stats data for upsert
            stats_dict = {
                "source_code": source_code,
                "publication_datetime": publication_datetime,
                "count": count,
            }

            # Perform UPSERT into stats_factiva_articles
            stmt = insert(Stats_Factiva_Article).values(**stats_dict)
            stmt = stmt.on_conflict_do_update(
                index_elements=["source_code", "publication_datetime"],
                set_={
                    "count": count,
                    "updated_at": datetime.now(timezone.utc),
                },
            )

            with self.engine.begin() as conn:
                conn.execute(stmt)

            logging.debug(f"Upserted stats for {source_code} at {publication_datetime}")
            return True

        except Exception as e:
            logging.error(f"Error processing single stats record: {e}")
            return False

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not date_str:
            return None
        try:
            dt = date_parser.parse(date_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception as e:
            logging.warning(f"Could not parse datetime '{date_str}': {e}")
            return None


class LemondeToFactivaUpserter:
    """Upsert recent Le Monde articles with keywords into factiva_articles."""

    def __init__(self):
        self.engine = connect_to_db(use_custom_json_serializer=True)

    def upsert_recent_articles_to_factiva(self, lookback_hours: int) -> int:
        """
        Upsert articles from lemonde_ftp_articles to factiva_articles if:
        - updated_at is within lookback_hours
        - AND (number_of_climat_no_hrfp >= 1 OR number_of_ressources_no_hrfp >= 1 OR number_of_biodiversite_no_hrfp >= 1)

        Returns:
            Number of articles upserted
        """
        try:
            cutoff_datetime = datetime.now(timezone.utc) - timedelta(
                hours=lookback_hours
            )

            logging.info(
                f"Upserting recent Le Monde articles with keywords (updated_at >= {cutoff_datetime})"
            )

            # Build SQL query to select relevant articles
            query = text(
                """
                SELECT * FROM lemonde_ftp_articles
                WHERE updated_at >= :cutoff_datetime
                AND (
                    number_of_climat_no_hrfp >= 1
                    OR number_of_ressources_no_hrfp >= 1
                    OR number_of_biodiversite_no_hrfp >= 1
                )
            """
            )

            with self.engine.connect() as conn:
                result = conn.execute(query, {"cutoff_datetime": cutoff_datetime})
                articles = result.fetchall()

            logging.info(
                f"Found {len(articles)} Le Monde articles with keywords to upsert to factiva_articles"
            )

            if not articles:
                return 0

            # Get column names from result
            column_names = list(result.keys()) if articles else []

            # Upsert each article into factiva_articles
            upserted_count = 0
            for article_row in articles:
                # Convert row to dictionary
                article_dict = dict(zip(column_names, article_row))

                # Remove id/created_at if present (let factiva_articles generate its own)
                article_dict.pop("created_at", None)

                # Update updated_at to current time
                article_dict["updated_at"] = datetime.now(timezone.utc)

                # Perform UPSERT into factiva_articles
                stmt = insert(Factiva_Article).values(**article_dict)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["an"],
                    set_=article_dict,
                )

                with self.engine.begin() as conn:
                    conn.execute(stmt)

                logging.debug(
                    f"Upserted article {article_dict['an']} to factiva_articles"
                )
                upserted_count += 1

            logging.info(f"Upserted {upserted_count} articles to factiva_articles")
            return upserted_count

        except Exception as e:
            logging.error(f"Error upserting Le Monde articles to factiva: {e}")
            raise


class LemondeProcessor:
    """Main processor for Le Monde FTP articles and stats."""

    def __init__(
        self,
        s3_config: S3Config,
        processing_config: ProcessingConfig,
    ):
        self.s3_config = s3_config
        self.processing_config = processing_config
        self.stats = ProcessingStats()

        # Create temp directory
        self.tmp_dir = Path(processing_config.local_tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        # Initialize S3 client
        self.s3_client = S3Client(s3_config)

    def run(self) -> None:
        """Run the complete Le Monde processing pipeline."""
        try:
            logging.info("=" * 80)
            logging.info("Starting Le Monde FTP to PostgreSQL processor")
            logging.info("=" * 80)

            # Step 1: Process Le Monde articles
            if self.processing_config.process_articles:
                self._process_articles()

            # Step 2: Process Le Monde statistics
            if self.processing_config.process_stats:
                self._process_stats()

            # Step 3: Upsert recent articles with keywords to factiva_articles
            if self.processing_config.upsert_to_factiva:
                self._upsert_to_factiva()

            # Summary
            logging.info("=" * 80)
            logging.info("Le Monde processing complete")
            logging.info(f"Articles processed: {self.stats.articles_processed}")
            logging.info(f"Articles upserted: {self.stats.articles_upserted}")
            logging.info(f"Articles deleted: {self.stats.articles_deleted}")
            logging.info(f"Stats processed: {self.stats.stats_processed}")
            logging.info(f"Stats upserted: {self.stats.stats_upserted}")
            logging.info(
                f"Articles upserted to factiva: {self.stats.articles_upserted_to_factiva}"
            )
            logging.info(f"Errors: {self.stats.errors}")
            logging.info("=" * 80)

        except Exception as e:
            logging.error(f"Fatal error in Le Monde processor: {e}")
            raise

    def _process_articles(self) -> None:
        """Process Le Monde article files from S3."""
        try:
            processor = LemondeArticleProcessor(
                self.s3_client, self.s3_config, self.tmp_dir
            )

            # Get unprocessed files (with optional date range filter)
            files = processor.get_unprocessed_article_files(
                self.processing_config.lookback_days,
                self.processing_config.start_date_upload,
                self.processing_config.end_date_upload,
            )

            if not files:
                logging.info("No unprocessed article files found")
                return

            # Process each file
            for file_key in files:
                try:
                    counts = processor.process_article_file(file_key)
                    self.stats.articles_processed += 1
                    self.stats.articles_upserted += counts["upserted"]
                    self.stats.articles_deleted += counts["deleted"]
                except Exception as e:
                    logging.error(f"Failed to process article file {file_key}: {e}")
                    self.stats.errors += 1

            logging.info(f"Processed {self.stats.articles_processed} article files")

        except Exception as e:
            logging.error(f"Error in article processing: {e}")
            self.stats.errors += 1
            raise

    def _process_stats(self) -> None:
        """Process Le Monde statistics files from S3."""
        try:
            processor = LemondeStatsProcessor(
                self.s3_client, self.s3_config, self.tmp_dir
            )

            # Get unprocessed files
            files = processor.get_unprocessed_stats_files(
                self.processing_config.lookback_days
            )

            if not files:
                logging.info("No unprocessed stats files found")
                return

            # Process each file
            for file_key in files:
                try:
                    upserted = processor.process_stats_file(file_key)
                    self.stats.stats_processed += 1
                    self.stats.stats_upserted += upserted
                except Exception as e:
                    logging.error(f"Failed to process stats file {file_key}: {e}")
                    self.stats.errors += 1

            logging.info(f"Processed {self.stats.stats_processed} stats files")

        except Exception as e:
            logging.error(f"Error in stats processing: {e}")
            self.stats.errors += 1
            raise

    def _upsert_to_factiva(self) -> None:
        """Upsert recent Le Monde articles with keywords to factiva_articles."""
        try:
            upserter = LemondeToFactivaUpserter()
            upserted_count = upserter.upsert_recent_articles_to_factiva(
                self.processing_config.lemonde_upsert_lookback_hours
            )
            self.stats.articles_upserted_to_factiva = upserted_count

        except Exception as e:
            logging.error(f"Error upserting to factiva: {e}")
            self.stats.errors += 1
            raise


def main():
    """Main entry point."""
    # Initialize logger
    logger = getLogger()
    logger.setLevel(logging.INFO)
    logging.info("Initializing Le Monde FTP to PostgreSQL processor")

    # Initialize Sentry
    sentry_init()

    # Load configuration
    try:
        s3_config = S3Config.from_env()
        processing_config = ProcessingConfig.from_env()

        logging.info(f"S3 Bucket: {s3_config.bucket_name}")
        logging.info(f"S3 Base Prefix: {s3_config.base_prefix}")
        logging.info(f"Process Articles: {processing_config.process_articles}")
        logging.info(f"Process Stats: {processing_config.process_stats}")
        logging.info(f"Upsert to Factiva: {processing_config.upsert_to_factiva}")
        logging.info(f"Lookback Days: {processing_config.lookback_days}")
        logging.info(
            f"Lemonde Upsert Lookback Hours: {processing_config.lemonde_upsert_lookback_hours}"
        )
        if processing_config.start_date_upload or processing_config.end_date_upload:
            logging.info(
                f"Custom Date Range: {processing_config.start_date_upload or 'N/A'} to {processing_config.end_date_upload or 'N/A'}"
            )

    except Exception as e:
        logging.error(f"Configuration error: {e}")
        raise

    # Run processor
    processor = LemondeProcessor(s3_config, processing_config)
    processor.run()


if __name__ == "__main__":
    # Start health check server in background
    run_health_check_server()

    # Run main processing
    main()
