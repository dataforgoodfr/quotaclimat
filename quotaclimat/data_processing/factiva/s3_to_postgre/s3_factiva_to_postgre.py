"""Process Factiva data from S3 and load into PostgreSQL.

This script handles two types of data:
1. Article JSON files (stream data) - processed into factiva_articles table with keyword extraction
2. Statistics JSON files (stats data) - processed into stats_factiva_articles table
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
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
from postgres.schemas.models import Factiva_Article, Stats_Factiva_Article
from quotaclimat.data_ingestion.factiva.utils_data_processing.utils_extract import (
    load_json_values,
)
from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    build_article_text,
    extract_keyword_data_from_article,
)
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.utils.sentry import sentry_init

# Path to followed sources configuration
FOLLOWED_SOURCES_PATH = "quotaclimat/data_ingestion/factiva/inputs/followed_sources.json"


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
    def from_env(cls) -> "S3Config":
        bucket_name = os.getenv("S3_BUCKET_NAME")
        if not bucket_name:
            raise ValueError("S3_BUCKET_NAME environment variable is required")

        region = os.getenv("S3_REGION", "fr-par")
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        base_prefix = os.getenv("S3_BASE_PREFIX", "factiva")

        # Handle S3 credentials - can be direct values or file paths (Docker secrets)
        access_key = os.getenv("S3_ACCESS_KEY")
        secret_key = os.getenv("S3_SECRET_KEY")
        
        # If they look like file paths, read the file content
        if access_key and access_key.startswith('/'):
            try:
                with open(access_key, 'r') as f:
                    access_key = f.read().strip()
            except Exception as e:
                raise ValueError(f"Could not read S3_ACCESS_KEY from file {access_key}: {e}")
        
        if secret_key and secret_key.startswith('/'):
            try:
                with open(secret_key, 'r') as f:
                    secret_key = f.read().strip()
            except Exception as e:
                raise ValueError(f"Could not read S3_SECRET_KEY from file {secret_key}: {e}")

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
    lookback_days: int
    local_tmp_dir: str

    @classmethod
    def from_env(cls) -> "ProcessingConfig":
        process_articles = os.getenv("PROCESS_ARTICLES", "true").lower() == "true"
        process_stats = os.getenv("PROCESS_STATS", "true").lower() == "true"
        lookback_days = int(os.getenv("LOOKBACK_DAYS", "30"))
        local_tmp_dir = os.getenv("LOCAL_TMP_DIR", "/tmp/s3_factiva_to_postgre")

        return cls(
            process_articles=process_articles,
            process_stats=process_stats,
            lookback_days=lookback_days,
            local_tmp_dir=local_tmp_dir,
        )


@dataclass(slots=True)
class ProcessingStats:
    """Statistics for the processing job."""

    articles_processed: int = 0
    articles_upserted: int = 0
    articles_deleted: int = 0
    sources_deleted: int = 0
    stats_processed: int = 0
    stats_upserted: int = 0
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
        
        for page in paginator.paginate(Bucket=self.config.bucket_name, Prefix=prefix):
            if "Contents" in page:
                for obj in page["Contents"]:
                    files.append(obj["Key"])
        
        logging.info(f"Found {len(files)} files in prefix {prefix}")
        return files

    def download_file(self, s3_key: str, local_path: Path) -> None:
        """Download a file from S3 to local path."""
        logging.debug(f"Downloading s3://{self.config.bucket_name}/{s3_key} to {local_path}")
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


class ArticleProcessor:
    """Process article JSON files from S3."""

    def __init__(self, s3_client: S3Client, config: S3Config, tmp_dir: Path):
        self.s3_client = s3_client
        self.config = config
        self.tmp_dir = tmp_dir
        # Use custom JSON serializer to preserve Unicode characters in JSON columns
        self.engine = connect_to_db(use_custom_json_serializer=True)
        
        # Load followed sources
        try:
            self.followed_sources = set(load_json_values(FOLLOWED_SOURCES_PATH))
            logging.info(f"Loaded {len(self.followed_sources)} followed sources")
        except Exception as e:
            logging.error(f"Failed to load followed sources from {FOLLOWED_SOURCES_PATH}: {e}")
            self.followed_sources = set()

    def get_unprocessed_article_files(self, lookback_days: int) -> List[str]:
        """Get list of unprocessed article files from the last N days."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        all_files = []
        
        # Look through year/month folders
        for year in range(cutoff_date.year, datetime.now(timezone.utc).year + 1):
            for month in range(1, 13):
                prefix = f"{self.config.base_prefix}/articles/year_{year}/month_{month:02d}/"
                files = self.s3_client.list_files_in_prefix(prefix)
                all_files.extend(files)
        
        # Filter for stream files without PROCESSED in name
        unprocessed = []
        for file_key in all_files:
            filename = file_key.split("/")[-1]
            
            # Must be a stream file and not already processed
            if "_stream.json" in filename and "PROCESSED" not in filename:
                # Extract date from filename (format: YYYY_MM_DD_HH_MM_SS_N_stream.json)
                try:
                    date_str = "_".join(filename.split("_")[:6])
                    file_date = datetime.strptime(date_str, "%Y_%m_%d_%H_%M_%S")
                    file_date = file_date.replace(tzinfo=timezone.utc)
                    
                    if file_date >= cutoff_date:
                        unprocessed.append(file_key)
                except (ValueError, IndexError) as e:
                    logging.warning(f"Could not parse date from filename {filename}: {e}")
                    continue
        
        logging.info(f"Found {len(unprocessed)} unprocessed article files")
        return unprocessed

    def process_article_file(self, s3_key: str) -> dict:
        """Process a single article JSON file.
        
        Returns:
            Dictionary with counts: {'upserted': int, 'deleted': int, 'sources_deleted': int}
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
            sources_deleted_count = 0
            
            # Process each item (can be article or bulk event)
            for item in articles:
                # Check if this is a bulk event (source_delete)
                if "event_type" in item and item.get("event_type") == "source_delete":
                    logging.info(f"Processing source_delete event in {filename}")
                    if self.process_source_delete_event(item):
                        sources_deleted_count += 1
                else:
                    # Regular article
                    result = self.process_single_article(item)
                    if result == "upserted":
                        upserted_count += 1
                    elif result == "deleted":
                        deleted_count += 1
            
            # Rename file in S3 to mark as processed
            new_filename = filename.replace("_stream.json", "_stream_PROCESSED.json")
            new_s3_key = s3_key.replace(filename, new_filename)
            self.s3_client.rename_file(s3_key, new_s3_key)
            
            # Clean up local file
            local_path.unlink(missing_ok=True)
            
            return {
                "upserted": upserted_count,
                "deleted": deleted_count,
                "sources_deleted": sources_deleted_count
            }
            
        except Exception as e:
            logging.error(f"Error processing article file {s3_key}: {e}")
            raise

    def process_single_article(self, article_data: dict) -> Optional[str]:
        """Process a single article and upsert/delete into database based on action.
        
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
            
            # Extract source_code and action
            source_code = attributes.get("source_code")
            action = attributes.get("action")
            
            # Filter: only process articles from followed sources
            if source_code not in self.followed_sources:
                logging.debug(f"Skipping article {an} - source {source_code} not in followed sources")
                return None
            
            # Handle deletion action
            if action == "del":
                if self.delete_article(an):
                    return "deleted"
                return None
            
            # Handle add/rep actions (upsert)
            if action not in ["add", "rep"]:
                logging.warning(f"Unknown action '{action}' for article {an}, skipping")
                return None
            
            # Build combined text for keyword extraction
            article_text = build_article_text(article_data)
            
            # Extract keyword data (counts + lists, non-HRFP only)
            keyword_data = extract_keyword_data_from_article(article_text)
            
            # Parse dates
            publication_datetime = self._parse_datetime(attributes.get("publication_datetime"))
            publication_date = self._parse_datetime(attributes.get("publication_date"))
            modification_datetime = self._parse_datetime(attributes.get("modification_datetime"))
            modification_date = self._parse_datetime(attributes.get("modification_date"))
            ingestion_datetime = self._parse_datetime(attributes.get("ingestion_datetime"))
            availability_datetime = self._parse_datetime(attributes.get("availability_datetime"))
            
            # Prepare article data for upsert
            article_dict = {
                "an": an,
                "document_type": attributes.get("document_type"),
                "action": attributes.get("action"),
                "event_type": attributes.get("event_type"),
                "title": attributes.get("title"),
                "body": attributes.get("body"),
                "snippet": attributes.get("snippet"),
                "art": attributes.get("art"),
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
                "company_codes_about": self._join_codes(attributes.get("company_codes_about")),
                "company_codes_association": self._join_codes(attributes.get("company_codes_association")),
                "company_codes_lineage": self._join_codes(attributes.get("company_codes_lineage")),
                "company_codes_occur": self._join_codes(attributes.get("company_codes_occur")),
                "company_codes_relevance": self._join_codes(attributes.get("company_codes_relevance")),
                "subject_codes": self._join_codes(attributes.get("subject_codes")),
                "region_codes": self._join_codes(attributes.get("region_codes")),
                "industry_codes": self._join_codes(attributes.get("industry_codes")),
                "person_codes": self._join_codes(attributes.get("person_codes")),
                "currency_codes": self._join_codes(attributes.get("currency_codes")),
                "market_index_codes": self._join_codes(attributes.get("market_index_codes")),
                "allow_translation": attributes.get("allow_translation"),
                "attrib_code": attributes.get("attrib_code"),
                "authors": attributes.get("authors"),
                "clusters": attributes.get("clusters"),
                "content_type_codes": self._join_codes(attributes.get("content_type_codes")),
                "footprint_company_codes": self._join_codes(attributes.get("footprint_company_codes")),
                "footprint_person_codes": self._join_codes(attributes.get("footprint_person_codes")),
                "industry_classification_benchmark_codes": self._join_codes(
                    attributes.get("industry_classification_benchmark_codes")
                ),
                "newswires_codes": self._join_codes(attributes.get("newswires_codes")),
                "org_type_codes": self._join_codes(attributes.get("org_type_codes")),
                "pub_page": attributes.get("pub_page"),
                "restrictor_codes": self._join_codes(attributes.get("restrictor_codes")),
                "is_deleted": False,
                **keyword_data,  # Add all keyword counts, lists, and aggregated counts
            }
            
            # Perform UPSERT
            stmt = insert(Factiva_Article).values(**article_dict)
            stmt = stmt.on_conflict_do_update(
                index_elements=["an"],
                set_={
                    **article_dict,
                    "updated_at": datetime.now(timezone.utc),
                },
            )
            
            with self.engine.begin() as conn:
                conn.execute(stmt)
            
            logging.debug(f"Upserted article {an}")
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
        """Delete an article from the database by its AN (article number)."""
        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("DELETE FROM factiva_articles WHERE an = :an"),
                    {"an": an}
                )
                
                if result.rowcount > 0:
                    logging.info(f"Deleted article {an}")
                    return True
                else:
                    logging.debug(f"Article {an} not found in database (already deleted or never inserted)")
                    return False
                    
        except Exception as e:
            logging.error(f"Error deleting article {an}: {e}")
            return False

    def process_source_delete_event(self, event_data: dict) -> bool:
        """Process a source_delete bulk event - delete all articles from a source."""
        try:
            source_code = event_data.get("source_code")
            modification_datetime = event_data.get("modification_datetime")
            description = event_data.get("description", "")
            
            if not source_code:
                logging.warning("source_delete event missing 'source_code' field, skipping")
                return False
            
            # Filter: only process if source is in followed sources
            if source_code not in self.followed_sources:
                logging.info(f"Skipping source_delete for {source_code} - not in followed sources")
                return False
            
            logging.info(f"Processing source_delete event for source {source_code}")
            logging.info(f"Modification datetime: {modification_datetime}")
            logging.info(f"Description: {description}")
            
            # Delete all articles from this source
            with self.engine.begin() as conn:
                result = conn.execute(
                    text("DELETE FROM factiva_articles WHERE source_code = :source_code"),
                    {"source_code": source_code}
                )
                
                deleted_count = result.rowcount
                logging.info(f"Deleted {deleted_count} articles from source {source_code}")
                
                return True
                
        except Exception as e:
            logging.error(f"Error processing source_delete event: {e}")
            return False


class StatsProcessor:
    """Process statistics JSON files from S3."""

    def __init__(self, s3_client: S3Client, config: S3Config, tmp_dir: Path):
        self.s3_client = s3_client
        self.config = config
        self.tmp_dir = tmp_dir
        # Stats may also use JSON in the future; keep behavior consistent
        self.engine = connect_to_db(use_custom_json_serializer=True)

    def get_unprocessed_stats_files(self, lookback_days: int) -> List[str]:
        """Get list of unprocessed stats files from the last N days."""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        all_files = []
        
        # Look through year/month folders
        for year in range(cutoff_date.year, datetime.now(timezone.utc).year + 1):
            for month in range(1, 13):
                prefix = f"{self.config.base_prefix}/nb_articles/year_{year}/month_{month:02d}/"
                files = self.s3_client.list_files_in_prefix(prefix)
                all_files.extend(files)
        
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
                    logging.warning(f"Could not parse date from filename {filename}: {e}")
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
        """Process a single stats record and upsert into database."""
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
                logging.warning(f"Could not parse publication_datetime: {publication_datetime_str}")
                return False
            
            # Prepare stats data for upsert
            stats_dict = {
                "source_code": source_code,
                "publication_datetime": publication_datetime,
                "count": count,
            }
            
            # Perform UPSERT
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


class S3ToPostgreProcessor:
    """Main processor for S3 to PostgreSQL data flow."""

    def __init__(
        self,
        s3_config: S3Config,
        processing_config: ProcessingConfig,
    ):
        self.s3_config = s3_config
        self.processing_config = processing_config
        self.s3_client = S3Client(s3_config)
        self.stats = ProcessingStats()
        
        # Create temp directory
        self.tmp_dir = Path(processing_config.local_tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """Execute the S3 to PostgreSQL processing."""
        await asyncio.to_thread(self._run_sync)
        self._log_summary()

    def _run_sync(self) -> None:
        """Synchronous processing logic."""
        logging.info("Starting S3 to PostgreSQL processing job")
        
        # Process articles
        if self.processing_config.process_articles:
            logging.info("=" * 80)
            logging.info("PROCESSING ARTICLES")
            logging.info("=" * 80)
            self._process_articles()
        else:
            logging.info("Skipping article processing (PROCESS_ARTICLES=false)")
        
        # Process stats
        if self.processing_config.process_stats:
            logging.info("=" * 80)
            logging.info("PROCESSING STATISTICS")
            logging.info("=" * 80)
            self._process_stats()
        else:
            logging.info("Skipping stats processing (PROCESS_STATS=false)")
        
        # TODO: DBT models processing
        # This will combine the two tables (factiva_articles and stats_factiva_articles)
        # using DBT transformations
        logging.info("=" * 80)
        logging.info("TODO: DBT MODELS PROCESSING")
        logging.info("This will be implemented later to combine article and stats data")
        logging.info("=" * 80)

    def _process_articles(self) -> None:
        """Process article files from S3."""
        try:
            processor = ArticleProcessor(
                self.s3_client, self.s3_config, self.tmp_dir
            )
            
            # Get unprocessed files
            files = processor.get_unprocessed_article_files(
                self.processing_config.lookback_days
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
                    self.stats.sources_deleted += counts["sources_deleted"]
                except Exception as e:
                    logging.error(f"Failed to process article file {file_key}: {e}")
                    self.stats.errors += 1
            
            logging.info(f"Processed {self.stats.articles_processed} article files")
            
        except Exception as e:
            logging.error(f"Error in article processing: {e}")
            self.stats.errors += 1
            raise

    def _process_stats(self) -> None:
        """Process stats files from S3."""
        try:
            processor = StatsProcessor(
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

    def _log_summary(self) -> None:
        """Log summary statistics."""
        logging.info("=" * 80)
        logging.info("S3 TO POSTGRESQL PROCESSING SUMMARY")
        logging.info(f"Article files processed: {self.stats.articles_processed}")
        logging.info(f"Articles upserted      : {self.stats.articles_upserted}")
        logging.info(f"Articles deleted       : {self.stats.articles_deleted}")
        logging.info(f"Sources deleted        : {self.stats.sources_deleted}")
        logging.info(f"Stats files processed  : {self.stats.stats_processed}")
        logging.info(f"Stats records upserted : {self.stats.stats_upserted}")
        logging.info(f"Errors                 : {self.stats.errors}")
        logging.info("=" * 80)


async def main() -> None:
    """Main entry point."""
    getLogger()
    logging.info("Launching S3 Factiva to PostgreSQL job")
    sentry_init()

    s3_config = S3Config.from_env()
    processing_config = ProcessingConfig.from_env()
    
    processor = S3ToPostgreProcessor(s3_config, processing_config)

    health_task = asyncio.create_task(run_health_check_server())
    try:
        await processor.run()
    finally:
        health_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover - graceful shutdown
        logging.info("Job interrupted by user")
        sys.exit(1)
    except Exception as error:
        logging.fatal(f"S3 to PostgreSQL job failed: {error}")
        sys.exit(1)
    sys.exit(0)

