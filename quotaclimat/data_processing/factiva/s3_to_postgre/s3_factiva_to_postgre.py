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
import subprocess
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
from postgres.schemas.factiva_models import Factiva_Article, Stats_Factiva_Article
from quotaclimat.data_ingestion.factiva.utils_data_processing.utils_extract import (
    load_json_values,
)
from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    build_article_text,
    extract_keyword_data_from_article,
)
from quotaclimat.data_processing.factiva.s3_to_postgre.update_dictionary_factiva import (
    update_dictionary_factiva,
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
class UpdateConfig:
    """Configuration for UPDATE mode (re-detect keywords on existing data)."""

    enabled: bool
    start_date: Optional[str]
    end_date: Optional[str]
    batch_size: int
    source_codes: List[str]
    biodiversity_only: bool
    ressource_only: bool
    climate_only: bool

    @classmethod
    def from_env(cls) -> "UpdateConfig":
        enabled = os.getenv("UPDATE", "false").lower() == "true"
        start_date = os.getenv("START_DATE_UPDATE")
        end_date = os.getenv("END_DATE")
        batch_size = int(os.getenv("BATCH_SIZE", "1000"))
        
        # SOURCE_CODE_UPDATE can be comma-separated list
        source_codes_str = os.getenv("SOURCE_CODE_UPDATE", "")
        source_codes = [s.strip() for s in source_codes_str.split(",") if s.strip()]
        
        biodiversity_only = os.getenv("BIODIVERSITY_ONLY", "false").lower() == "true"
        ressource_only = os.getenv("RESSOURCE_ONLY", "false").lower() == "true"
        climate_only = os.getenv("CLIMATE_ONLY", "false").lower() == "true"

        return cls(
            enabled=enabled,
            start_date=start_date,
            end_date=end_date,
            batch_size=batch_size,
            source_codes=source_codes,
            biodiversity_only=biodiversity_only,
            ressource_only=ressource_only,
            climate_only=climate_only,
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
    # For UPDATE mode
    update_articles_processed: int = 0  # Articles processed for keyword update
    update_articles_updated: int = 0    # Articles with actual keyword changes
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
        now = datetime.now(timezone.utc)
        
        all_files = []
        
        # Look through year/month folders - only scan months within the lookback period
        current_date = cutoff_date.replace(day=1)  # Start from first day of cutoff month
        end_date = now.replace(day=1)  # End at first day of current month
        
        while current_date <= end_date:
            prefix = f"{self.config.base_prefix}/articles/year_{current_date.year}/month_{current_date.month:02d}/"
            files = self.s3_client.list_files_in_prefix(prefix)
            all_files.extend(files)
            
            # Move to next month
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
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


class ArticleUpdater:
    """Update keywords on existing articles in PostgreSQL (UPDATE mode)."""

    def __init__(self, update_config: UpdateConfig):
        self.config = update_config
        # Use custom JSON serializer to preserve Unicode characters in JSON columns
        self.engine = connect_to_db(use_custom_json_serializer=True)

    def get_total_count(self) -> int:
        """Get total count of articles to update based on filters."""
        from sqlalchemy import func
        from sqlalchemy.orm import Session
        
        with Session(self.engine) as session:
            query = session.query(func.count(Factiva_Article.an))
            query = self._apply_filters(query)
            return query.scalar() or 0

    def _apply_filters(self, query):
        """Apply filters based on configuration."""
        
        # Date filters on publication_datetime
        if self.config.start_date:
            try:
                start_dt = date_parser.parse(self.config.start_date)
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                query = query.filter(Factiva_Article.publication_datetime >= start_dt)
            except Exception as e:
                logging.warning(f"Could not parse START_DATE_UPDATE '{self.config.start_date}': {e}")
        
        if self.config.end_date:
            try:
                end_dt = date_parser.parse(self.config.end_date)
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                query = query.filter(Factiva_Article.publication_datetime <= end_dt)
            except Exception as e:
                logging.warning(f"Could not parse END_DATE '{self.config.end_date}': {e}")
        
        # Source code filter
        if self.config.source_codes:
            query = query.filter(Factiva_Article.source_code.in_(self.config.source_codes))
        
        # Crisis type filters (only one can be active, or none)
        if self.config.biodiversity_only:
            query = query.filter(Factiva_Article.number_of_biodiversite_no_hrfp >= 1)
        elif self.config.ressource_only:
            query = query.filter(Factiva_Article.number_of_ressources_no_hrfp >= 1)
        elif self.config.climate_only:
            query = query.filter(Factiva_Article.number_of_climat_no_hrfp >= 1)
        
        # Only non-deleted articles
        query = query.filter(Factiva_Article.is_deleted.is_(False))
        
        return query

    def get_all_article_ids(self) -> List[str]:
        """Get ALL article IDs matching filters BEFORE any updates.
        
        This is crucial because filters like CLIMATE_ONLY may change after updates,
        causing OFFSET/LIMIT to skip or duplicate articles.
        """
        from sqlalchemy.orm import Session
        
        with Session(self.engine) as session:
            query = session.query(Factiva_Article.an)
            query = self._apply_filters(query)
            query = query.order_by(Factiva_Article.publication_datetime, Factiva_Article.an)
            
            # Get all IDs as a flat list
            result = query.all()
            return [row[0] for row in result]

    def get_articles_by_ids(self, article_ids: List[str]) -> List[Factiva_Article]:
        """Get articles by their IDs."""
        from sqlalchemy.orm import Session
        
        if not article_ids:
            return []
        
        with Session(self.engine) as session:
            query = session.query(Factiva_Article).filter(
                Factiva_Article.an.in_(article_ids)
            )
            
            # Detach from session to use outside
            articles = query.all()
            session.expunge_all()
            return articles

    def update_article_keywords(self, article: Factiva_Article) -> tuple[bool, bool]:
        """Re-detect keywords for a single article and update in database if changes detected.
        
        Returns:
            Tuple of (processed: bool, updated: bool)
            - processed: True if article was processed without error
            - updated: True if article had changes and was updated in DB
        """
        from sqlalchemy.orm import Session
        
        try:
            # Build article text from stored content
            article_text = self._build_article_text_from_db(article)
            
            # Re-detect keywords using the same function as import mode
            keyword_data = extract_keyword_data_from_article(article_text)
            
            # Check for differences in keyword counts
            has_changes = self._log_differences(article, keyword_data)
            
            if has_changes:
                # Only UPDATE if there are actual changes (more efficient)
                with Session(self.engine) as session:
                    session.query(Factiva_Article).filter(
                        Factiva_Article.an == article.an
                    ).update(
                        {
                            **keyword_data,
                            "updated_at": datetime.now(timezone.utc),
                        },
                        synchronize_session=False
                    )
                    session.commit()
                
                logging.info(f"Updated article {article.an} (keyword counts changed)")
                return (True, True)  # processed=True, updated=True
            else:
                logging.debug(f"Skipped article {article.an} (no changes in keyword counts)")
                return (True, False)  # processed=True, updated=False
                
        except Exception as e:
            logging.error(f"Error processing article {article.an}: {e}")
            return (False, False)  # processed=False, updated=False

    def _build_article_text_from_db(self, article: Factiva_Article) -> str:
        """Build combined text from article stored in database."""
        text_parts = []
        
        if article.title:
            text_parts.append(article.title)
        if article.body:
            text_parts.append(article.body)
        if article.snippet:
            text_parts.append(article.snippet)
        if article.art:
            text_parts.append(article.art)
        
        combined_text = " ".join(text_parts)
        return combined_text.lower()

    def _log_differences(self, article: Factiva_Article, new_keyword_data: dict) -> bool:
        """Log all differences between current and new keyword data. Returns True if any difference found.
        
        We compare only the keyword lists (source of truth). All counts (individual and aggregated)
        are derived from these lists, so if lists are identical, counts will be identical too.
        
        Special case: If any field is NULL or empty in the database, force update to populate all fields.
        """
        differences = []
        
        # Compare keyword lists (source of truth) - both non-HRFP and HRFP
        # These are the actual keywords detected, so if they change, we need to update
        list_fields = [
            # Non-HRFP lists
            "changement_climatique_constat_keywords",
            "changement_climatique_causes_keywords",
            "changement_climatique_consequences_keywords",
            "attenuation_climatique_solutions_keywords",
            "adaptation_climatique_solutions_keywords",
            "changement_climatique_solutions_keywords",
            "ressources_constat_keywords",
            "ressources_solutions_keywords",
            "biodiversite_concepts_generaux_keywords",
            "biodiversite_causes_keywords",
            "biodiversite_consequences_keywords",
            "biodiversite_solutions_keywords",
            # HRFP lists
            "changement_climatique_constat_keywords_hrfp",
            "changement_climatique_causes_keywords_hrfp",
            "changement_climatique_consequences_keywords_hrfp",
            "attenuation_climatique_solutions_keywords_hrfp",
            "adaptation_climatique_solutions_keywords_hrfp",
            "changement_climatique_solutions_keywords_hrfp",
            "ressources_constat_keywords_hrfp",
            "ressources_solutions_keywords_hrfp",
            "biodiversite_concepts_generaux_keywords_hrfp",
            "biodiversite_causes_keywords_hrfp",
            "biodiversite_consequences_keywords_hrfp",
            "biodiversite_solutions_keywords_hrfp",
            # Aggregated crisis lists
            "crises_keywords",
            "crises_keywords_hrfp",
        ]
        
        # First check if any field is NULL - if so, force update
        for field in list_fields:
            old_value = getattr(article, field, None)
            if old_value is None:
                logging.info(f"Force update for {article.an}: field '{field}' is NULL")
                return True
        
        # If no NULL/empty fields, check for actual differences
        for field in list_fields:
            old_list = getattr(article, field, None) or []
            new_list = new_keyword_data.get(field, [])
            # Compare as sets to ignore order and handle duplicates
            # This catches cases where same count but different keywords
            if set(old_list) != set(new_list):
                old_unique = sorted(set(old_list))
                new_unique = sorted(set(new_list))
                differences.append(f"{field}: {old_unique} -> {new_unique}")
        
        # Also check all_keywords field (compare as JSON)
        old_all_keywords = getattr(article, "all_keywords", None) or []
        new_all_keywords = new_keyword_data.get("all_keywords", [])
        # Sort both lists for consistent comparison
        old_sorted = sorted(old_all_keywords, key=lambda x: (x.get("keyword", ""), x.get("theme", ""), x.get("is_hrfp", False)))
        new_sorted = sorted(new_all_keywords, key=lambda x: (x.get("keyword", ""), x.get("theme", ""), x.get("is_hrfp", False)))
        if old_sorted != new_sorted:
            differences.append(f"all_keywords: changed (old: {len(old_all_keywords)} entries, new: {len(new_all_keywords)} entries)")
        
        if differences:
            logging.info(f"Differences detected for {article.an}:")
            for diff in differences:
                logging.info(f"  - {diff}")
            return True
        else:
            logging.debug(f"No differences for {article.an}")
            return False

    def run_update(self) -> int:
        """Run the keyword update process. Returns number of updated articles."""
        logging.info("=" * 80)
        logging.info("STARTING UPDATE MODE - Re-detecting keywords on existing articles")
        logging.info("=" * 80)
        
        # Log configuration
        logging.info("Configuration:")
        logging.info(f"  START_DATE_UPDATE: {self.config.start_date or 'Not set (all dates)'}")
        logging.info(f"  END_DATE: {self.config.end_date or 'Not set (all dates)'}")
        logging.info(f"  BATCH_SIZE: {self.config.batch_size}")
        logging.info(f"  SOURCE_CODE_UPDATE: {self.config.source_codes or 'Not set (all sources)'}")
        logging.info(f"  BIODIVERSITY_ONLY: {self.config.biodiversity_only}")
        logging.info(f"  RESSOURCE_ONLY: {self.config.ressource_only}")
        logging.info(f"  CLIMATE_ONLY: {self.config.climate_only}")
        
        # IMPORTANT: Get ALL article IDs BEFORE any updates
        # This prevents OFFSET issues when filters (CLIMATE_ONLY, etc.) change after updates
        logging.info("Fetching all article IDs matching filters (before any updates)...")
        all_article_ids = self.get_all_article_ids()
        total_count = len(all_article_ids)
        logging.info(f"Total articles to process: {total_count}")
        
        if total_count == 0:
            logging.warning("No articles found matching the filters. Check your START_DATE_UPDATE and END_DATE.")
            return 0
        
        # Process in batches using the fixed list of IDs
        processed_count = 0  # Articles processed without error
        updated_count = 0    # Articles with actual keyword changes
        batch_size = self.config.batch_size
        
        for batch_start in range(0, total_count, batch_size):
            batch_end = min(batch_start + batch_size, total_count)
            batch_ids = all_article_ids[batch_start:batch_end]
            
            logging.info(f"Processing batch: articles {batch_start + 1}-{batch_end} (total: {total_count})")
            
            articles = self.get_articles_by_ids(batch_ids)
            
            for article in articles:
                processed, updated = self.update_article_keywords(article)
                if processed:
                    processed_count += 1
                if updated:
                    updated_count += 1
            
            logging.info(f"Batch complete. Processed: {processed_count}, Updated: {updated_count}")
        
        logging.info("=" * 80)
        logging.info("UPDATE MODE COMPLETE")
        logging.info(f"  Articles processed for keyword update: {processed_count}")
        logging.info(f"  Articles updated (keyword counts changed): {updated_count}")
        logging.info("=" * 80)
        
        return (processed_count, updated_count)


class S3ToPostgreProcessor:
    """Main processor for S3 to PostgreSQL data flow."""

    def __init__(
        self,
        s3_config: Optional[S3Config],
        processing_config: Optional[ProcessingConfig],
        update_config: Optional[UpdateConfig] = None,
    ):
        self.s3_config = s3_config
        self.processing_config = processing_config
        self.update_config = update_config
        self.stats = ProcessingStats()
        
        # Only create S3 client and temp dir if NOT in UPDATE mode
        if update_config and update_config.enabled:
            # UPDATE mode: no S3 needed
            self.s3_client = None
            self.tmp_dir = None
        else:
            # IMPORT mode: need S3 client and temp dir
            self.s3_client = S3Client(s3_config)
            self.tmp_dir = Path(processing_config.local_tmp_dir)
            self.tmp_dir.mkdir(parents=True, exist_ok=True)

    async def run(self) -> None:
        """Execute the S3 to PostgreSQL processing."""
        await asyncio.to_thread(self._run_sync)
        self._log_summary()

    def _run_sync(self) -> None:
        """Synchronous processing logic."""
        # Update Dictionary and Keyword_Macro_Category tables at the start (both modes)
        # Controlled by UPDATE_DICTIONARY environment variable (default: true)
        update_dictionary = os.getenv("UPDATE_DICTIONARY", "false").lower() == "true"
        
        if update_dictionary:
            logging.info("=" * 80)
            logging.info("UPDATING DICTIONARY AND KEYWORD_MACRO_CATEGORY TABLES")
            logging.info("=" * 80)
            try:
                engine = connect_to_db(use_custom_json_serializer=True)
                update_dictionary_factiva(engine)
                engine.dispose()
                logging.info("Dictionary and Keyword_Macro_Category tables updated successfully")
            except Exception as e:
                logging.error(f"Failed to update dictionary tables: {e}")
                # Don't fail the entire job if dictionary update fails
                # The job can still process articles with the existing dictionary
        else:
            logging.info("=" * 80)
            logging.info("SKIPPING DICTIONARY UPDATE (UPDATE_DICTIONARY=false)")
            logging.info("=" * 80)
        
        # Check if UPDATE mode is enabled
        if self.update_config and self.update_config.enabled:
            logging.info("UPDATE mode enabled - Re-detecting keywords on existing data")
            self._run_update_mode()
        else:
            # IMPORT mode: process S3 files
            logging.info("Starting S3 to PostgreSQL processing job (normal import mode)")
            
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
        
        # Duplicate detection processing
        # This recalculates duplicate_status for all articles before DBT runs
        detect_duplicates = os.getenv("DETECT_DUPLICATES", "true").lower() == "true"
        if detect_duplicates:
            logging.info("=" * 80)
            logging.info("DUPLICATE DETECTION PROCESSING")
            logging.info("=" * 80)
            self._detect_and_mark_duplicates()
        else:
            logging.info("Skipping duplicate detection (DETECT_DUPLICATES=false)")
        
        # DBT models processing
        # This combines factiva_articles, stats_factiva_articles, and source_classification
        # to calculate environmental indicators
        run_dbt = os.getenv("RUN_DBT", "true").lower() == "true"
        if run_dbt:
            logging.info("=" * 80)
            logging.info("DBT MODELS PROCESSING")
            logging.info("=" * 80)
            self._run_dbt_models()
        else:
            logging.info("Skipping DBT models processing (RUN_DBT=false)")

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

    def _run_update_mode(self) -> None:
        """Run UPDATE mode - re-detect keywords on existing articles."""
        try:
            updater = ArticleUpdater(self.update_config)
            processed, updated = updater.run_update()
            self.stats.update_articles_processed = processed
            self.stats.update_articles_updated = updated
        except Exception as e:
            logging.error(f"Error in update mode: {e}")
            self.stats.errors += 1
            raise

    def _detect_and_mark_duplicates(self) -> None:
        """
        Detect and mark duplicate articles in the factiva_articles table.
        
        Duplicates are identified by matching:
        - source_code
        - title
        - snippet
        - body
        - word_count
        
        For each group of duplicates:
        - One article (most recent modification_datetime, then by AN) gets "DUP_UNIQUE_VERSION"
        - All others get "DUP"
        - Non-duplicates get "NOT_DUP"
        """
        try:
            logging.info("Starting duplicate detection for all articles...")
            engine = connect_to_db(use_custom_json_serializer=True)
            
            # SQL query to detect duplicates and assign status
            # Uses window functions to identify duplicates and rank them
            duplicate_detection_query = text("""
                WITH duplicate_groups AS (
                    -- Group articles by duplicate criteria and count duplicates
                    SELECT 
                        an,
                        source_code,
                        title,
                        snippet,
                        body,
                        word_count,
                        modification_datetime,
                        COUNT(*) OVER (
                            PARTITION BY 
                                source_code,
                                COALESCE(title, ''),
                                COALESCE(snippet, ''),
                                COALESCE(body, ''),
                                COALESCE(word_count, 0)
                        ) AS duplicate_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY 
                                source_code,
                                COALESCE(title, ''),
                                COALESCE(snippet, ''),
                                COALESCE(body, ''),
                                COALESCE(word_count, 0)
                            ORDER BY 
                                modification_datetime DESC NULLS LAST,
                                an ASC
                        ) AS duplicate_rank
                    FROM factiva_articles
                    WHERE is_deleted = FALSE
                ),
                duplicate_status_assignment AS (
                    -- Assign duplicate status based on count and rank
                    SELECT 
                        an,
                        CASE 
                            WHEN duplicate_count = 1 THEN 'NOT_DUP'
                            WHEN duplicate_count > 1 AND duplicate_rank = 1 THEN 'DUP_UNIQUE_VERSION'
                            ELSE 'DUP'
                        END AS new_duplicate_status
                    FROM duplicate_groups
                )
                -- Update the factiva_articles table
                UPDATE factiva_articles fa
                SET 
                    duplicate_status = dsa.new_duplicate_status,
                    updated_at = NOW()
                FROM duplicate_status_assignment dsa
                WHERE fa.an = dsa.an
                    AND (fa.duplicate_status IS DISTINCT FROM dsa.new_duplicate_status)
            """)
            
            with engine.begin() as conn:
                result = conn.execute(duplicate_detection_query)
                updated_count = result.rowcount
                
            logging.info(f"Duplicate detection complete. Updated {updated_count} articles.")
            
            # Log statistics about duplicates
            stats_query = text("""
                SELECT 
                    duplicate_status,
                    COUNT(*) as count
                FROM factiva_articles
                WHERE is_deleted = FALSE
                GROUP BY duplicate_status
                ORDER BY duplicate_status
            """)
            
            with engine.begin() as conn:
                stats_result = conn.execute(stats_query)
                stats = stats_result.fetchall()
                
            logging.info("Duplicate status distribution:")
            for status, count in stats:
                logging.info(f"  {status or 'NULL'}: {count:,} articles")
            
            engine.dispose()
            
        except Exception as e:
            logging.error(f"Error detecting duplicates: {e}")
            self.stats.errors += 1
            # Don't raise - allow the job to continue

    def _run_dbt_models(self) -> None:
        """Run DBT models to calculate environmental indicators."""
        try:
            # Get DBT project directory for print media
            dbt_project_dir = os.path.join(os.getcwd(), "my_dbt_project_print_media")
            
            # Check if DBT project exists
            if not os.path.exists(dbt_project_dir):
                logging.error(f"DBT project directory not found: {dbt_project_dir}")
                return
            
            # Get environment variables for DBT configuration
            multiplier_climat = os.getenv("MULTIPLIER_HRFP_CLIMAT", "0")
            multiplier_biodiv = os.getenv("MULTIPLIER_HRFP_BIODIV", "0")
            multiplier_ressource = os.getenv("MULTIPLIER_HRFP_RESSOURCE", "0")
            threshold_biod_clim_ress = os.getenv("THRESHOLD_BIOD_CLIM_RESS", "3,2,2")
            threshold_biod_causal = os.getenv("THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT", "1,1,1,1")
            threshold_clim_causal = os.getenv("THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT", "2,1,1,1")
            threshold_ress_causal = os.getenv("THRESHOLD_RESS_CONST_SOLUT", "1,1")
            
            logging.info("Running DBT models with configuration:")
            logging.info(f"  MULTIPLIER_HRFP_CLIMAT: {multiplier_climat}")
            logging.info(f"  MULTIPLIER_HRFP_BIODIV: {multiplier_biodiv}")
            logging.info(f"  MULTIPLIER_HRFP_RESSOURCE: {multiplier_ressource}")
            logging.info(f"  THRESHOLD_BIOD_CLIM_RESS: {threshold_biod_clim_ress}")
            logging.info(f"  THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT: {threshold_biod_causal}")
            logging.info(f"  THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT: {threshold_clim_causal}")
            logging.info(f"  THRESHOLD_RESS_CONST_SOLUT: {threshold_ress_causal}")
            
            # Build DBT command
            # Run only the print_media_crises_indicators model
            # Use --full-refresh to ensure all changes are captured (articles, stats, thresholds)
            dbt_command = [
                "dbt", "run",
                "--full-refresh",
                "--select", "print_media_crises_indicators",
                "--project-dir", dbt_project_dir,
            ]
            
            logging.info(f"Executing DBT command: {' '.join(dbt_command)}")
            
            # Run DBT command
            result = subprocess.run(
                dbt_command,
                capture_output=True,
                text=True,
                env={
                    **os.environ,
                    "MULTIPLIER_HRFP_CLIMAT": multiplier_climat,
                    "MULTIPLIER_HRFP_BIODIV": multiplier_biodiv,
                    "MULTIPLIER_HRFP_RESSOURCE": multiplier_ressource,
                    "THRESHOLD_BIOD_CLIM_RESS": threshold_biod_clim_ress,
                    "THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT": threshold_biod_causal,
                    "THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT": threshold_clim_causal,
                    "THRESHOLD_RESS_CONST_SOLUT": threshold_ress_causal,
                }
            )
            
            # Log output
            if result.stdout:
                logging.info(f"DBT stdout:\n{result.stdout}")
            if result.stderr:
                logging.warning(f"DBT stderr:\n{result.stderr}")
            
            # Check if command succeeded
            if result.returncode != 0:
                logging.error(f"DBT command failed with return code {result.returncode}")
                self.stats.errors += 1
            else:
                logging.info("DBT models executed successfully")
                
        except Exception as e:
            logging.error(f"Error running DBT models: {e}")
            self.stats.errors += 1

    def _log_summary(self) -> None:
        """Log summary statistics."""
        logging.info("=" * 80)
        logging.info("S3 TO POSTGRESQL PROCESSING SUMMARY")
        
        # Check if we were in UPDATE mode
        if self.update_config and self.update_config.enabled:
            logging.info("Mode                          : UPDATE (keyword re-detection)")
            logging.info(f"Articles processed for update : {self.stats.update_articles_processed}")
            logging.info(f"Articles updated              : {self.stats.update_articles_updated}")
        else:
            logging.info("Mode                   : IMPORT (S3 to PostgreSQL)")
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

    # Load configurations
    update_config = UpdateConfig.from_env()
    
    if update_config.enabled:
        logging.info("UPDATE mode detected - will re-detect keywords on existing data")
        # In UPDATE mode, we don't need S3 config
        s3_config = None
        processing_config = None
    else:
        logging.info("Normal import mode - will process S3 files")
        s3_config = S3Config.from_env()
        processing_config = ProcessingConfig.from_env()
    
    processor = S3ToPostgreProcessor(s3_config, processing_config, update_config)

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

