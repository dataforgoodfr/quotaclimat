#!/usr/bin/env python3
"""Fetch OuestFrance ecology articles from their API and save to S3 in Factiva format.

This script:
1. Fetches XML from the OuestFrance API (ecology articles only)
2. Parses each <article> element
3. Converts to Factiva-format JSON (validated via Pydantic schema)
4. Saves partitioned by date as {year}_{month}_{day}_stream.json
5. Uploads to S3 at the same location as Factiva articles

The existing Factiva S3→PostgreSQL pipeline then picks them up automatically.
"""

import json
import logging
import math
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import boto3
import requests

from quotaclimat.data_ingestion.factiva.schemas.article_schema import (
    FactivaArticleAttributes,
    FactivaArticleEnvelope,
    FactivaS3Document,
)
from quotaclimat.data_processing.mediatree.s3.s3_utils import upload_folder_to_s3


# --- Configuration from environment variables ---

def _get_secret(env_var: str) -> str:
    """Read env var value, or if it's a file path, read the file content."""
    value = os.environ.get(env_var, "")
    if value and os.path.exists(value):
        with open(value, "r") as f:
            return f.read().strip()
    return value


# OuestFrance API
OUESTFRANCE_API_URL = os.getenv("OUESTFRANCE_API_URL", "")
OUESTFRANCE_API_TOKEN = _get_secret("OUESTFRANCE_API_TOKEN")

# S3 configuration
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
S3_REGION = os.getenv("S3_REGION", "fr-par")
S3_ACCESS_KEY = _get_secret("S3_ACCESS_KEY")
S3_SECRET_KEY = _get_secret("S3_SECRET_KEY")
S3_BASE_PREFIX = os.getenv("S3_BASE_PREFIX", "country_france")

# Date filtering
START_DATE = os.getenv("OUESTFRANCE_START_DATE", datetime.now().strftime("%Y-%m-%d"))
NUMBER_DAYS_PRIOR = int(os.getenv("OUESTFRANCE_DAYS_PRIOR", "7"))

# Batching
ARTICLES_BATCH_SAVE = int(os.getenv("ARTICLES_BATCH_SAVE", "1000"))

# Constants
SOURCE_CODE = "OUESTFR"
SOURCE_NAME = "Ouest-France"


# --- HTML stripping ---

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def strip_html(text: Optional[str]) -> str:
    """Remove HTML tags from text, returning plain text."""
    if not text:
        return ""
    return _HTML_TAG_RE.sub("", text).strip()


def get_text(element: Optional[ET.Element]) -> str:
    """Get text content from an XML element, or empty string if None."""
    if element is None or element.text is None:
        return ""
    return element.text.strip()


# --- API fetching ---

def fetch_xml_from_api(api_url: str, token: str, start_date: str, end_date: str) -> str:
    """Fetch XML content from the OuestFrance API.

    Args:
        api_url: Base URL of the OuestFrance API
        token: Authentication token
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        Raw XML string from the API response
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/xml",
    }
    params = {
        "start_date": start_date,
        "end_date": end_date,
    }

    logging.info(f"Fetching OuestFrance articles from {api_url} ({start_date} to {end_date})")
    response = requests.get(api_url, headers=headers, params=params, timeout=300)
    response.raise_for_status()

    logging.info(f"Received {len(response.content)} bytes from API")
    return response.text


def fetch_xml_from_file(file_path: str) -> str:
    """Read XML from a local file (for testing / manual import)."""
    logging.info(f"Reading XML from local file: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()


# --- XML parsing ---

def parse_article_xml(article_elem: ET.Element) -> Optional[FactivaArticleEnvelope]:
    """Parse a single <article> XML element into a Factiva-format article.

    Returns:
        FactivaArticleEnvelope validated by Pydantic, or None if parsing fails.
    """
    try:
        # Article ID
        article_id = get_text(article_elem.find("id"))
        if not article_id:
            logging.warning("Article missing <id>, skipping")
            return None

        an = f"{SOURCE_CODE}:{article_id}"

        # Publication date from first <parution>
        parution = article_elem.find("parutions/parution")
        if parution is not None:
            pub_date_str = get_text(parution.find("dateParution"))
        else:
            pub_date_str = ""

        # Format dates for Factiva
        publication_datetime = pub_date_str if pub_date_str else None
        publication_date = pub_date_str.split("T")[0] if pub_date_str else None

        # Content fields (strip HTML from body and snippet)
        surtitre = get_text(article_elem.find("surtitre"))
        title = get_text(article_elem.find("titre"))
        if surtitre:
            title = f"{surtitre} - {title}" if title else surtitre
        body = strip_html(get_text(article_elem.find("texte")))
        snippet = strip_html(get_text(article_elem.find("chapeau")))

        # Author
        byline = get_text(article_elem.find("signature"))

        # Word count
        word_count_str = get_text(article_elem.find("nombreMots"))
        word_count = int(word_count_str) if word_count_str else None

        # Photo captions → art field
        photo_captions = []
        photo_credits = []
        for photo in article_elem.findall("photos/photo"):
            legende = get_text(photo.find("legende"))
            if legende:
                photo_captions.append(strip_html(legende))
            credit = get_text(photo.find("credit"))
            if credit:
                photo_credits.append(strip_html(credit))

        art = " ".join(photo_captions) if photo_captions else ""
        credit = ", ".join(photo_credits) if photo_credits else ""

        # Article URL
        article_url = get_text(article_elem.find("url")) or None

        # Tags
        tags = []
        for tag_elem in article_elem.findall("tags/tag"):
            tag_text = get_text(tag_elem) if tag_elem is not None else ""
            if not tag_text and tag_elem is not None and tag_elem.text:
                tag_text = tag_elem.text.strip()
            if tag_text:
                tags.append(tag_text)

        # Localisations → region_of_origin
        localisations = []
        for loc_elem in article_elem.findall("localisations/localisation"):
            loc_name = get_text(loc_elem.find("nom"))
            if loc_name:
                localisations.append(loc_name)
        region_of_origin = ", ".join(localisations) if localisations else "France"

        # Section from first tag if available
        section = tags[0] if tags else ""

        # Publications list (for reference, stored in section or ignored)
        publications = []
        if parution is not None:
            for pub_elem in parution.findall("publications/publication"):
                pub_text = get_text(pub_elem)
                if pub_text:
                    publications.append(pub_text)

        # Copyright
        copyright_text = f"Copyright {SOURCE_NAME}"

        # Build validated Pydantic model
        attributes = FactivaArticleAttributes(
            an=an,
            source_code=SOURCE_CODE,
            source_name=SOURCE_NAME,
            action="add",
            document_type="paper",
            title=title,
            body=body,
            snippet=snippet,
            art=art,
            byline=byline,
            credit=credit,
            dateline=None,
            publisher_name=SOURCE_NAME,
            section=section,
            copyright=copyright_text,
            publication_datetime=publication_datetime,
            publication_date=publication_date,
            modification_datetime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            modification_date=datetime.now().strftime("%Y-%m-%d"),
            ingestion_datetime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            availability_datetime=publication_datetime,
            language_code="fr",
            region_of_origin=region_of_origin,
            word_count=word_count,
            article_url=article_url,
            tags=tags if tags else None,
        )

        return FactivaArticleEnvelope(
            id=an,
            type="article",
            attributes=attributes,
        )

    except Exception as e:
        logging.error(f"Error parsing article XML: {e}")
        return None


DC_NAMESPACE = "http://purl.org/dc/elements/1.1/"


def parse_rss_item(item_elem: ET.Element) -> Optional[FactivaArticleEnvelope]:
    """Parse a single RSS <item> element into a Factiva-format article.

    Returns:
        FactivaArticleEnvelope, or None if parsing fails.
    """
    try:
        # Article ID from <guid>
        article_id = get_text(item_elem.find("guid"))
        if not article_id:
            logging.warning("RSS item missing <guid>, skipping")
            return None

        an = f"{SOURCE_CODE}:{article_id}"

        # Publication date from <pubDate> (RFC 2822 format)
        pub_date_raw = get_text(item_elem.find("pubDate"))
        publication_datetime = None
        publication_date = None
        if pub_date_raw:
            try:
                dt = parsedate_to_datetime(pub_date_raw)
                publication_datetime = dt.strftime("%Y-%m-%dT%H:%M:%S")
                publication_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                logging.warning(f"Could not parse pubDate: {pub_date_raw}")

        # Content fields
        title = get_text(item_elem.find("title"))
        body = strip_html(get_text(item_elem.find("description")))

        # Word count (computed from body since RSS has no <nombreMots>)
        word_count = len(body.split()) if body else None

        # Author from <dc:creator>
        byline = get_text(item_elem.find(f"{{{DC_NAMESPACE}}}creator"))

        # Photo from <enclosure>
        enclosure = item_elem.find("enclosure")
        art = ""
        if enclosure is not None:
            enclosure_url = enclosure.get("url", "")
            art = enclosure_url if enclosure_url else ""

        # Article URL from <link>
        article_url = get_text(item_elem.find("link")) or None

        copyright_text = f"Copyright {SOURCE_NAME}"

        attributes = FactivaArticleAttributes(
            an=an,
            source_code=SOURCE_CODE,
            source_name=SOURCE_NAME,
            action="add",
            document_type="web",
            title=title,
            body=body,
            snippet="",
            art=art,
            byline=byline,
            credit="",
            dateline=None,
            publisher_name=SOURCE_NAME,
            section="",
            copyright=copyright_text,
            publication_datetime=publication_datetime,
            publication_date=publication_date,
            modification_datetime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            modification_date=datetime.now().strftime("%Y-%m-%d"),
            ingestion_datetime=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            availability_datetime=publication_datetime,
            language_code="fr",
            region_of_origin="France",
            word_count=word_count,
            article_url=article_url,
            tags=None,
        )

        return FactivaArticleEnvelope(
            id=an,
            type="article",
            attributes=attributes,
        )

    except Exception as e:
        logging.error(f"Error parsing RSS item: {e}")
        return None


def parse_all_articles(xml_content: str) -> List[FactivaArticleEnvelope]:
    """Parse OuestFrance XML (auto-detects <contenus> or RSS format)."""
    root = ET.fromstring(xml_content)

    # Detect format: RSS has <rss> or <channel> root, contenus has <contenus>
    if root.tag == "rss" or root.find("channel") is not None:
        items = root.findall(".//item")
        logging.info(f"Detected RSS format with {len(items)} items")
        parser = parse_rss_item
        elements = items
    else:
        elements = root.findall(".//article")
        logging.info(f"Detected contenus format with {len(elements)} articles")
        parser = parse_article_xml

    articles = []
    for elem in elements:
        envelope = parser(elem)
        if envelope is not None:
            articles.append(envelope)

    logging.info(f"Parsed {len(articles)} articles from XML")
    return articles


# --- Partitioning and saving ---

def partition_articles_by_date(
    articles: List[FactivaArticleEnvelope],
) -> Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]]:
    """Partition articles by year/month/day.

    Returns:
        Nested dict: {year: {month: {day: [article_dicts]}}}
    """
    partitioned: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]] = {}

    for envelope in articles:
        pub_date = envelope.attributes.publication_date
        if not pub_date:
            logging.warning(f"Article {envelope.id} has no publication_date, skipping")
            continue

        parts = pub_date.split("-")
        if len(parts) < 3:
            logging.warning(f"Article {envelope.id} has invalid publication_date: {pub_date}")
            continue

        year, month, day = parts[0], parts[1], parts[2].split("T")[0]

        partitioned.setdefault(year, {}).setdefault(month, {}).setdefault(day, [])
        partitioned[year][month][day].append(envelope.to_dict())

    return partitioned


def save_articles_to_local(
    partitioned: Dict[str, Dict[str, Dict[str, List[Dict[str, Any]]]]],
    output_dir: str,
) -> None:
    """Save partitioned articles as Factiva-format JSON files."""
    for year in partitioned:
        for month in partitioned[year]:
            dir_path = os.path.join(output_dir, f"year_{year}", f"month_{month}")
            os.makedirs(dir_path, exist_ok=True)

            for day in partitioned[year][month]:
                article_dicts = partitioned[year][month][day]
                n_articles = len(article_dicts)
                n_batches = math.ceil(n_articles / ARTICLES_BATCH_SAVE)

                for batch_idx in range(n_batches):
                    batch_start = batch_idx * ARTICLES_BATCH_SAVE
                    batch_end = min((batch_idx + 1) * ARTICLES_BATCH_SAVE, n_articles)
                    batch = article_dicts[batch_start:batch_end]

                    # Validate the batch via Pydantic before writing
                    doc = FactivaS3Document(data=batch)

                    filename = f"{year}_{month}_{day}_00_00_00_{batch_idx + 1}_stream.json"
                    filepath = os.path.join(dir_path, filename)

                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(doc.to_dict(), f, ensure_ascii=False, indent=2)

                    logging.info(f"Saved {len(batch)} articles to {filepath}")


# --- Main ---

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    output_dir = "/tmp/ouest_france_articles"
    os.makedirs(output_dir, exist_ok=True)

    # Determine date range
    end_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    start_date = end_date - timedelta(days=NUMBER_DAYS_PRIOR)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Fetch XML from API or local file
    local_xml_path = os.getenv("OUESTFRANCE_LOCAL_XML")
    if local_xml_path:
        xml_content = fetch_xml_from_file(local_xml_path)
    else:
        if not OUESTFRANCE_API_URL:
            logging.error("OUESTFRANCE_API_URL is required (or set OUESTFRANCE_LOCAL_XML for local file)")
            sys.exit(1)
        xml_content = fetch_xml_from_api(
            OUESTFRANCE_API_URL, OUESTFRANCE_API_TOKEN, start_date_str, end_date_str
        )

    # Parse articles
    articles = parse_all_articles(xml_content)
    if not articles:
        logging.info("No articles found, exiting")
        return

    # Partition by date and save
    partitioned = partition_articles_by_date(articles)
    save_articles_to_local(partitioned, output_dir)

    # Upload to S3
    s3_client = boto3.client(
        service_name="s3",
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=f"https://s3.{S3_REGION}.scw.cloud",
    )

    upload_folder_to_s3(
        output_dir,
        bucket_name=S3_BUCKET,
        base_s3_path=f"{S3_BASE_PREFIX}/articles",
        s3_client=s3_client,
    )

    logging.info("OuestFrance API to S3 upload complete")


if __name__ == "__main__":
    main()
