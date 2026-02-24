#!/usr/bin/env python3
import math
import os
import sys
import zipfile
import ftplib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import boto3
from quotaclimat.data_processing.mediatree.s3.s3_utils import upload_folder_to_s3


def get_secret_docker(secret_name):
    secret_value = os.environ.get(secret_name, "")

    if secret_value and os.path.exists(secret_value):
        with open(secret_value, "r") as file:
            return file.read().strip()
    return secret_value


# FTP configuration - these should be configured as environment variables
FTP_HOST = get_secret_docker("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = get_secret_docker("FTP_PASS")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))

# S3 configuration - these should be configured as environment variables
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION")
S3_ACCESS_KEY = get_secret_docker("S3_ACCESS_KEY")
S3_SECRET_KEY = get_secret_docker("S3_SECRET_KEY")

# Date filtering configuration
START_DATE = os.getenv(
    "START_DATE", datetime.now().strftime("%Y%m%d")
)  # Format: YYYYMMDD
NUMBER_DAYS_PRIOR = int(os.getenv("NUMBER_DAYS_PRIOR", 7))
OVERRIDE = os.getenv("OVERRIDE", "false").lower() == "true"

ARTICLES_BATCH_SAVE = int(os.getenv("ARTICLES_BATCH_SAVE", 1000))


def connect_to_ftp():
    """Connect to the FTP server"""
    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USER, FTP_PASS)
        files = ftp.nlst()
        print(files)
        sys.exit(1)
        return ftp
    except Exception as e:
        print(f"Failed to connect to FTP server: {e}")
        sys.exit(1)


def check_s3_date_exists(s3_client, bucket_name, year, month, day):
    """Check if data for a given date already exists in S3"""
    try:
        # Construct the S3 path for the given date
        s3_path = f"year={year}/month={month}/day={day}/"

        # List objects in the S3 bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_path)

        # If there are objects, data for this date already exists
        return "Contents" in response and len(response["Contents"]) > 0
    except Exception as e:
        print(f"Error checking S3 for date {year}-{month}-{day}: {e}")
        return False


def download_files(ftp, local_dir):
    """Download all zip files from FTP server to local directory"""
    try:
        # Create local directory if it doesn't exist
        os.makedirs(local_dir, exist_ok=True)

        # Get list of files
        files = ftp.nlst()
        print(files)
        # Filter for zip files
        zip_files = [f for f in files if f.endswith(".zip")]

        # Filter zip files based on date range
        # Parse start date
        end_date_obj = datetime.strptime(START_DATE, "%Y%m%d")
        # Calculate end date (start date - number of days)
        start_date_obj = end_date_obj - timedelta(days=NUMBER_DAYS_PRIOR)

        # Filter files based on date in filename
        filtered_zip_files = []
        for zip_file in zip_files:
            # Extract date from filename (format: MOFR_MAIN_NonCom_YYYYMMDD_YYYYMMDDHHMMSS.zip)
            # The date is the 4th field in the filename (after splitting by '_')
            try:
                date_part = zip_file.split("_")[3]  # Get the date part from filename
                file_date_obj = datetime.strptime(date_part, "%Y%m%d")
                print(start_date_obj, file_date_obj, end_date_obj)
                # Check if file date is within the specified range
                if start_date_obj <= file_date_obj and file_date_obj <= end_date_obj:
                    filtered_zip_files.append(zip_file)
                    # If OVERRIDE is true, don't check S3
                    # if OVERRIDE:
                    #     filtered_zip_files.append(zip_file)
                    # else:
                    #     # Check if this date already exists in S3
                    #     s3_client = boto3.client(
                    #         service_name="s3",
                    #         region_name=S3_REGION,
                    #         aws_access_key_id=S3_ACCESS_KEY,
                    #         aws_secret_access_key=S3_SECRET_KEY,
                    #         endpoint_url=f"https://s3.{S3_REGION}.scw.cloud",
                    #     )

                    #     # Extract year, month, day from the file date
                    #     year = file_date_obj.year
                    #     month = file_date_obj.month
                    #     day = file_date_obj.day

                    #     # Check if data for this date already exists in S3
                    #     if not check_s3_date_exists(
                    #         s3_client, S3_BUCKET, year, month, day
                    #     ):
                    #         filtered_zip_files.append(zip_file)
                    #     else:
                    #         print(
                    #             f"Skipping {zip_file} - data for {year}-{month}-{day} already exists in S3"
                    #         )
            except (IndexError, ValueError):
                # If we can't extract date from filename, skip this file
                print(f"Warning: Could not extract date from filename {zip_file}")
                continue

        zip_files = filtered_zip_files
        print(zip_files)

        downloaded_files = []
        for zip_file in zip_files:
            local_path = os.path.join(local_dir, zip_file)
            print(f"Downloading {zip_file}...")
            with open(local_path, "wb") as f:
                ftp.retrbinary(f"RETR {zip_file}", f.write)
            downloaded_files.append(local_path)
            print(f"Downloaded {zip_file}")

        return downloaded_files
    except Exception as e:
        print(f"Error downloading files: {e}")
        return []


def extract_zip(zip_path, extract_to):
    """Extract a zip file to the specified directory"""
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {zip_path} to {extract_to}")
        return True
    except Exception as e:
        print(f"Error extracting {zip_path}: {e}")
        return False


def parse_index_xml(index_path):
    """Parse the index.xml file to get article information"""
    try:
        tree = ET.parse(index_path)
        root = tree.getroot()

        # Define namespace
        namespaces = {"edd": "http://ressources.edd.fr/xml/EddfPublication"}

        articles = []
        articles_element = root.find(".//edd:articles", namespaces)

        if articles_element is not None:
            for article in articles_element.findall("edd:article", namespaces):
                article_info = {
                    "id": article.get("id"),
                    "file": article.get("file"),
                    "typeMime": article.get("typeMime"),
                    "size": article.get("size"),
                }
                articles.append(article_info)

        return articles
    except Exception as e:
        print(f"Error parsing index.xml: {e}")
        return []


def parse_article_xml(article_path):
    """Parse an article XML file and extract required information"""
    try:
        tree = ET.parse(article_path)
        root = tree.getroot()

        # Define namespaces
        namespaces = {
            "eddf": "http://ressources.edd.fr/xml/EDDFWebDocument",
            "emd": "http://ressources.edd.fr/xml/EddMetadata",
            "xhtml": "http://www.w3.org/1999/xhtml",
        }

        # Extract metadata
        metadata = root.find(".//eddf:documentMeta", namespaces)

        if metadata is None:
            return None

        # Extract required fields
        article_data = {}

        # Identifier
        identifier_elem = metadata.find(".//emd:identifier", namespaces)
        article_data["an"] = (
            identifier_elem.text if identifier_elem is not None else None
        )

        article_data["document_type"] = "article"
        article_data["action"] = ""
        article_data["source_code"] = "LEMFR"
        article_data["source_name"] = "Le Monde.fr"

        # Publication date
        pubdate_elem = metadata.find(".//emd:publicationdate", namespaces)
        article_data["publication_date"] = (
            pubdate_elem.text + "T00:00:00.000Z" if pubdate_elem is not None else None
        )
        article_data["publication_datetime"] = (
            pubdate_elem.text + "T02:00:00.000Z" if pubdate_elem is not None else None
        )
        article_data["modification_datetime"] = (
            datetime.now().strftime("%Y-%m-d") + "T00:00:00.000Z"
            if identifier_elem is not None
            else None
        )
        article_data["modification_date"] = (
            datetime.now().strftime("%Y-%m-d") + "T00:00:00.000Z"
            if identifier_elem is not None
            else None
        )
        article_data["ingestion_datetime"] = (
            datetime.strptime(identifier_elem.text.split(":")[2], "%Y%m%d").strftime("%Y-%m-d")
            + "T00:00:00.000Z"
            if identifier_elem is not None
            else None
        )
        # Title
        title_elem = metadata.find(".//emd:title", namespaces)
        article_data["title"] = title_elem.text if title_elem is not None else None
        article_data["snippet"] = None

        # Content
        content_elem = root.find(".//eddf:content", namespaces)
        if content_elem is not None:
            # Extract all text content from the content section
            content_text = []
            for elem in content_elem.iter():
                if elem.text and elem.tag != "{http://www.w3.org/1999/xhtml}div":
                    content_text.append(elem.text.strip())
            article_data["body"] = " ".join(content_text)
        else:
            article_data["body"] = None

        article_data["art"] = ""
        article_data["credit"] = ""

        author_elem = metadata.find(".//emd:author", namespaces)
        article_data["byline"] = author_elem.text if author_elem is not None else None
        # Language
        language_elem = metadata.find(".//emd:language", namespaces)
        article_data["language"] = (
            language_elem.text.split("-")[0] if language_elem is not None else None
        )
        # Copyright
        copy_elem = metadata.find(".//emd:rights", namespaces)
        article_data["copyright"] = copy_elem.text if copy_elem is not None else None
        article_data["region_of_origin"] = " "
        # Publisher code
        publisher_elem = metadata.find(".//emd:publisher", namespaces)
        # article_data["publisher_code"] = (
        #     publisher_elem.get("code") if publisher_elem is not None else None
        # )
        article_data["publisher_name"] = (
            publisher_elem.text if publisher_elem is not None else None
        )

        article_data["section"] = ""
        # Word count
        wordcount_elem = metadata.find(".//emd:wordCount", namespaces)
        article_data["word_count"] = (
            int(wordcount_elem.text) if wordcount_elem is not None else None
        )
        article_data["company_codes"] = ""
        article_data["subject_codes"] = ""
        article_data["region_codes"] = ""
        article_data["industry_codes"] = ""
        article_data["person_codes"] = ""
        article_data["currency_codes"] = ""
        article_data["market_index_codes"] = ""
        article_data["company_codes_about"] = ""
        article_data["company_codes_association"] = ""
        article_data["company_codes_lineage"] = ""
        article_data["company_codes_occur"] = ""
        article_data["company_codes_relevance"] = ""
        article_data["company_codes_about_ticker_exchange"] = ""
        article_data["company_codes_association_ticker_exchange"] = ""
        article_data["company_codes_lineage_ticker_exchange"] = ""
        article_data["company_codes_occur_ticker_exchange"] = ""
        article_data["company_codes_relevance_ticker_exchange"] = ""
        article_data["company_codes_ticker_exchange"] = ""
        article_data["availability_datetime"] = article_data["publication_datetime"]
        article_data["attrib_code"] = article_data["source_code"]
        article_data["authors"] = (
            [author_elem.text] if author_elem is not None else None
        )
        article_data["clusters"] = []
        article_data["usage_rights_permitted"] = "analytics"
        article_data["dateline"] = None

        return {"id": article_data["an"], "type": "article", "attributes": article_data}
    except Exception as e:
        print(f"Error parsing article XML {article_path}: {e}")
        return None


def partition_article(article_collection, article_data):
    year = article_data["attributes"]["publication_date"].split("-")[0]
    month = article_data["attributes"]["publication_date"].split("-")[1]
    day = article_data["attributes"]["publication_date"].split("-")[2].split("T")[0]
    if not article_collection.get(year):
        article_collection[year] = {}
    if not article_collection[year].get(month):
        article_collection[year][month] = {}
    if not article_collection[year][month].get(day):
        article_collection[year][month][day] = {"data": []}
    article_collection[year][month][day]["data"].append(article_data)
    return article_collection


def process_downloaded_files(local_dir):
    """Process all downloaded files"""
    processed_articles = {}

    # Find all directories with press_package
    for root_dir, dirs, files in os.walk(local_dir):
        if "press_package" in dirs:
            press_package_path = os.path.join(root_dir, "press_package")
            index_path = os.path.join(press_package_path, "index.xml")

            if os.path.exists(index_path):
                print(f"Processing index file: {index_path}")

                # Parse index.xml to get article information
                articles_info = parse_index_xml(index_path)

                # Process each article
                articles_dir = os.path.join(press_package_path, "articles")
                if os.path.exists(articles_dir):
                    for article_info in articles_info:
                        article_file = article_info.get("file")
                        if article_file:
                            article_path = os.path.join(articles_dir, article_file)
                            if os.path.exists(article_path):
                                print(f"Processing article: {article_file}")
                                article_data = parse_article_xml(article_path)
                                if article_data:
                                    partition_article(processed_articles, article_data)
    return processed_articles


def main():
    """Main function to orchestrate the process"""
    # Create temporary directory for downloads
    temp_dir = "/tmp/ftp_downloads"
    output_dir = "/articles"
    stats_dir = "/stats"

    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(stats_dir, exist_ok=True)

    # Connect to FTP
    ftp = connect_to_ftp()
    s3_client = boto3.client(
        service_name="s3",
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=f"https://s3.{S3_REGION}.scw.cloud",
    )

    # Download files
    downloaded_files = download_files(ftp, temp_dir)
    upload_folder_to_s3(
        temp_dir, bucket_name=S3_BUCKET, base_s3_path="raw", s3_client=s3_client
    )

    # Extract files
    extracted_dirs = []
    for zip_file in downloaded_files:
        extract_dir = zip_file.replace(".zip", "")
        if extract_zip(zip_file, extract_dir):
            extracted_dirs.append(extract_dir)

    # Process extracted files
    articles = process_downloaded_files(temp_dir)

    # Save each article in partitioned directory based on its publication date
    saved_files = []
    for year in articles:
        for month in articles[year]:
            partitioned_dir = f"year_{year}/month_{month}/"
            full_output_dir = os.path.join(output_dir, partitioned_dir)
            full_stats_dir = os.path.join(stats_dir, partitioned_dir)
            os.makedirs(full_output_dir, exist_ok=True)
            os.makedirs(full_stats_dir, exist_ok=True)

            for day in articles[year][month]:
                n_articles = len(articles[year][month][day]["data"])
                n_articles_over_100 = len(
                    [
                        article
                        for article in articles[year][month][day]["data"]
                        if article["attributes"]["word_count"] >= 100
                    ]
                )
                n_batches = math.ceil(n_articles / ARTICLES_BATCH_SAVE)
                stats_filename = f"{year}_{month}_{day}_00_00_00_1_stats.json"
                output_stats_file = os.path.join(full_stats_dir, stats_filename)
                with open(output_stats_file, "w", encoding="utf-8") as f:
                    json.dump(
                        {
                            "count": str(n_articles_over_100),
                            "source_code": "LEMFR",
                            "publication_datetime": f"{year}-{month}-{day}",
                        },
                        f,
                        ensure_ascii=False,
                    )

                for batch_idx in range(n_batches):
                    data = articles[year][month][day]["data"][
                        batch_idx * ARTICLES_BATCH_SAVE : min(
                            (batch_idx + 1) * ARTICLES_BATCH_SAVE, n_articles - 1
                        )
                    ]
                    filename = (
                        f"{year}_{month}_{day}_00_00_00_{batch_idx+1}_stream.json"
                    )
                    output_file = os.path.join(full_output_dir, filename)
                    # Save individual article to JSON file
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump({"data": data}, f, ensure_ascii=False, indent=2)


    
    upload_folder_to_s3(
        output_dir, bucket_name=S3_BUCKET, base_s3_path="country_france/articles", s3_client=s3_client
    )
    upload_folder_to_s3(
        stats_dir, bucket_name=S3_BUCKET, base_s3_path="country_france/nb_articles", s3_client=s3_client
    )
    # Close FTP connection
    ftp.quit()


if __name__ == "__main__":
    main()
