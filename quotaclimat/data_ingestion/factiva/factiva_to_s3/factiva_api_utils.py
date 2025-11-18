import logging
import os
import time
from typing import Dict, List, Optional, Union

import pandas as pd
import requests

# Set up module-level logger
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def submit_time_series(
    source_codes: List[str],
    start_date: str,
    end_date: str,
    minimal_word_count: int,
    language_code: str,
    regex_pattern: Optional[str] = None,
    frequency: str = "DAY",
    date_field: str = "publication_datetime",
    group_dimensions: str = "source_code",
    top: int = -1,
    includes_list: Optional[Dict] = None,
    excludes_list: Optional[Dict] = None,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 300,
) -> Dict:
    """
    Step 1: Submit a Time Series job to the Factiva API.

    Args:
        source_codes: List of source codes (e.g., ['LEMOND', 'LEFIG'])
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        minimal_word_count: Minimal word count for articles
        language_code: Language code (e.g., 'fr', 'en')
        regex_pattern: Optional regex pattern to filter articles content
        frequency: Time period grouping ('DAY', 'MONTH', 'YEAR'). Default: 'DAY'
        date_field: Date type for article search ('publication_datetime', 'modification_datetime', 'ingestion_datetime'). Default: 'publication_datetime'
        group_dimensions: Article property to group counts by (e.g., 'source_code', 'region_codes', 'industry_codes', etc.). Default: 'source_code'
        top: Number of top-ranked (source, date) pairs to return. Default: -1 (no limit, returns all results). 
             Use a positive number to limit results to top N pairs with most articles
        includes_list: Dictionary of Factiva lists to include (e.g., {'industry_codes': ['ivicu']})
        excludes_list: Dictionary of Factiva lists to exclude
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 300)

    Returns:
        Dict: Dictionary containing:
            - success: Boolean indicating if the submission was successful
            - analytics_id: Analytics job ID (if success)
            - error: Error message (if failure)
            - error_details: Error details (if failure)
            - full_response: Full API response

    Raises:
        ValueError: If the user key is not provided
    """
    # Retrieve user key
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    # Build the WHERE clause
    source_codes_str = "', '".join(source_codes)
    where_clause = (
        f"source_code IN ('{source_codes_str}') "
        f"AND publication_datetime >= '{start_date} 00:00:00' "
        f"AND publication_datetime <= '{end_date} 23:59:59' "
        f"AND language_code = '{language_code}' "
    )

    if minimal_word_count > 0:
        where_clause += f" AND word_count >= {minimal_word_count}"

    # Add regex pattern if provided
    if regex_pattern:
        where_clause += (
            " AND REGEXP_CONTAINS(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), "
            f"r'{regex_pattern}')"
        )

    # Build the query object
    query = {
        "where": where_clause,
        "frequency": frequency,
        "date_field": date_field,
        "group_dimensions": group_dimensions,
        "top": top,
    }

    # Add optional parameters
    if includes_list:
        query["includesList"] = includes_list
    if excludes_list:
        query["excludesList"] = excludes_list

    # API headers
    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }

    logger.info("Submitting Time Series job...")
    logger.info(f"Source codes: {source_codes}")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Regex pattern (original): {regex_pattern if regex_pattern else 'None'}")
    logger.info(f"Frequency: {frequency}")
    logger.info(f"Date field: {date_field}")
    logger.info(f"Group dimensions: {group_dimensions}")
    logger.info(f"Top: {top} ({'all results' if top == -1 else f'top {top} pairs'})")
    logger.info(f"Query: {query}")

    try:
        response = requests.post(
            f"{base_url}/analytics",
            headers=headers,
            json={"query": query},
            timeout=timeout,
        )

        if response.status_code not in [200, 201]:
            return {
                "success": False,
                "error": f"Error creating job: {response.status_code}",
                "error_details": response.text,
                "analytics_id": None,
                "full_response": response.json() if response.text else None,
            }

        result = response.json()
        analytics_id = result.get("data", {}).get("id") or result.get("id")
        logger.info(f"Job successfully created! Analytics ID: {analytics_id}")

        return {
            "success": True,
            "analytics_id": analytics_id,
            "error": None,
            "error_details": None,
            "full_response": result,
        }

    except requests.RequestException as e:
        logger.error(f"Request error while creating job: {str(e)}")
        return {
            "success": False,
            "error": f"Request error while creating job: {str(e)}",
            "error_details": None,
            "analytics_id": None,
            "full_response": None,
        }


def poll_time_series(
    analytics_id: str,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    max_attempts: int = 20,
    wait_seconds: int = 60,
    timeout: int = 30,
) -> Dict:
    """
    Step 2: Polls a Time Series job until its completion.

    Args:
        analytics_id: Analytics job ID to monitor
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        max_attempts: Maximum number of polling attempts (default: 30)
        wait_seconds: Waiting time between attempts in seconds (default: 10)
        timeout: HTTP requests timeout in seconds (default: 30)

    Returns:
        Dict: Dictionary containing the results with the keys:
            - success: Boolean indicating if the operation succeeded
            - analytics_id: Analytics job ID
            - status: Final status of the job
            - download_link: URL to download results (if success)
            - full_response: Full API response
            - error: Error message (if failure)

    Raises:
        ValueError: If the user key is not provided
    """
    # Retrieve user key
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    # API headers
    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }

    logger.info(f"Polling job {analytics_id} (max {max_attempts} attempts)...")

    for attempt in range(1, max_attempts + 1):
        time.sleep(wait_seconds)

        try:
            status_response = requests.get(
                f"{base_url}/analytics/{analytics_id}",
                headers=headers,
                timeout=timeout,
            )

            if status_response.status_code == 200:
                status_data = status_response.json()
                current_state = (
                    status_data.get("data", {})
                    .get("attributes", {})
                    .get("current_state")
                )

                logger.info(f"Attempt {attempt}/{max_attempts}: State = {current_state}")

                if current_state == "JOB_STATE_DONE":
                    download_link = (
                        status_data.get("data", {})
                        .get("attributes", {})
                        .get("download_link")
                    )
                    logger.info(f"✅ Job finished! Download link: {download_link}")

                    return {
                        "success": True,
                        "analytics_id": analytics_id,
                        "status": "JOB_STATE_DONE",
                        "download_link": download_link,
                        "full_response": status_data,
                        "error": None,
                    }
                elif current_state == "JOB_STATE_FAILED":
                    logger.error("❌ The job failed!")
                    return {
                        "success": False,
                        "analytics_id": analytics_id,
                        "status": "JOB_STATE_FAILED",
                        "download_link": None,
                        "full_response": status_data,
                        "error": "Job failed",
                    }
                # Else, keep polling (job in progress)

            else:
                logger.warning(
                    f"Attempt {attempt}/{max_attempts}: HTTP error {status_response.status_code}"
                )

        except requests.RequestException as e:
            logger.error(f"Attempt {attempt}/{max_attempts}: Request error - {str(e)}")

        # If this is the last attempt
        if attempt == max_attempts:
            return {
                "success": False,
                "error": f"The job was not completed after {max_attempts} attempts",
                "analytics_id": analytics_id,
                "status": "TIMEOUT",
                "download_link": None,
                "full_response": None,
            }

    # Should not happen, but for safety
    return {
        "success": False,
        "error": f"The job was not completed after {max_attempts} attempts",
        "analytics_id": analytics_id,
        "status": "TIMEOUT",
        "download_link": None,
        "full_response": None,
    }


def download_time_series_results(
    download_link: str,
    user_key: Optional[str] = None,
    timeout: int = 60,
) -> Union[List[dict], pd.DataFrame]:
    """
    Download and parse Time Series results from the download link.

    Args:
        download_link: Download link obtained from poll_time_series
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        timeout: HTTP requests timeout in seconds (default: 60)

    Returns:
        List of dictionaries containing the time series data (JSON Lines format)
        Each dictionary contains: count, source_code (if grouped), publication_datetime, etc.

    Raises:
        ValueError: If the user key is not provided
        ValueError: If the download fails
    """
    # Retrieve user key
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    # API headers
    headers = {
        "user-key": user_key,
        "X-API-VERSION": "3.0",
    }

    logger.info(f"Downloading Time Series results from: {download_link}")

    try:
        response = requests.get(download_link, headers=headers, timeout=timeout)

        if response.status_code != 200:
            raise ValueError(
                f"Error downloading results: {response.status_code} - {response.text[:200]}"
            )

        # Parse JSON Lines format
        results = []
        for line in response.text.strip().split("\n"):
            if line.strip():
                import json

                results.append(json.loads(line))

        logger.info(f"✅ Downloaded {len(results)} time series records")
        return results

    except requests.RequestException as e:
        logger.error(f"Request error while downloading results: {str(e)}")
        raise ValueError(f"Request error while downloading results: {str(e)}")
