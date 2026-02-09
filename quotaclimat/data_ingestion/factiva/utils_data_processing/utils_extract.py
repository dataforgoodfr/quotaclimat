"""
Utility functions for extracting data from the Factiva API
"""

import gzip
import io
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import boto3
import fastavro
import pandas as pd
import requests
from botocore.config import Config as BotoConfig

from quotaclimat.data_ingestion.factiva.inputs.classification_source import (
    SOURCE_CLASSIFICATION,
)


def fetch_taxonomy_api(
    taxonomy_type: str,
    file_format: str,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com/taxonomies",
    timeout: int = 30,
) -> requests.Response:
    """
    Makes a request to Factiva API to retrieve a taxonomy.

    Args:
        taxonomy_type: Taxonomy type (e.g., 'sources', 'regions', 'industries', etc.)
        file_format: File format ('csv', 'json', or 'avro')
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com/taxonomies)
        timeout: Request timeout in seconds (default: 30)

    Returns:
        Response: requests Response object

    Raises:
        ValueError: If the format is not supported
        ValueError: If the user key is not provided
    """
    # Format validation
    valid_formats = ["csv", "json", "avro"]
    if file_format.lower() not in valid_formats:
        raise ValueError(
            f"Unsupported format: {file_format}. Valid formats: {valid_formats}"
        )

    # Retrieve user key
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    # Headers configuration
    headers = {"user-key": user_key, "X-API-VERSION": "3.0"}

    # URL construction
    url = f"{base_url}/{taxonomy_type}/{file_format.lower()}"

    # GET request
    response = requests.get(url, headers=headers, timeout=timeout)

    return response


def read_api_response(
    response: requests.Response,
    file_format: Optional[str] = None,
    return_dataframe: bool = False,
) -> Union[List[str], pd.DataFrame, List[dict]]:
    """
    Reads an API response and returns the data in an appropriate format depending on the type.

    Args:
        response: requests Response object containing the data
        file_format: File format ('csv', 'json', or 'avro').
                     If None, tries to detect from Content-Type or URL
        return_dataframe: For Avro only, if True returns a pandas DataFrame,
                          otherwise returns a list of dictionaries

    Returns:
        - For CSV: List of lines (strings)
        - For JSON: List of JSON lines (strings, JSON Lines format)
        - For Avro: If return_dataframe=True, pandas DataFrame, else list of dictionaries

    Raises:
        ValueError: If the format is not supported or the HTTP response is not 200
    """
    # HTTP status check
    if response.status_code != 200:
        raise ValueError(f"HTTP error {response.status_code}: {response.text[:200]}")

    # Auto format detection if not provided
    if file_format is None:
        # Try to detect from Content-Type
        content_type = response.headers.get("Content-Type", "").lower()
        if "csv" in content_type or "text/csv" in content_type:
            file_format = "csv"
        elif "json" in content_type or "application/json" in content_type:
            file_format = "json"
        elif "avro" in content_type or "application/avro" in content_type:
            file_format = "avro"
        else:
            # Try to detect from URL
            url = response.url.lower()
            if ".csv" in url or "/csv" in url:
                file_format = "csv"
            elif ".json" in url or "/json" in url:
                file_format = "json"
            elif ".avro" in url or "/avro" in url:
                file_format = "avro"
            else:
                raise ValueError(
                    "Could not detect file format. Specify 'file_format' explicitly."
                )

    file_format_lower = file_format.lower()

    # Process according to format
    if file_format_lower == "csv":
        # Return CSV lines
        lines = response.text.strip().split("\n")
        # Filter empty lines
        return [line for line in lines if line.strip()]

    elif file_format_lower == "json":
        # Return JSON lines (JSON Lines format)
        # One line = one JSON object
        lines = response.text.strip().split("\n")
        # Filter empty lines
        return [line for line in lines if line.strip()]

    elif file_format_lower == "avro":
        # Read Avro records
        avro_bytes = io.BytesIO(response.content)
        avro_records = []
        try:
            avro_reader = fastavro.reader(avro_bytes)
            for record in avro_reader:
                avro_records.append(record)
        except Exception as e:
            raise ValueError(f"Error while reading Avro file: {e}")

        # Return DataFrame or list of dicts according to parameter
        if return_dataframe:
            if not avro_records:
                return pd.DataFrame()
            return pd.DataFrame(avro_records)
        else:
            return avro_records

    else:
        raise ValueError(
            f"Unsupported format: {file_format}. Valid formats: csv, json, avro"
        )


def avro_to_dataframe(avro_content: Union[bytes, io.BytesIO]) -> pd.DataFrame:
    """
    Converts an Avro file into a pandas DataFrame.

    Args:
        avro_content: Avro content (bytes or BytesIO)

    Returns:
        pd.DataFrame: DataFrame containing the data from Avro file

    Raises:
        ValueError: If the Avro content cannot be read
    """
    # Convert to BytesIO if needed
    if isinstance(avro_content, bytes):
        avro_bytes = io.BytesIO(avro_content)
    else:
        avro_bytes = avro_content

    # Read Avro records
    avro_records = []
    try:
        avro_reader = fastavro.reader(avro_bytes)
        for record in avro_reader:
            avro_records.append(record)
    except Exception as e:
        raise ValueError(f"Error while reading Avro file: {e}")

    # Create pandas DataFrame
    if not avro_records:
        return pd.DataFrame()

    df = pd.DataFrame(avro_records)
    return df


def save_api_response(
    response: requests.Response,
    file_format: str,
    output_path: str,
    df: Optional[pd.DataFrame] = None,
) -> None:
    """
    Saves the API response to a file in the specified format.
    Generic function working for any endpoint.

    Args:
        response: requests Response object containing the data
        file_format: File format ('csv', 'json', or 'avro')
        output_path: Path to the output file
        df: Optional pandas DataFrame (used only for Avro if provided)

    Raises:
        ValueError: If the format is not supported
        ValueError: If the HTTP response is not 200
    """
    # Format validation
    valid_formats = ["csv", "json", "avro"]
    if file_format.lower() not in valid_formats:
        raise ValueError(
            f"Unsupported format: {file_format}. Valid formats: {valid_formats}"
        )

    # HTTP status check
    if response.status_code != 200:
        raise ValueError(f"HTTP error {response.status_code}: {response.text[:200]}")

    file_format_lower = file_format.lower()

    # Save according to format
    if file_format_lower == "csv":
        # Save CSV (text)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

    elif file_format_lower == "json":
        # Save JSON Lines (raw, one line per json)
        # No parsing needed, keep raw text
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(response.text)

    elif file_format_lower == "avro":
        # Save Avro (binary)
        with open(output_path, "wb") as f:
            f.write(response.content)

        # If a DataFrame is provided, save additional formats
        if df is not None:
            base_path = output_path.rsplit(".", 1)[0]
            # Also save as CSV from the DataFrame
            csv_path = f"{base_path}_from_dataframe.csv"
            df.to_csv(csv_path, index=False, encoding="utf-8")


def _build_factiva_where_clause(
    source_codes: List[str],
    language_code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    minimal_word_count: int = 0,
    regex_pattern: Optional[str] = None,
    stream_clause: Optional[bool] = False,
) -> str:
    """
    Build a Factiva-compatible WHERE clause, appending filters only when provided.
    """
    if not source_codes:
        raise ValueError("At least one source code must be provided")

    clauses: List[str] = []

    if language_code:
        clauses.append(f"language_code = '{language_code}'")

    source_codes_str = "', '".join(source_codes)
    clauses.append(f"source_code IN ('{source_codes_str}')")

    if start_date:
        clauses.append(f"publication_datetime >= '{start_date} 00:00:00'")

    if end_date:
        clauses.append(f"publication_datetime <= '{end_date} 23:59:59'")

    if minimal_word_count and minimal_word_count > 0:
        clauses.append(f"word_count >= {minimal_word_count}")

    if regex_pattern:
        if stream_clause:
            clauses.append(
                "REGEXP_LIKE(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), "
                f"'{regex_pattern.replace('(?i)', '')}', 'i')"
            )
        else:
            clauses.append(
                "REGEXP_CONTAINS(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), "
                f"r'{regex_pattern}')"
            )

    if not clauses:
        raise ValueError("Failed to build WHERE clause; no filters supplied")

    return " AND ".join(clauses)


def get_streams(
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 60,
) -> Dict:
    """
    Retrieve the list of existing Streams for the authenticated account.

    Args:
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        extended: When True, append extended=true to fetch richer stream metadata
        timeout: HTTP requests timeout in seconds (default: 60)

    Returns:
        Dict with success flag, data payload, error message/details when applicable.
    """
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }


    print("Fetching Factiva Streams...")


    try:
        response = requests.get(
            f"{base_url}/streams/",
            headers=headers,
            timeout=timeout,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Error fetching streams: {response.status_code}",
                "error_details": response.text,
                "streams": None,
            }

        data = response.json()
        print(f"Retrieved {len(data.get('data', []))} stream(s)")
        return {
            "success": True,
            "streams": data,
            "error": None,
        }

    except requests.RequestException as exc:
        return {
            "success": False,
            "error": f"Request error while fetching streams: {exc}",
            "error_details": None,
            "streams": None,
        }


def get_stream_extended(
    stream_id: str,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 60,
) -> Dict:
    """
    Retrieve the extended status of a specific Stream by its stream-id.

    Args:
        stream_id: The unique identifier of the stream
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 60)

    Returns:
        Dict with success flag, extended stream data, error message/details when applicable.
        Example successful response:
        {
            "success": True,
            "stream_data": {...},  # Extended stream information
            "error": None,
        }

    Example:
        >>> result = get_stream_extended("my-stream-id-123")
        >>> if result["success"]:
        >>>     print(result["stream_data"])
    """
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    headers = {
        "Content-Type": "application/json",
        "user-key": user_key,
        "X-API-VERSION": "3.0",
    }

    print(f"Fetching extended status for stream: {stream_id}")

    try:
        response = requests.get(
            f"{base_url}/streams/{stream_id}?extended=true",
            headers=headers,
            timeout=timeout,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Error fetching stream {stream_id}: {response.status_code}",
                "error_details": response.text,
                "stream_data": None,
            }

        data = response.json()
        print(f"Successfully retrieved extended status for stream {stream_id}")
        return {
            "success": True,
            "stream_data": data,
            "error": None,
        }

    except requests.RequestException as exc:
        return {
            "success": False,
            "error": f"Request error while fetching stream {stream_id}: {exc}",
            "error_details": None,
            "stream_data": None,
        }


def delete_stream(
    stream_id: str,
    subscription_id: Optional[str] = None,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 60,
) -> Dict:
    """
    Delete a Stream or a subscription from a Stream.

    If subscription_id is provided, deletes the subscription.
    If subscription_id is None, deletes the entire Stream.

    Args:
        stream_id: The unique identifier of the stream
        subscription_id: The unique identifier of the subscription to delete.
                        If None, deletes the entire stream (default: None)
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 60)

    Returns:
        Dict with success flag, error message/details when applicable.
        Example successful response:
        {
            "success": True,
            "message": "Stream deleted successfully" or "Subscription deleted successfully",
            "error": None,
        }
    """
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    headers = {
        "Content-Type": "application/json",
        "user-key": user_key,
        "X-API-VERSION": "3.0",
    }

    # Determine which endpoint to use
    if subscription_id is None:
        # Delete the entire stream
        url = f"{base_url}/streams/{stream_id}/"
        print(f"Deleting stream {stream_id}")
        success_message = "Stream deleted successfully"
        error_message_prefix = f"Error deleting stream {stream_id}"
    else:
        # Delete a subscription
        url = f"{base_url}/streams/{stream_id}/subscriptions/{subscription_id}/"
        print(f"Deleting subscription {subscription_id} from stream {stream_id}")
        success_message = "Subscription deleted successfully"
        error_message_prefix = f"Error deleting subscription {subscription_id}"

    try:
        response = requests.delete(
            url,
            headers=headers,
            timeout=timeout,
        )

        if response.status_code == 204:
            # 204 No Content is the typical success response for DELETE
            print(success_message)
            return {
                "success": True,
                "message": success_message,
                "error": None,
            }
        elif response.status_code == 200:
            # Some APIs return 200 OK for successful deletion
            print(success_message)
            return {
                "success": True,
                "message": success_message,
                "error": None,
            }
        else:
            return {
                "success": False,
                "error": f"{error_message_prefix}: {response.status_code}",
                "error_details": response.text,
            }

    except requests.RequestException as exc:
        return {
            "success": False,
            "error": f"Request error while deleting: {exc}",
            "error_details": None,
        }


def get_account_statistics(
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 60,
) -> Dict:
    """
    Retrieve account statistics from the Factiva SNS Accounts API.

    Args:
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 60)

    Returns:
        Dict with success flag, account statistics data, error message/details when applicable.
        Example successful response:
        {
            "success": True,
            "account_statistics": {...},  # Account statistics information
            "error": None,
        }

    Example:
        >>> result = get_account_statistics()
        >>> if result["success"]:
        >>>     print(result["account_statistics"])
    """
    if user_key is None:
        user_key = os.getenv("FACTIVA_USERKEY")
        if user_key is None:
            raise ValueError(
                "User key not provided. Provide 'user_key' or set the FACTIVA_USERKEY environment variable"
            )

    headers = {
        "Content-Type": "application/json",
        "user-key": user_key,
        "X-API-VERSION": "3.0",
    }

    print("Fetching account statistics...")

    try:
        response = requests.get(
            f"{base_url}/sns-accounts/",
            headers=headers,
            timeout=timeout,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Error fetching account statistics: {response.status_code}",
                "error_details": response.text,
                "account_statistics": None,
            }

        data = response.json()
        print("Successfully retrieved account statistics")
        return {
            "success": True,
            "account_statistics": data,
            "error": None,
        }

    except requests.RequestException as exc:
        return {
            "success": False,
            "error": f"Request error while fetching account statistics: {exc}",
            "error_details": None,
            "account_statistics": None,
        }


def submit_snapshot_explain(
    source_codes: List[str],
    start_date: str,
    end_date: str,
    minimal_word_count: int,
    language_code: str,
    stream_clause: Optional[bool] = False,
    regex_pattern: Optional[str] = None,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 300,
) -> Dict:
    """
    Step 1: Submit a Snapshot Explain job to the Factiva API.

    Args:
        source_codes: List of source codes (e.g., ['LEMOND', 'LEFIG'])
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        language_code: Language code (e.g., 'fr', 'en')
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 300)

    Returns:
        Dict: Dictionary containing:
            - success: Boolean indicating if the submission was successful
            - explain_id: Explain job ID (if success)
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

    where_clause = _build_factiva_where_clause(
        source_codes=source_codes,
        language_code=language_code,
        start_date=start_date,
        end_date=end_date,
        minimal_word_count=minimal_word_count,
        regex_pattern=regex_pattern,
        stream_clause=stream_clause,
    )

    explain_query = {"query": {"where": where_clause}}

    # API headers
    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }

    print("Submitting Snapshot Explain job...")
    print(f"Source codes: {source_codes}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Regex pattern (original): {regex_pattern if regex_pattern else 'None'}")
    print(f"Query: {explain_query}")

    try:
        response = requests.post(
            f"{base_url}/extractions/documents/_explain",
            headers=headers,
            json=explain_query,
            timeout=timeout,
        )

        if response.status_code not in [200, 201]:
            return {
                "success": False,
                "error": f"Error creating job: {response.status_code}",
                "error_details": response.text,
                "explain_id": None,
                "full_response": response.json() if response.text else None,
            }

        result = response.json()
        explain_id = result.get("data", {}).get("id") or result.get("id")
        print(f"Job successfully created! Explain ID: {explain_id}")

        return {
            "success": True,
            "explain_id": explain_id,
            "error": None,
            "error_details": None,
            "full_response": result,
        }

    except requests.RequestException as e:
        return {
            "success": False,
            "error": f"Request error while creating job: {str(e)}",
            "error_details": None,
            "explain_id": None,
            "full_response": None,
        }


def poll_snapshot_explain(
    explain_id: str,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    max_attempts: int = 30,
    wait_seconds: int = 10,
    timeout: int = 30,
) -> Dict:
    """
    Step 2: Polls a Snapshot Explain job until its completion.

    Args:
        explain_id: Explain job ID to monitor
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        max_attempts: Maximum number of polling attempts (default: 30)
        wait_seconds: Waiting time between attempts in seconds (default: 10)
        timeout: HTTP requests timeout in seconds (default: 30)

    Returns:
        Dict: Dictionary containing the results with the keys:
            - success: Boolean indicating if the operation succeeded
            - explain_id: Explain job ID
            - status: Final status of the job
            - document_count: Estimated document count (if success)
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

    print(f"Polling job {explain_id} (max {max_attempts} attempts)...")

    for attempt in range(1, max_attempts + 1):
        time.sleep(wait_seconds)

        try:
            status_response = requests.get(
                f"{base_url}/extractions/documents/{explain_id}/_explain",
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

                print(f"Attempt {attempt}/{max_attempts}: State = {current_state}")

                if current_state == "JOB_STATE_DONE":
                    nb_articles = (
                        status_data.get("data", {}).get("attributes", {}).get("counts")
                    )
                    print(f"✅ Job finished! Number of articles: {nb_articles}")

                    return {
                        "success": True,
                        "explain_id": explain_id,
                        "status": "JOB_STATE_DONE",
                        "document_count": nb_articles,
                        "full_response": status_data,
                        "error": None,
                    }
                elif current_state == "JOB_STATE_FAILED":
                    print("❌ The job failed!")
                    return {
                        "success": False,
                        "explain_id": explain_id,
                        "status": "JOB_STATE_FAILED",
                        "document_count": None,
                        "full_response": status_data,
                        "error": "Job failed",
                    }
                # Else, keep polling (job in progress)

            else:
                print(
                    f"Attempt {attempt}/{max_attempts}: HTTP error {status_response.status_code}"
                )

        except requests.RequestException as e:
            print(f"Attempt {attempt}/{max_attempts}: Request error - {str(e)}")

        # If this is the last attempt
        if attempt == max_attempts:
            return {
                "success": False,
                "error": f"The job was not completed after {max_attempts} attempts",
                "explain_id": explain_id,
                "status": "TIMEOUT",
                "document_count": None,
                "full_response": None,
            }

    # Should not happen, but for safety
    return {
        "success": False,
        "error": f"The job was not completed after {max_attempts} attempts",
        "explain_id": explain_id,
        "status": "TIMEOUT",
        "document_count": None,
        "full_response": None,
    }

def load_json_values(json_path: str) -> list:
    """
    Charge un fichier JSON et renvoie toutes les valeurs (sans les clés).

    Args:
        json_path: Chemin vers le fichier JSON
    Returns:
        List des valeurs trouvées dans le JSON
    Raises:
        FileNotFoundError: Si le fichier n'existe pas.
        json.JSONDecodeError: Si le fichier n'est pas un JSON valide.
    """
    import json
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return list(data.values())


def load_source_classification(
    field_name: str, source_classification: Dict = None
) -> List[str]:
    """
    Extract all values of a specific field from SOURCE_CLASSIFICATION.

    Args:
        field_name: Name of the field to extract (e.g., 'source_code', 'source_name', 
            'source_owner', 'media_all', 'source_region')
        source_classification: SOURCE_CLASSIFICATION dictionary containing categories
            (PQN, PQR, Magazine, Web, Agence Presse) and their entries.
            Defaults to SOURCE_CLASSIFICATION imported from the module.

    Returns:
        List of all values of the specified field across all categories.
        Empty values ('') are excluded from the list.

    Example:
        >>> source_codes = load_source_classification('source_code')
        >>> # Returns ['AUFRA', 'ECHOS', 'FIGARO', ...]
    """
    if source_classification is None:
        source_classification = SOURCE_CLASSIFICATION
    
    values = []
    for category, entries in source_classification.items():
        for entry in entries:
            value = entry.get(field_name, "")
            if value:  # Exclude empty values
                values.append(value)
    return values


def submit_snapshot_extraction(
    source_codes: List[str],
    start_date: str,
    end_date: str,
    minimal_word_count: int,
    language_code: str,
    regex_pattern: Optional[str] = None,
    file_format: str = "json",
    shards: Optional[int] = None,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 300,
) -> Dict:
    """
    Submit a Snapshot Extraction job to the Factiva API.

    Args:
        source_codes: List of source codes (e.g., ['LEMOND', 'LEFIG'])
        start_date: Start date in format 'YYYY-MM-DD'
        end_date: End date in format 'YYYY-MM-DD'
        minimal_word_count: Minimum word count for articles
        language_code: Language code (e.g., 'fr', 'en')
        regex_pattern: Optional regex pattern to filter content
        file_format: Output format ('json', 'avro', 'csv', 'parquet'). Default: 'json'
        shards: Optional number of files to split the results into
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 300)

    Returns:
        Dict: Dictionary containing:
            - success: Boolean indicating if the submission was successful
            - extraction_id: Extraction job ID (if success)
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

    where_clause = _build_factiva_where_clause(
        source_codes=source_codes,
        language_code=language_code,
        start_date=start_date,
        end_date=end_date,
        minimal_word_count=minimal_word_count,
        regex_pattern=regex_pattern,
        stream_clause=False,
    )

    # Build the extraction query
    extraction_query = {
        "query": {
            "where": where_clause,
            "format": file_format.lower()
        }
    }
    
    # Add shards parameter if specified
    if shards is not None:
        extraction_query["query"]["shards"] = shards

    # API headers
    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }

    print("Submitting Snapshot Extraction job...")
    print(f"Source codes: {source_codes}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Language: {language_code}")
    print(f"Format: {file_format}")
    print(f"Shards: {shards if shards else 'default'}")
    print(f"Regex pattern: {regex_pattern if regex_pattern else 'None'}")
    print(f"Query: {extraction_query}")

    try:
        response = requests.post(
            f"{base_url}/extractions/documents",
            headers=headers,
            json=extraction_query,
            timeout=timeout,
        )

        if response.status_code not in [200, 201]:
            return {
                "success": False,
                "error": f"Error creating extraction: {response.status_code}",
                "error_details": response.text,
                "extraction_id": None,
                "full_response": response.json() if response.text else None,
            }

        result = response.json()
        extraction_id = result.get("data", {}).get("id")
        print(f"Extraction job successfully created! Extraction ID: {extraction_id}")

        return {
            "success": True,
            "extraction_id": extraction_id,
            "error": None,
            "error_details": None,
            "full_response": result,
        }

    except requests.RequestException as e:
        return {
            "success": False,
            "error": f"Request error while creating extraction: {str(e)}",
            "error_details": None,
            "extraction_id": None,
            "full_response": None,
        }


def poll_snapshot_extraction(
    extraction_id: str,
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    max_attempts: int = 120,
    wait_seconds: int = 30,
    timeout: int = 30,
    extended: bool = True,
) -> Dict:
    """
    Poll a Snapshot Extraction job until its completion.

    Args:
        extraction_id: Extraction job ID to monitor
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        max_attempts: Maximum number of polling attempts (default: 120)
        wait_seconds: Waiting time between attempts in seconds (default: 30)
        timeout: HTTP requests timeout in seconds (default: 30)
        extended: If True, request extended status information (default: True)

    Returns:
        Dict: Dictionary containing the results with the keys:
            - success: Boolean indicating if the operation succeeded
            - extraction_id: Extraction job ID
            - status: Final status of the job
            - files: List of file URIs to download (if success)
            - article_count: Number of articles extracted (if extended=True and success)
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

    # Build URL with extended parameter if requested
    extended_param = "?extended=true" if extended else ""
    url = f"{base_url}/extractions/documents/{extraction_id}{extended_param}"

    print(f"Polling extraction job {extraction_id} (max {max_attempts} attempts, {wait_seconds}s interval)...")
    if extended:
        print("Extended status information will be retrieved")

    for attempt in range(1, max_attempts + 1):
        time.sleep(wait_seconds)

        try:
            status_response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
            )

            if status_response.status_code == 200:
                status_data = status_response.json()
                current_state = (
                    status_data.get("data", {})
                    .get("attributes", {})
                    .get("state") or 
                    status_data.get("data", {})
                    .get("attributes", {})
                    .get("current_state")
                )

                print(f"Attempt {attempt}/{max_attempts}: State = {current_state}")

                if current_state == "JOB_STATE_DONE":
                    files = (
                        status_data.get("data", {})
                        .get("attributes", {})
                        .get("files", [])
                    )
                    article_count = (
                        status_data.get("data", {})
                        .get("attributes", {})
                        .get("article_count")
                    )
                    
                    print(f"✅ Extraction completed successfully!")
                    print(f"Number of files: {len(files)}")
                    if article_count:
                        print(f"Number of articles: {article_count}")

                    return {
                        "success": True,
                        "extraction_id": extraction_id,
                        "status": "JOB_STATE_DONE",
                        "files": files,
                        "article_count": article_count,
                        "full_response": status_data,
                        "error": None,
                    }
                elif current_state == "JOB_STATE_FAILED":
                    print("❌ The extraction job failed!")
                    return {
                        "success": False,
                        "extraction_id": extraction_id,
                        "status": "JOB_STATE_FAILED",
                        "files": None,
                        "article_count": None,
                        "full_response": status_data,
                        "error": "Job failed",
                    }
                # Else, keep polling (job in progress)

            else:
                print(
                    f"Attempt {attempt}/{max_attempts}: HTTP error {status_response.status_code}"
                )

        except requests.RequestException as e:
            print(f"Attempt {attempt}/{max_attempts}: Request error - {str(e)}")

        # If this is the last attempt
        if attempt == max_attempts:
            return {
                "success": False,
                "error": f"The extraction job was not completed after {max_attempts} attempts",
                "extraction_id": extraction_id,
                "status": "TIMEOUT",
                "files": None,
                "article_count": None,
                "full_response": None,
            }

    # Should not happen, but for safety
    return {
        "success": False,
        "error": f"The extraction job was not completed after {max_attempts} attempts",
        "extraction_id": extraction_id,
        "status": "TIMEOUT",
        "files": None,
        "article_count": None,
        "full_response": None,
    }


def download_snapshot_file(
    file_uri: str,
    output_path: str,
    user_key: Optional[str] = None,
    timeout: int = 600,
    use_stream_delivery: bool = False,
) -> Dict:
    """
    Download a single Snapshot file from Factiva API.

    Args:
        file_uri: URI of the file to download
        output_path: Local path where to save the file
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        timeout: HTTP requests timeout in seconds (default: 600)
        use_stream_delivery: If True, use X-DELIVERY-METHOD: stream header to avoid Google domains

    Returns:
        Dict: Dictionary containing:
            - success: Boolean indicating if the download was successful
            - file_path: Path to the downloaded file (if success)
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
        "X-API-VERSION": "3.0",
    }
    
    # Add stream delivery header if requested
    if use_stream_delivery:
        headers["X-DELIVERY-METHOD"] = "stream"

    print(f"Downloading file from: {file_uri}")
    print(f"Saving to: {output_path}")

    try:
        response = requests.get(
            file_uri,
            headers=headers,
            timeout=timeout,
            stream=True,
        )

        if response.status_code != 200:
            return {
                "success": False,
                "error": f"Error downloading file: HTTP {response.status_code}",
                "error_details": response.text[:500],
                "file_path": None,
            }

        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write file
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"✅ File downloaded successfully! Size: {file_size_mb:.2f} MB")

        return {
            "success": True,
            "file_path": output_path,
            "file_size_mb": file_size_mb,
            "error": None,
        }

    except requests.RequestException as e:
        return {
            "success": False,
            "error": f"Request error while downloading file: {str(e)}",
            "file_path": None,
        }
    except IOError as e:
        return {
            "success": False,
            "error": f"IO error while saving file: {str(e)}",
            "file_path": None,
        }


def create_streaming_instance(
    source_codes: List[str],
    start_date: str,
    minimal_word_count: int,
    regex_pattern: str,
    language_code: str = "fr",
    user_key: Optional[str] = None,
    base_url: str = "https://api.dowjones.com",
    timeout: int = 300,
) -> Dict:
    """
    Create a Streaming instance using the Factiva Streams API.
    
    This function creates a streaming instance based on custom queries that will
    continuously monitor and filter content according to specified criteria.

    Args:
        source_codes: List of source codes (e.g., ['LEMOND', 'LEFIG'])
        start_date: Start date in format 'YYYY-MM-DD' for minimum publication date
        minimal_word_count: Minimum word count for articles
        regex_pattern: Regex pattern to filter content (mandatory)
        language_code: Language code (default: 'fr' for French)
        user_key: Factiva user key. If None, uses the FACTIVA_USERKEY environment variable
        base_url: Base URL for the API (default: https://api.dowjones.com)
        timeout: HTTP requests timeout in seconds (default: 300)

    Returns:
        Dict: Dictionary containing:
            - success: Boolean indicating if the creation was successful
            - stream_id: Streaming instance ID (if success)
            - subscription_id: Subscription ID (if success)
            - job_status: Job status (if success)
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

    where_clause = _build_factiva_where_clause(
        source_codes=source_codes,
        language_code=language_code,
        start_date=start_date,
        minimal_word_count=minimal_word_count,
        regex_pattern=regex_pattern,
        stream_clause=True,
    )

    # Build the streaming query according to the new API format
    streaming_query = {
        "data": {
            "attributes": {
                "where": where_clause
            }
        }
    }

    # API headers
    headers = {
        "user-key": user_key,
        "Content-Type": "application/json",
        "X-API-VERSION": "3.0",
    }

    print("Creating Streaming instance...")
    print(f"Source codes: {source_codes}")
    print(f"Start date: {start_date}")
    print(f"Language: {language_code}")
    print(f"Minimal word count: {minimal_word_count}")
    print(f"Regex pattern: {regex_pattern.replace('(?i)', '')}")
    print(f"Query: {streaming_query}")


        
    response = requests.post(
        f"{base_url}/streams/",
        json=streaming_query,
        headers=headers,
        timeout=timeout,
    )
    
    
    print(f"Response status code: {response.status_code}")
    
    if response.status_code == 201:
        # Successful creation
        response_data = response.json()
        print("Streaming instance created successfully!")
        
        # Extract stream ID and subscription ID from response
        stream_id = response_data.get("data", {}).get("id")
        job_status = response_data.get("data", {}).get("attributes", {}).get("job_status")
        
        # Extract subscription ID
        subscription_id = None
        subscriptions = response_data.get("data", {}).get("relationships", {}).get("subscriptions", {}).get("data", [])
        if subscriptions and len(subscriptions) > 0:
            subscription_id = subscriptions[0].get("id")
        
        print(f"Stream ID: {stream_id}")
        print(f"Job Status: {job_status}")
        print(f"Subscription ID: {subscription_id}")
        
        return {
            "success": True,
            "stream_id": stream_id,
            "subscription_id": subscription_id,
            "job_status": job_status,
            "full_response": response_data,
        }
    else:
        # Error occurred
        try:
            error_data = response.json()
            error_message = error_data.get("errors", [{}])[0].get("detail", "Unknown error")
            error_code = error_data.get("errors", [{}])[0].get("code", "Unknown code")
        except json.JSONDecodeError:
            error_message = response.text
            error_code = "JSON_DECODE_ERROR"
        
        print(f"Error creating streaming instance: {error_message}")
        
        return {
            "success": False,
            "error": error_message,
            "error_code": error_code,
            "status_code": response.status_code,
            "full_response": response.text,
        }


def upload_snapshot_files_to_s3(
    downloaded_files_dir: str,
    first_article: int = 1,
    last_article: int = 1,
    delete_local_after_upload: bool = False,
) -> Dict:
    """
    Extract snapshot .json.gz files, transform them to the expected format, and upload to S3.
    
    IMPORTANT: Each source .json.gz file is split into 20 separate JSON files for S3 upload.
    This prevents uploading files that are too large.
    
    This function:
    1. Reads .json.gz files from the downloaded directory
    2. Extracts and decompresses them
    3. Transforms JSONL format (one article per line) to {"data": [articles]} format and process timestamps (EPOCH to ISO format)
    4. Splits each source file into 20 batches (to avoid large files)
    5. Uploads to S3 at: country_france/articles/year_YYYY/month_MM/
    6. Filenames: YYYY_MM_DD_HH_MM_SS_{file_number}_snapshot_extract.json
    
    Example: If you process 2 .json.gz files:
    - File 1 → 20 S3 files (file_index 1-20)
    - File 2 → 20 S3 files (file_index 21-40)
    - Total: 40 files uploaded to S3
    
    Args:
        downloaded_files_dir: Directory containing the downloaded .json.gz files
        first_article: Number of the first source file to process (1-indexed, default: 1)
        last_article: Number of the last source file to process (1-indexed, default: 1)
        delete_local_after_upload: Whether to delete local .json.gz files after successful upload
    
    Returns:
        Dict containing:
            - success: Boolean indicating if the operation was successful
            - uploaded_files: List of S3 keys for uploaded files (10 per source file)
            - total_articles: Total number of articles processed
            - error: Error message (if failure)
    
    Example:
        # Process file 1 → uploads 20 files to S3 (file_index 1-10)
        result = upload_snapshot_files_to_s3("data/factiva_snapshots/abc123/raw/", first_article=1, last_article=1)
        
        # Process files 2 and 3 → uploads 40 files to S3 (file_index 11-30)
        result = upload_snapshot_files_to_s3("data/factiva_snapshots/abc123/raw/", first_article=2, last_article=3)
    """
    print(f"\n{'='*70}")
    print(f"STARTING S3 UPLOAD PROCESS")
    print(f"{'='*70}")
    print(f"Processing files {first_article} to {last_article}")
    print(f"Source directory: {downloaded_files_dir}")
    
    # Load S3 configuration from environment variables
    try:
        bucket_name = os.getenv("FACTIVA_S3_BUCKET")
        if not bucket_name:
            return {
                "success": False,
                "error": "FACTIVA_S3_BUCKET environment variable not set",
                "uploaded_files": [],
                "total_articles": 0,
            }
        
        access_key = os.getenv("BUCKET")
        secret_key = os.getenv("BUCKET_SECRET")
        
        if not access_key or not secret_key:
            return {
                "success": False,
                "error": "BUCKET or BUCKET_SECRET environment variables not set",
                "uploaded_files": [],
                "total_articles": 0,
            }
        
        endpoint_url = os.getenv("FACTIVA_S3_ENDPOINT", "https://s3.fr-par.scw.cloud")
        region = os.getenv("FACTIVA_S3_REGION", "fr-par")
        
        print("\n📦 S3 Configuration:")
        print(f"   Bucket: {bucket_name}")
        print(f"   Region: {region}")
        print(f"   Endpoint: {endpoint_url}")
        
    except Exception as e:
        return {
            "success": False,
            "error": f"Error loading S3 configuration: {str(e)}",
            "uploaded_files": [],
            "total_articles": 0,
        }
    
    # Initialize S3 client
    try:
        session_config = BotoConfig(signature_version="s3v4")
        s3_client = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url,
            config=session_config,
        )
        print("✅ S3 client initialized successfully")
    except Exception as e:
        return {
            "success": False,
            "error": f"Error initializing S3 client: {str(e)}",
            "uploaded_files": [],
            "total_articles": 0,
        }
    
    # Find all .json.gz files in the directory
    source_dir = Path(downloaded_files_dir)
    if not source_dir.exists():
        return {
            "success": False,
            "error": f"Directory does not exist: {downloaded_files_dir}",
            "uploaded_files": [],
            "total_articles": 0,
        }
    
    # Look for both .json.gz and .json files
    all_gz_files = sorted(list(source_dir.glob("*.json.gz")) + list(source_dir.glob("*.json")))
    
    if not all_gz_files:
        return {
            "success": False,
            "error": f"No .json.gz or .json files found in {downloaded_files_dir}",
            "uploaded_files": [],
            "total_articles": 0,
        }
    
    print(f"\n📂 Found {len(all_gz_files)} file(s) in directory")
    
    # Validate file indices
    if first_article < 1 or last_article < first_article or last_article > len(all_gz_files):
        return {
            "success": False,
            "error": f"Invalid file range: first_article={first_article}, last_article={last_article}, available files={len(all_gz_files)}",
            "uploaded_files": [],
            "total_articles": 0,
        }
    
    # Select files to process (convert to 0-indexed)
    files_to_process = all_gz_files[first_article - 1:last_article]
    
    print("\n📋 Files to process:")
    for idx, file_path in enumerate(files_to_process, first_article):
        print(f"   {idx}. {file_path.name}")
    
    uploaded_files = []
    total_articles = 0
    global_file_index = 0  # Global counter for all uploaded files
    
    # Process each file
    for source_file_num, file_path in enumerate(files_to_process, first_article):
        print(f"\n{'='*70}")
        print(f"PROCESSING SOURCE FILE {source_file_num}: {file_path.name}")
        print(f"{'='*70}")
        
        try:
            # Step 1: Extract and read the file (JSONL format)
            print("  [1/5] Reading and extracting file...")
            articles = []
            
            # Check if file is gzipped or plain JSON
            is_gzipped = file_path.suffix == '.gz'
            
            if is_gzipped:
                file_handle = gzip.open(file_path, 'rt', encoding='utf-8')
            else:
                file_handle = open(file_path, 'r', encoding='utf-8')
            
            try:
                with file_handle:
                    for line_num, line in enumerate(file_handle, 1):
                        line = line.strip()
                        if not line:
                            continue
                        
                        try:
                            article_data = json.loads(line)
                            articles.append(article_data)
                        except json.JSONDecodeError as e:
                            print(f"  ⚠️  Warning: Could not parse line {line_num}: {str(e)[:100]}")
                            continue
            finally:
                pass  # Context manager handles closing
            
            if not articles:
                print(f"  ⚠️  Warning: No valid articles found in {file_path.name}, skipping")
                continue
            
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            print(f"  ✅ Extracted {len(articles):,} articles from {file_size_mb:.2f} MB file")
            
            # Step 2: Transform to expected format
            print("  [2/5] Transforming articles to expected format...")
            formatted_articles = []
            
            # List of timestamp fields to convert from EPOCH to ISO format
            timestamp_fields = [
                'ingestion_datetime',
                'modification_datetime',
                'modification_date',
                'publication_date',
                'publication_datetime',
                'availability_datetime'
            ]
            
            for article in articles:
                # Get the article ID from "an" field
                article_id = article.get("an", "unknown")
                
                # Convert EPOCH timestamps to ISO format (with timezone UTC)
                for field in timestamp_fields:
                    if field in article and article[field]:
                        try:
                            # Convert EPOCH milliseconds to seconds, then to datetime UTC
                            epoch_ms = int(article[field])
                            epoch_s = epoch_ms / 1000.0
                            # Use utcfromtimestamp for compatibility (returns UTC datetime)
                            dt = datetime.utcfromtimestamp(epoch_s)
                            # Format as ISO 8601 with milliseconds and UTC timezone (Z suffix)
                            article[field] = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                        except (ValueError, TypeError):
                            # Keep original value if conversion fails
                            pass
                
                # Create formatted article
                formatted_article = {
                    "id": article_id,
                    "type": "article",
                    "attributes": article
                }
                formatted_articles.append(formatted_article)
            
            print(f"  ✅ Transformed {len(formatted_articles):,} articles to standard format")
            
            # Step 3: Split into 20 batches
            num_batches = 20
            total_articles_in_file = len(formatted_articles)
            batch_size = total_articles_in_file // num_batches
            remainder = total_articles_in_file % num_batches
            
            print(f"  [3/5] Splitting into {num_batches} batches...")
            print(f"      Total articles: {total_articles_in_file:,}")
            print(f"      Articles per batch: ~{batch_size:,}")
            
            batches = []
            start_idx = 0
            for i in range(num_batches):
                # Add one extra article to the first 'remainder' batches
                current_batch_size = batch_size + (1 if i < remainder else 0)
                end_idx = start_idx + current_batch_size
                
                batch = formatted_articles[start_idx:end_idx]
                batches.append(batch)
                start_idx = end_idx
            
            print(f"  ✅ Created {len(batches)} batches")
            
            # Step 4: Upload each batch to S3
            print(f"  [4/5] Uploading {num_batches} batches to S3...")
            
            for batch_num, batch_articles in enumerate(batches, 1):
                global_file_index += 1
                
                # Create final JSON structure for this batch
                final_json = {
                    "data": batch_articles
                }
                
                # Generate filename with current timestamp and global index
                now = datetime.now()
                filename = f"{now.strftime('%Y_%m_%d_%H_%M_%S')}_{global_file_index}_snapshot_extract.json"
                
                # Build S3 key (path in S3)
                year = now.year
                month = now.month
                s3_key = f"country_france/articles/year_{year}/month_{month:02d}/{filename}"
                
                # Create temporary file
                with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.json', delete=False) as temp_file:
                    temp_file_path = temp_file.name
                    json.dump(final_json, temp_file, ensure_ascii=False, indent=2)
                
                temp_file_size_mb = os.path.getsize(temp_file_path) / (1024 * 1024)
                
                # Upload to S3
                try:
                    s3_client.upload_file(temp_file_path, bucket_name, s3_key)
                    uploaded_files.append(s3_key)
                    total_articles += len(batch_articles)
                    
                    print(f"      ✅ Batch {batch_num}/20 uploaded (file_index={global_file_index}, {len(batch_articles):,} articles, {temp_file_size_mb:.2f} MB)")
                except Exception as e:
                    print(f"      ❌ Failed to upload batch {batch_num}: {str(e)}")
                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_file_path)
                    except Exception as e:
                        pass
            
            print(f"  ✅ All batches uploaded for {file_path.name}")
            
            # Step 5: Optionally delete local file after successful upload
            if delete_local_after_upload:
                try:
                    file_path.unlink()
                    print(f"  ✅ Deleted local file: {file_path.name}")
                except Exception as e:
                    print(f"  ⚠️  Warning: Could not delete local file: {str(e)}")
            
        except Exception as e:
            print(f"  ❌ Error processing file {file_path.name}: {str(e)}")
            import traceback
            traceback.print_exc()
            continue
    
    # Summary
    print(f"\n{'='*70}")
    print(f"UPLOAD PROCESS COMPLETED")
    print(f"{'='*70}")
    print(f"✅ Successfully uploaded: {len(uploaded_files)} file(s)")
    print(f"📊 Total articles processed: {total_articles:,}")
    
    if uploaded_files:
        print("\n📦 Uploaded files:")
        for s3_key in uploaded_files:
            print(f"   • s3://{bucket_name}/{s3_key}")
    
    return {
        "success": True,
        "uploaded_files": uploaded_files,
        "total_articles": total_articles,
        "error": None,
    }
        

