"""
Utility functions for extracting data from the Factiva API
"""

import io
import json
import os
import time
from typing import Dict, List, Optional, Union

import fastavro
import pandas as pd
import requests


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


def submit_snapshot_explain(
    source_codes: List[str],
    start_date: str,
    end_date: str,
    minimal_word_count: int,
    language_code: str,
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

    # Build the request
    source_codes_str = "', '".join(source_codes)

    # Build WHERE clause
    where_clause = (
        f"source_code IN ('{source_codes_str}') "
        f"AND publication_datetime >= '{start_date} 00:00:00' "
        f"AND publication_datetime <= '{end_date} 23:59:59' "
        f"AND language_code = '{language_code}' "
        f"AND word_count >= {minimal_word_count}"
    )

    # Add regex pattern if provided
    if regex_pattern:
        # Utiliser un raw string literal BigQuery r'...'; le pattern BigQuery ne contient plus d'apostrophes
        where_clause += (
            " AND REGEXP_CONTAINS(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), "
            f"r'{regex_pattern}')"
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

    # Build the WHERE clause for streaming
    source_codes_str = "', '".join(source_codes)
    where_clause = (
        f"language_code = '{language_code}' "
        f"AND source_code IN ('{source_codes_str}') "
        f"AND publication_datetime >= '{start_date} 00:00:00' "
        f"AND word_count >= {minimal_word_count} "
        f"AND REGEXP_CONTAINS(CONCAT(title, ' ', IFNULL(snippet, ''), ' ', IFNULL(body, '')), r'{regex_pattern}')"
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
    print(f"Regex pattern: {regex_pattern}")
    print(f"Query: {streaming_query}")

    try:
        response = requests.post(
            f"{base_url}/streams/",
            headers=headers,
            json=streaming_query,
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
            
    except requests.exceptions.Timeout:
        error_msg = f"Request timeout after {timeout} seconds"
        print(f"Error: {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "error_code": "TIMEOUT",
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        print(f"Error: {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "error_code": "REQUEST_EXCEPTION",
        }
