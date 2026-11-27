from mediatree_utils import get_url_mediatree
from secret_utils import get_secret_docker
from whisper_utils import WHISPER_COLUMN_NAME
import modin.pandas as pd
import base64
import json
import logging
import time
import requests
import os
from uuid import UUID

LABEL_STUDIO_URL = os.getenv("LABEL_STUDIO_URL", "")
HEALTH_ENDPOINT = f"{LABEL_STUDIO_URL}/health"

# Storage ID (not project id @see https://api.labelstud.io/api-reference/api-reference/import-storage/s-3/sync)
LABEL_STUDIO_PROJECT_ID = os.getenv("LABEL_STUDIO_PROJECT_ID", "")
SYNC_ENDPOINT = f"{LABEL_STUDIO_URL}/api/storages/s3/{LABEL_STUDIO_PROJECT_ID}/sync"
# Either the key itself or the path of a docker secret file holding it
API_LABEL_STUDIO_KEY = get_secret_docker("API_LABEL_STUDIO_KEY", "")


def get_sync_endopoint(label_studio_project_id: int):
    return f"{LABEL_STUDIO_URL}/api/storages/s3/{label_studio_project_id}/sync"


# @see https://github.com/HumanSignal/label-studio/issues/1492#issuecomment-924522609
def encode_audio_base64(audio_bytes):
    return f"data:audio/mp3;base64,{base64.b64encode(audio_bytes).decode('utf-8')}"


# https://labelstud.io/guide/storage#Amazon-S3
def get_label_studio_format(row) -> str:
    url_mediatree = url_mediatree = get_url_mediatree(row["start"], row["channel_name"])
    start_time = (
        row["start"].isoformat()
        if isinstance(row["start"], pd.Timestamp)
        else row["start"]
    )

    # TODO make it labelstudio compatible
    # media = encode_audio_base64(row['media'])

    # safe id
    row["id"] = row.get("id", "")
    if pd.isna(row["id"]):
        row["id"] = ""

    task_data = {
        "data": {
            "item": {
                "id": row["id"],  # from keywords.id
                "plaintext": row["plaintext"],
                WHISPER_COLUMN_NAME: row[WHISPER_COLUMN_NAME],
                # "media": media,  # TODO make it labelstudio compatible
                "start": start_time,
                "channel_title": row["channel_title"],
                "channel_name": row["channel_name"],
                "channel_program": row["channel_program"],
                "channel_program_type": row["channel_program_type"],
                "model_name": row["model_name"],
                "prompt_version": row.get("prompt_version", ""),
                "pipeline_version": row.get("pipeline_version", ""),
                "model_result": row["model_result"],
                "model_reason": row["model_reason"],
                "year": int(row["year"]),
                "month": int(row["month"]),
                "day": int(row["day"]),
                "channel": row["channel"],
                "country": row["country"],
                "url_mediatree": url_mediatree,
            }
        },
        "annotations": [],
        "predictions": [],
    }

    return task_data


def wait_for_health(url, timeout=240, interval=5):
    """Waits until the given URL returns a 200 status or timeout is reached."""
    logging.info("Waiting for the service to become healthy...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            logging.info(f"Sending get to {url}")
            headers = {"Content-Type": "application/json"}
            response = requests.get(url, timeout=5, headers=headers)
            logging.info(f"Response {response.content}")
            if response.status_code == 200:
                logging.info("Label Studio Service is healthy!")
                return True
        except requests.RequestException as e:
            logging.info(f"Health check failed: {e}")

        logging.info(f"Label Studio Service not ready yet {url}. Retrying...")
        time.sleep(interval)

    logging.warning("Timed out waiting for the service to become healthy.")
    return False


# https://api.labelstud.io/api-reference/api-reference/import-storage/s-3/sync
def sync_s3_storage(api_key, label_studio_project_id: int):
    """Triggers the S3 sync in Label Studio."""
    headers = {"Authorization": f"Token {api_key}"}
    sync_endpoint = get_sync_endopoint(label_studio_project_id)
    try:
        timeout = 60 * 5
        logging.info(
            f"POST to {sync_endpoint} and waiting a maximum of {timeout} seconds for the sync..."
        )
        response = requests.post(sync_endpoint, headers=headers, timeout=timeout)
        if response.status_code == 200:
            logging.info("Label Studio - S3 sync successful!")
            return True
        else:
            logging.warning(
                f"Failed to sync S3 storage: {response.status_code} - {response.text}"
            )
            return False
    except requests.RequestException as e:
        logging.warning(f"Error syncing S3 storage: {e}")


def wait_and_sync_label_studio(label_studio_project_id: int):
    if label_studio_project_id != "" and label_studio_project_id is not None:
        logging.info("Syncronize S3 data to Label Studio API...")
        if wait_for_health(HEALTH_ENDPOINT):
            return sync_s3_storage(API_LABEL_STUDIO_KEY, label_studio_project_id)
    else:
        logging.warning("To activate label studio import, set LABEL_STUDIO_PROJECT_ID")


def convert_nested_timestamps(d):
    if isinstance(d, str):
        return d
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = convert_nested_timestamps(v)
        elif isinstance(v, list):
            d[k] = [convert_nested_timestamps(item) for item in v]
        elif isinstance(v, pd.Timestamp):
            d[k] = v.isoformat()
        elif pd.isnull(v):
            d[k] = None
        elif isinstance(v, UUID):
            d[k] = str(v)
    return d


def edit_labelstudio_record_data(
    row: pd.Series, update_dict: dict, annotations: list
) -> dict:
    record = row.to_dict()
    if not isinstance(record["data"], dict):
        record = json.loads(row.data)
    if "date" in record["data"]["item"]:
        del record["data"]["item"]["date"]
    record["data"]["item"].update(update_dict)
    record["annotations"] = annotations
    record["predictions"] = []
    record = convert_nested_timestamps(record)
    return record
