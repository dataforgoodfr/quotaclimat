import logging
import os
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
from quotaclimat.data_processing.mediatree.api_import import *

import shutil
from typing import List, Optional
from tenacity import *
from sentry_sdk.crons import monitor
import modin.pandas as pd
import boto3

# Configuration for Scaleway Object Storage
ACCESS_KEY = os.environ.get('BUCKET')
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION = 'fr-par'
ENDPOINT_URL = f'https://s3.{REGION}.scw.cloud'

def get_s3_client():
    s3_client = boto3.client(
        service_name='s3',
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=ENDPOINT_URL,
    )
    return s3_client

def get_bucket_key(date, channel, filename:str="*", suffix:str="parquet"):
    (year, month, day) = (date.year, date.month, date.day)
    return f'year={year}/month={month:1}/day={day:1}/channel={channel}/{filename}.{suffix}'

def get_bucket_key_folder(date, channel):
    (year, month, day) = (date.year, date.month, date.day)
    return f'year={year}/month={month:1}/day={day:1}/channel={channel}/'

# Function to upload folder to S3
def upload_folder_to_s3(local_folder, bucket_name, base_s3_path, s3_client):
    logging.info(f"Reading local folder {local_folder} and uploading to S3")
    for root, _, files in os.walk(local_folder):
        logging.info(f"Reading files {len(files)}")
        for file in files:
            logging.info(f"Reading {file}")
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_folder)
            s3_key = os.path.join(base_s3_path, relative_path).replace("\\", "/")  # Replace backslashes for S3 compatibility
            
            # Upload file
            s3_client.upload_file(local_file_path, bucket_name, s3_key)
            logging.info(f"Uploaded: {s3_key}")
            # Delete the local folder after successful upload
            shutil.rmtree(local_folder)
            logging.info(f"Deleted local folder: {local_folder}")

# Function to read parquet folders from S3
def read_folder_from_s3(bucket_name, base_s3_path, s3_client):
    logging.info(f"Reading S3 folder {bucket_name} - {base_s3_path})")
    
    s3_key = base_s3_path
    df = pd.read_parquet(path=s3_key)

    return df


def check_if_object_exists_in_s3(day, channel, s3_client) -> bool:
    folder_prefix = get_bucket_key_folder(day, channel)  # Adjust this to return the folder path
    
    logging.debug(f"Checking if folder exists: {folder_prefix}")
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_prefix, MaxKeys=1)
        if "Contents" in response:
            logging.info(f"Folder exists in S3: {folder_prefix}")
            return True
        else:
            logging.info(f"Folder does not exist in S3: {folder_prefix}")
            return False
    except Exception as e:
        logging.error(f"Error while checking folder in S3: {folder_prefix}\n{e}")
        return False