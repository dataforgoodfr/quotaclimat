import logging
import os
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
# from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
# from quotaclimat.data_processing.mediatree.api_import import *
import numpy as np
import shutil
from typing import List, Optional
from tenacity import *
from sentry_sdk.crons import monitor
import modin.pandas as pd
import boto3

def get_secret_docker(secret_name):
    secret_value = os.environ.get(secret_name, "")

    if secret_value and os.path.exists(secret_value):
        with open(secret_value, "r") as file:
            return file.read().strip()
    return secret_value

# Configuration for Scaleway Object Storage
ACCESS_KEY = get_secret_docker('BUCKET')
SECRET_KEY = get_secret_docker("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
MEDIATREE_PASSWORD = os.environ.get("MEDIATREE_PASSWORD")
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

def get_bucket_key(date, channel, filename:str="*", suffix:str="parquet", country: CountryMediaTree = FRANCE):
    (year, month, day) = (date.year, date.month, date.day)
    if country.code == FRANCE_CODE:
        logging.debug(f"france does not have country partition - {country.code} - {FRANCE_CODE}")
        return f'year={year}/month={month:1}/day={day:1}/channel={channel}/{filename}.{suffix}'
    else:
        logging.debug(f"country_code is not france : {country.code} / {country.name}")
        return f'country={country.name}/year={year}/month={month:1}/day={day:1}/channel={channel}/{filename}.{suffix}'

def get_bucket_key_folder(date, channel, country: CountryMediaTree = FRANCE):
    (year, month, day) = (date.year, date.month, date.day)
    if country.code == FRANCE_CODE:
        return f'year={year}/month={month:1}/day={day:1}/channel={channel}/'
    else: # no country for old france
        return f'country={country.name}/year={year}/month={month:1}/day={day:1}/channel={channel}/'

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
            # shutil.rmtree(local_folder)
            # logging.info(f"Deleted local folder: {local_folder}")

def read_file_from_s3(path, bucket_name = BUCKET_NAME):
    s3_key: tuple[str] = f"s3://{bucket_name}/{path}"
    logging.info(f"Reading S3 file {s3_key}...")
    return pd.read_parquet(path=s3_key,
                                    storage_options={
                                        "key": ACCESS_KEY,
                                        "secret": SECRET_KEY,
                                        "endpoint_url": ENDPOINT_URL,
                                        })

def read_folder_from_s3(date, channel: str, country: CountryMediaTree = FRANCE, bucket_name = BUCKET_NAME):
    s3_path: str = get_bucket_key_folder(date=date, channel=channel, country=country)
    s3_key: tuple[str] = f"s3://{bucket_name}/{s3_path}"
    logging.info(f"Reading S3 folder {s3_key}")

    try:
        df = pd.read_parquet(path=s3_key,
                                    storage_options={
                                        "key": ACCESS_KEY,
                                        "secret": SECRET_KEY,
                                        "endpoint_url": ENDPOINT_URL,
                                    })
        
        if str(df['start'].dt.tz) == 'UTC':
            logging.warning(f"Timezone is UTC, converting to {country.timezone}")
            df['start'] = df['start'].dt.tz_convert(country.timezone)

        return df
    except Exception as e:
        logging.warning(f"Error reading S3 folder {s3_key}: {e}")
        return None


def check_if_object_exists_in_s3(day, channel, s3_client, country: CountryMediaTree = FRANCE) -> bool:
    folder_prefix = get_bucket_key_folder(day, channel, country=country)  # Adjust this to return the folder path
    
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
    
# Data extraction function definition
# https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def transform_raw_keywords(
        df: pd.DataFrame
        ,stop_words: list[str] = []
        ,df_programs = None,
        country=FRANCE
    ) -> Optional[pd.DataFrame]: 
    try:
        if(df is not None):
            df: pd.DataFrame = filter_and_tag_by_theme(df=df, stop_words=stop_words, country=country)    
            df['keywords_with_timestamp'] = df['keywords_with_timestamp'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
            df['srt'] = df['srt'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
            
            # channels perimeter might have changed, so we need to update programs names
            df = update_programs_and_filter_out_of_scope_programs_from_df(df, df_programs=df_programs)

            logging.info(f"Adding primary key to save to PG and have idempotent results")
            df["id"] = df.apply(lambda x: add_primary_key(x), axis=1)
            df = df.drop_duplicates(subset=['id'], keep='last')
            return df
        else:
            None
    except Exception as err:
        logging.error("Could not query API :(%s) %s" % (type(err).__name__, err))
        return None