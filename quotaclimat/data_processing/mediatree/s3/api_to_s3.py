import logging
import asyncio
from time import sleep
import sys
import os
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
from quotaclimat.data_processing.mediatree.api_import import *

import shutil
from typing import List, Optional
from tenacity import *
import sentry_sdk
from sentry_sdk.crons import monitor
import modin.pandas as pd
from modin.pandas import json_normalize
import ray
import s3fs
import boto3

from quotaclimat.utils.sentry import sentry_init
logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
logging.getLogger('distributed').setLevel(logging.ERROR)
logging.getLogger('worker').setLevel(logging.ERROR)
sentry_init()

#read whole file to a string
password = get_password()
AUTH_URL = get_auth_url()
USER = get_user()
KEYWORDS_URL = get_keywords_url()
# Configuration for Scaleway Object Storage
ACCESS_KEY = os.environ.get("BUCKET")
SECRET_KEY = os.environ.get("BUCKET_SECRET")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
REGION = 'fr-par'

ENDPOINT_URL = f'https://s3.{REGION}.scw.cloud'

s3_client = boto3.client(
    service_name='s3',
    region_name=REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT_URL,
)

def get_bucket_key(date, channel, filename:str="*", suffix:str="parquet"):
    (year, month, day) = (date.year, date.month, date.day)
    return f'year={year}/month={month:1}/day={day:1}/channel={channel}/{filename}.{suffix}'

def get_bucket_key_folder(date, channel):
    (year, month, day) = (date.year, date.month, date.day)
    return f'year={year}/month={month:1}/day={day:1}/channel={channel}/'

# Function to upload folder to S3
def upload_folder_to_s3(local_folder, bucket_name, base_s3_path):
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

def save_to_s3(df: pd.DataFrame, channel: str, date: pd.Timestamp):
    logging.info(f"Saving DF with {len(df)} elements to S3 for {date} and channel {channel}")

    # to create partitions
    object_key = get_bucket_key(date, channel)
    logging.debug(f"Uploading partition: {object_key}")

    try:
        # add partition columns year, month, day to dataframe
        df['year'] = date.year
        df['month'] = date.month
        df['day'] = date.day
        df['channel'] = channel

        df = df._to_pandas() # collect data accross ray workers to avoid multiple subfolders
        based_path = "s3/parquet"
        df.to_parquet(based_path,
                       compression='gzip'
                       ,partition_cols=['year', 'month', 'day', 'channel'])

        #saving full_path folder parquet to s3
        s3_path = f"{get_bucket_key_folder(date, channel)}"
        local_folder = f"{based_path}/{s3_path}"
        upload_folder_to_s3(local_folder, BUCKET_NAME, s3_path)
        
    except Exception as e:
        logging.error(Exception)
        exit()

def check_if_object_exists_in_s3(day, channel):
    folder_prefix = get_bucket_key_folder(day, channel)  # Adjust this to return the folder path
    
    logging.debug(f"Checking if folder exists: {folder_prefix}")
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_prefix, MaxKeys=1)
        if "Contents" in response:
            logging.info(f"Folder exists in S3: {folder_prefix}")
            return True
        else:
            logging.debug(f"Folder does not exist in S3: {folder_prefix}")
            return False
    except Exception as e:
        logging.error(f"Error while checking folder in S3: {folder_prefix}\n{e}")
        return False

async def get_and_save_api_data(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_api_data"):
        try:
            logging.warning(f"Available CPUS {os.cpu_count()} - MODIN_CPUS config : {os.environ.get('MODIN_CPUS', 3)}")

            token=get_auth_token(password=password, user_name=USER)
            type_sub = 's2t'
            start_date = int(os.environ.get("START_DATE", 0))
            number_of_previous_days = int(os.environ.get("NUMBER_OF_PREVIOUS_DAYS", 7))
            (start_date_to_query, end_date) = get_start_end_date_env_variable_with_default(start_date, minus_days=number_of_previous_days)
            df_programs = get_programs()
            channels = get_channels()
            
            day_range = get_date_range(start_date_to_query, end_date, number_of_previous_days)
            logging.info(f"Number of days to query : {len(day_range)} - day_range : {day_range}")
            for day in day_range:
                token = refresh_token(token, day)
                
                for channel in channels:
                    df_res = pd.DataFrame()
                    
                    # if object already exists, skip
                    if not check_if_object_exists_in_s3(day, channel):
                        try:
                            programs_for_this_day = get_programs_for_this_day(day.tz_localize("Europe/Paris"), channel, df_programs)

                            for program in programs_for_this_day.itertuples(index=False):
                                start_epoch = program.start
                                end_epoch = program.end
                                channel_program = str(program.program_name)
                                channel_program_type = str(program.program_type)
                                logging.info(f"Querying API for {channel} - {channel_program} - {channel_program_type} - {start_epoch} - {end_epoch}")
                                df = get_df_api(token, type_sub, start_epoch, channel, end_epoch, channel_program, channel_program_type)

                                if(df is not None):
                                    df_res = pd.concat([df_res, df ], ignore_index=True)
                                else:
                                    logging.info("Nothing to extract")

                            # save to S3
                            save_to_s3(df_res, channel, day)
                            
                        except Exception as err:
                            logging.error(f"continuing loop but met error : {err}")
                            continue
                    else:
                        logging.info(f"Object already exists for {day} and {channel}, skipping")
            exit_event.set()
        except Exception as err:
            logging.fatal("get_and_save_api_data (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)

async def main():
    with monitor(monitor_slug='api-to-s3'): #https://docs.sentry.io/platforms/python/crons/
        try:
            logging.info("Start api to S3")
            
            event_finish = asyncio.Event()
            # Start the health check server in the background
            health_check_task = asyncio.create_task(run_health_check_server())

            context = ray.init(
                dashboard_host="0.0.0.0", # for docker dashboard
                # runtime_env=dict(worker_process_setup_hook=sentry_init),
            )
            logging.info(f"Ray context dahsboard available at : {context.dashboard_url}")
            logging.warning(f"Ray Information about the env: {ray.available_resources()}")

            asyncio.create_task(get_and_save_api_data(event_finish))

            # Wait for both tasks to complete
            await event_finish.wait()

            res=health_check_task.cancel()
        except Exception as err:
            logging.fatal("Main crash (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)
    logging.info("Exiting with success")
    sys.exit(0)

if __name__ == "__main__":
    getLogger()
    asyncio.run(main())
    sys.exit(0)

