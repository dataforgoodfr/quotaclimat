### Library imports
import requests
import json

import logging
import asyncio
from time import sleep
import sys
import os
import gc
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
from quotaclimat.data_processing.mediatree.api_import import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, connect_to_db, get_db_session
from postgres.schemas.models import keywords_table

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from typing import List, Optional
from tenacity import *
import sentry_sdk
from sentry_sdk.crons import monitor
import modin.pandas as pd
from modin.pandas import json_normalize
import ray
import s3fs
import boto3
from io import StringIO

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

def get_bucket_key(date, channel):
    (year, month, day) = (date.year, date.month, date.day)
    return f'year={year}/month={month:02}/day={day:02}/channel={channel}/data.json'

def save_to_s3(df: pd.DataFrame, channel: str, date: pd.Timestamp):
    logging.info(f"Saving DF with {len(df)} elements to S3 for {date} and channel {channel}")

    # to create partitions
    object_key = get_bucket_key(date, channel)
    logging.debug(f"Uploading partition: {object_key}")
    # json_to_save = df.to_json(None, orient='records', lines=False)
    logging.info(f"s3://{BUCKET_NAME}/{object_key}")

    try:
        json_buffer = StringIO()
        json_buffer = df.to_json(
            None,
            index=False,
            orient='records',
            lines=False
        )
        s3_client.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=json_buffer)

        logging.info(f"Uploaded partition: {object_key}")
    except Exception as e:
        logging.error(Exception)
        exit()

async def get_and_save_api_data(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_api_data"):
        try:
            logging.warning(f"Available CPUS {os.cpu_count()} - MODIN_CPUS config : {os.environ.get('MODIN_CPUS', 3)}")

            token=get_auth_token(password=password, user_name=USER)
            type_sub = 's2t'

            (start_date_to_query, end_date) = get_start_end_date_env_variable_with_default()
            df_programs = get_programs()
            channels = get_channels()
            
            day_range = get_date_range(start_date_to_query, end_date)
            logging.info(f"Number of days to query : {len(day_range)} - day_range : {day_range}")
            for day in day_range:
                token = refresh_token(token, day)
                
                for channel in channels:
                    df_res = pd.DataFrame()
                    try:
                        programs_for_this_day = get_programs_for_this_day(day.tz_localize("Europe/Paris"), channel, df_programs)

                        for program in programs_for_this_day.itertuples(index=False):
                            start_epoch = program.start
                            end_epoch = program.end
                            channel_program = str(program.program_name)
                            channel_program_type = str(program.program_type)
                            logging.info(f"Querying API for {channel} - {channel_program} - {channel_program_type} - {start_epoch} - {end_epoch}")
                            df = get_df_api(token, type_sub, start_epoch, channel, end_epoch, channel_program, channel_program_type)
                            df.drop(columns=['srt'], axis=1, inplace=True)
                            if(df is not None):
                                df_res = pd.concat([df_res, df ], ignore_index=True)
                            else:
                                logging.info("Nothing to extract")

                        # save to S3
                        save_to_s3(df_res, channel, day)
                        
                    except Exception as err:
                        logging.error(f"continuing loop but met error : {err}")
                        continue
            exit_event.set()
        except Exception as err:
            logging.fatal("get_and_save_api_data (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)

async def main():
    with monitor(monitor_slug='mediatree'): #https://docs.sentry.io/platforms/python/crons/
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

