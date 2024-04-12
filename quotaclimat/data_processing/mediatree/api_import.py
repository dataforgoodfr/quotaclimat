### Library imports
import requests
import json

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

def refresh_token(token, date):
    if is_it_tuesday(date): # refresh token every weekday for batch import
        logging.info("refreshing api token every weekday in case it's expired")
        return get_auth_token(password=password, user_name=USER)
    else:
        return token

# reapply word detector logic to all saved keywords
# use when word detection is changed
async def update_pg_data(exit_event):
    start_offset = int(os.environ.get("START_OFFSET", 0))
    batch_size = int(os.environ.get("BATCH_SIZE", 50000))
    logging.warning(f"Updating already saved data from Postgresql from offset {start_offset} - env variable START_OFFSET")
    try:
        session = get_db_session()
        update_keywords(session, batch_size=batch_size, start_offset=start_offset)
        exit_event.set()
    except Exception as err:
        logging.error("Could update_pg_data %s:(%s) %s" % (type(err).__name__, err))

def get_channels():
    if(os.environ.get("ENV") == "docker" or os.environ.get("CHANNEL") is not None):
        default_channel = os.environ.get("CHANNEL") or "france2"
        logging.warning(f"Only one channel of env var CHANNEL {default_channel} (default to france2) is used")

        channels = [default_channel]
    else: #prod  - all channels
        logging.warning("All channels are used")
        return ["tf1", "france2", "fr3-idf", "france5", "m6", "arte", "d8", "tmc", "bfmtv", "lci", "franceinfotv", "itele",
        "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]

    return channels

async def get_and_save_api_data(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_api_data"):
        conn = connect_to_db()
        token=get_auth_token(password=password, user_name=USER)
        type_sub = 's2t'

        (start_date_to_query, end_epoch) = get_start_end_date_env_variable_with_default()

        channels = get_channels()
        
        range = get_date_range(start_date_to_query, end_epoch)
        logging.info(f"Number of date to query : {len(range)}")
        for date in range:
            token = refresh_token(token, date)
            
            date_epoch = get_epoch_from_datetime(date)
            logging.info(f"Date: {date} - {date_epoch}")
            for channel in channels :
                try:
                    df = extract_api_sub(token, channel, type_sub, date_epoch)
                    if(df is not None):
                        # must ._to_pandas() because modin to_sql is not working
                        save_to_pg(df._to_pandas(), keywords_table, conn)
                    else: 
                        logging.info("Nothing to save to Postgresql")
                except Exception as err:
                    logging.error(f"continuing loop but met error : {err}")
                    continue
    exit_event.set()

# "Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards"
# @see https://github.com/jd/tenacity/tree/main
@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_auth_token(password=password, user_name=USER):
    logging.info(f"Getting a token for user {user_name}")
    try:
        post_arguments = {
            'grant_type': 'password'
            , 'username': user_name
            , 'password': password
        }
        response = requests.post(
            AUTH_URL, 
            data=post_arguments
        )
        output = response.json()
        token = output['data']['access_token']
        return token 
    except Exception as err:
        logging.error("Could not get token %s:(%s) %s" % (type(err).__name__, err))

# @TODO filter by keyword when api is updated (to filter first on the API side instead of filter_and_tag_by_theme )
# see : https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def get_param_api(token, type_sub, start_epoch, channel = 'm6', page = 0):
    number_of_hours = get_hour_frequency()
    one_hour = 3600
    return {
        "channel": channel,
        "token": token,
        "start_gte": start_epoch,
        "start_lte": start_epoch + (one_hour * number_of_hours), # Start date lower or equal
        "type": type_sub,
        "size": "1000", #  range 1-1000
        # "from": page TODO fix me
    }


# "Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards"
# @see https://github.com/jd/tenacity/tree/main
@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_post_request(media_tree_token, type_sub, start_epoch, channel, page: int = 0):
    try:
        params = get_param_api(media_tree_token, type_sub, start_epoch, channel, page)
        logging.info(f"Query {KEYWORDS_URL} with params:\n {get_param_api('fake_token_for_log', type_sub, start_epoch, channel, page)}")
        response = requests.post(KEYWORDS_URL, json=params)
        if response.status_code == 401:
            logging.warning(f"401 - Expired token - retrying to get a new one {response.content}")
            media_tree_token = get_auth_token(password, USER)
            raise Exception
        
        return parse_raw_json(response)
    except Exception as err:
        logging.error("Retry - Could not query API :(%s) %s" % (type(err).__name__, err))
        raise Exception

# use API pagination to be sure to query all data from a date
def get_number_of_page_by_channel(media_tree_token, type_sub, start_epoch, channel) -> int:
    logging.info("get how many pages we have to parse from the API (number_pages)")
    response_sub = get_post_request(media_tree_token, type_sub, start_epoch, channel)

    return parse_number_pages(response_sub)

@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_df_api(media_tree_token, type_sub, start_epoch, channel, page):
    try:
        response_sub = get_post_request(media_tree_token, type_sub, start_epoch, channel, page)

        return parse_reponse_subtitle(response_sub, channel)
    except Exception as err:
        logging.error("Retry - get_df_api:(%s) %s" % (type(err).__name__, err))
        raise Exception

# Data extraction function definition
# https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def extract_api_sub(
        media_tree_token, 
        channel = 'm6',
        type_sub = "s2t",
        start_epoch = None,
        page = 0
    ) -> Optional[pd.DataFrame]: 
    try:
        df = get_df_api(media_tree_token, type_sub, start_epoch, channel, page)

        if(df is not None):
            df = filter_and_tag_by_theme(df)
            df["id"] = add_primary_key(df)
            df["channel_program"] = add_channel_program(df)
            return df
        else:
            None
    except Exception as err:
        logging.error("Could not query API :(%s) %s" % (type(err).__name__, err))
        return None

def parse_raw_json(response):
    if response.status_code == 504:
        logging.error(f"Mediatree API server error 504 (retry enabled)\n {response.content}")
        raise Exception
    else:
        return json.loads(response.content.decode('utf_8'))

def parse_total_results(response_sub) -> int :
    return response_sub.get('total_results')

def parse_number_pages(response_sub) -> int :
    return int(response_sub.get('number_pages'))

def parse_reponse_subtitle(response_sub, channel = None) -> Optional[pd.DataFrame]:
    with sentry_sdk.start_transaction(op="task", name="parse_reponse_subtitle"):
        total_results = parse_total_results(response_sub)
        logging.getLogger("modin.logging.default").setLevel(logging.WARNING)
        if(total_results > 0):
            logging.info(f"{total_results} 'total_results' field")
            
            new_df : pd.DataFrame = json_normalize(response_sub.get('data'))
            logging.debug("Schema from API before formatting :\n%s", new_df.dtypes)
            new_df.drop('channel.title', axis=1, inplace=True) # keep only channel.name

            new_df['timestamp'] = pd.to_datetime(new_df['start'], unit='s', utc=True).dt.tz_convert('Europe/Paris')
            new_df.drop('start', axis=1, inplace=True) # keep only channel.name

            new_df.rename(columns={'channel.name':'channel_name', 'channel.radio': 'channel_radio', 'timestamp':'start'}, inplace=True)

            log_dataframe_size(new_df, channel)
            
            logging.debug("Parsed Schema\n%s", new_df.dtypes)
            
            return new_df
        else:
            logging.warning("No result (total_results = 0) for this channel")
            return None

def log_dataframe_size(df, channel):
    if(len(df) == 1000):
        logging.error(f"We might lose data for {channel} - df size is 1000 out of 1000 - we should divide this querry")


async def main():
    with monitor(monitor_slug='mediatree'): #https://docs.sentry.io/platforms/python/crons/
        logging.info("Start api mediatree import")
        create_tables()

        event_finish = asyncio.Event()
        # Start the health check server in the background
        health_check_task = asyncio.create_task(run_health_check_server())

        # Start batch job
        if(os.environ.get("UPDATE") == "true"):
            asyncio.create_task(update_pg_data(event_finish))
        else:
            asyncio.create_task(get_and_save_api_data(event_finish))

        # Wait for both tasks to complete
        await event_finish.wait()

        res=health_check_task.cancel()
    logging.info("Exiting with success")
    sys.exit(0)

if __name__ == "__main__":
    getLogger()
    asyncio.run(main())
    sys.exit(0)


