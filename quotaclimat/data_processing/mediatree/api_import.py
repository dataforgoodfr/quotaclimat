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
    start_date = os.environ.get("START_DATE_UPDATE", "2023-04-01")
    tmp_end_date = get_end_of_month(start_date)
    end_date = os.environ.get("END_DATE", tmp_end_date)
    batch_size = int(os.environ.get("BATCH_SIZE", 50000))
    program_only = os.environ.get("UPDATE_PROGRAM_ONLY", "false") == "true"
    channel = os.environ.get("CHANNEL", "")
    if(program_only):
        logging.warning("Update : Program only mode activated - UPDATE_PROGRAM_ONLY")
    else:
        logging.warning("Update : programs will not be updated for performance issue - use UPDATE_PROGRAM_ONLY to true for this")

    logging.warning(f"Updating already saved data for channel {channel} from Postgresql from date {start_date} - env variable START_DATE_UPDATE until {end_date} - you can use END_DATE to set it (optional)")
    try:
        session = get_db_session()
        update_keywords(session, batch_size=batch_size, start_date=start_date, program_only=program_only, end_date=end_date, channel=channel)
        exit_event.set()
    except Exception as err:
        logging.error("Could update_pg_data %s:(%s)" % (type(err).__name__, err))

def get_channels():
    if(os.environ.get("ENV") == "docker" or os.environ.get("CHANNEL") is not None):
        default_channel = os.environ.get("CHANNEL") or "france2"
        logging.warning(f"Only one channel of env var CHANNEL {default_channel} (default to france2) is used")

        channels = [default_channel]
    else: #prod  - all channels
        logging.warning("All channels are used")
        return ["tf1", "france2", "fr3-idf", "m6", "arte", "d8", "bfmtv", "lci", "franceinfotv", "itele",
        "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]

    return channels

async def get_and_save_api_data(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_api_data"):
        try:
            logging.warning(f"Available CPUS {os.cpu_count()} - MODIN_CPUS config : {os.environ.get('MODIN_CPUS', 3)}")

            conn = connect_to_db()
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
                    try:
                        programs_for_this_day = get_programs_for_this_day(day, channel, df_programs)

                        for program in programs_for_this_day.itertuples(index=False):
                            start_epoch = program.start
                            end_epoch = program.end
                            channel_program = str(program.program_name)
                            channel_program_type = str(program.program_type)
                            logging.info(f"Querying API for {channel} - {channel_program} - {channel_program_type} - {start_epoch} - {end_epoch}")
                            df = extract_api_sub(token, channel, type_sub, start_epoch,end_epoch, channel_program,channel_program_type) 
                            if(df is not None):
                                logging.debug(f"Memory df {df.memory_usage()}")
                                save_to_pg(df, keywords_table, conn)
                                del df
                            else:
                                logging.info("Nothing to save to Postgresql")
                        gc.collect()
                    except Exception as err:
                        logging.error(f"continuing loop but met error : {err}")
                        continue
            exit_event.set()
        except Exception as err:
            logging.fatal("get_and_save_api_data (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)

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

# see : https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def get_param_api(token, type_sub, start_epoch, channel, end_epoch):
    epoch_5min_margin = 300
    return {
        "channel": channel,
        "token": token,
        "start_gte": int(start_epoch) - epoch_5min_margin,
        "start_lte": int(end_epoch) + epoch_5min_margin,
        "type": type_sub,
        "size": "1000" #  range 1-1000
    }

# "Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards"
# @see https://github.com/jd/tenacity/tree/main
@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_post_request(media_tree_token, type_sub, start_epoch, channel, end_epoch):
    try:
        params = get_param_api(media_tree_token, type_sub, start_epoch, channel, end_epoch)
        logging.info(f"Query {KEYWORDS_URL} with params:\n {get_param_api('fake_token_for_log', type_sub, start_epoch, channel, end_epoch)}")
        response = requests.post(KEYWORDS_URL, json=params)
        if response.status_code >= 400:
            logging.warning(f"{response.status_code} - Expired token ? - retrying to get a new one {response.content}")
            media_tree_token = get_auth_token(password, USER)
            raise Exception
        
        return parse_raw_json(response)
    except Exception as err:
        logging.error("Retry - Could not query API :(%s) %s" % (type(err).__name__, err))
        raise Exception

@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_df_api(media_tree_token, type_sub, start_epoch, channel, end_epoch, channel_program, channel_program_type):
    try:
        response_sub = get_post_request(media_tree_token, type_sub, start_epoch, channel, end_epoch)

        return parse_reponse_subtitle(response_sub, channel, channel_program, channel_program_type)
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
        end_epoch = None
        ,channel_program: str = ""
        ,channel_program_type : str = ""
    ) -> Optional[pd.DataFrame]: 
    try:
        df = get_df_api(media_tree_token, type_sub, start_epoch, channel, end_epoch, channel_program, channel_program_type)

        if(df is not None):
            df = filter_and_tag_by_theme(df)
            logging.info(f"Adding primary key to save to PG and have idempotent results")
            df["id"] = df.apply(lambda x: add_primary_key(x), axis=1)
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

def parse_reponse_subtitle(response_sub, channel = None, channel_program = "", channel_program_type = "") -> Optional[pd.DataFrame]:
    with sentry_sdk.start_transaction(op="task", name="parse_reponse_subtitle"):
        total_results = parse_total_results(response_sub)
        logging.getLogger("modin.logging.default").setLevel(logging.WARNING)
        if(total_results > 0):
            logging.info(f"{total_results} 'total_results' field")
            new_df : pd.DataFrame = json_normalize(response_sub.get('data')) # TODO UserWarning: json_normalize is not currently supported by PandasOnRay, defaulting to pandas implementation.
            logging.debug("Schema from API before formatting :\n%s", new_df.dtypes)
            pd.set_option('display.max_columns', None)
            logging.debug("head:  :\n%s", new_df.head())
           
            logging.debug("setting timestamp")
            new_df['timestamp'] = new_df.apply(lambda x: pd.to_datetime(x['start'], unit='s', utc=True), axis=1)
            logging.debug("timestamp was set")

            logging.debug("droping start column")
            new_df.drop('start', axis=1, inplace=True)
            logging.debug("renaming columns")
            new_df.rename(columns={'channel.name':'channel_name', 
                                   'channel.title':'channel_title',
                                   'channel.radio': 'channel_radio',
                                    'timestamp':'start'
                                  },
                        inplace=True
            )

            logging.debug(f"setting program {channel_program}")
            # weird error if not using this way: (ValueError) format number 1 of "20h30 le samedi" is not recognized
            new_df['channel_program'] = new_df.apply(lambda x: channel_program, axis=1)
            new_df['channel_program_type'] = new_df.apply(lambda x: channel_program_type, axis=1)
            logging.debug("programs were set")
           
            return new_df
        else:
            logging.warning("No result (total_results = 0) for this channel")
            return None

async def main():
    with monitor(monitor_slug='mediatree'): #https://docs.sentry.io/platforms/python/crons/
        try:
            logging.info("Start api mediatree import")
            create_tables()

            event_finish = asyncio.Event()
            # Start the health check server in the background
            health_check_task = asyncio.create_task(run_health_check_server())

            context = ray.init(
                dashboard_host="0.0.0.0", # for docker dashboard
                # runtime_env=dict(worker_process_setup_hook=sentry_init),
            )
            logging.info(f"Ray context dahsboard available at : {context.dashboard_url}")
            logging.warning(f"Ray Information about the env: {ray.available_resources()}")

            if(os.environ.get("COMPARE_DURATION") == "true"):
                logging.warning(f"Comparaison between number_of_15/20/30/40 is activated")
            else:
                logging.warning(f"Comparaison between 15/20/30/40 is OFF")

            # Start batch job
            if(os.environ.get("UPDATE") == "true"):
                asyncio.create_task(update_pg_data(event_finish))
            else:
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


