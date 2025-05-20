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
from quotaclimat.data_processing.mediatree.s3.s3_utils import *
from quotaclimat.data_processing.mediatree.i8n.country import *

import shutil
from typing import List, Optional
from tenacity import *
import sentry_sdk
from sentry_sdk.crons import monitor
import modin.pandas as pd
import ray

from quotaclimat.utils.sentry import sentry_init
logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
logging.getLogger('distributed').setLevel(logging.ERROR)
logging.getLogger('worker').setLevel(logging.ERROR)


#read whole file to a string
password = get_password()
AUTH_URL = get_auth_url()
USER = get_user()
KEYWORDS_URL = get_keywords_url()

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

    return {
        "channel": channel,
        "token": token,
        "start_gte": int(start_epoch) - EPOCH__5MIN_MARGIN,
        "start_lte": int(end_epoch) + EPOCH__5MIN_MARGIN,
        "type": type_sub,
        "size": "1000" #  range 1-1000
    }


def parse_reponse_subtitle(response_sub, channel = None, channel_program = "", channel_program_type = ""
                           , program_metadata_id = None, country: CountryMediaTree = FRANCE) -> Optional[pd.DataFrame]:
    with sentry_sdk.start_transaction(op="task", name="parse_reponse_subtitle"):
        total_results = parse_total_results(response_sub)
        logging.getLogger("modin.logging.default").setLevel(logging.WARNING)
        if(total_results > 0):
            logging.info(f"{total_results} 'total_results' field")
           
            # To avoid  UserWarning: json_normalize is not currently supported by PandasOnRay, defaulting to pandas implementation.
            flattened_data = response_sub.get("data", [])
            new_df : pd.DataFrame = pd.DataFrame(flattened_data)
            new_df["channel.name"] = new_df["channel"].apply(lambda x: x["name"])
            new_df["channel.title"] = new_df["channel"].apply(lambda x: x["title"])
            new_df["channel.radio"] = new_df["channel"].apply(lambda x: x["radio"])
            new_df.drop("channel", axis=1, inplace=True)

            logging.debug("Schema from API before formatting :\n%s", new_df.dtypes)
            pd.set_option('display.max_columns', None)
            logging.debug("setting timestamp")

            timezone = country.timezone
            new_df['timestamp'] = new_df.apply(lambda x: pd.to_datetime(x['start'], unit='s',utc=True).tz_convert(timezone), axis=1)
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

            logging.debug("setting channel_title")
            new_df['channel_title'] = new_df.apply(lambda x: get_channel_title_for_name(x['channel_name'], country=country), axis=1)
            
            logging.debug(f"setting program {channel_program}")
            # weird error if not using this way: (ValueError) format number 1 of "20h30 le samedi" is not recognized
            new_df['channel_program'] = new_df.apply(lambda x: channel_program, axis=1)
            new_df['channel_program_type'] = new_df.apply(lambda x: channel_program_type, axis=1)
            new_df['program_metadata_id'] = new_df.apply(lambda x: program_metadata_id, axis=1)
            
            logging.debug("programs were set")
            new_df['country'] = new_df.apply(lambda x: country.name, axis=1)
            return new_df
        else:
            logging.warning(f"No result (total_results = 0) for this channel {channel} - {channel_program}")
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
def get_df_api(media_tree_token, type_sub, start_epoch, channel, end_epoch, channel_program, channel_program_type, program_metadata_id, country: CountryMediaTree = FRANCE):
    try:
        response_sub = get_post_request(media_tree_token, type_sub, start_epoch, channel, end_epoch)

        return parse_reponse_subtitle(response_sub, channel, channel_program, channel_program_type, program_metadata_id, country=country)
    except Exception as err:
        logging.error("Retry - get_df_api:(%s) %s" % (type(err).__name__, err))
        raise Exception
    
def refresh_token(token, date):
    if is_it_tuesday(date): # refresh token every weekday for batch import
        logging.info("refreshing api token every weekday in case it's expired")
        return get_auth_token(password=password, user_name=USER)
    else:
        return token

def get_partition_s3(country: CountryMediaTree = FRANCE) -> list[str]:
    if country.code != FRANCE.code:
        partition_cols = ['country','year', 'month', 'day', 'channel']
    else:
        partition_cols = ['year', 'month', 'day', 'channel'] # legacy for france

    return partition_cols

def save_to_s3(df: pd.DataFrame, channel: str, date: pd.Timestamp, s3_client, country: CountryMediaTree = FRANCE):
    logging.info(f"Saving DF with {len(df)} elements to S3 for {date} and channel {channel}")

    # to create partitions
    object_key = get_bucket_key(date=date, channel=channel, country=country)
    logging.debug(f"Uploading partition: {object_key}")

    try:
        # add partition columns year, month, day to dataframe
        df['year'] = date.year
        df['month'] = date.month
        df['day'] = date.day
        df['channel'] = channel
        df['country'] = country.name

        df = df._to_pandas() # collect data accross ray workers to avoid multiple subfolders
        based_path = "s3/parquet"

        partition_cols = get_partition_s3(country)
       
        df.to_parquet(based_path,
                       compression='gzip'
                       ,partition_cols=partition_cols)

        #saving full_path folder parquet to s3
        s3_path = f"{get_bucket_key_folder(date, channel, country=country)}"
        local_folder = f"{based_path}/{s3_path}"
        upload_folder_to_s3(local_folder, BUCKET_NAME, s3_path,s3_client=s3_client)
        
    except Exception as err:
        logging.fatal("get_and_save_api_data (%s) %s" % (type(err).__name__, err))
        sys.exit(1)

async def get_and_save_api_data(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_api_data"):
        try:
            logging.warning(f"Available CPUS {os.cpu_count()} - MODIN_CPUS config : {os.environ.get('MODIN_CPUS', 3)}")
            s3_client = get_s3_client()
            token=get_auth_token(password=password, user_name=USER)
            type_sub = 's2t'
            start_date = int(os.environ.get("START_DATE", 0))
            number_of_previous_days = int(os.environ.get("NUMBER_OF_PREVIOUS_DAYS", 7))
            country_code: str = os.environ.get("COUNTRY", FRANCE_CODE)
            logging.info(f"Country used is (default {FRANCE_CODE}) : {country_code}")
            countries = get_countries_array(country_code=country_code, no_belgium=True)

            for country in countries:
                logging.info(f"Country : {country}")
                df_programs = get_programs(country)
                channels = country.channels
                if country == GERMANY:
                    logging.warning(f"Removing channels daserste and zdf-neo and using GERMANY_CHANNELS_MEDIATREE as import via SRT")
                    channels = GERMANY_CHANNELS_MEDIATREE
                timezone = country.timezone
                (start_date_to_query, end_date) = get_start_end_date_env_variable_with_default(start_date, \
                                                                                            minus_days=number_of_previous_days,\
                                                                                            timezone=country.timezone)

                day_range: pd.DatetimeIndex = get_date_range(start_date_to_query, end_date, number_of_previous_days, country=country)
                logging.info(f"Number of days to query : {len(day_range)} - day_range : {day_range}")
                for day in day_range:
                    token = refresh_token(token, day)
                    
                    for channel in channels:
                        df_res = pd.DataFrame()
                        
                        # if object already exists, skip
                        if not check_if_object_exists_in_s3(day, channel,s3_client=s3_client, country=country):
                            try:
                                programs_for_this_day = get_programs_for_this_day(day.tz_localize(timezone), channel, df_programs, timezone=timezone)
                                if programs_for_this_day is None:
                                    logging.info(f"No program for {day} and {channel}, skipping")
                                    continue

                                for program in programs_for_this_day.itertuples(index=False):
                                    start_epoch = program.start
                                    end_epoch = program.end
                                    channel_program = str(program.program_name)
                                    channel_program_type = str(program.program_type)
                                    program_metadata_id = program.id
                                    logging.info(f"Querying API for {channel} - {channel_program} - {channel_program_type} - {start_epoch} - {end_epoch}")
                                    df = get_df_api(token, type_sub, start_epoch, channel, end_epoch, \
                                                    channel_program, channel_program_type,program_metadata_id=program_metadata_id, country=country)

                                    if(df is not None):
                                        df_res = pd.concat([df_res, df ], ignore_index=True)
                                    else:
                                        logging.info(f"Nothing to extract for {channel} {channel_program} - {start_epoch} - {end_epoch}")

                                # save to S3
                                save_to_s3(df_res, channel, day, s3_client=s3_client, country=country)
                                
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
            sentry_init()
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

