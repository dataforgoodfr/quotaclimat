### Library imports
import requests
import pandas as pd
import datetime
import json

import logging
import asyncio
import time
import sys
import os
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import CustomFormatter
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, connect_to_db
from postgres.schemas.models import keywords_table
from pandas import json_normalize
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from typing import List, Optional
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
import re
import swifter
from tenacity import *

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

async def get_and_save_api_data(exit_event):
    conn = connect_to_db()
    token=get_auth_token(password=password, user_name=USER)
    type_sub = 's2t'

    (start_date_to_query, end_epoch) = get_start_end_date_env_variable_with_default()

    if(os.environ.get("ENV") == "docker"):
        logging.warning("Docker cases - only some channels are used")
        channels = ["france2"]
    else: #prod    
        channels = ["tf1", "france2", "m6", "arte", "d8", "tmc", "bfmtv", "lci", "franceinfotv", "itele",
        "europe1", "france-culture", "france-inter", "nrj", "rmc", "rtl", "rtl2"]

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
                    save_to_pg(df, keywords_table, conn)
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
    logger.info(f"Getting a token for user {user_name}")
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


# TODO use it when API updated
def get_includes_or_query(array_words: List[str]) -> str: 
    return " OU ".join(array_words)

def get_theme_query_includes(theme_dict):
    return {key: get_includes_or_query(values) for key, values in theme_dict.items()}

def transform_theme_query_includes(themes_with_keywords = THEME_KEYWORDS):
    return list(map(get_theme_query_includes, themes_with_keywords))

def get_cts_in_ms_for_keywords(subtitle_duration: List[dict], keywords: List[str], theme: str) -> List[dict]:
    result = []

    logging.debug(f"Looking for timecode for {keywords}")
    for multiple_keyword in keywords:
        all_keywords = multiple_keyword.split() # case with multiple words such as 'économie circulaire'
        match = next((item for item in subtitle_duration if is_word_in_sentence(all_keywords[0], item.get('text'))), None)  
        logging.debug(f"match found {match} with {all_keywords[0].lower()}")     
        if match is not None:
            logging.debug(f'Result added due to this match {match} based on {all_keywords[0]}')
            result.append(
                {
                    "keyword" :multiple_keyword.lower(),
                    "timestamp" : match['cts_in_ms'],
                    "theme" : theme
                })

    logging.debug(f"Timecode found {result}")
    return result

# be able to detect singular or plural for a word
def format_word_regex(word: str) -> str:
    word = word.replace('\'', '\' ?') # case for d'eau -> d' eau
    if not word.endswith('s') and not word.endswith('x') and not word.endswith('à'):
        return word + "s?"
    elif word.endswith('s'):
        return word + '?'
    else:
        return word

def is_word_in_sentence(words: str, sentence: str) -> bool :
    # words can contain plurals and several words
    words = ' '.join(list(map(( lambda x: format_word_regex(x)), words.split(" "))))
    logging.debug(f"testing {words}")
    #  test https://regex101.com/r/ilvs9G/1/
    if re.search(rf"\b{words}(?![\w-])", sentence, re.IGNORECASE):
        logging.debug(f"words {words} found in {sentence}")
        return True
    else:
        return False

def get_themes_keywords_duration(plaintext: str, subtitle_duration: List[str]) -> List[Optional[List[str]]]:
    matching_themes = []
    keywords_with_timestamp = []

    for theme, keywords in THEME_KEYWORDS.items():
        logging.debug(f"searching {theme} for {keywords}")

        matching_words = [word for word in keywords if is_word_in_sentence(word, plaintext)]  
        if matching_words:
            logging.debug(f"theme found : {theme} with word {matching_words}")
            matching_themes.append(theme)
            # look for cts_in_ms inside matching_words (['économie circulaire', 'panneaux solaires', 'solaires'] from subtitle_duration 
            keywords_to_add = get_cts_in_ms_for_keywords(subtitle_duration, matching_words, theme)
            if(len(keywords_to_add) == 0):
                logging.warning(f"Check regex - Empty keywords but themes is there {theme} - matching_words {matching_words} - {subtitle_duration}")
            keywords_with_timestamp.extend(keywords_to_add)
    
    if len(matching_themes) > 0:
        return [matching_themes, keywords_with_timestamp, int(len(keywords_with_timestamp))]
    else:
        return [None, None, None]

def log_min_max_date(df):
    max_date = max(df['start'])
    min_date = min(df['start'])
    logging.info(f"Date min : {min_date}, max : {max_date}")

def filter_and_tag_by_theme(df: pd.DataFrame) -> pd.DataFrame :
    count_before_filtering = len(df)
    logging.info(f"{count_before_filtering} subtitles to filter by keywords and tag with themes")
    log_min_max_date(df)

    logging.info(f'tagging plaintext subtitle with keywords and theme : regexp - search taking time...')
    # using swifter to speed up apply https://github.com/jmcarpenter2/swifter
    df[['theme', u'keywords_with_timestamp', 'number_of_keywords']] = df[['plaintext','srt']].swifter.apply(lambda row: get_themes_keywords_duration(*row), axis=1, result_type='expand')

    # remove all rows that does not have themes
    df = df.dropna(subset=['theme'])

    df.drop('srt', axis=1, inplace=True)

    logging.info(f"After filtering with out keywords, we have {len(df)} out of {count_before_filtering} subtitles left that are insteresting for us")

    return df

def add_primary_key(df):
    logging.info("Adding primary key to save to PG and have idempotent result")
    try:
        return (
            df["start"].astype(str) + df["channel_name"]
        ).apply(get_consistent_hash)
    except (Exception) as error:
        logging.error(error)
        return get_consistent_hash("empty") #  TODO improve - should be a None ?

# "Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60 seconds, then randomly up to 60 seconds afterwards"
# @see https://github.com/jd/tenacity/tree/main
@retry(wait=wait_random_exponential(multiplier=1, max=60),stop=stop_after_attempt(7))
def get_post_request(media_tree_token, type_sub, start_epoch, channel, page: int = 0):
    try:
        params = get_param_api(media_tree_token, type_sub, start_epoch, channel, page)
        logging.info(f"Query {KEYWORDS_URL} page {page} with params:\n {get_param_api('fake_token_for_log', type_sub, start_epoch, channel, page)}")
        response = requests.post(KEYWORDS_URL, json=params)
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
            return df
        else:
            None
    except Exception as err:
        logging.error("Could not query API :(%s) %s" % (type(err).__name__, err))
        return None

def parse_raw_json(response):
    if response.status_code == 504:
        logger.error(f"Mediatree API server error 504 (retry enabled)\n {response.content}")
        raise Exception
    else:
        return json.loads(response.content.decode('utf_8'))

def parse_total_results(response_sub) -> int :
    return response_sub.get('total_results')

def parse_number_pages(response_sub) -> int :
    return int(response_sub.get('number_pages'))

def parse_reponse_subtitle(response_sub, channel = None) -> Optional[pd.DataFrame]: 
    logging.debug(f"Parsing json response:\n {response_sub}")
    
    total_results = parse_total_results(response_sub)
    if(total_results > 0):
        logging.info(f"{total_results} 'total_results' field")
        
        new_df = json_normalize(response_sub.get('data'))
        logging.info("Schema from API before formatting :\n%s", new_df.dtypes)
        new_df.drop('channel.title', axis=1, inplace=True) # keep only channel.name

        new_df['timestamp'] = (pd.to_datetime(new_df['start'], unit='s').dt.tz_localize('utc').dt.tz_convert('Europe/Paris'))
        new_df.drop('start', axis=1, inplace=True) # keep only channel.name

        new_df.rename(columns={'channel.name':'channel_name', 'channel.radio': 'channel_radio', 'timestamp':'start'}, inplace=True)

        log_dataframe_size(new_df, channel)

        logging.debug("Parsed %s" % (new_df.head(1).to_string()))
        logging.debug("Parsed Schema\n%s", new_df.dtypes)
        
        return new_df
    else:
        logging.warning("No result (total_results = 0) for this channel")
        return None

def count_keywords_duration_overlap(keywords_with_timestamp: List[dict]) -> int:
    # in case keywords are not in the right order
    sorted_keywords = iter(sorted(keywords_with_timestamp, key=lambda x: x['timestamp']))

    count = 1
    previous_timestamp = next(sorted_keywords)['timestamp']

    for keyword_info in sorted_keywords:
        current_timestamp = keyword_info['timestamp']
        overlap_time = current_timestamp - previous_timestamp
        
        if is_time_distance_between_keyword_enough(overlap_time):
            logging.debug(f"No overlapping keyword {count} + 1 : {overlap_time}")
            count += 1
            previous_timestamp = current_timestamp
        else:
            logging.debug(f"Keyword timestamp overlap : {overlap_time} - current count is {count}")

    return count

def log_dataframe_size(df, channel):
    bytes_size = sys.getsizeof(df)
    logging.info(f"Dataframe size : {bytes_size / (1000 * 1000)} Megabytes")
    if(bytes_size > 50 * 1000 * 1000): # 50Mb
        logging.warning(f"High Dataframe size : {bytes_size / (1000 * 1000)}")
    if(len(df) == 1000):
        logging.error("We might lose data - df size is 1000 out of 1000 - we should divide this querry")
    
async def main():    
    logger.info("Start api mediatree import")
    create_tables()

    event_finish = asyncio.Event()
    # Start the health check server in the background
    health_check_task = asyncio.create_task(run_health_check_server())

    # Start batch job
    asyncio.create_task(get_and_save_api_data(event_finish))

    # Wait for both tasks to complete
    await event_finish.wait()

   # only for scaleway - delay for serverless container
   # Without this we have a CrashLoopBackOff (Kubernetes health error)
    if (os.environ.get("ENV") != "dev" and os.environ.get("ENV") != "docker"):
        minutes = 15
        logging.warning(f"Sleeping {minutes} before safely exiting scaleway container")
        time.sleep(60 * minutes)

    res=health_check_task.cancel()
    logging.info("Exiting with success")
    sys.exit(0)

if __name__ == "__main__":
    # create logger with 'spam_application'
    logger = logging.getLogger()
    logger.setLevel(level=os.getenv('LOGLEVEL', 'INFO').upper())

    # create console handler with a higher log level
    if (logger.hasHandlers()):
        logger.handlers.clear()
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    asyncio.run(main())
    sys.exit(0)


