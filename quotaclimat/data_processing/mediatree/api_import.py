### Library imports
import requests
import pandas as pd
import datetime
import json

import logging
import asyncio
from utils import *
import time
import sys
import os
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import CustomFormatter
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
password = os.environ.get("MEDIATREE_PASSWORD")
if(password == '/run/secrets/pwd_api'):
    password= open("/run/secrets/pwd_api", "r").read()
AUTH_URL = os.environ.get("MEDIATREE_AUTH_URL") # 
USER = os.environ.get("MEDIATREE_USER")
if(USER == '/run/secrets/username_api'):
    USER=open("/run/secrets/username_api", "r").read()
KEYWORDS_URL = os.environ.get("KEYWORDS_URL") #https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList

async def get_and_save_api_data(exit_event):
    conn = connect_to_db()
    token=get_auth_token(password=password, user_name=USER)

    (start_date_to_query, end_epoch) = get_start_end_date_env_variable_with_default()

    if(os.environ.get("ENV") == "docker"):
        logging.warning("Docker cases - only some channels are used")
        channels = ["france2"]
    else: #prod    
        channels = ["tf1", "france2", "m6", "arte", "d8", "tmc", "bfmtv", "lci", "franceinfotv", "itele",
        "europe1", "france-culture", "france-inter", "nrj", "rmc", "rtl", "rtl2"]

    logging.info(f"Importing {channels}")

    # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
    range = get_date_range(start_date_to_query, end_epoch)
    logging.info(f"Number of days to query : {len(range)}")
    for date in range:
        date_epoch = get_epoch_from_datetime(date)
        logging.info(f"Date: {date} - {date_epoch}")
        for channel in channels :
            try:
                df = extract_api_sub(token, channel, 's2t', get_epoch_from_datetime(date) )
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
def get_param_api(token, type_sub, start_epoch, channel = 'm6'):
    return {
        "channel": channel,
        "token": token,
        "start_gte": start_epoch,
        "type": type_sub,
        "size": "1000", #  range 1-1000
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
def get_post_request(media_tree_token, type_sub, start_epoch, channel):
    params = get_param_api(media_tree_token, type_sub, start_epoch, channel)
    logging.info(f"Query {KEYWORDS_URL} with params {get_param_api('fake_token_for_log', type_sub, start_epoch, channel)}")
    response = requests.post(KEYWORDS_URL, json=params)
    return parse_raw_json(response)

# Data extraction function definition
# https://keywords.mediatree.fr/docs/#api-Subtitle-SubtitleList
def extract_api_sub(
        media_tree_token, 
        channel = 'm6',
        type_sub = "s2t",
        start_epoch = None
    ) -> Optional[pd.DataFrame]: 
    """

    Fonction d'extraction des données de l'API
    
    ----------

    Parameters
    ----------
    start : string, optional
        Date de départ sous le format 'YYYY-MM-DD HH:MM:SS', à l'heure fr,
        by default '2023-05-10 12:00:00'
    end : string, optional
        Date de fin sous le format 'YYYY-MM-DD HH:MM:SS', à l'heure fr, 
        by default '2023-05-21 12:00:00'
    channels : list of string, optional
        Liste des chaînes voulues, 
        Clés de chaînes possibles : 
        TV : tf1, france2, fr3-idf, france5, m6, arte, d8, tmc, bfmtv, lci, franceinfotv, itele
        Radios: europe1, france-culture, france-inter, nrj, rfm, rmc, rtl, rtl2
        by default ['m6']
    timestamp : boolean
        False pour avoir le dataframe par émission, True pour celui avec les mots par timestamp
        by default False
    """
    if start_epoch is None:
        logging.info("Default start date: Yesterday")
        start_epoch = get_yesterday()
    else: 
        logging.debug(f"Date epoch: {start_epoch}")
    logging.info(f"parsing channel : {channel}")


    try:
        response_sub = get_post_request(media_tree_token, type_sub, start_epoch, channel)

        df = parse_reponse_subtitle(response_sub, channel)
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

def log_dataframe_size(df, channel):
    bytes_size = sys.getsizeof(df)
    logging.info(f"Dataframe size : {bytes_size / (1000 * 1000)} Megabytes")
    if(bytes_size > 50 * 1000 * 1000): # 50Mb
        logging.warning(f"High Dataframe size : {bytes_size / (1000 * 1000)}")
    if(len(df)== 1000):
        logging.error(f"{channel} : We might lose data as we have 1000 out of 1000 results possible - we should divide this query")

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
