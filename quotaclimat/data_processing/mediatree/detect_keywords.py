import logging

from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from postgres.schemas.models import keywords_table
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from typing import List, Optional
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
import re
import swifter
from itertools import groupby
import sentry_sdk
import modin.pandas as pd
import dask
from quotaclimat.utils.logger import getLogger
dask.config.set({'dataframe.query-planning': True})

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
    elif word.endswith('x'):
        return word + '?'
    else:
        return word


def is_word_in_sentence(words: str, sentence: str) -> bool :
    # words can contain plurals and several words
    words = ' '.join(list(map(( lambda x: format_word_regex(x)), words.split(" "))))

    #  test https://regex101.com/r/ilvs9G/1/
    if re.search(rf"\b{words}(?![\w-])", sentence, re.IGNORECASE):
        logging.debug(f"words {words} found in {sentence}")
        return True
    else:
        return False


def set_timestamp_with_margin(keywords_with_timestamp: List[dict]) -> List[dict]:
    number_of_keywords = len(keywords_with_timestamp)
    if number_of_keywords > 1:
        for i in range(len(keywords_with_timestamp) - 1):
            current_timestamp = keywords_with_timestamp[i].get("timestamp")
            next_timestamp = keywords_with_timestamp[i + 1].get("timestamp")
            current_keyword = keywords_with_timestamp[i].get("keyword")
            next_keyword = keywords_with_timestamp[i + 1].get("keyword")

            if current_timestamp is not None and next_timestamp is not None: 
                difference = next_timestamp - current_timestamp
                if difference < 1000 and difference != 0:
                    logging.debug("margin of 1 second detected")
                    current_keyword = keywords_with_timestamp[i].get("keyword")
                    next_keyword = keywords_with_timestamp[i + 1].get("keyword")
                    if len(current_keyword) > len(next_keyword):
                        shortest_word = next_keyword
                        longest_word = current_keyword
                        timestamp_to_change = current_timestamp
                    else:
                        shortest_word = current_keyword
                        longest_word = next_keyword
                        timestamp_to_change = next_timestamp
                    
                    if shortest_word in longest_word:
                        logging.info(f"Close keywords - we group them {shortest_word} - {longest_word}")
                        keywords_with_timestamp[i]["timestamp"] = timestamp_to_change
                        keywords_with_timestamp[i+1]["timestamp"] = timestamp_to_change

    return keywords_with_timestamp

# some keywords are contained inside other keywords, we need to filter them
# some keyword are tagged with the same timestamp and different theme
def filter_keyword_with_same_timestamp(keywords_with_timestamp: List[dict])-> List[dict]:
    logging.debug(f"Filtering keywords with same timestamp with a margin of one second")
    number_of_keywords = len(keywords_with_timestamp)

    # we want to keep them
    same_keyword_different_theme = [item for item in keywords_with_timestamp if len(list(filter(lambda x: x.get('keyword') == item.get('keyword') and x.get('theme') != item.get('theme'), keywords_with_timestamp))) > 0]
    logging.debug(f"Same keyword different theme {same_keyword_different_theme}")
    # keep the longest keyword based on almost or the same timestamp
    unique_keywords = [item for item in keywords_with_timestamp if len(list(filter(lambda x: x.get('keyword') == item.get('keyword') and x.get('theme') != item.get('theme'), keywords_with_timestamp))) == 0]
    logging.debug(f"Unique keywords {unique_keywords}")
    keywords_with_timestamp = set_timestamp_with_margin(unique_keywords)
    # Group keywords by timestamp - with a margin of 1 second 
    grouped_keywords = {timestamp: list(group) for timestamp, group in groupby(keywords_with_timestamp, key=lambda x: x['timestamp'])}

    # Filter out keywords with the same timestamp and keep the longest keyword
    result = [
        max(group, key=lambda x: len(x['keyword']))
        for group in grouped_keywords.values()
    ]
    logging.debug(f"result keywords {result}")
    result = result + same_keyword_different_theme

    final_result = len(result)

    if final_result < number_of_keywords:
        logging.info(f"Filtering keywords {final_result} out of {number_of_keywords} | {keywords_with_timestamp} with final result {result}")

    return result

@sentry_sdk.trace
def get_themes_keywords_duration(plaintext: str, subtitle_duration: List[str], start: datetime) -> List[Optional[List[str]]]:
    matching_themes = []
    keywords_with_timestamp = []

    logging.debug(f"display datetime start {start}")

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
        keywords_with_timestamp = filter_keyword_with_same_timestamp(keywords_with_timestamp)
        return [matching_themes, keywords_with_timestamp, count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start)]
    else:
        return [None, None, None]

def log_min_max_date(df):
    max_date = max(df['start'])
    min_date = min(df['start'])
    logging.info(f"Date min : {min_date}, max : {max_date}")


def filter_and_tag_by_theme(df: pd.DataFrame) -> pd.DataFrame :
        with sentry_sdk.start_transaction(op="task", name="filter_and_tag_by_theme"):
            count_before_filtering = len(df)
            logging.info(f"{count_before_filtering} subtitles to filter by keywords and tag with themes")
            log_min_max_date(df)

            logging.info(f'tagging plaintext subtitle with keywords and theme : regexp - search taking time...')
            # using swifter to speed up apply https://github.com/jmcarpenter2/swifter
            df[['theme', u'keywords_with_timestamp', 'number_of_keywords']] = df[['plaintext','srt', 'start']].swifter.apply(lambda row: get_themes_keywords_duration(*row), axis=1, result_type='expand')

            # remove all rows that does not have themes
            df = df.dropna(subset=['theme'])

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

def filter_indirect_words(keywords_with_timestamp: List[dict]) -> List[dict]:
    return list(filter(lambda kw: 'indirectes' not in kw['theme'], keywords_with_timestamp))

def get_keyword_by_fifteen_second_window(filtered_themes: List[dict], start: datetime) -> List[int]:
    window_size_seconds = get_keyword_time_separation_ms()
    total_seconds_in_window = get_chunk_duration_api()
    number_of_windows = int(total_seconds_in_window // window_size_seconds)
    fifteen_second_window = [0] * number_of_windows

    for keyword_info in filtered_themes:
        window_number = int( (keyword_info['timestamp'] - start.timestamp() * 1000) // (window_size_seconds) )
        logging.debug(f"Window number {window_number} - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000}")
        if window_number >= number_of_windows and window_number >= 0:
            if(window_number == number_of_windows): # give some slack to mediatree subtitle edge case
                logging.warning(f"Edge cases around 2 minutes - still counting for one - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000}")
                window_number = number_of_windows - 1
                fifteen_second_window[window_number] = 1
            else:
                logging.error(f"Window number {window_number} is out of range - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000}")
        else:
            fifteen_second_window[window_number] = 1
    
    return fifteen_second_window

def count_keywords_duration_overlap_without_indirect(keywords_with_timestamp: List[dict], start: datetime) -> int:
    total_keywords = len(keywords_with_timestamp)
    if(total_keywords) == 0:
        return 0
    else:
        filtered_themes = filter_indirect_words(keywords_with_timestamp)
        length_filtered_items = len(filtered_themes)
        logging.debug(f"Before filtering {total_keywords} - After filtering indirect kw {length_filtered_items}")
        if length_filtered_items > 0:
            fifteen_second_window = get_keyword_by_fifteen_second_window(filtered_themes, start)

            return sum(fifteen_second_window)
        else:
            return 0
    