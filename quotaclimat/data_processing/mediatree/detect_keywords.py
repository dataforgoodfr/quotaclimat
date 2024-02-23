import pandas as pd

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

# some keywords are contained inside other keywords, we need to filter them
def filter_keyword_with_same_timestamp(keywords_with_timestamp: List[dict]) -> List[dict]:
    # Group keywords by timestamp
    grouped_keywords = {timestamp: list(group) for timestamp, group in groupby(keywords_with_timestamp, key=lambda x: x['timestamp'])}

    # Filter out keywords with the same timestamp and keep the longest keyword
    result = [
        max(group, key=lambda x: len(x['keyword']))
        for group in grouped_keywords.values()
    ]

    return result
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
        keywords_with_timestamp = filter_keyword_with_same_timestamp(keywords_with_timestamp)
        return [matching_themes, keywords_with_timestamp, count_keywords_duration_overlap_without_indirect(keywords_with_timestamp)]
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

def filter_indirect_words(keywords_with_timestamp: List[dict]) -> List[dict]:
    return list(filter(lambda kw: 'indirectes' not in kw['theme'], keywords_with_timestamp))

def count_keywords_duration_overlap_without_indirect(keywords_with_timestamp: List[dict]) -> int:
    total_keywords = len(keywords_with_timestamp)
    if(total_keywords) == 0:
        return 0
    else:
        # in case keywords are not in the right order
        sorted_keywords = sorted(keywords_with_timestamp, key=lambda x: x['timestamp'])
        filtered_themes = filter_indirect_words(sorted_keywords)
        length_filtered_items = len(filtered_themes)
        logging.debug(f"Before filtering {total_keywords} - After filtering indirect kw {length_filtered_items}")
        if length_filtered_items > 0:
            iter_filtered_themes = iter(filtered_themes)
            count = 1
            previous_timestamp = next(iter_filtered_themes)['timestamp']

            for keyword_info in filtered_themes:
                current_timestamp = keyword_info['timestamp']
                overlap_time = current_timestamp - previous_timestamp
                
                if is_time_distance_between_keyword_enough(overlap_time):
                    logging.debug(f"No overlapping keyword {count} + 1 : {overlap_time}")
                    count += 1
                    previous_timestamp = current_timestamp
                else:
                    logging.debug(f"Keyword timestamp overlap : {overlap_time} - current count is {count}")

            return count
        else:
            return 0