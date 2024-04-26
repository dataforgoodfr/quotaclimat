import logging

from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from postgres.schemas.models import keywords_table
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.keyword.stop_words import STOP_WORDS
from typing import List, Optional
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
import re
import swifter
from itertools import groupby
import sentry_sdk
import modin.pandas as pd
import dask
import copy
from quotaclimat.utils.logger import getLogger
logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
dask.config.set({'dataframe.query-planning': True})

indirectes = 'indirectes'

def get_cts_in_ms_for_keywords(subtitle_duration: List[dict], keywords: List[dict], theme: str) -> List[dict]:
    result = []

    logging.debug(f"Looking for timecode for {keywords}")
    for multiple_keyword in keywords:
        category = multiple_keyword["category"]
        all_keywords = multiple_keyword["keyword"].split() # case with multiple words such as 'economie circulaire'
        match = next((item for item in subtitle_duration if is_word_in_sentence(all_keywords[0], item.get('text'))), None)  
        logging.debug(f"match found {match} with {all_keywords[0].lower()}")     
        if match is not None:
            logging.debug(f'Result added due to this match {match} based on {all_keywords[0]}')
            result.append(
                {
                    "keyword" :multiple_keyword["keyword"].lower(),
                    "timestamp" : match['cts_in_ms'],
                    "theme" : theme,
                    "category": category
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
        logging.debug(f"Filtering keywords {final_result} out of {number_of_keywords} | {keywords_with_timestamp} with final result {result}")

    return result

def remove_stopwords(plaintext: str) -> str:
    stopwords = STOP_WORDS
    for word in stopwords:
        plaintext = plaintext.replace(word, '')

    return plaintext

@sentry_sdk.trace
def get_themes_keywords_duration(plaintext: str, subtitle_duration: List[str], start: datetime):
    keywords_with_timestamp = []

    plaitext_without_stopwords = remove_stopwords(plaintext)
    logging.debug(f"display datetime start {start}")

    for theme, keywords_dict in THEME_KEYWORDS.items():
        logging.debug(f"searching {theme} for {keywords_dict}")
        matching_words = []
        for keyword_dict in keywords_dict:
            if is_word_in_sentence(keyword_dict["keyword"], plaitext_without_stopwords):
                matching_words.append({"keyword": keyword_dict["keyword"], "category": keyword_dict["category"]})

        if matching_words:
            logging.debug(f"theme found : {theme} with word {matching_words}")

            # look for cts_in_ms inside matching_words (['keyword':'economie circulaire', 'category':'air'}] from subtitle_duration 
            keywords_to_add = get_cts_in_ms_for_keywords(subtitle_duration, matching_words, theme)
            if(len(keywords_to_add) == 0):
                logging.warning(f"Check regex - Empty keywords but themes is there {theme} - matching_words {matching_words} - {subtitle_duration}")
            keywords_with_timestamp.extend(keywords_to_add)
    
    if len(keywords_with_timestamp) > 0:
        keywords_with_timestamp = filter_keyword_with_same_timestamp(keywords_with_timestamp)
        # count false positive near of 15" of positive keywords
        keywords_with_timestamp = tag_fifteen_second_window_number(keywords_with_timestamp, start)
        keywords_with_timestamp = transform_false_positive_keywords_to_positive(keywords_with_timestamp, start)
        filtered_keywords_with_timestamp = filter_indirect_words(keywords_with_timestamp)
    
        return [
            get_themes(keywords_with_timestamp),
            clean_metadata(keywords_with_timestamp),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"changement_climatique_constat"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"changement_climatique_causes"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"changement_climatique_consequences"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"attenuation_climatique_solutions"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"adaptation_climatique_solutions"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"ressources_naturelles_concepts_generaux"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"ressources_naturelles_causes"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"ressources_naturelles_solutions"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"biodiversite_concepts_generaux"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"biodiversite_causes"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"biodiversite_consequences"),
            count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,"biodiversite_solutions")
        ]

    else:
        return [None,None,None,None,None,None,None,None,None,None,None,None,None,None,None]

def get_themes(keywords_with_timestamp: List[dict]) -> List[str]:
    return list(set([kw['theme'] for kw in keywords_with_timestamp]))

def clean_metadata(keywords_with_timestamp): 
    keywords_with_timestamp_copy = copy.deepcopy(keywords_with_timestamp) # immutable
    if( len(keywords_with_timestamp_copy)) > 0:
        for item in keywords_with_timestamp_copy:
            item.pop('window_number', None)

        return keywords_with_timestamp_copy
    else:
        return keywords_with_timestamp_copy

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
            df[
                ['theme',
                 u'keywords_with_timestamp',
                 'number_of_keywords',
                 'number_of_changement_climatique_constat',
                 'number_of_changement_climatique_causes_directes',
                 'number_of_changement_climatique_consequences',
                 'number_of_attenuation_climatique_solutions_directes',
                 'number_of_adaptation_climatique_solutions_directes',
                 'number_of_ressources_naturelles_concepts_generaux',
                 'number_of_ressources_naturelles_causes',
                 'number_of_ressources_naturelles_solutions',
                 'number_of_biodiversite_concepts_generaux',
                 'number_of_biodiversite_causes_directes',
                 'number_of_biodiversite_consequences',
                 'number_of_biodiversite_solutions_directes'
                ]
            ] = df[['plaintext','srt', 'start']]\
                .swifter.apply(\
                    lambda row: get_themes_keywords_duration(*row),\
                        axis=1,
                        result_type='expand'
                )

            # remove all rows that does not have themes
            df = df.dropna(subset=['theme'], how='any') # any is for None values

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
    return list(filter(lambda kw: indirectes not in kw['theme'], keywords_with_timestamp))

def count_keywords_duration_overlap(keywords_with_timestamp: List[dict], start: datetime, theme: str = None) -> int:
    total_keywords = len(keywords_with_timestamp)
    if(total_keywords) == 0:
        return 0
    else:
        if theme is not None:
            logging.debug(f"filter theme {theme}")
            keywords_with_timestamp = list(filter(lambda kw: kw['theme'] == theme, keywords_with_timestamp))
        else: # does not count "resources" for main counter - as not ready yet
            logging.debug("filtering ressources_ theme")
            keywords_with_timestamp = list(filter(lambda kw: "ressources_" not in kw['theme'], keywords_with_timestamp))

        length_filtered_items = len(keywords_with_timestamp)

        if length_filtered_items > 0:
            return count_different_window_number(keywords_with_timestamp, start)
        else:
            return 0

def count_different_window_number(keywords_with_timestamp: List[dict], start: datetime) -> int:
    window_numbers = [item['window_number'] for item in keywords_with_timestamp if 'window_number' in item]
    final_count = len(set(window_numbers))
    logging.debug(f"Count with 15 second logic: {final_count} keywords")

    return final_count

def contains_direct_keywords(keywords_with_timestamp: List[dict]) -> bool:
    return any(indirectes not in kw['theme'] for kw in keywords_with_timestamp)

# we want to count false positive near of 15" of positive keywords
def transform_false_positive_keywords_to_positive(keywords_with_timestamp: List[dict], start) -> List[dict]:
    for keyword_info in keywords_with_timestamp:
        # get 15-second neighbouring keywords
        neighbour_keywords = list(
            filter(
                lambda kw:
                            1 == abs(keyword_info['window_number'] - kw['window_number']) or
                            0 == abs(keyword_info['window_number'] - kw['window_number'])
            , keywords_with_timestamp)
        )

        if( contains_direct_keywords(neighbour_keywords) ) :
            keyword_info['theme'] = remove_indirect(keyword_info['theme'])

    return keywords_with_timestamp

def tag_fifteen_second_window_number(keywords_with_timestamp: List[dict], start) -> List[dict]:
    window_size_seconds = get_keyword_time_separation_ms()
    total_seconds_in_window = get_chunk_duration_api()
    number_of_windows = int(total_seconds_in_window // window_size_seconds)

    for keyword_info in keywords_with_timestamp:
        window_number = int( (keyword_info['timestamp'] - start.timestamp() * 1000) // (window_size_seconds))
        logging.debug(f"Window number {window_number} out of {number_of_windows} - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000}")
        if window_number >= number_of_windows and window_number >= 0:
            if(window_number == number_of_windows): # give some slack to mediatree subtitle edge case
                logging.warning(f"Edge cases around 2 minutes - still counting for one - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000}")
                window_number = number_of_windows - 1
                keyword_info['window_number'] = window_number
            else:
                logging.error(f"Window number {window_number} is out of range - kwtimestamp {keyword_info['timestamp']} - start {start.timestamp() * 1000} - {keyword_info['keyword']} - {keyword_info['theme']}")
                raise Exception
        else:
            keyword_info['window_number'] = window_number

    return keywords_with_timestamp

def remove_indirect(theme: str) -> str:
    if indirectes in theme:
        return theme.replace(f'_{indirectes}', '')
    else:
        return theme
