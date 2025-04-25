import logging

from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from postgres.schemas.models import keywords_table
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from typing import List, Optional
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
from quotaclimat.data_processing.mediatree.i8n.country import *
import re
import swifter
from itertools import groupby
import sentry_sdk
import modin.pandas as pd
import dask
import copy
from quotaclimat.utils.logger import getLogger
from collections import defaultdict
logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
dask.config.set({'dataframe.query-planning': True})

indirectes = 'indirectes'
MEDIATREE_TRANSCRIPTION_PROBLEM = "<unk> "
DEFAULT_WINDOW_DURATION = int(os.environ.get("DEFAULT_WINDOW_DURATION", 20))


def get_keyword_with_timestamp(theme: str, category: str, keyword : str, cts_in_ms: int):
    return {
            "keyword" : keyword,
            "timestamp" : cts_in_ms,
            "theme" : theme,
            "category": category
    }

def find_matching_subtitle(subtitles, keyword):
    for item in subtitles:
        logging.debug(f"Testing {item} with {keyword} full subtitle is {subtitles}")
        if is_word_in_sentence(keyword, item.get("text", "")):
            logging.debug(f"match found {item} with {keyword}")
            return item

    logging.warning(f"SRT match not found - default timestamp is now 0, possible error inside srt which is acceptable {e} - {keyword} - {subtitles}")
    return None

def get_cts_in_ms_for_keywords(subtitle_duration: List[dict], keywords: List[dict], theme: str) -> List[dict]:
    result = []

    logging.debug(f"Looking for timecode for {keywords} inside subtitle_duration {subtitle_duration}")
    for multiple_keyword in keywords:
        category = multiple_keyword["category"]
        all_keywords = multiple_keyword["keyword"].split() # case with multiple words such as 'economie circulaire'

        match = match = find_matching_subtitle(subtitle_duration, all_keywords[0])

        if match is not None:
            cts_in_ms = match['cts_in_ms']
        else:
            cts_in_ms = 0

        result.append(get_keyword_with_timestamp(theme=theme,
                                                 category=category, 
                                                 keyword=multiple_keyword["keyword"].lower(),
                                                 cts_in_ms=cts_in_ms)
        )
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

def filter_already_contained_keyword(keywords_with_timestamp: List[dict]) -> List[dict]:
    number_of_keywords = len(keywords_with_timestamp)

    if number_of_keywords > 1:
        keywords_to_remove = [] # get keyword that are in another keyword
        for item in keywords_with_timestamp:
            logging.debug(f"filtered_list testing item {item} inside keywords_with_timestamp:\n{keywords_with_timestamp}")
            keywords_with_timestamp_without_current_item = list(filter(lambda x: x != item, keywords_with_timestamp))
            for x in keywords_with_timestamp_without_current_item:
                keyword_match = (x.get('keyword') in item.get('keyword')) and x.get('keyword') != item.get('keyword')
                timestamp_match = abs(x.get('timestamp') - item.get('timestamp')) < 1000
                if keyword_match and timestamp_match and x not in keywords_to_remove:
                    logging.debug(f"Element to filter : {x.get('keyword')} because inside {item.get('keyword')}")
                    keywords_to_remove.append(x)

        logging.debug(f"keywords_to_remove: {keywords_to_remove}")
        # we want to remove all keywords of keywords_to_remove from keywords_with_timestamp
        if(len(keywords_to_remove) > 0):
            for i in keywords_to_remove:
                keywords_with_timestamp.remove(i)

    return keywords_with_timestamp

# some keywords are contained inside other keywords, we need to filter them
# some keyword are tagged with the same timestamp and different theme
def filter_keyword_with_same_timestamp(keywords_with_timestamp: List[dict])-> List[dict]:
    logging.debug(f"Filtering keywords with same timestamp with a margin of one second")
    number_of_keywords = len(keywords_with_timestamp)

    keywords_with_timestamp = filter_already_contained_keyword(keywords_with_timestamp)

    final_result = len(keywords_with_timestamp)

    if final_result < number_of_keywords:
        logging.debug(f"Filtering keywords {final_result} out of {number_of_keywords} | {keywords_with_timestamp} with final result")

    return keywords_with_timestamp

def replace_word_with_context(text: str, word = "groupe verlaine", length_to_remove= 50) -> str:
    replacement = ""
    pattern = f".{{0,{length_to_remove}}}{re.escape(word)}.{{0,{length_to_remove}}}"
    
    # Replace the matched word along with its surrounding context
    result = re.sub(pattern, replacement, text)
    
    return result

def remove_stopwords(plaintext: str, stopwords: list[str], country = FRANCE) -> str:
    logging.debug(f"Removing stopwords {plaintext}")

    if len(stopwords) == 0 and country == FRANCE:
        logging.warning(f"Stop words list empty for {country.name}")
        return plaintext

    # TODO: should be remove and only rely on stop words list, was added due to S2T issues
    if country == FRANCE:
        if "groupe verlaine" in plaintext:
            logging.info(f"special groupe verlaine case")
            plaintext = replace_word_with_context(plaintext, word="groupe verlaine")

        if "industries point com" in plaintext:
            logging.info(f"special industries point com case")
            plaintext = replace_word_with_context(plaintext, word="industries point com", length_to_remove=150)

        if "fleuron industrie" in plaintext:
            logging.info(f"special fleuron industrie com case")
            plaintext = replace_word_with_context(plaintext, word="fleuron industrie", length_to_remove=150)

    for word in stopwords:
        plaintext = plaintext.replace(word, '')
    
    return plaintext

def get_keyword_matching_json(keyword_dict: List[dict], country=FRANCE) -> dict:
    return {
            "keyword": keyword_dict["keyword"],
            "category": keyword_dict["category"]
    }

def get_detected_keywords(plaitext_without_stopwords: str, keywords_dict, country=FRANCE):
    matching_words = []

    logging.debug(f"Keeping only {country.language} keywords...")
    keywords_dict = list(filter(lambda x: x["language"] == country.language, keywords_dict))
    logging.debug(f"Got {len(keywords_dict)} keywords")
    for keyword_dict in keywords_dict:
        if is_word_in_sentence(keyword_dict["keyword"], plaitext_without_stopwords):
            matching_words.append(get_keyword_matching_json(keyword_dict, country=country))

    return matching_words

@sentry_sdk.trace
def get_themes_keywords_duration(plaintext: str, subtitle_duration: List[str], start: datetime,
                                stop_words: List[str] = [], country=FRANCE):
    keywords_with_timestamp = []
    number_of_elements_in_array = 29
    default_window_in_seconds = DEFAULT_WINDOW_DURATION
    plaintext = replace_word_with_context(text=plaintext, word=MEDIATREE_TRANSCRIPTION_PROBLEM, length_to_remove=0)
    plaitext_without_stopwords = remove_stopwords(plaintext=plaintext, stopwords=stop_words, country=country)
    logging.debug(f"display datetime start {start}")

    logging.debug(f"Keeping only {country.language} keywords...")
    
    for theme, keywords_dict in THEME_KEYWORDS.items():
        logging.debug(f"searching {theme} for {keywords_dict}")
        matching_words = get_detected_keywords(plaitext_without_stopwords, keywords_dict, country=country)
       
        if matching_words:
            logging.debug(f"theme found : {theme} with word {matching_words}")

            # look for cts_in_ms inside matching_words (['keyword':'economie circulaire', 'category':'air'}] from subtitle_duration 
            keywords_to_add = get_cts_in_ms_for_keywords(subtitle_duration, matching_words, theme)
            if(len(keywords_to_add) == 0):
                logging.debug(f"Check regex - Empty keywords but themes is there {theme} - matching_words {matching_words} - {subtitle_duration}")
            keywords_with_timestamp.extend(keywords_to_add)
    
    if len(keywords_with_timestamp) > 0:
        # count false positive near of default_window_in_seconds of positive keywords
        keywords_with_timestamp_default = get_keywords_with_timestamp_with_false_positive(keywords_with_timestamp, start, duration_seconds=default_window_in_seconds)
        filtered_keywords_with_timestamp = filter_indirect_words(keywords_with_timestamp_default)

        theme= get_themes(keywords_with_timestamp_default)
        keywords_with_timestamp= clean_metadata(keywords_with_timestamp_default)
        number_of_keywords= count_keywords_duration_overlap(filtered_keywords_with_timestamp, start)
        
        themes_climat = ["changement_climatique_constat",
                        "changement_climatique_causes",
                        "changement_climatique_consequences",
                        "attenuation_climatique_solutions",
                        "adaptation_climatique_solutions"
        ]
        number_of_keywords_climat= count_keywords_duration_overlap(filtered_keywords_with_timestamp, start, theme=themes_climat)
        themes_biodiversite = [
            "biodiversite_concepts_generaux",
            "biodiversite_causes",
            "biodiversite_consequences",
            "biodiversite_solutions",
        ]
        number_of_keywords_biodiversite= count_keywords_duration_overlap(filtered_keywords_with_timestamp, start, themes_biodiversite)

        themes_ressources = ["ressources",
                "ressources_solutions",
        ]
        number_of_keywords_ressources= count_keywords_duration_overlap(filtered_keywords_with_timestamp, start, themes_ressources)

        number_of_changement_climatique_constat = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_constat"])
        number_of_changement_climatique_causes = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_causes"])
        number_of_changement_climatique_consequences = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_consequences"])
        number_of_attenuation_climatique_solutions = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["attenuation_climatique_solutions"])
        number_of_adaptation_climatique_solutions = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["adaptation_climatique_solutions"])
        number_of_ressources = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["ressources"])
        number_of_ressources_solutions = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["ressources_solutions"])
        number_of_biodiversite_concepts_generaux = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_concepts_generaux"])
        number_of_biodiversite_causes = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_causes"])
        number_of_biodiversite_consequences = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_consequences"])
        number_of_biodiversite_solutions = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_solutions"])
        
        # No high risk of false positive counters
        number_of_changement_climatique_constat_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_constat"], \
            count_high_risk_false_positive=False)
        number_of_changement_climatique_causes_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_causes"], \
            count_high_risk_false_positive=False)
        number_of_changement_climatique_consequences_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["changement_climatique_consequences"], \
            count_high_risk_false_positive=False)
        number_of_attenuation_climatique_solutions_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["attenuation_climatique_solutions"], \
            count_high_risk_false_positive=False)
        number_of_adaptation_climatique_solutions_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["adaptation_climatique_solutions"], \
            count_high_risk_false_positive=False)
        number_of_ressources_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["ressources"], \
            count_high_risk_false_positive=False)
        number_of_ressources_solutions_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["ressources_solutions"], \
            count_high_risk_false_positive=False)
        number_of_biodiversite_concepts_generaux_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_concepts_generaux"], \
            count_high_risk_false_positive=False)
        number_of_biodiversite_causes_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_causes"], \
            count_high_risk_false_positive=False)
        number_of_biodiversite_consequences_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_consequences"], \
            count_high_risk_false_positive=False)
        number_of_biodiversite_solutions_no_hrfp = count_keywords_duration_overlap(filtered_keywords_with_timestamp, start,theme=["biodiversite_solutions"], \
            count_high_risk_false_positive=False)

        return [ # Change number_of_elements_in_array if a new element is added here
            theme
            ,keywords_with_timestamp 
            ,number_of_keywords
            ,number_of_changement_climatique_constat
            ,number_of_changement_climatique_causes
            ,number_of_changement_climatique_consequences
            ,number_of_attenuation_climatique_solutions
            ,number_of_adaptation_climatique_solutions
            ,number_of_ressources
            ,number_of_ressources_solutions
            ,number_of_biodiversite_concepts_generaux
            ,number_of_biodiversite_causes
            ,number_of_biodiversite_consequences
            ,number_of_biodiversite_solutions
            ,number_of_keywords_climat
            ,number_of_keywords_biodiversite
            ,number_of_keywords_ressources
            ,number_of_changement_climatique_constat_no_hrfp
            ,number_of_changement_climatique_causes_no_hrfp
            ,number_of_changement_climatique_consequences_no_hrfp
            ,number_of_attenuation_climatique_solutions_no_hrfp
            ,number_of_adaptation_climatique_solutions_no_hrfp
            ,number_of_ressources_no_hrfp
            ,number_of_ressources_solutions_no_hrfp
            ,number_of_biodiversite_concepts_generaux_no_hrfp
            ,number_of_biodiversite_causes_no_hrfp
            ,number_of_biodiversite_consequences_no_hrfp
            ,number_of_biodiversite_solutions_no_hrfp
            ,country.name
        ]
    else:
        logging.debug("Empty keywords")
        return [None] * number_of_elements_in_array

def get_keywords_with_timestamp_with_false_positive(keywords_with_timestamp, start, duration_seconds: int = 20):
    logging.debug(f"Using duration_seconds {duration_seconds}")

    # Shallow copy to avoid unnecessary deep copying (wip: for memory leak)
    keywords_with_timestamp_copy = [item.copy() for item in keywords_with_timestamp]

    keywords_with_timestamp_copy = tag_wanted_duration_second_window_number(keywords_with_timestamp_copy, start, duration_seconds=duration_seconds)
    keywords_with_timestamp_copy = transform_false_positive_keywords_to_positive(keywords_with_timestamp_copy, start)
    keywords_with_timestamp_copy = filter_keyword_with_same_timestamp(keywords_with_timestamp_copy)

    return keywords_with_timestamp_copy

def get_themes(keywords_with_timestamp: List[dict]) -> List[str]:
    return list(set([kw['theme'] for kw in keywords_with_timestamp]))

def clean_metadata(keywords_with_timestamp):
    if not keywords_with_timestamp:
        return keywords_with_timestamp

    # Shallow copy instead of deep copy (wip: for memory leak)
    keywords_with_timestamp_copy = [item.copy() for item in keywords_with_timestamp]

    for item in keywords_with_timestamp_copy:
        item.pop('window_number', None)

    return keywords_with_timestamp_copy

def log_min_max_date(df):
    max_date = max(df['start'])
    min_date = min(df['start'])
    logging.info(f"Date min : {min_date}, max : {max_date}")


def filter_and_tag_by_theme(df: pd.DataFrame, stop_words: list[str] = [], country=FRANCE) -> pd.DataFrame :
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
                 'number_of_ressources',
                 'number_of_ressources_solutions',
                 'number_of_biodiversite_concepts_generaux',
                 'number_of_biodiversite_causes_directes',
                 'number_of_biodiversite_consequences',
                 'number_of_biodiversite_solutions_directes'
                 ,"number_of_keywords_climat"
                 ,"number_of_keywords_biodiversite"
                 ,"number_of_keywords_ressources"
                 ,"number_of_changement_climatique_constat_no_hrfp"
                 ,"number_of_changement_climatique_causes_no_hrfp"
                 ,"number_of_changement_climatique_consequences_no_hrfp"
                 ,"number_of_attenuation_climatique_solutions_no_hrfp"
                 ,"number_of_adaptation_climatique_solutions_no_hrfp"
                 ,"number_of_ressources_no_hrfp"
                 ,"number_of_ressources_solutions_no_hrfp"
                 ,"number_of_biodiversite_concepts_generaux_no_hrfp"
                 ,"number_of_biodiversite_causes_no_hrfp"
                 ,"number_of_biodiversite_consequences_no_hrfp"
                 ,"number_of_biodiversite_solutions_no_hrfp"
                 ,'country'
                ]
            ] = df[['plaintext','srt', 'start']]\
                .swifter.apply(\
                    lambda row: get_themes_keywords_duration(*row, stop_words=stop_words, country=country),\
                        axis=1,
                        result_type='expand'
                )

            # remove all rows that does not have themes
            df = df.dropna(subset=['theme'], how='any') # any is for None values
            logging.info(f"After filtering with out keywords, we have {len(df)} out of {count_before_filtering} subtitles left that are insteresting for us")

            return df

def add_primary_key(row):
    try:
        if str(row['start'].tzinfo) == 'Europe/Paris':
            # legacy
            logging.info("PK must be UTC - Timezone is Europe/Paris, converting to UTC")
            row['start'] = row['start'].tz_convert('UTC')
        hash_id = get_consistent_hash(str(row["start"]) + row["channel_name"])
        return hash_id

    except (Exception) as error:
        logging.error(f"{error} with row: start: {row['start']} - channel_name:{row['channel_name']}")
        raise Exception

def filter_indirect_words(keywords_with_timestamp: List[dict]) -> List[dict]:
    return list(filter(lambda kw: indirectes not in kw['theme'], keywords_with_timestamp))

def filter_high_risk_false_positive(keywords_with_timestamp: List[dict]) -> List[dict]:
    return list(filter(lambda kw: 'hrfp' not in kw, keywords_with_timestamp))

def count_keywords_duration_overlap(keywords_with_timestamp: List[dict], start: datetime, theme: List[str] = None, count_high_risk_false_positive: bool = True) -> int:
    total_keywords = len(keywords_with_timestamp)
    if(total_keywords) == 0:
        return 0
    else:
        logging.debug(f"keywords_with_timestamp is {keywords_with_timestamp}")
        if theme is not None:
            logging.debug(f"filter theme {theme}")
            keywords_with_timestamp = list(filter(lambda kw: kw['theme'] in theme, keywords_with_timestamp))
        if count_high_risk_false_positive is False:
            keywords_with_timestamp = filter_high_risk_false_positive(keywords_with_timestamp)
        logging.debug(f"keywords_with_timestamp is after filtering {keywords_with_timestamp}")
        length_filtered_items = len(keywords_with_timestamp)

        if length_filtered_items > 0:
            return count_different_window_number(keywords_with_timestamp, start)
        else:
            return 0

def count_different_window_number(keywords_with_timestamp: List[dict], start: datetime) -> int:
    window_numbers = [item['window_number'] for item in keywords_with_timestamp if 'window_number' in item]
    final_count = len(set(window_numbers))
    logging.debug(f"Count with {DEFAULT_WINDOW_DURATION} second logic: {final_count} keywords")

    return final_count

def get_subject_from_theme(theme: str) -> str:
    if 'climatique' in theme:
        return 'climat'
    elif 'biodiversite' in theme:
        return 'biodiversite'
    elif 'ressources' in theme:
        return 'ressources'
    else:
        return 'unknown'

# only of the same subject (climate/biodiv/ressources) 
def contains_direct_keywords_same_suject(keywords_with_timestamp: List[dict], theme: str) -> bool:
    subject = get_subject_from_theme(theme)
    logging.debug(f"subject {subject}")
    # keep only keywords with timestamp from the same subject
    keywords_with_timestamp = list(filter(lambda kw: get_subject_from_theme(kw['theme']) == subject, keywords_with_timestamp))
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

        if( contains_direct_keywords_same_suject(neighbour_keywords, keyword_info['theme']) ) :
            logging.debug(f"Transforming false positive to positive { keyword_info['keyword']} { keyword_info['theme']}")
            if indirectes in keyword_info['theme']:
                keyword_info['theme'] = remove_indirect(keyword_info['theme'])
                keyword_info['hrfp'] = True # to store if a keyword was a transformed to a direct keyword

    return keywords_with_timestamp

def tag_wanted_duration_second_window_number(keywords_with_timestamp: List[dict], start, duration_seconds: int = 20) -> List[dict]:
    window_size_seconds = get_keyword_time_separation_ms(duration_seconds=duration_seconds)
    total_seconds_in_window = get_chunk_duration_api()
    number_of_windows = int(total_seconds_in_window // window_size_seconds)
    logging.debug(f"number_of_windows { number_of_windows} - window_size_seconds {window_size_seconds} using duration_seconds {duration_seconds}")
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
    return theme.replace(f'_{indirectes}', '')