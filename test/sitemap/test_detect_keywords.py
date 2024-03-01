import pytest

from utils import get_localhost, debug_df

from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS

import pandas as pd
localhost = get_localhost()
original_timestamp = 1706437079004
start = datetime.utcfromtimestamp(original_timestamp / 1000)


subtitles = [{
        "duration_ms": 34,
        "cts_in_ms": original_timestamp,
        "text": "gilets"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 6,
        "text": "solaires"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 38,
        "text": "jaunes"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 72,
        "text": "économie"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 76,
        "text": "circulaire"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 76,
        "text": "abusive"
    }
]
def test_default_get_themes_keywords_duration():
    plaintext_nothing = "cheese pizza"
    assert get_themes_keywords_duration(plaintext_nothing, subtitles, start) == [None,None, None]
   
def test_one_theme_get_themes_keywords_duration():
    plaintext_climat = "climatique test"
    assert get_themes_keywords_duration(plaintext_climat, subtitles, start) == [["changement_climatique_constat"],[], 0]
    
def test_multiple_get_themes_keywords_duration():
    plaintext_multiple_themes = "climatique test bovin migrations climatiques"
    assert get_themes_keywords_duration(plaintext_multiple_themes, subtitles, start) == [["changement_climatique_constat", "changement_climatique_consequences"],[], 0]

def test_nothing_get_themes_keywords_duration():
    # should not accept theme 'bus' for keyword "abusive"
    plaintext_regression_incomplete_word = "abusive"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word, subtitles, start) == [None,None, None]
    
def test_regression_included_get_themes_keywords_duration():
    # should not accept theme 'ngt' for keyword "vingt"
    plaintext_regression_incomplete_word_ngt = "vingt"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word_ngt, subtitles, start) == [None,None, None]
    

def test_three_get_themes_keywords_duration():
    assert get_themes_keywords_duration("record de température pizza adaptation au dérèglement climatique", subtitles, start) == [[
      "changement_climatique_constat"
     ,"changement_climatique_consequences"
     ,"adaptation_climatique_solutions_directes"
    ],[], 0]

def test_long_get_themes_keywords_duration():
    assert get_themes_keywords_duration("il rencontre aussi une crise majeure de la pénurie de l' offre laetitia jaoude des barrages sauvages", subtitles, start) == [
    ["adaptation_climatique_solutions_indirectes"],[], 0]

def test_get_cts_in_ms_for_keywords():
    str = [{
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 79004,
          "text": "gilets"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 80006,
          "text": "Solaires"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 79038,
          "text": "jaunes"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 79072,
          "text": "économie"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 79076,
          "text": "circulaire"
        }
    ]
    keywords = ['économie circulaire', 'panneaux solaires', 'solaires']
    theme = "changement_climatique_constat"
    expected = [
        {
            "keyword":'économie circulaire',
            "timestamp" : original_timestamp + 79072,
            "theme": theme
        },
        {
            "keyword":'solaires',
            "timestamp" : original_timestamp + 80006,
            "theme": theme
        },
    ]
    assert get_cts_in_ms_for_keywords(str, keywords, theme) == expected

def test_complex_hyphen_get_cts_in_ms_for_keywords():
    str = [
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 80006,
          "text": "vagues-submersion"
        }
    ]
    keywords = ['submersion']
    theme = "changement_climatique_consequences"
    expected = [
        {
            "keyword":'submersion',
            "timestamp" : original_timestamp + 80006,
            "theme": theme
        }
    ]
    assert get_cts_in_ms_for_keywords(str, keywords, theme) == expected



def test_filter_and_tag_by_theme():
    srt = [{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 79004,
            "text": "adaptation"
            }
    ]
    df1 = pd.DataFrame([{
        "start": start,
        "plaintext": "cheese pizza",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        },{
            "start": start,
            "plaintext": "tomato screen",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
        },{
            "start": start,
            "plaintext": "méthane bovin anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
        },
        {
            "start": start,
            "plaintext": "cheese pizza",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
        },{
            "start": start,
            "plaintext": "pizza année la plus chaude",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
    }])

    expected_result = pd.DataFrame([{
        "start": start,
        "plaintext": "méthane bovin anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_directes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [],
        "number_of_keywords": 0.0
    },
    {
        "start": start,
        "plaintext": "pizza année la plus chaude",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": ["changement_climatique_consequences"],
        "keywords_with_timestamp": [],
        "number_of_keywords": 0.0
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))


def test_lower_case_filter_and_tag_by_theme():
    srt = [{
                "duration_ms": 34,
                "cts_in_ms": original_timestamp,
                "text": "méthane"
                }
    ]
    df1 = pd.DataFrame([{
            "start": start,
            "plaintext": "méthane BOVIN Anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
    }])

    expected_result = pd.DataFrame([{
        "start": start,
        "plaintext":  "méthane BOVIN Anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_directes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"méthane",
                "timestamp": original_timestamp,
                "theme": "changement_climatique_causes_directes",
        }]
        ,"number_of_keywords": 1
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_singular_plural_case_filter_and_tag_by_theme():
    srt = [{
                "duration_ms": 34,
                "cts_in_ms": original_timestamp,
                "text": "méthane"
                }
    ]
    df1 = pd.DataFrame([{
            "start": start,
            "plaintext": "méthane BOVIN Anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": srt,
    }])

    expected_result = pd.DataFrame([{
        "start": start,
        "plaintext":  "méthane BOVIN Anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_directes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"méthane",
                "timestamp": original_timestamp,
                "theme": "changement_climatique_causes_directes",
        }]
        ,"number_of_keywords": 1
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_complexe_filter_and_tag_by_theme():
    original_timestamp_first_keyword = original_timestamp + 6
    srt = [{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp,
            "text": "cheese"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 5,
            "text": "pizza"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp_first_keyword,
            "text": "habitabilité"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 7,
            "text": "de"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 8,
            "text": "la"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 9,
            "text": "planète"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + get_keyword_time_separation_ms(),
            "text": "conditions"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 11,
            "text": "de"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 11,
            "text": "vie"
            },{
            "duration_ms": 34,
            "cts_in_ms":original_timestamp + 11,
            "text": "sur"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp_first_keyword + get_keyword_time_separation_ms(),
            "text": "terre"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 12,
            "text": "animal"
            },
    ]
   
    df1 = pd.DataFrame([{
        "start": start,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
    }])

    expected_result = pd.DataFrame([{
        "start": start,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": [
            "changement_climatique_constat",
        ],
        "keywords_with_timestamp": [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp_first_keyword, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(), # timestamp too close
                "theme":"changement_climatique_constat",
            }
        ]
        ,"number_of_keywords": 2
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))


def test_is_word_in_sentence():
    assert is_word_in_sentence("bus", "abusive") == False
    assert is_word_in_sentence("bus", "le bus est à l'heure") == True
    assert is_word_in_sentence("bus électrique", "le bus est à l'heure") == False
    assert is_word_in_sentence("bus électrique", "le bus électrique est à l'heure") == True
    assert is_word_in_sentence("bus électrique", "bus électrique est à l'heure") == True
    assert is_word_in_sentence("bus électrique", "le village se déplace en bus électrique") == True

    assert is_word_in_sentence("bus électriques", "les bus électriques sont à l'heure") == True
    
    assert is_word_in_sentence("Voitures électriques", "le village se déplace en voitures électriques") == True
    assert is_word_in_sentence("Voitures électriques", "le village se déplace en voiture électrique") == True
    assert is_word_in_sentence("$-BreakingReg!-\\fezz$'", "le bus électrique est à l'heure") == False

    assert is_word_in_sentence("terre", "la région de terre-neuve se déplace") == False
    assert is_word_in_sentence("submersion", 'vagues-submersion') == True

def test_format_word_regex():
    assert format_word_regex("voitures") == "voitures?"
    assert format_word_regex("voiture") == "voitures?"
    assert format_word_regex("coraux") == "coraux?"
    assert format_word_regex("d'eau") == "d' ?eaus?"
    assert format_word_regex("réseaux") == "réseaux?"

def test_overlap_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 1,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 3,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 1
  
def test_no_overlap_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 1 * get_keyword_time_separation_ms(),
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 2 * get_keyword_time_separation_ms(),
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 3 * get_keyword_time_separation_ms(),
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 4

def test_with_a_mix_of_overlap_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(), # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 2000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 10000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 2,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 3

def test_with_15second_window_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(), # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 2000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 10000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 2 + 10000,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 3,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 4

def test_only_one_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 1

def test_indirect_count_keywords_duration_overlap_without_indirect():
    keywords_with_timestamp = [{
                "keyword" : 'digue',
                "timestamp": original_timestamp,
                "theme":"adaptation_climatique_solutions_indirectes",
            }
    ]
    
    assert count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start) == 0

def test_filter_indirect_words():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 1,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 3,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'malbouffe', # should be removed
                "timestamp": original_timestamp + 4,
                "theme":"changement_climatique_causes_indirectes",
            }
    ]

    expected = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 1,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 3,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    output = filter_indirect_words(keywords_with_timestamp)
    assert output == expected

def test_keyword_inside_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [{
                "keyword" : 'pénurie',
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_consequences",
            },
            {
                "keyword" : 'pénurie de neige',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"changement_climatique_consequences",
            }
    ]

    expected = [{
                "keyword" : 'pénurie de neige',
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_consequences",
            }
    ]
    
    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == expected

def test_keyword_different_theme_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [
        {'keyword': 'climatique', 'timestamp': 1693757470012, 'theme': 'changement_climatique_constat'},
        {'keyword': 'sécheresse', 'timestamp': 1693757450073, 'theme': 'changement_climatique_consequences'},
        {'keyword': 'sécheresse', 'timestamp': 1693757450073, 'theme': 'ressources_naturelles_concepts_generaux'}
    ]
        
    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == keywords_with_timestamp

def test_keyword_2words_inside_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [{
                "keyword" : 'agriculture',
                "timestamp": original_timestamp,
                "theme":"changement_climatique_causes_indirectes",
            },
            {
                "keyword" : 'agriculture industrielle',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"biodiversité_causes", # different theme, keep this one
            }
    ]

    expected = [{
                "keyword" : 'agriculture industrielle',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"biodiversité_causes", # different theme, keep this one
            }
    ]

    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == expected

# we should keep the longest keyword, even it's come before the first one
def test_keyword_second_word_a_bit_later_inside_keyword_filter_keyword_with_same_timestamp():
    later_timestamp = original_timestamp + 960 # from real data
    keywords_with_timestamp = [{
                "keyword" : 'carbone',
                "timestamp": later_timestamp,
                "theme":"changement_climatique_causes_directes",
            },
            {
                "keyword" : 'béton bas carbone',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"atténuation_climatique_solutions_directes", # different theme, keep this one
            }
    ]

    expected = [{
                "keyword" : 'béton bas carbone',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"atténuation_climatique_solutions_directes", # different theme, keep this one
            }
    ]

    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == expected

# we should keep the longest keyword, even it's come before the first one
def test_keyword_second_word_to_keep_inside_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [{
                    "theme": "changement_climatique_consequences",
                    "timestamp": 1707627703040,
                    "keyword": "pénurie"
            },
            {
                "theme":"atténuation_climatique_solutions_directes", # different theme, keep this one
                "timestamp": 1707627708051,
                "keyword": "barrages"
            },
    ]

    expected = [
        {
                "keyword": "pénurie",
                "timestamp": 1707627703040,
                "theme": "changement_climatique_consequences",
        },
        {
            "keyword" : 'barrages',
            "timestamp": 1707627708051, # same timestamp, so we take longest keyword
            "theme":"atténuation_climatique_solutions_directes", # different theme, keep this one
        }
    ]

    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == expected

def test_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [{ #nothing to filter
                "keyword" : "période la plus chaude",
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_consequences",
            },
            {
                "keyword" : "élévation du niveau de la mer",
                "timestamp": original_timestamp + 1200, # margin superior to 1000ms
                "theme":"changement_climatique_consequences",
            }
    ]
    
    assert filter_keyword_with_same_timestamp(keywords_with_timestamp) == keywords_with_timestamp

def test_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(), # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 2000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 10000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 2 + 10000,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 3,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [1, 1, 1, 1, 0, 0, 0, 0]

def test_full_house_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(), # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 2000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() + 10000,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 2 + 10000,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 3,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 4,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 5,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 6,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 7,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 8 - 100,  # count for one
                "theme":"ressources_naturelles_concepts_generaux",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [1, 1, 1, 1, 1, 1, 1, 1]


def test_simple_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() / 2,
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [1, 0, 0, 0, 0, 0, 0, 0]

def test_edge_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 8 + 10, # edge case - still counting for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [0, 0, 0, 0, 0, 0, 0, 1]

def test_really_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 15 + 10, # edge case - still counting for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [0, 0, 0, 0, 0, 0, 0, 0]

def test_almost_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms() * 8 - 10,
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert get_keyword_by_fifteen_second_window(keywords_with_timestamp, start) == [0, 0, 0, 0, 0, 0, 0, 1]