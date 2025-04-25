import pytest

from test_utils import get_localhost, debug_df, compare_unordered_lists_of_dicts

from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from datetime import datetime, timezone
from quotaclimat.data_processing.mediatree.keyword.stop_words import STOP_WORDS


import pandas as pd
localhost = get_localhost()
original_timestamp = 1706437079004
start = datetime.fromtimestamp(original_timestamp / 1000, timezone.utc)

array_of_none = [None] * 29

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
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 98,
        "text": "barrage"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1000,
        "text": "record"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1100,
        "text": "de"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1200,
        "text": "température"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1000,
        "text": "adaptation"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1212,
        "text": "réchauffement"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1300,
        "text": "planétaire"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1500,
        "text": "recyclage"
    },
    {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 1800,
        "text": "climatique"
    }
]
keywords =  [{
            'category': 'General',
            'keyword': 'réchauffement planétaire',
            'theme': 'changement_climatique_constat',
            'timestamp': 1706437080216,
            }
        ]
def test_default_get_themes_keywords_duration():
    plaintext_nothing = "cheese pizza"
    assert get_themes_keywords_duration(plaintext_nothing, subtitles, start) == array_of_none
   
def test_one_theme_get_themes_keywords_duration():
    plaintext_climat = "réchauffement planétaire test"
    
    themes = ['changement_climatique_constat', 'ressources_indirectes']

    (themes_output, keywords_output, 
        number_of_keywords,
        number_of_changement_climatique_constat,
        number_of_changement_climatique_causes_directes,
        number_of_changement_climatique_consequences,
        number_of_attenuation_climatique_solutions_directes,
        number_of_adaptation_climatique_solutions_directes,
        number_of_ressources,
        number_of_ressources_solutions,
        number_of_biodiversite_concepts_generaux,
        number_of_biodiversite_causes_directes,
        number_of_biodiversite_consequences,
        number_of_biodiversite_solutions_directes
        , number_of_keywords_climat,
        number_of_keywords_biodiversite,
        number_of_keywords_ressources
        , number_of_changement_climatique_constat_no_hrfp,
        number_of_changement_climatique_causes_no_hrfp,
        number_of_changement_climatique_consequences_no_hrfp,
        number_of_attenuation_climatique_solutions_no_hrfp,
        number_of_adaptation_climatique_solutions_no_hrfp,
        number_of_ressources_no_hrfp,
        number_of_ressources_solutions_no_hrfp,
        number_of_biodiversite_concepts_generaux_no_hrfp,
        number_of_biodiversite_causes_no_hrfp,
        number_of_biodiversite_consequences_no_hrfp,
        number_of_biodiversite_solutions_no_hrfp, country) = get_themes_keywords_duration(plaintext_climat, subtitles, start)
    assert set(themes_output) == set(themes)
    # assert compare_unordered_lists_of_dicts(keywords_output, keywords)

    assert number_of_keywords == 1
    assert number_of_keywords_climat == 1
    assert number_of_keywords_biodiversite == 0
    assert number_of_keywords_ressources == 0
    assert number_of_changement_climatique_constat == 1
    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 0
    assert number_of_adaptation_climatique_solutions_directes == 0
    assert number_of_ressources == 0
    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0

    assert number_of_changement_climatique_constat_no_hrfp == 1
    assert number_of_changement_climatique_causes_no_hrfp == 0
    assert number_of_changement_climatique_consequences_no_hrfp == 0
    assert number_of_attenuation_climatique_solutions_no_hrfp == 0
    assert number_of_adaptation_climatique_solutions_no_hrfp == 0
    assert number_of_ressources_no_hrfp == 0
    assert number_of_ressources_solutions_no_hrfp == 0
    assert number_of_biodiversite_concepts_generaux_no_hrfp == 0
    assert number_of_biodiversite_causes_no_hrfp == 0
    assert number_of_biodiversite_consequences_no_hrfp == 0
    assert number_of_biodiversite_solutions_no_hrfp == 0

def test_two_themes_one_hrfp_get_themes_keywords_duration():
    plaintext_climat = "pizza recyclage climatique pizza"
    original_timestamp = 1706437080216
    keywords_with_timestamp = [
        {
         'keyword': 'recyclage',
         'timestamp': original_timestamp,
         'theme': 'attenuation_climatique_solutions_indirectes'  # should be transformed to direct
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        }
    ]
    themes = ['changement_climatique_constat', 'attenuation_climatique_solutions', 'ressources_solutions']

    (themes_output, keywords_output, 
        number_of_keywords,
        number_of_changement_climatique_constat,
        number_of_changement_climatique_causes_directes,
        number_of_changement_climatique_consequences,
        number_of_attenuation_climatique_solutions_directes,
        number_of_adaptation_climatique_solutions_directes,
        number_of_ressources,
        number_of_ressources_solutions,
        number_of_biodiversite_concepts_generaux,
        number_of_biodiversite_causes_directes,
        number_of_biodiversite_consequences,
        number_of_biodiversite_solutions_directes
        , number_of_keywords_climat,
        number_of_keywords_biodiversite,
        number_of_keywords_ressources
        , number_of_changement_climatique_constat_no_hrfp,
        number_of_changement_climatique_causes_no_hrfp,
        number_of_changement_climatique_consequences_no_hrfp,
        number_of_attenuation_climatique_solutions_no_hrfp,
        number_of_adaptation_climatique_solutions_no_hrfp,
        number_of_ressources_no_hrfp,
        number_of_ressources_solutions_no_hrfp,
        number_of_biodiversite_concepts_generaux_no_hrfp,
        number_of_biodiversite_causes_no_hrfp,
        number_of_biodiversite_consequences_no_hrfp,
        number_of_biodiversite_solutions_no_hrfp, country) = get_themes_keywords_duration(plaintext_climat, subtitles, start)
    
    logging.info(f"Test got keywords_output: {keywords_output}")
    assert set(themes_output) == set(themes)
    # assert compare_unordered_lists_of_dicts(keywords_output, keywords)

    assert number_of_keywords == 1
    assert number_of_keywords_climat == 1
    assert number_of_keywords_biodiversite == 0
    assert number_of_keywords_ressources == 1
    assert number_of_changement_climatique_constat == 1
    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 1
    assert number_of_adaptation_climatique_solutions_directes == 0
    assert number_of_ressources == 0
    assert number_of_ressources_solutions == 1
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0
    assert number_of_changement_climatique_constat_no_hrfp == 1
    assert number_of_changement_climatique_causes_no_hrfp == 0
    assert number_of_changement_climatique_consequences_no_hrfp == 0
    assert number_of_attenuation_climatique_solutions_no_hrfp == 1
    assert number_of_adaptation_climatique_solutions_no_hrfp == 0
    assert number_of_ressources_no_hrfp == 0
    assert number_of_ressources_solutions_no_hrfp == 1
    assert number_of_biodiversite_concepts_generaux_no_hrfp == 0
    assert number_of_biodiversite_causes_no_hrfp == 0
    assert number_of_biodiversite_consequences_no_hrfp == 0
    assert number_of_biodiversite_solutions_no_hrfp == 0


def test_long_sentence_theme_get_themes_keywords_duration():
    conditions_ts = original_timestamp + 15000
    habitabilite_ts = original_timestamp + 6
    subtitles = [{
            "duration_ms": 34,
            "cts_in_ms": habitabilite_ts,
            "text": "habitabilité"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": habitabilite_ts + 10,
            "text": "de"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": habitabilite_ts + 11,
            "text": "la"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": habitabilite_ts + 12,
            "text": "planète"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": conditions_ts,
            "text": "conditions"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": conditions_ts + 50,
            "text": "de"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": conditions_ts + 100,
            "text": "vie"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": conditions_ts + 150,
            "text": "sur"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": conditions_ts + 200,
            "text": "terre"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 32000,
            "text": "digue"
            }
    ]
    plaintext_climat = "cheese pizza habitabilité de la planète conditions de vie sur terre animal digue"
    keywords = [
        {
        'category': 'Ecosystème',
        'keyword': 'conditions de vie sur terre',
        'theme': 'changement_climatique_constat',
        'timestamp': conditions_ts,
        },
        {
        'category': 'Ecosystème',
        'keyword': 'habitabilité de la planète',
        'theme': 'changement_climatique_constat',
        'timestamp': habitabilite_ts,
        },
        {
        'category': 'General',
        'keyword': 'digue',
        'theme': 'adaptation_climatique_solutions',
        'timestamp': original_timestamp + 32000,
        },
        ]
    themes = ['adaptation_climatique_solutions','changement_climatique_constat']

    (themes_output, keywords_output, 
        number_of_keywords,
        number_of_changement_climatique_constat,
        number_of_changement_climatique_causes_directes,
        number_of_changement_climatique_consequences,
        number_of_attenuation_climatique_solutions_directes,
        number_of_adaptation_climatique_solutions_directes,
        number_of_ressources,
        number_of_ressources_solutions,
        number_of_biodiversite_concepts_generaux,
        number_of_biodiversite_causes_directes,
        number_of_biodiversite_consequences,
        number_of_biodiversite_solutions_directes
        , number_of_keywords_climat,
        number_of_keywords_biodiversite,
        number_of_keywords_ressources
        , number_of_changement_climatique_constat_no_hrfp,
        number_of_changement_climatique_causes_no_hrfp,
        number_of_changement_climatique_consequences_no_hrfp,
        number_of_attenuation_climatique_solutions_no_hrfp,
        number_of_adaptation_climatique_solutions_no_hrfp,
        number_of_ressources_no_hrfp,
        number_of_ressources_solutions_no_hrfp,
        number_of_biodiversite_concepts_generaux_no_hrfp,
        number_of_biodiversite_causes_no_hrfp,
        number_of_biodiversite_consequences_no_hrfp,
        number_of_biodiversite_solutions_no_hrfp, country) = get_themes_keywords_duration(plaintext_climat, subtitles, start)

    assert set(themes_output) == set(themes)
    # assert compare_unordered_lists_of_dicts(keywords_output, keywords)
    assert number_of_keywords == 2
    assert number_of_keywords_climat == 2
    assert number_of_changement_climatique_constat == 1
    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 0
    assert number_of_adaptation_climatique_solutions_directes == 1
    assert number_of_ressources == 0
    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0
    assert number_of_changement_climatique_constat_no_hrfp == 1
    assert number_of_changement_climatique_causes_no_hrfp == 0
    assert number_of_changement_climatique_consequences_no_hrfp == 0
    assert number_of_attenuation_climatique_solutions_no_hrfp == 0
    assert number_of_adaptation_climatique_solutions_no_hrfp == 0
    assert number_of_ressources_no_hrfp == 0
    assert number_of_ressources_solutions_no_hrfp == 0
    assert number_of_biodiversite_concepts_generaux_no_hrfp == 0
    assert number_of_biodiversite_causes_no_hrfp == 0
    assert number_of_biodiversite_consequences_no_hrfp == 0
    assert number_of_biodiversite_solutions_no_hrfp == 0


def test_nothing_get_themes_keywords_duration():
    # should not accept theme 'bus' for keyword "abusive"
    plaintext_regression_incomplete_word = "abusive"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word, subtitles, start) == array_of_none
    
def test_regression_included_get_themes_keywords_duration():
    # should not accept theme 'ngt' for keyword "vingt"
    plaintext_regression_incomplete_word_ngt = "vingt"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word_ngt, subtitles, start) == array_of_none
    
def test_filter_high_risk_false_positive_without_hrfp():
    result = filter_high_risk_false_positive(keywords_with_timestamp=keywords)
    assert result == keywords

    
def test_filter_high_risk_false_positive_with_hrfp():
    keywords = keywords =  [{
            'category': 'General',
            'keyword': 'réchauffement planétaire',
            'theme': 'changement_climatique_constat',
            'timestamp': 1706437080216,
            'hrfp': True
            }
        ]

    result = filter_high_risk_false_positive(keywords_with_timestamp=keywords)
    assert result == []

def test_three_get_themes_keywords_duration():
    themes = set([
     "adaptation_climatique_solutions", 'changement_climatique_consequences'
    ])

    (themes_output, keywords_output, 
        number_of_keywords,
        number_of_changement_climatique_constat,
        number_of_changement_climatique_causes_directes,
        number_of_changement_climatique_consequences,
        number_of_attenuation_climatique_solutions_directes,
        number_of_adaptation_climatique_solutions_directes,
        number_of_ressources,
        number_of_ressources_solutions,
        number_of_biodiversite_concepts_generaux,
        number_of_biodiversite_causes_directes,
        number_of_biodiversite_consequences,
        number_of_biodiversite_solutions_directes
        ,number_of_keywords_climat,
        number_of_keywords_biodiversite,
        number_of_keywords_ressources
        ,number_of_changement_climatique_constat_no_hrfp,
        number_of_changement_climatique_causes_no_hrfp,
        number_of_changement_climatique_consequences_no_hrfp,
        number_of_attenuation_climatique_solutions_no_hrfp,
        number_of_adaptation_climatique_solutions_no_hrfp,
        number_of_ressources_no_hrfp,
        number_of_ressources_solutions_no_hrfp,
        number_of_biodiversite_concepts_generaux_no_hrfp,
        number_of_biodiversite_causes_no_hrfp,
        number_of_biodiversite_consequences_no_hrfp,
        number_of_biodiversite_solutions_no_hrfp, country) = get_themes_keywords_duration("record de température pizza adaptation au dérèglement climatique", subtitles, start)

    assert set(themes_output)== themes
    logging.info(f"Got keywords:  {keywords_output}")
    assert number_of_keywords == 1
    assert number_of_changement_climatique_constat == 0
    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 1
    assert number_of_attenuation_climatique_solutions_directes == 0
    assert number_of_adaptation_climatique_solutions_directes == 1
    assert number_of_ressources == 0
    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0
    assert number_of_changement_climatique_constat_no_hrfp == 0
    assert number_of_changement_climatique_causes_no_hrfp == 0
    assert number_of_changement_climatique_consequences_no_hrfp == 1
    assert number_of_attenuation_climatique_solutions_no_hrfp == 0
    assert number_of_adaptation_climatique_solutions_no_hrfp == 1
    assert number_of_ressources_no_hrfp == 0
    assert number_of_ressources_solutions_no_hrfp == 0
    assert number_of_biodiversite_concepts_generaux_no_hrfp == 0
    assert number_of_biodiversite_causes_no_hrfp == 0
    assert number_of_biodiversite_consequences_no_hrfp == 0
    assert number_of_biodiversite_solutions_no_hrfp == 0

def test_long_get_themes_keywords_duration():
    themes= set([
        'adaptation_climatique_solutions_indirectes',
        'biodiversite_causes_indirectes',
        'ressources_indirectes',
        'ressources_solutions_indirectes',
    ])
    keywords= [
        {
            'category': 'Eau',
            'keyword': 'barrage',
            'theme': 'adaptation_climatique_solutions_indirectes',
            'timestamp': 1706437079102,
        },
        {
            'category': 'Energie',
            'keyword': 'barrage',
            'theme': 'ressources_indirectes',
            'timestamp': 1706437079102,
        },
        {
            'category': 'Eau',
            'keyword': 'barrage',
            'theme': 'ressources_solutions_indirectes',
            'timestamp': 1706437079102,
    }]

    (themes_output, keywords_output, 
        number_of_keywords,
        number_of_changement_climatique_constat,
        number_of_changement_climatique_causes_directes,
        number_of_changement_climatique_consequences,
        number_of_attenuation_climatique_solutions_directes,
        number_of_adaptation_climatique_solutions_directes,
        number_of_ressources,
        number_of_ressources_solutions,
        number_of_biodiversite_concepts_generaux,
        number_of_biodiversite_causes_directes,
        number_of_biodiversite_consequences,
        number_of_biodiversite_solutions_directes
        ,number_of_keywords_climat,
        number_of_keywords_biodiversite,
        number_of_keywords_ressources
        ,number_of_changement_climatique_constat_no_hrfp,
        number_of_changement_climatique_causes_no_hrfp,
        number_of_changement_climatique_consequences_no_hrfp,
        number_of_attenuation_climatique_solutions_no_hrfp,
        number_of_adaptation_climatique_solutions_no_hrfp,
        number_of_ressources_no_hrfp,
        number_of_ressources_solutions_no_hrfp,
        number_of_biodiversite_concepts_generaux_no_hrfp,
        number_of_biodiversite_causes_no_hrfp,
        number_of_biodiversite_consequences_no_hrfp,
        number_of_biodiversite_solutions_no_hrfp, country) = get_themes_keywords_duration("il rencontre aussi une crise majeure de la pénurie de l' offre laetitia jaoude des barrages sauvages", subtitles, start)
    assert set(themes_output) == set(themes)
    # too hard to maintain
    #assert compare_unordered_lists_of_dicts(keywords_output, keywords)
    assert number_of_keywords == 0
    assert number_of_changement_climatique_constat == 0
    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 0
    assert number_of_adaptation_climatique_solutions_directes == 0
    assert number_of_ressources == 0
    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0
    assert number_of_changement_climatique_constat_no_hrfp == 0
    assert number_of_changement_climatique_causes_no_hrfp == 0
    assert number_of_changement_climatique_consequences_no_hrfp == 0
    assert number_of_attenuation_climatique_solutions_no_hrfp == 0
    assert number_of_adaptation_climatique_solutions_no_hrfp == 0
    assert number_of_ressources_no_hrfp == 0
    assert number_of_ressources_solutions_no_hrfp == 0
    assert number_of_biodiversite_concepts_generaux_no_hrfp == 0
    assert number_of_biodiversite_causes_no_hrfp == 0
    assert number_of_biodiversite_consequences_no_hrfp == 0
    assert number_of_biodiversite_solutions_no_hrfp == 0


def test_stop_word_get_themes_keywords_duration():
    plaintext = "haute isolation thermique fabriqué en france pizza"
    assert get_themes_keywords_duration(plaintext, subtitles, start, stop_words=STOP_WORDS) == array_of_none
   
def test_train_stop_word_get_themes_keywords_duration():
    plaintext = "en train de fabrique en france pizza"
    assert get_themes_keywords_duration(plaintext, subtitles, start, stop_words=STOP_WORDS) == array_of_none
   

def test_get_cts_in_ms_for_keywords():
    str = [{
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 79004,
          "text": "gilets"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": original_timestamp + 80036,
          "text": "panneaux"
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

    my_category = 'my_category'
    keywords = [
        {'keyword':'économie circulaire', 'category':my_category},
        {'keyword':'panneaux solaires', 'category':my_category},
    ]
    theme = "changement_climatique_constat"
    expected = [
        {
            "keyword":'économie circulaire',
            "timestamp" : original_timestamp + 79072,
            "theme": theme,
            "category":my_category
        },
        {
            "keyword":'panneaux solaires',
            "timestamp" : original_timestamp + 80036,
            "theme": theme,
            "category":my_category
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

    my_category = "test category"
    keywords = [{'keyword': 'submersion', 'category': my_category}]
    theme = "changement_climatique_consequences"
    expected = [
        {
            "keyword":'submersion',
            "timestamp" : original_timestamp + 80006,
            "theme": theme,
            "category": my_category
        }
    ]
    assert get_cts_in_ms_for_keywords(str, keywords, theme) == expected


def test_none_theme_filter_and_tag_by_theme():
    df1 = pd.DataFrame([{
        "start": start,
        "plaintext": "cheese pizza",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": []
        }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)

    assert len(df) == 0

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
            "changement_climatique_causes", "changement_climatique_constat", "ressource"
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"méthane",
                "timestamp": original_timestamp,
                "theme": "changement_climatique_causes",
                'category': 'General'
        }],
        "number_of_keywords": 2,
        "number_of_changement_climatique_constat": 1,
        "number_of_changement_climatique_causes_directes": 1,
        "number_of_changement_climatique_consequences": 0,
        "number_of_attenuation_climatique_solutions_directes": 0,
        "number_of_adaptation_climatique_solutions_directes": 0,
        "number_of_ressources": 1,
        "number_of_ressources_solutions": 0,
        "number_of_biodiversite_concepts_generaux": 0,
        "number_of_biodiversite_causes_directes": 0,
        "number_of_biodiversite_consequences": 0,
        "number_of_biodiversite_solutions_directes" :0
        ,'number_of_keywords_climat':2,
        'number_of_keywords_biodiversite':0,
        'number_of_keywords_ressources':1
        ,"number_of_changement_climatique_constat_no_hrfp": 1
        ,"number_of_changement_climatique_causes_no_hrfp": 1
        ,"number_of_changement_climatique_consequences_no_hrfp": 0
        ,"number_of_attenuation_climatique_solutions_no_hrfp": 0
        ,"number_of_adaptation_climatique_solutions_no_hrfp": 0
        ,"number_of_ressources_no_hrfp": 1
        ,"number_of_ressources_solutions_no_hrfp": 0
        ,"number_of_biodiversite_concepts_generaux_no_hrfp": 0
        ,"number_of_biodiversite_causes_no_hrfp": 0
        ,"number_of_biodiversite_consequences_no_hrfp": 0
        ,"number_of_biodiversite_solutions_no_hrfp":0
        ,"country": "france"
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    df.drop(columns=['theme'], inplace=True)
    expected_result.drop(columns=['theme'], inplace=True)
    expected_result.drop(columns=['keywords_with_timestamp'], inplace=True)
    df.drop(columns=['keywords_with_timestamp'], inplace=True)
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
            "changement_climatique_causes",
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"méthane",
                "timestamp": original_timestamp,
                "theme": "changement_climatique_causes",
                'category': 'General'
        }],
        "number_of_keywords": 1,
        "number_of_changement_climatique_constat": 0,
        "number_of_changement_climatique_causes_directes": 1,
        "number_of_changement_climatique_consequences": 0,
        "number_of_attenuation_climatique_solutions_directes": 0,
        "number_of_adaptation_climatique_solutions_directes": 0,
        "number_of_ressources": 0,
        "number_of_ressources_solutions": 0,
        "number_of_biodiversite_concepts_generaux": 0,
        "number_of_biodiversite_causes_directes": 0,
        "number_of_biodiversite_consequences": 0,
        "number_of_biodiversite_solutions_directes" :0
        ,'number_of_keywords_climat':1,
        'number_of_keywords_biodiversite':0,
        'number_of_keywords_ressources':0
        ,"number_of_changement_climatique_constat_no_hrfp": 0
        ,"number_of_changement_climatique_causes_no_hrfp": 1
        ,"number_of_changement_climatique_consequences_no_hrfp": 0
        ,"number_of_attenuation_climatique_solutions_no_hrfp": 0
        ,"number_of_adaptation_climatique_solutions_no_hrfp": 0
        ,"number_of_ressources_no_hrfp": 0
        ,"number_of_ressources_solutions_no_hrfp": 0
        ,"number_of_biodiversite_concepts_generaux_no_hrfp": 0
        ,"number_of_biodiversite_causes_no_hrfp": 0
        ,"number_of_biodiversite_consequences_no_hrfp": 0
        ,"number_of_biodiversite_solutions_no_hrfp":0
        ,"country": "france"
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
            "text": "dépolluer"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + get_keyword_time_separation_ms(15),
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
            "cts_in_ms": original_timestamp_first_keyword + get_keyword_time_separation_ms(15),
            "text": "terre"
            },{
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 12,
            "text": "animal"
            },
    ]
   
    plaintext= "cheese pizza dépolluer conditions de vie sur terre animal"
    df1 = pd.DataFrame([{
        "start": start,
        "plaintext": plaintext,
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
    }])
    
    expected_result = pd.DataFrame([{
        "start": start,
        "plaintext": plaintext,
        "channel_name": "m6",
        "channel_radio": False,
        "srt": srt,
        "theme": [
            "attenuation_climatique_solutions",
            "changement_climatique_constat"
        ],
        "keywords_with_timestamp": [
            {
                'category': 'Ecosystème',
                'keyword': 'conditions de vie sur terre',
                'theme': 'changement_climatique_constat',
                'timestamp': 1706437094004,
            },
            {
                'category': 'General',
                "keyword" : 'dépolluer',
                "timestamp": original_timestamp_first_keyword, # count for one
                "theme":"changement_climatique_constat",
            }
        ],
        "number_of_keywords": 1,
        "number_of_changement_climatique_constat": 1,
        "number_of_changement_climatique_causes_directes": 0,
        "number_of_changement_climatique_consequences": 0,
        "number_of_attenuation_climatique_solutions_directes": 1,
        "number_of_adaptation_climatique_solutions_directes": 0,
        "number_of_ressources": 0,
        "number_of_ressources_solutions": 0,
        "number_of_biodiversite_concepts_generaux": 0,
        "number_of_biodiversite_causes_directes": 0,
        "number_of_biodiversite_consequences": 0,
        "number_of_biodiversite_solutions_directes" :0
        ,'number_of_keywords_climat':1,
        'number_of_keywords_biodiversite':0,
        'number_of_keywords_ressources':0
        ,"number_of_changement_climatique_constat_no_hrfp": 0
        ,"number_of_changement_climatique_causes_no_hrfp": 0
        ,"number_of_changement_climatique_consequences_no_hrfp": 0
        ,"number_of_attenuation_climatique_solutions_no_hrfp": 0
        ,"number_of_adaptation_climatique_solutions_no_hrfp": 0
        ,"number_of_ressources_no_hrfp": 0
        ,"number_of_ressources_solutions_no_hrfp": 0
        ,"number_of_biodiversite_concepts_generaux_no_hrfp": 0
        ,"number_of_biodiversite_causes_no_hrfp": 0
        ,"number_of_biodiversite_consequences_no_hrfp": 0
        ,"number_of_biodiversite_solutions_no_hrfp":0
        ,"country": "france"
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    assert df["channel_name"].head(1)[0] == expected_result["channel_name"].head(1)[0]
    assert set(df["theme"].head(1)[0]) == set(expected_result["theme"].head(1)[0])
    # too hard to maintain
    #assert compare_unordered_lists_of_dicts(df["keywords_with_timestamp"].head(1)[0], expected_result["keywords_with_timestamp"].head(1)[0])
    assert df["number_of_keywords"].head(1)[0] == expected_result["number_of_keywords"].head(1)[0]
    assert df["number_of_changement_climatique_constat"].head(1)[0] == expected_result["number_of_changement_climatique_constat"].head(1)[0]
    assert df["number_of_changement_climatique_causes_directes"].head(1)[0] == expected_result["number_of_changement_climatique_causes_directes"].head(1)[0]
    assert df["number_of_changement_climatique_consequences"].head(1)[0] == expected_result["number_of_changement_climatique_consequences"].head(1)[0]
    assert df["number_of_attenuation_climatique_solutions_directes"].head(1)[0] == expected_result["number_of_attenuation_climatique_solutions_directes"].head(1)[0]
    assert df["number_of_adaptation_climatique_solutions_directes"].head(1)[0] == expected_result["number_of_adaptation_climatique_solutions_directes"].head(1)[0]
    assert df["number_of_ressources"].head(1)[0] == expected_result["number_of_ressources"].head(1)[0]
    assert df["number_of_ressources_solutions"].head(1)[0] == expected_result["number_of_ressources_solutions"].head(1)[0]
    assert df["number_of_biodiversite_concepts_generaux"].head(1)[0] == expected_result["number_of_biodiversite_concepts_generaux"].head(1)[0]
    assert df["number_of_biodiversite_causes_directes"].head(1)[0] == expected_result["number_of_biodiversite_causes_directes"].head(1)[0]
    assert df["number_of_biodiversite_consequences"].head(1)[0] == expected_result["number_of_biodiversite_consequences"].head(1)[0]
    assert df["number_of_biodiversite_solutions_directes"].head(1)[0] == expected_result["number_of_biodiversite_solutions_directes"].head(1)[0]


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

def test_overlap_count_keywords_duration_overlap():
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
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources",
            }
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1
  
def test_20_seconds_no_overlap_count_keywords_duration_overlap():
    duration = 20
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 1 * get_keyword_time_separation_ms(duration),
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 2 * get_keyword_time_separation_ms(duration),
                "theme":"ressources", # resources does count now
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 3 * get_keyword_time_separation_ms(duration),
                "theme":"ressources", # resources does count now
            },
            {
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 4 * get_keyword_time_separation_ms(duration), 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 5 * get_keyword_time_separation_ms(duration),
                "theme":"changement_climatique_constat",
            },
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration),start) == 6

def test_with_a_mix_of_overlap_count_keywords_duration_overlap():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15), # count for one
                "theme":"ressources", # resources does count now
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 2000,
                "theme":"ressources",  # resources does count now
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 10000,
                "theme":"ressources",  # resources does count now
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 2, 
                "theme":"ressources", # resources does not count because of 'conditions de vie sur terre'
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 2,
                "theme":"changement_climatique_constat",
            },
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15),start) == 3

def test_with_15second_window_count_keywords_duration_overlap():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            }, # window 0
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) / 2,
                "theme":"changement_climatique_constat",
            }, # window 0 # does not count as 2nd in the window
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15), # count for one
                "theme":"ressources", # does count now ressources
            }, # window 1
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 2000,
                "theme":"ressources", # does count now ressources
            }, # window 1 # does not count as 2nd in the window
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 10000,
                "theme":"ressources", # does count now ressources
            }, # window 1 # does not count as 2nd in the window
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 2 + 10000,  # count for one
                "theme":"ressources", # does count now ressources
            },  # window 2
            { 
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 3,  # count for one
                "theme":"ressources", # does count now ressources
            } # window 3
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15),start) == 4

def test_only_one_count_keywords_duration_overlap():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1

def test_only_one_count_keywords_duration_overlap_no_hrfp():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_keywords_duration_overlap(\
        tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15),\
             start, count_high_risk_false_positive=False) == 1

def test_only_one_hrfp_count_keywords_duration_overlap_no_hrfp():
    keywords_with_timestamp = [{
                "keyword" : 'abeille',
                "timestamp": original_timestamp,
                "theme":"biodiversite_concepts_generaux_indirectes", # transform to direct via solutions biodiv
            },
            {
                "keyword" : 'agriculture bio',
                "timestamp": original_timestamp + 1000,
                "theme":"biodiversite_solutions",
            }
    ]

    tag = tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15)
    tag = get_keywords_with_timestamp_with_false_positive(tag, start, duration_seconds=15)

    expected = [{'keyword': 'abeille', 'timestamp': 1706437079004, 'theme': 'biodiversite_concepts_generaux', 'window_number': 0, 'hrfp': True}, 
                {'keyword': 'agriculture bio', 'timestamp': 1706437080004, 'theme': 'biodiversite_solutions', 'window_number': 0}
    ]
    assert expected == tag
    assert count_keywords_duration_overlap(tag, start, count_high_risk_false_positive=False) == 1
    
def test_indirect_count_keywords_duration_overlap():
    keywords_with_timestamp = [{
                "keyword" : 'digue',
                "timestamp": original_timestamp,
                "theme":"adaptation_climatique_solutions_indirectes",
            }
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1

def test_resources_count_keywords_duration_overlap():
    keywords_with_timestamp = [{
                "keyword" : 'lithium',
                "timestamp": original_timestamp,
                "theme":"ressources", # should count now
            }
    ]
    
    assert count_keywords_duration_overlap(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15),start) == 1

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
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources",
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
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 4,
                "theme":"ressources",
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
                "keyword" : 'pénurie de neige',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"changement_climatique_consequences",
            }
    ]

    expected = [{
                "keyword" : 'pénurie de neige',
                "timestamp": original_timestamp, 
                "theme":"changement_climatique_consequences",
            }
    ]
    
    assert compare_unordered_lists_of_dicts(filter_keyword_with_same_timestamp(keywords_with_timestamp), expected)

def test_keyword_different_theme_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [
        {'keyword': 'climatique', 'timestamp': 1693757470012, 'theme': 'changement_climatique_constat'},
        {'keyword': 'sécheresse', 'timestamp': 1693757450073, 'theme': 'changement_climatique_consequences'},
        {'keyword': 'sécheresse', 'timestamp': 1693757450073, 'theme': 'ressources'}
    ]
        
    assert compare_unordered_lists_of_dicts(filter_keyword_with_same_timestamp(keywords_with_timestamp), keywords_with_timestamp)

def test_keyword_2words_inside_keyword_filter_keyword_with_same_timestamp():
    keywords_with_timestamp = [{
                "keyword" : 'agriculture',
                "timestamp": original_timestamp,
                "theme":"changement_climatique_causes_indirectes",
            },
            {
                "keyword" : 'agriculture industrielle',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"biodiversite_causes", # different theme, keep this one
            }
    ]

    expected = [{
                "keyword" : 'agriculture industrielle',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"biodiversite_causes", # different theme, keep this one
            }
    ]

    assert compare_unordered_lists_of_dicts(filter_keyword_with_same_timestamp(keywords_with_timestamp), expected)

# we should keep the longest keyword, even it's come before the first one
def test_keyword_second_word_a_bit_later_inside_keyword_filter_keyword_with_same_timestamp():
    later_timestamp = original_timestamp + 960 # from real data
    keywords_with_timestamp = [{
                "keyword" : 'carbone',
                "timestamp": later_timestamp,
                "theme":"changement_climatique_causes",
            },
            {
                "keyword" : 'béton bas carbone',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"attenuation_climatique_solutions", # different theme, keep this one
            }
    ]

    expected = [{
                "keyword" : 'béton bas carbone',
                "timestamp": original_timestamp, # same timestamp, so we take longest keyword
                "theme":"attenuation_climatique_solutions", # different theme, keep this one
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
                "theme":"attenuation_climatique_solutions", # different theme, keep this one
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
            "theme":"attenuation_climatique_solutions", # different theme, keep this one
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

def test_get_keyword_by_twenty_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15), # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 2000,
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 10000,
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 2 + 10000,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 3,  # count for one
                "theme":"ressources",
            }
    ]
    
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 4

def test_full_house_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) / 2,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15), # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 2000,
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) + 10000,
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 2 + 10000,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 3,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 4,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 5,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 6,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 7,  # count for one
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 8 - 100,  # count for one
                "theme":"ressources",
            }
    ]
    
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 8


def test_simple_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp, # count for one
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) / 2,
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1

def test_edge_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 8 + 10, # edge case - still counting for one
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1

def test_really_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 15 + 10, # edge case - still counting for one
                "theme":"changement_climatique_constat",
            }
    ]
    with pytest.raises(Exception):
        count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start)

def test_almost_out_of_bound_get_keyword_by_fifteen_second_window():
    keywords_with_timestamp = [
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + get_keyword_time_separation_ms(15) * 8 - 10,
                "theme":"changement_climatique_constat",
            }
    ]
    
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15), start) == 1

def test_tag_wanted_duration_second_window_number():
    keywords_with_timestamp = [
        {'keyword': 'recyclage',
         'timestamp': original_timestamp,
         'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000, # should be transformed to direct
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        }
    ]   
    
    expected = [
        {'keyword': 'recyclage',
         'timestamp': original_timestamp,
         'window_number': 0,
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'window_number': 0,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000,
         'window_number': 1,
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 ,
          'window_number': 2,
          'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 ,
          'window_number': 6,
          'theme': 'attenuation_climatique_solutions_indirectes'
        }
    ]
    assert tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds=15) == expected

def test_transform_false_positive_keywords_to_positive():
    keywords_with_timestamp = [
        {
         'keyword': 'recyclage',
         'timestamp': original_timestamp,
         'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {
         'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000, # should be transformed to direct
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {
         'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be stayed to indirect
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 5 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be stayed to indirect
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 7,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be stayed to indirect
        }
    ]

    expected_output = [
        {
         'hrfp': True, 
         'keyword': 'recyclage',
         'timestamp': original_timestamp,
         'theme': 'attenuation_climatique_solutions' # was indirect
         ,'window_number': 0
        },
        {
         'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat' # our positive keyword that transform false positive
         ,'window_number': 0
        },
        {
        'hrfp': True,
         'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000, # should be transformed to direct
         'theme': 'attenuation_climatique_solutions'
         ,'window_number': 1
        },
        {
         'hrfp': True,
         'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 ,
          'theme': 'attenuation_climatique_solutions' # should be transformed to direct
         ,'window_number': 2
        },
        {
         'hrfp': True,   
         'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 ,
          'theme': 'attenuation_climatique_solutions'# should be transformed to direct
         ,'window_number': 3
        },
        {
         'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 5 ,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay to indirect
         ,'window_number': 5
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 7,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay to indirect
         ,'window_number': 7
        }
    ]
    
    assert transform_false_positive_keywords_to_positive(tag_wanted_duration_second_window_number(keywords_with_timestamp,start, duration_seconds=15), start) == expected_output

def test_different_steps_transform_false_positive_keywords_to_positive():
    keywords_with_timestamp = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay indirect
        },
        {'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay indirect
        },
        {'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay indirect
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'theme': 'attenuation_climatique_solutions_indirectes' # should stay indirect
        }
    ]

    expected_output = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'window_number': 0,
         'theme': 'changement_climatique_constat'
        },
        {'hrfp': True,   
         'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'window_number': 1,
          'theme': 'attenuation_climatique_solutions' # should be transformed to direct
        },
        {'hrfp': True,   
         'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'window_number': 2,
          'theme': 'attenuation_climatique_solutions' # should be transformed to direct
        },
        {'hrfp': True,   
         'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'window_number': 3,
          'theme': 'attenuation_climatique_solutions' # should be transformed to direct
        },
        {'hrfp': True,   
         'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'window_number': 4,
          'theme': 'attenuation_climatique_solutions' # should be transformed to direct
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'window_number': 6,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be stayed to indirect
        }
    ]
    
    assert transform_false_positive_keywords_to_positive(tag_wanted_duration_second_window_number(keywords_with_timestamp,start, duration_seconds=15), start) == expected_output

def test_transform_false_positive_keywords_to_positive_different_and_same_subject():
    keywords_with_timestamp = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        { 
         'keyword': "activisme climatique",
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 151,
          'theme': 'attenuation_climatique_solutions_indirectes' # should be transformed to direct
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        }
    ]

    expected_output = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'window_number': 0,
         'theme': 'changement_climatique_constat'
        },
        {
         'hrfp': True,
         'keyword': "activisme climatique",
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 151,
         'window_number': 1,
         'theme': 'attenuation_climatique_solutions' # should be transformed to direct
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'window_number': 1,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'window_number': 2,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'window_number': 3,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'window_number': 4,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'window_number': 6,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        }
    ]
    
    assert transform_false_positive_keywords_to_positive(tag_wanted_duration_second_window_number(keywords_with_timestamp,start, duration_seconds=15), start) == expected_output



def test_transform_false_positive_keywords_to_positive_different_subject():
    keywords_with_timestamp = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        }
    ]

    expected_output = [
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'window_number': 0,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 1 + 150,
          'window_number': 1,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'agroforesterie',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 + 150,
          'window_number': 2,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'alternative durable',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 + 150,
          'window_number': 3,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'planification écologique',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 4 + 150,
          'window_number': 4,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        },
        {'keyword': 'nucléaire',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 6 + 150,
          'window_number': 6,
          'theme': 'biodiversite_concepts_generaux_indirectes' # should stay indirect
        }
    ]
    
    assert transform_false_positive_keywords_to_positive(tag_wanted_duration_second_window_number(keywords_with_timestamp,start, duration_seconds=15), start) == expected_output


def test_count_different_window_number():
    keywords_with_timestamp = [
        {'keyword': 'recyclage',
         'timestamp': original_timestamp, # count
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000,
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 , # count
          'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 , # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 5 , # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 7, # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        }
    ]
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, duration_seconds = 15),start) == 6

def test_count_different_window_number_40():
    keywords_with_timestamp = [
        {'keyword': 'recyclage',
         'timestamp': original_timestamp, # count
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'climatique',
         'timestamp': original_timestamp + 150,
         'theme': 'changement_climatique_constat'
        },
        {'keyword': 'covoiturage',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) + 10000,
         'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 2 , # count
          'theme': 'attenuation_climatique_solutions_indirectes'
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 3 , # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 5 , # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        },
        {'keyword': 'industrie verte',
         'timestamp': original_timestamp + get_keyword_time_separation_ms(15) * 7, # count
          'theme': 'attenuation_climatique_solutions_indirectes' 
        }
    ]
    assert count_different_window_number(tag_wanted_duration_second_window_number(keywords_with_timestamp, start, 40),start) == 3