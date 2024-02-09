import pytest
import pandas as pd

from bs4 import BeautifulSoup
from utils import get_localhost, debug_df
from quotaclimat.data_processing.mediatree.api_import import format_word_regex, is_word_in_sentence, get_themes_keywords_duration, get_cts_in_ms_for_keywords, filter_and_tag_by_theme, parse_reponse_subtitle, get_includes_or_query, transform_theme_query_includes
import json 
from postgres.insert_data import save_to_pg
from postgres.schemas.models import keywords_table, connect_to_db, get_keyword, drop_tables
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS

import datetime

localhost = get_localhost()

drop_tables()

plaintext1="test1"
plaintext2="test2"
json_response = json.loads("""
{"total_results":214,
"number_pages":43,
"data":[
        {
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 1706437079004,
                "text": "gilets"
                },
                {
                "duration_ms": 34,
                "cts_in_ms": 1706437079038,
                "text": "jaunes"
                },
                {
                "duration_ms": 34,
                "cts_in_ms": 1706437079072,
                "text": "en"
                },
                {
                "duration_ms": 34,
                "cts_in_ms": 1706437080006,
                "text": "france"
                }
            ],
            "channel":{"name":"m6","title":"M6","radio":false},"start":1704798000,
            "plaintext":"test1"
        },
        {
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 1706437079004,
                "text": "adaptation"
                }
            ],
            "channel":{"name":"tf1","title":"M6","radio":false},"start":1704798120,
            "plaintext":"test2"}
    ],
    "elapsed_time_ms":335}
""")

def test_parse_reponse_subtitle():
    expected_result = pd.DataFrame([{
        "srt": [{
            "duration_ms": 34,
            "cts_in_ms": 1706437079004,
            "text": "gilets"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": 1706437079038,
            "text": "jaunes"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": 1706437079072,
            "text": "en"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": 1706437080006,
            "text": "france"
            }
        ],
        "plaintext" : plaintext1,
        "channel_name" : "m6",
        "channel_radio" : False,
        "start" : 1704798000,
    },
    {
        "srt": [{
            "duration_ms": 34,
            "cts_in_ms": 1706437079004,
            "text": "adaptation"
            }
        ],
        "plaintext" : plaintext2,
        "channel_name" : "tf1",
        "channel_radio" : False,
        "start" : 1704798120,
    }])

    expected_result['start'] = pd.to_datetime(expected_result['start'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')
    df = parse_reponse_subtitle(json_response)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_get_includes_or_query():
    words = ["velo", "marche"]
    output = get_includes_or_query(words)
    assert output  == "velo OU marche"

def test_transform_theme_query_includes():
    theme = [{
    "causes_directes" : [
           "inaction climatique","insuffisance climatique",
            "gaz à effet de serre"
    ]},{
    "causes_indirectes": [
        "vache", "élevage", "élevage bovin"
      ]
    }]
    output = transform_theme_query_includes(theme)
    expected = [
    {"causes_directes" : "inaction climatique OU insuffisance climatique OU gaz à effet de serre"},
    {"causes_indirectes": "vache OU élevage OU élevage bovin"}
    ]

    assert output == expected


def test_get_themes_keywords_duration():
    subtitles = [{
          "duration_ms": 34,
          "cts_in_ms": 1706437079004,
          "text": "gilets"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437080006,
          "text": "solaires"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079038,
          "text": "jaunes"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079072,
          "text": "économie"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079076,
          "text": "circulaire"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079076,
          "text": "abusive"
        }
    ]

    plaintext_nothing = "cheese pizza"
    assert get_themes_keywords_duration(plaintext_nothing, subtitles) == [None,None, None]
    plaintext_climat = "climatique test"
    assert get_themes_keywords_duration(plaintext_climat, subtitles) == [["changement_climatique_constat"],[], 0]
    plaintext_multiple_themes = "climatique test bovin migrations climatiques"
    assert get_themes_keywords_duration(plaintext_multiple_themes, subtitles) == [["changement_climatique_constat", "changement_climatique_consequences"],[], 0]

    # should not accept theme 'bus' for keyword "abusive"
    plaintext_regression_incomplete_word = "abusive"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word, subtitles) == [None,None, None]
    
    # should not accept theme 'ngt' for keyword "vingt"
    plaintext_regression_incomplete_word_ngt = "vingt"
    assert get_themes_keywords_duration(plaintext_regression_incomplete_word_ngt, subtitles) == [None,None, None]
    

    assert get_themes_keywords_duration("record de température pizza adaptation au dérèglement climatique", subtitles) == [[
      "changement_climatique_constat"
     ,"changement_climatique_consequences"
     ,"adaptation_climatique_solutions_directes"
    ],[], 0]

def test_get_cts_in_ms_for_keywords():
    str = [{
          "duration_ms": 34,
          "cts_in_ms": 1706437079004,
          "text": "gilets"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437080006,
          "text": "Solaires"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079038,
          "text": "jaunes"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079072,
          "text": "économie"
        },
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437079076,
          "text": "circulaire"
        }
    ]
    keywords = ['économie circulaire', 'panneaux solaires', 'solaires']
    theme = "changement_climatique_constat"
    expected = [
        {
            "keyword":'économie circulaire',
            "timestamp" : 1706437079072,
            "theme": theme
        },
        {
            "keyword":'solaires',
            "timestamp" : 1706437080006,
            "theme": theme
        },
    ]
    assert get_cts_in_ms_for_keywords(str, keywords, theme) == expected

def test_complex_hyphen_get_cts_in_ms_for_keywords():
    str = [
        {
          "duration_ms": 34,
          "cts_in_ms": 1706437080006,
          "text": "vagues-submersion"
        }
    ]
    keywords = ['submersion']
    theme = "changement_climatique_consequences"
    expected = [
        {
            "keyword":'submersion',
            "timestamp" : 1706437080006,
            "theme": theme
        }
    ]
    assert get_cts_in_ms_for_keywords(str, keywords, theme) == expected



def test_filter_and_tag_by_theme():
    df1 = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "cheese pizza",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": [{
            "duration_ms": 34,
            "cts_in_ms": 1706437079004,
            "text": "adaptation"
            }
        ],
        },{
            "start": 1704798000,
            "plaintext": "tomato screen",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 1706437079004,
                "text": "adaptation"
                }
            ],
        },{
            "start": 1704798000,
            "plaintext": "vache bovin anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 1706437079004,
                "text": "adaptation"
                }
            ],
        },
        {
            "start": 1704798000,
            "plaintext": "cheese pizza",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                    "duration_ms": 34,
                    "cts_in_ms": 1706437079004,
                    "text": "adaptation"
                }
            ],
        },{
            "start": 1704798000,
            "plaintext": "pizza année la plus chaude",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 1706437079004,
                "text": "adaptation"
                }
            ],
    }])

    expected_result = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "vache bovin anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_indirectes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [],
        "number_of_keywords": 0.0
    },
    {
        "start": 1704798000,
        "plaintext": "pizza année la plus chaude",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": ["changement_climatique_consequences"],
        "keywords_with_timestamp": [],
        "number_of_keywords": 0.0
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))


def test_lower_case_filter_and_tag_by_theme():
    df1 = pd.DataFrame([{
            "start": 1704798000,
            "plaintext": "VACHE BOVIN Anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 111,
                "text": "Vache"
                }
            ],
    }])

    expected_result = pd.DataFrame([{
        "start": 1704798000,
        "plaintext":  "VACHE BOVIN Anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_indirectes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"vache",
                "timestamp": 111,
                "theme": "changement_climatique_causes_indirectes",
        }]
        ,"number_of_keywords": 1
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_singular_plural_case_filter_and_tag_by_theme():
    df1 = pd.DataFrame([{
            "start": 1704798000,
            "plaintext": "VACHE BOVIN Anthropocène",
            "channel_name": "m6",
            "channel_radio": False,
            "srt": [{
                "duration_ms": 34,
                "cts_in_ms": 111,
                "text": "vaches"
                }
            ],
    }])

    expected_result = pd.DataFrame([{
        "start": 1704798000,
        "plaintext":  "VACHE BOVIN Anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": [
            "changement_climatique_constat",
            "changement_climatique_causes_indirectes",
            "ressources_naturelles_concepts_generaux"
        ],
        "keywords_with_timestamp": [
            {
                "keyword" :"vache",
                "timestamp": 111,
                "theme": "changement_climatique_causes_indirectes",
        }]
        ,"number_of_keywords": 1
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_complexe_filter_and_tag_by_theme():
    df1 = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": "m6",
        "channel_radio": False,
        "srt": [{
            "duration_ms": 34,
            "cts_in_ms": 1706437079004,
            "text": "cheese"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079005,
            "text": "pizza"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079006,
            "text": "habitabilité"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079007,
            "text": "de"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079008,
            "text": "la"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079009,
            "text": "planète"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079010,
            "text": "conditions"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079011,
            "text": "de"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079011,
            "text": "vie"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079011,
            "text": "sur"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079011,
            "text": "terre"
            },{
            "duration_ms": 34,
            "cts_in_ms": 1706437079012,
            "text": "animal"
            },
        ],
    }])

    expected_result = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": [
            "changement_climatique_constat",
            "ressources_naturelles_concepts_generaux",
        ],
        "keywords_with_timestamp": [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": 1706437079006,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": 1706437079010,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": 1706437079009,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": 1706437079011,
                "theme":"ressources_naturelles_concepts_generaux",
            }
        ]
        ,"number_of_keywords": 4
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))

def test_save_to_pg_keyword():
    conn = connect_to_db()
    primary_key = "test_save_to_pg_keyword"
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": 1706437079006, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": 1706437079010,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": 1706437079009,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": 1706437079011,
                "theme":"ressources_naturelles_concepts_generaux",
            }
        ]
    themes = [
            "changement_climatique_constat",
            "ressources_naturelles_concepts_generaux",
        ]
    channel_name = "m6"
    df = pd.DataFrame([{
        "id" : primary_key,
        "start": 1706437079006,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 4
    }])

    df['start'] = pd.to_datetime(df['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')
   
    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    result = get_keyword(primary_key)

    assert result.id == primary_key
    assert result.channel_name == channel_name
    assert result.channel_radio == False
    assert result.theme == themes 
    assert result.keywords_with_timestamp == keywords_with_timestamp
    assert result.number_of_keywords == 4
    assert result.start == datetime.datetime(2024, 1, 28, 10, 17, 59, 6000)

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
    assert format_word_regex("coraux") == "coraux"
    assert format_word_regex("d'eau") == "d' ?eaus?"