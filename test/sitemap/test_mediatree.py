import pytest
import pandas as pd

from bs4 import BeautifulSoup
from utils import get_localhost, debug_df
from quotaclimat.data_processing.mediatree.api_import import get_themes_keywords_duration, get_cts_in_ms_for_keywords, filter_and_tag_by_theme, parse_reponse_subtitle, get_includes_or_query, transform_theme_query_includes
import json 
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from postgres.schemas.models import drop_tables
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
        }
    ]

    plaintext_nothing = "cheese pizza"
    assert get_themes_keywords_duration(plaintext_nothing, subtitles) == [None,None]
    plaintext_climat = "climatique test"
    assert get_themes_keywords_duration(plaintext_climat, subtitles) == [["changement_climatique_constat"],[]]
    plaintext_multiple_themes = "climatique test bovin migrations climatiques"
    assert get_themes_keywords_duration(plaintext_multiple_themes, subtitles) == [["changement_climatique_constat", "changement_climatique_consequences"],[]]

    assert get_themes_keywords_duration("record de température pizza adaptation au dérèglement climatique", subtitles) == [[
      "changement_climatique_constat"
     ,"changement_climatique_consequences"
     ,"adaptation_climatique_solutions_directes"
    ],[]]

def test_get_cts_in_ms_for_keywords():
    str = [{
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
        "keywords_with_timestamp": []
    },
    {
        "start": 1704798000,
        "plaintext": "pizza année la plus chaude",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": ["changement_climatique_consequences"],
        "keywords_with_timestamp": []
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
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)
    debug_df(df)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))
