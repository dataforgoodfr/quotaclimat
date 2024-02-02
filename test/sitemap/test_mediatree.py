import pytest
import pandas as pd
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_news, get_hat_20minutes, get_url_content
from quotaclimat.data_ingestion.scrap_sitemap import get_description_article
from bs4 import BeautifulSoup
from utils import get_localhost, debug_df
from quotaclimat.data_processing.mediatree.api_import import find_themes, filter_and_tag_by_theme, parse_reponse_subtitle, get_includes_or_query, transform_theme_query_includes
import json 
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
localhost = get_localhost()

plaintext1="test1"
plaintext2="test2"
json_response = json.loads("""
{"total_results":214,
"number_pages":43,
"data":[
    {"channel":{"name":"m6","title":"M6","radio":false},"start":1704798000,
        "plaintext":"test1"},
    {"channel":{"name":"tf1","title":"M6","radio":false},"start":1704798120,
        "plaintext":"test2"}
],
"elapsed_time_ms":335}
""")

def test_parse_reponse_subtitle():
    theme = "test_theme"
    expected_result = pd.DataFrame([{
        "plaintext" : plaintext1,
        "channel_name" : "m6",
        "channel_radio" : False,
         "start" : 1704798000
    },
    {
        "plaintext" : plaintext2,
        "channel_name" : "tf1",
        "channel_radio" : False,
         "start" : 1704798120
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

def test_find_themes():
    plaintext_nothing = "cheese pizza"
    assert find_themes(plaintext_nothing) == None
    plaintext_climat = "climatique test"
    assert find_themes(plaintext_climat) == ["constat_et_concepts_globaux"]
    plaintext_multiple_themes = "climatique test bovin migrations climatiques"
    assert find_themes(plaintext_multiple_themes) == ["constat_et_concepts_globaux", "consequences"]

def test_filter():
    df1 = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "cheese pizza",
        "channel_name": "m6",
        "channel_radio": False
    },{
        "start": 1704798000,
        "plaintext": "tomato screen",
        "channel_name": "m6",
        "channel_radio": False
    },{
        "start": 1704798000,
        "plaintext": "vache bovin anthropocène",
        "channel_name": "m6",
        "channel_radio": False
    },
    {
        "start": 1704798000,
        "plaintext": "cheese pizza",
        "channel_name": "m6",
        "channel_radio": False
    },{
        "start": 1704798000,
        "plaintext": "pizza année la plus chaude",
        "channel_name": "m6",
        "channel_radio": False
    }])

    expected_result = pd.DataFrame([{
        "start": 1704798000,
        "plaintext": "vache bovin anthropocène",
        "channel_name": "m6",
        "channel_radio": False,
        "theme": ["constat_et_concepts_globaux", "causes_indirectes"]
    },
    {
        "start": 1704798000,
        "plaintext": "pizza année la plus chaude",
        "channel_name": "m6",
        "channel_radio": False,
         "theme": ["consequences"]
    }])

    # List of words to filter on
    df = filter_and_tag_by_theme(df1)

    pd.testing.assert_frame_equal(df.reset_index(drop=True), expected_result.reset_index(drop=True))
