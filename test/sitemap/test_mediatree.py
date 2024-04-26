import pytest

from test_utils import get_localhost, debug_df
from quotaclimat.data_processing.mediatree.api_import import *
from quotaclimat.data_processing.mediatree.utils import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import keywords_table, connect_to_db, get_keyword, drop_tables

import pandas as pd
import datetime

localhost = get_localhost()

drop_tables()
create_tables()
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
        "channel_program" : "",
        "channel_program_type" : "",
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
        "channel_program" : "",
        "channel_program_type" : "",
    }])

    expected_result['start'] = pd.to_datetime(expected_result['start'], unit='s').dt.tz_localize('UTC')
    df = parse_reponse_subtitle(json_response)
    debug_df(df)

    pd.testing.assert_frame_equal(df._to_pandas().reset_index(drop=True), expected_result.reset_index(drop=True))

def test_get_channels():
    if(os.environ.get("ENV") == "docker"):
        assert get_channels() == ["france2"] # default for docker compose config
    else:
        assert get_channels() == ["tf1", "france2", "fr3-idf", "m6", "arte", "d8", "bfmtv", "lci", "franceinfotv", "itele",
        "europe1", "france-culture", "france-inter", "sud-radio", "rmc", "rtl", "france24", "france-info", "rfi"]

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
                "theme":"ressources",
            },
            {
                "keyword" : 'terre',
                "timestamp": 1706437079011,
                "theme":"ressources",
            }
        ]
    themes = [
            "changement_climatique_constat",
            "ressources_concepts_generaux",
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
        ,"number_of_keywords": 1
    }])

    df['start'] = pd.to_datetime(df['start'], unit='ms').dt.tz_localize('UTC')#.dt.tz_convert('Europe/Paris')
   
    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    result = get_keyword(primary_key)

    assert result.id == primary_key
    assert result.channel_name == channel_name
    assert result.channel_radio == False
    assert result.theme == themes 
    assert result.keywords_with_timestamp == keywords_with_timestamp
    assert result.number_of_keywords == 1
    assert result.start == datetime.datetime(2024, 1, 28, 10, 17, 59, 6000)