import pytest


from quotaclimat.data_processing.mediatree.channel_program import *
from utils import get_localhost, debug_df
import logging
import pandas as pd

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
channel_name = "france2"
thrusday_morning = 1712815351
df = pd.DataFrame([{
    "id" : primary_key,
    "start": thrusday_morning,
    "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
    "channel_name": channel_name,
    "channel_radio": False,
    "theme": themes,
    "keywords_with_timestamp": keywords_with_timestamp
    ,"number_of_keywords": 1
}])

df['start'] = pd.to_datetime(df['start'], unit='s', utc=True).dt.tz_convert('Europe/Paris')

def test_add_channel_program():
    output = add_channel_program(df)

    expected = pd.DataFrame([{
        "id" : primary_key,
        "start": thrusday_morning,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 1
        ,"program_name": "Télématin"
        ,"program_type": "Information - Autres émissions"
    }])
    expected['start'] = pd.to_datetime(thrusday_morning, unit='s', utc=True).tz_convert('Europe/Paris')
    debug_df(output)
    pd.testing.assert_frame_equal(output._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))


def test_add_channel_program_evening_jt():
    jt_20h02 = 1712772151 # wednesday - 3 

    df['start'] = pd.to_datetime(jt_20h02, unit='s', utc=True).tz_convert('Europe/Paris')
    
    output = add_channel_program(df)

    expected = pd.DataFrame([{
        "id" : primary_key,
        "start": jt_20h02,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 1
        ,"program_name": "JT 20h + météo"
        ,"program_type": "Information - Journal"
    }])
    expected['start'] = pd.to_datetime(jt_20h02, unit='s', utc=True).tz_convert('Europe/Paris')
    #debug_df(output)
    pd.testing.assert_frame_equal(output._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

