import pytest

from quotaclimat.data_processing.mediatree.channel_program import *
from test_utils import get_localhost, debug_df
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
channel_name = "france2"
thrusday_morning = 1712815351 #Thu Apr 11 2024 08:02:31 GMT+0200
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

def test_add_channel_program_france2():
    telematin = 1712808646,
    df['start'] = pd.to_datetime(telematin, unit='s', utc=True).tz_convert('Europe/Paris')
    output = add_channel_program(df)

    expected = pd.DataFrame([{
        "id" : primary_key,
        "start": telematin, # 6:02
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 1
        ,"channel_program": "Le 6h Info"
        ,"channel_program_type": "Information - Journal"
    }])
    expected['start'] = pd.to_datetime(telematin, unit='s', utc=True).tz_convert('Europe/Paris')
    debug_df(output)
    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))


def test_add_channel_program_france2_telematin():
    df['start'] = pd.to_datetime(thrusday_morning, unit='s', utc=True).tz_convert('Europe/Paris')
    output = add_channel_program(df)

    expected = pd.DataFrame([{
        "id" : primary_key,
        "start": thrusday_morning, # 8:02
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 1
        ,"channel_program": "Télématin"
        ,"channel_program_type": "Information - Autres émissions"
    }])
    expected['start'] = pd.to_datetime(thrusday_morning, unit='s', utc=True).tz_convert('Europe/Paris')
    debug_df(output)
    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))

def test_add_channel_program_wrong_time():
    sunday_night_no_show = 1713054037
    df['start'] = pd.to_datetime(sunday_night_no_show, unit='s', utc=True).tz_convert('Europe/Paris')
    output = add_channel_program(df)
    debug_df(output)

    expected = pd.DataFrame([{
        "id" : primary_key,
        "start": sunday_night_no_show,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": 1
        ,"channel_program": ""
        ,"channel_program_type": ""
    }])
    expected['start'] = pd.to_datetime(sunday_night_no_show, unit='s', utc=True).tz_convert('Europe/Paris')

    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))

def test_get_programs():
    programs = get_programs()

    assert len(programs) > 0

def test_add_channel_program_france2_jt():
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
        ,"channel_program": "JT 20h + météo"
        ,"channel_program_type": "Information - Journal"
    }])
    expected['start'] = pd.to_datetime(jt_20h02, unit='s', utc=True).tz_convert('Europe/Paris')
    #debug_df(output)
    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))

def test_get_programs_for_this_day_thusday_morning_france2():
    df_programs = get_programs()
    programs = get_programs_for_this_day(pd.to_datetime(thrusday_morning, unit='s').normalize(), channel_name, df_programs)
    debug_df(programs)
    expected = pd.DataFrame([
        {"channel_name":"france2","start":1712808000,"end":1712809500,"program_name":"Le 6h Info", "program_type":"Information - Journal"},
        {"channel_name":"france2","start":1712809800,"end":1712820600,"program_name":"Télématin","program_type":"Information - Autres émissions"},
        {"channel_name":"france2","start":1712833200,"end":1712835600,"program_name":"JT 13h","program_type":"Information - Journal"},
        {"channel_name":"france2","start":1712858100,"end":1712860800,"program_name":"JT 20h + météo","program_type":"Information - Journal"},
        {"channel_name":"france2","start":1712862600,"end":1712869200,"program_name":"Envoyé spécial","program_type":"Information - Magazine"},
 ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_hour_minute():
    output = get_hour_minute(pd.Timestamp(pd.to_datetime(thrusday_morning, unit='s')))

    assert output == pd.Timestamp('1970-01-01 06:02:00')
 
def test_get_programs_for_this_day_thusday_morning_franceinfo():
    df_programs = get_programs()
    programs = get_programs_for_this_day(pd.to_datetime(thrusday_morning, unit='s').normalize(), "france-info", df_programs)
    debug_df(programs)
    expected = pd.DataFrame([
        {"channel_name":"france-info","start":1712808000,"end":1712869200,"program_name":"Information en continu", "program_type":"Information en continu"},
 ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_a_program_with_start_timestamp():
    df_programs = get_programs()
    thursday_13h34 = 1713958465 # normaly up to 13h40
    program_name, program_type = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(thursday_13h34, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"

def test_get_13h_program_with_start_timestamp():
    df_programs = get_programs()
    saturday_13h18 = 1717240693
    program_name, program_type = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(saturday_13h18, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "13h15 le samedi"
    assert program_type == "Information - Journal"

def test_compare_weekday_string():
    assert compare_weekday('*', 0) == True
    assert compare_weekday('*', 3) == True
    assert compare_weekday('weekday', 4) == True
    assert compare_weekday('weekday', 6) == False
    assert compare_weekday('weekday', 5) == False
    assert compare_weekday('weekend', 5) == True
    assert compare_weekday('weekend', 6) == True
    assert compare_weekday('weekend', 3) == False
    
def test_compare_weekday_int():
    assert compare_weekday(1, 5) == False
    assert compare_weekday(1, 1) == True
    assert compare_weekday(4, 4) == True