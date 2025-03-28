from modin.pandas.dataframe import DataFrame
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
        ,"program_metadata_id": "3fbbf26e8a207a8fc701c1568a89449509840bec71704a5aa1b924387999f7f9"
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
        ,"program_metadata_id": "119a697547071c795804ebcc23b4d6ddc0c25ae350fe630decb508e241ba5923"
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
        # ,"program_metadata_id": None # TODO None != None
    }])
    expected['start'] = pd.to_datetime(sunday_night_no_show, unit='s', utc=True).tz_convert('Europe/Paris')
    output.drop(columns=['program_metadata_id'], inplace=True)
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
        ,"program_metadata_id": "5eeea6922b53ec62e416a6d786b4a4cddfc71a6488c25a37be7e0457845f77a4"
    }])
    expected['start'] = pd.to_datetime(jt_20h02, unit='s', utc=True).tz_convert('Europe/Paris')
    #debug_df(output)
    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))


def test_get_programs_for_this_day_thusday_morning_france2():
    df_programs: DataFrame = get_programs()
    programs = get_programs_for_this_day(pd.to_datetime(thrusday_morning, unit='s').tz_localize('Europe/Paris'), channel_name, df_programs)
    debug_df(programs)

    envoye_special_id = "3b15cb4724ddf7a3c73ce4d140176c9816c7cbfdb3204839bdc139961e40bef2"
    expected = pd.DataFrame([
        {"channel_name":"france2","start":1712808000,"end":1712809500,"program_name":"Le 6h Info", "program_type":"Information - Journal","id": "3fbbf26e8a207a8fc701c1568a89449509840bec71704a5aa1b924387999f7f9"},
        {"channel_name":"france2","start":1712809800,"end":1712820600,"program_name":"Télématin","program_type":"Information - Autres émissions","id": "119a697547071c795804ebcc23b4d6ddc0c25ae350fe630decb508e241ba5923"},
        {"channel_name":"france2","start":1712833200,"end":1712835600,"program_name":"JT 13h","program_type":"Information - Journal","id": "24fc3810571747e173f01248f253de02543fe84fb61addf5061ca25cdc8e57af"},
        {"channel_name":"france2","start":1712858100,"end":1712860800,"program_name":"JT 20h + météo","program_type":"Information - Journal","id": "046d5d696c00001f38d7aef02b4fc26cca632486104f2579c834d7265a34edfa"},
        {"channel_name":"france2","start":1712862600,"end":1712869200,"program_name":"Envoyé spécial","program_type":"Information - Magazine","id": envoye_special_id}, # problem here
    ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_hour_minute():
    output = get_hour_minute(pd.Timestamp(pd.to_datetime(thrusday_morning, unit='s')))

    assert output == pd.Timestamp('1970-01-01 06:02:00')
 
def test_format_hour_minute_one_digit():
    hour_min = '6:55'
    output = format_hour_minute(hour_min)

    assert output == pd.Timestamp(f'1970-01-01 0{hour_min}:00')

def test_format_hour_minute_double_digit_with_zero():
    hour_min = '06:55'
    output = format_hour_minute(hour_min)

    assert output == pd.Timestamp(f'1970-01-01 {hour_min}:00')
  
def test_format_hour_minute_double_digit():
    hour_min = '16:55'
    output = format_hour_minute(hour_min)

    assert output == pd.Timestamp(f'1970-01-01 {hour_min}:00')
 
def test_get_programs_for_this_day_thusday_morning_franceinfo():
    df_programs = get_programs()
    
    thrusday_morning_ts = pd.to_datetime(thrusday_morning, unit='s').tz_localize('Europe/Paris')
    programs = get_programs_for_this_day(thrusday_morning_ts, "france-info", df_programs)
    debug_df(programs)
    expected = pd.DataFrame([
        {"channel_name":"france-info",
         "start":1712808000,
         "end":1712869200,
         "program_name":"Information en continu", 
         "program_type":"Information en continu",
         "id":"036016c7bc8feb4651ecfc09a75e596e2b3ec7c87b94ad3ac7b7da773419172a",
         },
 ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_a_program_with_start_timestamp():
    df_programs = get_programs()
    thursday_13h34 = 1713958465 # normaly up to 13h40
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(thursday_13h34, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"
    assert id == "3a180d999f32b6e62d9fe946686497e22b5426b815efb4e4cbbc5c395252355a"

def test_get_13h_program_with_start_timestamp():
    df_programs = get_programs()
    saturday_13h18 = 1717240693
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, \
                                                                    pd.to_datetime(saturday_13h18, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    channel_name)
    assert program_name == "13h15 le samedi"
    assert program_type == "Information - Journal"
    assert id == "23a86cefdb11c7b1faef276237c0fa4752be820457684a8ee2da7ece9652914c"

def test_get_13h_monday_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h18 = 1724671082
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(monday_13h18, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"
    assert id == "2cc2ed9f3da6b98b326df891d40a8f9119397159f353416404076c0d697f50be"

def test_get_13h_monday_rfi_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h05 = 1726398337
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h05, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "239167a0018a88fb9c7824d122d0a5660da9a204a1d5157611b986f778adb9cc"


def test_get_13h_monday_rfi_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h12 = 1726398730
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h12, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "239167a0018a88fb9c7824d122d0a5660da9a204a1d5157611b986f778adb9cc"


def test_get_6h26_friday_fr2_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    friday_6h26 = 1726719981
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_6h26, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "Le 6h Info"
    assert program_type == "Information - Journal"
    assert id == "3fbbf26e8a207a8fc701c1568a89449509840bec71704a5aa1b924387999f7f9"


def test_get_old_jt_20hweekday_20h19_friday_fr2():
    df_programs = get_programs()
    friday_20h19 = 1722622741
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_20h19, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "JT 20h + météo"
    assert program_type == "Information - Journal"
    assert id == "85e9ced55fd82e9e1432fa46f779086d3759b614cd009031701a3f24429d64f7"

def test_get_old_no_match_on_new_program_date_jt_20hweekday_20h55_friday_fr2():
    df_programs = get_programs()
    friday_20h19 = 1722624901
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_20h19, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == ""
    assert program_type == ""
    assert id == None

def test_get_new_jt_20hweekday_20h55_friday_fr2():
    df_programs = get_programs()
    friday_20h55 = 1727376901
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_20h55, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "JT 20h + météo"
    assert program_type == "Information - Journal"
    assert id == "72b43bb2486e4be0d0e7fd1bb670b876e0051de070e78413a0c66b397fe9c743"

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
    assert compare_weekday("1", 5) == False
    assert compare_weekday("1", 1) == True
    assert compare_weekday("4", 4) == True


def test_update_programs_and_filter_out_of_scope_programs_from_df():
    date_friday_20h19 = pd.to_datetime(1722622741, unit='s', utc=True).tz_convert('Europe/Paris')
    date_friday_6h26 = pd.to_datetime(1726719981, unit='s', utc=True).tz_convert('Europe/Paris')
    df = pd.DataFrame({
        'start': [date_friday_20h19, date_friday_20h19, date_friday_6h26],
        'channel_name': ['tf1', 'france2', 'france2'],
        'channel_program': ['to_be_updated', 'to_be_updated Show', None], 
        'channel_program_type': ['to_be_updated', 'to_be_updated', None],
        'id': ['to_be_updated', 'to_be_updated', None]
    })

    df_programs = get_programs()

    expected_df = pd.DataFrame({
        'start': [date_friday_20h19, date_friday_20h19, date_friday_6h26],
        'channel_name': ['tf1', 'france2', 'france2'],
        'channel_program': ['JT 20h + météo', 'JT 20h + météo', 'Le 6h Info'],
        'channel_program_type': ["Information - Journal", "Information - Journal", 'Information - Journal'],
        'program_metadata_id': ["ac5b72ec462313e5bad33b6f97325414fa4d5d55a4bcfe25ff46b3c62c9ea307", "85e9ced55fd82e9e1432fa46f779086d3759b614cd009031701a3f24429d64f7", "3fbbf26e8a207a8fc701c1568a89449509840bec71704a5aa1b924387999f7f9"]
    })

    # Run the function
    result_df = update_programs_and_filter_out_of_scope_programs_from_df(df, df_programs)

    # Assert the results match the expected DataFrame
    debug_df(result_df)
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
