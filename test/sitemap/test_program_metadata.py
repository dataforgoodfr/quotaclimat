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
        ,"program_metadata_id": "6fc646caf4ed30c62298b482634a2a537a69467e3abfd9098d397e826a67454f"
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
        ,"program_metadata_id": "662e61acd54356947ee012d44ffafbd568f3b1b3c0186c7b0a239b8ccf5949c4"
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

def test_get_programs_weekday():
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
        ,"program_metadata_id": "046d5d696c00001f38d7aef02b4fc26cca632486104f2579c834d7265a34edfa"
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
        {"channel_name":"france2","start":1712808000,"end":1712809500,"program_name":"Le 6h Info", "program_type":"Information - Journal","id": "6fc646caf4ed30c62298b482634a2a537a69467e3abfd9098d397e826a67454f"},
        {"channel_name":"france2","start":1712809800,"end":1712820600,"program_name":"Télématin","program_type":"Information - Autres émissions","id": "662e61acd54356947ee012d44ffafbd568f3b1b3c0186c7b0a239b8ccf5949c4"},
        {"channel_name":"france2","start":1712833200,"end":1712835600,"program_name":"JT 13h","program_type":"Information - Journal","id": "f35ea774b8a1cbe224737c66684a8be172a8d33076bce1b99b34fd1835eeb8bf"},
        {"channel_name":"france2","start":1712858100,"end":1712860800,"program_name":"JT 20h + météo","program_type":"Information - Journal","id": "85e9ced55fd82e9e1432fa46f779086d3759b614cd009031701a3f24429d64f7"},
        {"channel_name":"france2","start":1712862600,"end":1712869200,"program_name":"Envoyé spécial","program_type":"Information - Magazine","id": envoye_special_id},
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
         "id":"4729d280b57a07648c047368a6c1ef319c1a962fab6fced47a445e0c790d4090",
         },
 ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_a_program_with_start_timestamp():
    df_programs = get_programs()
    thursday_13h34 = 1713958465 # normaly up to 13h40
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(thursday_13h34, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"
    assert id == "24fc3810571747e173f01248f253de02543fe84fb61addf5061ca25cdc8e57af"

def test_get_13h_program_with_start_timestamp():
    df_programs = get_programs()
    saturday_13h18 = 1717240693
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, \
                                                                    pd.to_datetime(saturday_13h18, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    channel_name)
    assert program_name == "13h15 le samedi"
    assert program_type == "Information - Journal"
    assert id == "be66f227ac2514e29b8667ae77d26fcd91cfee7d04f9b135462b6d8a4364d2d5"

def test_get_13h_monday_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h18 = 1724671082
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(monday_13h18, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"
    assert id == "7d89450a404f17ddecefa36118a4d62535560b9bbf5952b5702799a309a77e16"

def test_get_13h_monday_rfi_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h05 = 1726398337
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h05, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "454e9b977a5a1904763f2cfeb8d347c2315ad227e23dad73bb310cbe712bb8ab"


def test_get_13h_monday_rfi_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h12 = 1726398730
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h12, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "454e9b977a5a1904763f2cfeb8d347c2315ad227e23dad73bb310cbe712bb8ab"


def test_get_6h26_friday_fr2_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    friday_6h26 = 1726719981
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_6h26, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "Le 6h Info"
    assert program_type == "Information - Journal"
    assert id == "6fc646caf4ed30c62298b482634a2a537a69467e3abfd9098d397e826a67454f"


def test_get_old_jt_20hweekday_20h19_friday_fr2():
    df_programs = get_programs()
    friday_20h19 = 1722622741
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_20h19, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "JT 20h + météo"
    assert program_type == "Information - Journal"
    assert id == "c6a310a4985f04dbdad1267097f2725da92e08dc7948dbceb4ed35cd62641d57"

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
    assert id == "b2672944c68cbb449781106633b9a7600089173d9d4282f3920aa9e051c9b7c7"

def test_compare_weekday_string():
    assert compare_weekday('*', 0) == True
    assert compare_weekday('*', 3) == True
    assert compare_weekday('weekday', 4) == True
    assert compare_weekday('weekday', 5) == True
    assert compare_weekday('weekday', 6) == False
    assert compare_weekday('weekday', 7) == False
    assert compare_weekday('weekend', 7) == True
    assert compare_weekday('weekend', 6) == True
    assert compare_weekday('weekend', 3) == False
    assert compare_weekday('weekend', 5) == False
    
def test_compare_weekday_int():
    # WARNING : left paramater is from 0 to 6 where as the right one is from 1 to 7
    assert compare_weekday("1", 5) == False
    assert compare_weekday("3", 4) == True
    assert compare_weekday("0", 1) == True

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
        'program_metadata_id': ["005a5c81a377cd39a53302b246a436d456f1873bcbd6f6f5b24fef6adb57f083", "c6a310a4985f04dbdad1267097f2725da92e08dc7948dbceb4ed35cd62641d57", "6fc646caf4ed30c62298b482634a2a537a69467e3abfd9098d397e826a67454f"]
    })

    result_df = update_programs_and_filter_out_of_scope_programs_from_df(df, df_programs)

    # Assert the results match the expected DataFrame
    debug_df(result_df)
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)
