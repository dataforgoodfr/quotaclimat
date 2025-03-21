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
        ,"program_metadata_id": "2f2f98f25039e5da9c612f949cc26b8e56c7a09598b9b8493876254f7c36b8f1"
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
        ,"program_metadata_id": "0b7c372429898afdd4ce2ca8a23ac24cb0ff35e9df2e3294c99dbb01e86c94c8"
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
        ,"program_metadata_id": ""
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
        ,"program_metadata_id": "638c2b3a28772f742291433dfbc78238cb5fa4299cffbbc8da7da6dc9ae68fb3"
    }])
    expected['start'] = pd.to_datetime(jt_20h02, unit='s', utc=True).tz_convert('Europe/Paris')
    #debug_df(output)
    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected.reset_index(drop=True))

def test_get_programs_for_this_day_thusday_morning_france2():
    df_programs = get_programs()
    programs = get_programs_for_this_day(pd.to_datetime(thrusday_morning, unit='s').tz_localize('Europe/Paris'), channel_name, df_programs)
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
         "program_type":"Information en continu"
         },
 ])

    pd.testing.assert_frame_equal(programs._to_pandas().reset_index(drop=True), expected.reset_index(drop=True))

def test_get_a_program_with_start_timestamp():
    df_programs = get_programs()
    thursday_13h34 = 1713958465 # normaly up to 13h40
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs, pd.to_datetime(thursday_13h34, unit='s', utc=True).tz_convert('Europe/Paris'), channel_name)
    assert program_name == "JT 13h"
    assert program_type == "Information - Journal"
    assert id == "2d0336de95eeb7f9a88bafe02b0350641f9a0ad1ce7f3bdd55c2aa97724e574d"

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
    assert id == "332db0e88e0ff6e7b49351ebee449fd06ee8aa2f25a735d639bd01be3f41d120"

def test_get_13h_monday_rfi_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h05 = 1726398337
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h05, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "9a80a5dd4be51d256587b03d1418acc1e13afc1d68d5582ebd26c91a6db1c4d5"


def test_get_13h_monday_rfi_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    monday_13h12 = 1726398730
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(monday_13h12, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "rfi")
    assert program_name == "Journal - 13h"
    assert program_type == "Information - Journal"
    assert id == "9a80a5dd4be51d256587b03d1418acc1e13afc1d68d5582ebd26c91a6db1c4d5"


def test_get_6h26_friday_fr2_with_margin_program_with_start_timestamp():
    df_programs = get_programs()
    friday_6h26 = 1726719981
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_6h26, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "Le 6h Info"
    assert program_type == "Information - Journal"
    assert id == "9bcbdd5a9e2877d13a842fa030b9f40a30d15e573915ae5903cc27418bc0f188"


def test_get_old_jt_20hweekday_20h19_friday_fr2():
    df_programs = get_programs()
    friday_20h19 = 1722622741
    program_name, program_type, id = get_a_program_with_start_timestamp(df_programs,\
                                                                    pd.to_datetime(friday_20h19, unit='s', utc=True).tz_convert('Europe/Paris'),\
                                                                    "france2")
    assert program_name == "JT 20h + météo"
    assert program_type == "Information - Journal"
    assert id == "8c49d82424cf80e107b2b89b0144b4ccf1719372bd68a6638d13147e9d6a5c02"

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
    assert id == "2a44150d6f83344ac1b618a8c600b8e8128ac305aa2696cfe8ad00e5737b7ee7"

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
        'program_metadata_id': ["78edf144b1a33df9da26ce736e5f14cc7b754eb38d67a19c37569c45a73667cd", "8c49d82424cf80e107b2b89b0144b4ccf1719372bd68a6638d13147e9d6a5c02", "9bcbdd5a9e2877d13a842fa030b9f40a30d15e573915ae5903cc27418bc0f188"]
    })

    # Run the function
    result_df = update_programs_and_filter_out_of_scope_programs_from_df(df, df_programs)

    # Assert the results match the expected DataFrame
    debug_df(result_df)
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)


def test_generate_program_id():
    generate_program_id()