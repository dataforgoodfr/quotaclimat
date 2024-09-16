import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)
from quotaclimat.data_ingestion.scrap_sitemap import (add_primary_key, get_consistent_hash)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
import pandas as pd
from test_utils import get_localhost, debug_df, compare_unordered_lists_of_dicts

logging.getLogger().setLevel(logging.INFO)
original_timestamp = 1706271523 * 1000 # Sun Jan 28 2024 13:18:54 GMT+0100
start = pd.to_datetime("2024-01-26 12:18:54", utc=True).tz_convert('Europe/Paris')
create_tables()

plaintext = "cheese pizza habitabilité de la planète conditions de vie sur terre animal digue"
srt = [{
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 6,
        "text": "habitabilité"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 10,
        "text": "de"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 11,
        "text": "la"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 12,
        "text": "planète"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 15000,
        "text": "conditions"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 15000 + 10,
        "text": "de"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 15000  + 15,
        "text": "vie"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 15000  + 20,
        "text": "sur"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 15000  + 25,
        "text": "terre"
        },
        {
        "duration_ms": 34,
        "cts_in_ms": original_timestamp + 32000,
        "text": "digue"
        }
]

keywords_with_timestamp = [
    {
        "keyword": "conditions de vie sur terre",
        "timestamp": 1706437094004,
        "theme": "changement_climatique_constat"
    },
    {
        "keyword": "habitabilité de la planète",
        "timestamp": 1706444334006,
        "theme": "changement_climatique_constat"
    },
    {
        "keyword": "digue",
        "timestamp": 1706444366000,
        "theme": "adaptation_climatique_solutions_indirectes"
    }
]
themes = [
    "changement_climatique_constat",
    "adaptation_climatique_solutions",
    "ressources" # should be removed
]

def test_delete_keywords():
    conn = connect_to_db()
    primary_key = "delete_me"
    wrong_value = 0
    df = pd.DataFrame([{
    "id" : primary_key,
    "start": start,
    "plaintext": "test",
    "channel_name": "test",
    "channel_radio": False,
    "theme":[],
    "keywords_with_timestamp": [],
    "srt": [],
    "number_of_keywords": wrong_value, # wrong data to reapply our custom logic for "new_value"
    "number_of_changement_climatique_constat":  wrong_value,
    "number_of_changement_climatique_causes_directes":  wrong_value,
    "number_of_changement_climatique_consequences":  wrong_value,
    "number_of_attenuation_climatique_solutions_directes":  wrong_value,
    "number_of_adaptation_climatique_solutions_directes":  wrong_value,
    "number_of_ressources":  wrong_value,
    "number_of_ressources_solutions":  wrong_value,
    "number_of_biodiversite_concepts_generaux":  wrong_value,
    "number_of_biodiversite_causes_directes":  wrong_value,
    "number_of_biodiversite_consequences":  wrong_value,
    "number_of_biodiversite_solutions_directes" : wrong_value,
    "channel_program_type": "to change",
    "channel_program":"to change"
    ,"channel_title":"channel_title"
    ,"number_of_keywords_climat": wrong_value
    ,"number_of_keywords_biodiversite": wrong_value
    ,"number_of_keywords_ressources": wrong_value
    }])
    assert save_to_pg(df, keywords_table, conn) == 1
    session = get_db_session(conn)
    assert get_keyword(primary_key) != None
    update_keyword_row(session, primary_key,
            0,
            None,
            None
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,0
            ,"M6"
            ,0
            ,0
            ,0
            )
    session.commit()
    assert get_keyword(primary_key) == None

def test_first_update_keywords():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key = "test_save_to_pg_keyword"
   
    keywords_with_timestamp = [
        {
            "keyword": "conditions de vie sur terre",
            "timestamp": 1706437094004,
            "theme": "changement_climatique_constat"
        },
        {
            "keyword": "habitabilité de la planète",
            "timestamp": 1706444334006,
            "theme": "changement_climatique_constat"
        },
        {
            "keyword": "digue",
            "timestamp": 1706444366000,
            "theme": "adaptation_climatique_solutions_indirectes"
        }
    ]
    themes = [
        "changement_climatique_constat",
        "adaptation_climatique_solutions",
        "ressources" # should be removed
    ]
    channel_name = "m6"
    df = pd.DataFrame([{
        "id" : primary_key,
        "start": start,
        "plaintext": plaintext,
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp,
        "srt": srt,
        "number_of_keywords": wrong_value, # wrong data to reapply our custom logic for "new_value"
        "number_of_changement_climatique_constat":  wrong_value,
        "number_of_changement_climatique_causes_directes":  wrong_value,
        "number_of_changement_climatique_consequences":  wrong_value,
        "number_of_attenuation_climatique_solutions_directes":  wrong_value,
        "number_of_adaptation_climatique_solutions_directes":  wrong_value,
        "number_of_ressources":  wrong_value,
        "number_of_ressources_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  wrong_value,
        "number_of_biodiversite_causes_directes":  wrong_value,
        "number_of_biodiversite_consequences":  wrong_value,
        "number_of_biodiversite_solutions_directes" : wrong_value,
        "channel_program_type": "to change",
        "channel_program":"to change"
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    result_before_update = get_keyword(primary_key)
    session = get_db_session(conn)
    update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30")
    result_after_update = get_keyword(primary_key)

    new_theme, new_keywords_with_timestamp, new_value \
        ,number_of_changement_climatique_constat \
        ,number_of_changement_climatique_causes_directes \
        ,number_of_changement_climatique_consequences \
        ,number_of_attenuation_climatique_solutions_directes \
        ,number_of_adaptation_climatique_solutions_directes \
        ,number_of_ressources \
        ,number_of_ressources_solutions \
        ,number_of_biodiversite_concepts_generaux \
        ,number_of_biodiversite_causes_directes \
        ,number_of_biodiversite_consequences \
        ,number_of_biodiversite_solutions_directes  \
        ,number_of_keywords_climat \
        ,number_of_keywords_biodiversite \
        ,number_of_keywords_ressources = get_themes_keywords_duration(plaintext, srt, start)

    assert result_after_update.id == result_before_update.id

    # theme
    assert set(new_theme) == set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(result_after_update.theme) == set(["adaptation_climatique_solutions", "changement_climatique_constat"])
        
    # keywords_with_timestamp
    assert len(result_after_update.keywords_with_timestamp) == len(new_keywords_with_timestamp)
    # Too hard to maintain for every new dict
    # assert compare_unordered_lists_of_dicts(expected_keywords_with_timestamp, new_keywords_with_timestamp)


    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update.number_of_keywords == new_value
    assert result_before_update.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat
    assert number_of_changement_climatique_constat == 2
    assert result_after_update.number_of_changement_climatique_constat == 2

    # number_of_adaptation_climatique_solutions_directes
    assert number_of_adaptation_climatique_solutions_directes == 1
    assert result_after_update.number_of_adaptation_climatique_solutions_directes == 1


    assert number_of_ressources == 0

    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 0

    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0

    # program - only when UPDATE_PROGRAM_ONLY for speed issues
    # assert result_after_update.channel_program == "1245 le mag"
    # assert result_after_update.channel_program_type == "Information - Magazine"

    #channel_title
    assert result_after_update.channel_title == "M6"

    # number_of_keywords_climat
    assert result_after_update.number_of_keywords_climat == number_of_keywords_climat

def test_update_only_one_channel():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_m6"
    primary_key_tf1 = "test_save_to_pg_keyword_tf1"
    m6 = "m6"
    tf1 = "tf1"
    df = pd.DataFrame([{
        "id" : primary_key_m6,
        "start": start,
        "plaintext": plaintext,
        "channel_name": m6,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp,
        "srt": srt,
        "number_of_keywords": wrong_value, # wrong data to reapply our custom logic for "new_value"
        "number_of_changement_climatique_constat":  wrong_value,
        "number_of_changement_climatique_causes_directes":  wrong_value,
        "number_of_changement_climatique_consequences":  wrong_value,
        "number_of_attenuation_climatique_solutions_directes":  wrong_value,
        "number_of_adaptation_climatique_solutions_directes":  wrong_value,
        "number_of_ressources":  wrong_value,
        "number_of_ressources_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  wrong_value,
        "number_of_biodiversite_causes_directes":  wrong_value,
        "number_of_biodiversite_consequences":  wrong_value,
        "number_of_biodiversite_solutions_directes" : wrong_value,
        "channel_program_type": "to change",
        "channel_program":"to change"
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
    }, {
        "id" : primary_key_tf1,
        "start": start,
        "plaintext": plaintext,
        "channel_name": tf1,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp,
        "srt": srt,
        "number_of_keywords": wrong_value, # wrong data to reapply our custom logic for "new_value"
        "number_of_changement_climatique_constat":  wrong_value,
        "number_of_changement_climatique_causes_directes":  wrong_value,
        "number_of_changement_climatique_consequences":  wrong_value,
        "number_of_attenuation_climatique_solutions_directes":  wrong_value,
        "number_of_adaptation_climatique_solutions_directes":  wrong_value,
        "number_of_ressources":  wrong_value,
        "number_of_ressources_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  wrong_value,
        "number_of_biodiversite_causes_directes":  wrong_value,
        "number_of_biodiversite_consequences":  wrong_value,
        "number_of_biodiversite_solutions_directes" : wrong_value,
        "channel_program_type": "to change",
        "channel_program":"to change"
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 2

    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6)
    result_before_update_tf1 = get_keyword(primary_key_tf1)

    session = get_db_session(conn)
    # Should only update tf1 because channel=tf1)
    update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30", channel=tf1)
    result_after_update_m6 = get_keyword(primary_key_m6)
    result_after_update_tf1 = get_keyword(primary_key_tf1)

    new_theme, new_keywords_with_timestamp, new_value \
        ,number_of_changement_climatique_constat \
        ,number_of_changement_climatique_causes_directes \
        ,number_of_changement_climatique_consequences \
        ,number_of_attenuation_climatique_solutions_directes \
        ,number_of_adaptation_climatique_solutions_directes \
        ,number_of_ressources \
        ,number_of_ressources_solutions \
        ,number_of_biodiversite_concepts_generaux \
        ,number_of_biodiversite_causes_directes \
        ,number_of_biodiversite_consequences \
        ,number_of_biodiversite_solutions_directes  \
        ,number_of_keywords_climat \
        ,number_of_keywords_biodiversite \
        ,number_of_keywords_ressources = get_themes_keywords_duration(plaintext, srt, start)

    assert result_after_update_tf1.id == result_before_update_tf1.id
    assert result_after_update_m6.id == result_before_update_m6.id

    # theme
    assert set(new_theme) == set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(result_after_update_tf1.theme) == set(["adaptation_climatique_solutions", "changement_climatique_constat"])
        
    # keywords_with_timestamp
    assert len(result_after_update_tf1.keywords_with_timestamp) == len(new_keywords_with_timestamp)
   
    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update_tf1.number_of_keywords == new_value
    assert result_before_update_tf1.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat
    assert number_of_changement_climatique_constat == 2
    assert result_after_update_tf1.number_of_changement_climatique_constat == 2

    # number_of_adaptation_climatique_solutions_directes
    assert number_of_adaptation_climatique_solutions_directes == 1
    assert result_after_update_tf1.number_of_adaptation_climatique_solutions_directes == 1


    assert number_of_ressources == 0

    assert number_of_changement_climatique_causes_directes == 0
    assert number_of_changement_climatique_consequences == 0
    assert number_of_attenuation_climatique_solutions_directes == 0

    assert number_of_ressources_solutions == 0
    assert number_of_biodiversite_concepts_generaux == 0
    assert number_of_biodiversite_causes_directes == 0
    assert number_of_biodiversite_consequences == 0
    assert number_of_biodiversite_solutions_directes == 0

    #channel_title
    assert result_after_update_tf1.channel_title == "TF1"
    assert result_after_update_m6.channel_title == None # M6 was NOT updated because of parameter channel

    # number_of_keywords_climat
    assert result_after_update_tf1.number_of_keywords_climat == number_of_keywords_climat

def test_update_only_program():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_m6"
   
    m6 = "m6"
    df = pd.DataFrame([{
        "id" : primary_key_m6,
        "start": start,
        "plaintext": plaintext,
        "channel_name": m6,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp,
        "srt": srt,
        "number_of_keywords": wrong_value, # wrong data to reapply our custom logic for "new_value"
        "number_of_changement_climatique_constat":  wrong_value,
        "number_of_changement_climatique_causes_directes":  wrong_value,
        "number_of_changement_climatique_consequences":  wrong_value,
        "number_of_attenuation_climatique_solutions_directes":  wrong_value,
        "number_of_adaptation_climatique_solutions_directes":  wrong_value,
        "number_of_ressources":  wrong_value,
        "number_of_ressources_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  wrong_value,
        "number_of_biodiversite_causes_directes":  wrong_value,
        "number_of_biodiversite_consequences":  wrong_value,
        "number_of_biodiversite_solutions_directes" : wrong_value,
        "channel_program_type": "to change",
        "channel_program":"to change"
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6)
    
    session = get_db_session(conn)
    # Should only update tf1 because channel=tf1)
    update_keywords(session, batch_size=50, start_date="2024-01-01", program_only = True, end_date="2024-01-30")
    result_after_update_m6 = get_keyword(primary_key_m6)

    new_theme, new_keywords_with_timestamp, new_value \
        ,number_of_changement_climatique_constat \
        ,number_of_changement_climatique_causes_directes \
        ,number_of_changement_climatique_consequences \
        ,number_of_attenuation_climatique_solutions_directes \
        ,number_of_adaptation_climatique_solutions_directes \
        ,number_of_ressources \
        ,number_of_ressources_solutions \
        ,number_of_biodiversite_concepts_generaux \
        ,number_of_biodiversite_causes_directes \
        ,number_of_biodiversite_consequences \
        ,number_of_biodiversite_solutions_directes  \
        ,number_of_keywords_climat \
        ,number_of_keywords_biodiversite \
        ,number_of_keywords_ressources = get_themes_keywords_duration(plaintext, srt, start)

    assert result_after_update_m6.id == result_before_update_m6.id

    # theme
    assert set(new_theme) == set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(result_after_update_m6.theme) == set(["adaptation_climatique_solutions", "changement_climatique_constat"])
        
    # keywords_with_timestamp
    assert len(result_after_update_m6.keywords_with_timestamp) == len(new_keywords_with_timestamp)
   
    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update_m6.number_of_keywords == wrong_value
    assert result_before_update_m6.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat
    assert number_of_changement_climatique_constat == 2
    assert result_after_update_m6.number_of_changement_climatique_constat == wrong_value

    # number_of_adaptation_climatique_solutions_directes
    assert number_of_adaptation_climatique_solutions_directes == wrong_value
    assert result_after_update_m6.number_of_adaptation_climatique_solutions_directes == wrong_value


    assert number_of_ressources == wrong_value

    assert number_of_changement_climatique_causes_directes == wrong_value
    assert number_of_changement_climatique_consequences == wrong_value
    assert number_of_attenuation_climatique_solutions_directes == wrong_value

    assert number_of_ressources_solutions == wrong_value
    assert number_of_biodiversite_concepts_generaux == wrong_value
    assert number_of_biodiversite_causes_directes == wrong_value
    assert number_of_biodiversite_consequences == wrong_value
    assert number_of_biodiversite_solutions_directes == wrong_value

    # program - only when UPDATE_PROGRAM_ONLY for speed issues
    assert result_after_update_m6.channel_program == "1245 le mag"
    assert result_after_update_m6.channel_program_type == "Information - Magazine"

    #channel_title
    assert result_after_update_m6.channel_title == None

    # number_of_keywords_climat
    assert result_after_update_m6.number_of_keywords_climat == wrong_value
