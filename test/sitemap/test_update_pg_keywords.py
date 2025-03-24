import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from quotaclimat.data_ingestion.scrap_sitemap import (get_consistent_hash)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db, drop_tables, empty_tables,keywords_table
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
import pandas as pd
from quotaclimat.data_processing.mediatree.stop_word.main import *

logging.getLogger().setLevel(logging.INFO)
original_timestamp = 1706271523 * 1000 # Sun Jan 28 2024 13:18:54 GMT+0100
start = pd.to_datetime("2024-01-26 12:18:54", utc=True).tz_convert('Europe/Paris')
start_tf1 = pd.to_datetime("2024-01-26 12:18:54", utc=True).tz_convert('Europe/Paris')
create_tables()

wrong_value = 0
m6 = "m6"
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
    assert get_keyword(primary_key, session) != None
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
            )
    session.commit()
    assert get_keyword(primary_key) == None
    session.close()

def test_first_update_keywords():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key = "test_save_to_pg_keyword"
   
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    session = get_db_session(conn)
    result_before_update = get_keyword(primary_key, session)
    update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30")
    result_after_update = get_keyword(primary_key, session)

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
        ,number_of_keywords_ressources \
        ,number_of_changement_climatique_constat_no_hrfp \
        ,number_of_changement_climatique_causes_no_hrfp \
        ,number_of_changement_climatique_consequences_no_hrfp \
        ,number_of_attenuation_climatique_solutions_no_hrfp \
        ,number_of_adaptation_climatique_solutions_no_hrfp \
        ,number_of_ressources_no_hrfp \
        ,number_of_ressources_solutions_no_hrfp \
        ,number_of_biodiversite_concepts_generaux_no_hrfp \
        ,number_of_biodiversite_causes_no_hrfp \
        ,number_of_biodiversite_consequences_no_hrfp \
        ,number_of_biodiversite_solutions_no_hrfp = get_themes_keywords_duration(plaintext, srt, start)

    assert result_after_update.id == result_before_update.id

    # theme
    assert set(new_theme) == set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(result_after_update.theme) == set(["adaptation_climatique_solutions", "changement_climatique_constat"])
        
    # keywords_with_timestamp
    assert len(result_after_update.keywords_with_timestamp) == len(new_keywords_with_timestamp)

    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update.number_of_keywords == new_value
    # assert result_before_update.number_of_keywords == wrong_value

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

    #channel_title
    assert result_after_update.channel_title == "M6"

    conn.dispose()
    session.close()
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }, {
        "id" : primary_key_tf1,
        "start": start_tf1,
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 2

    session = get_db_session(conn)
    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6, session)
    result_before_update_tf1 = get_keyword(primary_key_tf1, session)

    # Should only update tf1 because channel=tf1)
    update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30", channel=tf1)
    result_after_update_m6 = get_keyword(primary_key_m6, session)
    result_after_update_tf1 = get_keyword(primary_key_tf1, session)

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
        ,number_of_keywords_ressources \
        ,number_of_changement_climatique_constat_no_hrfp \
        ,number_of_changement_climatique_causes_no_hrfp \
        ,number_of_changement_climatique_consequences_no_hrfp \
        ,number_of_attenuation_climatique_solutions_no_hrfp \
        ,number_of_adaptation_climatique_solutions_no_hrfp \
        ,number_of_ressources_no_hrfp \
        ,number_of_ressources_solutions_no_hrfp \
        ,number_of_biodiversite_concepts_generaux_no_hrfp \
        ,number_of_biodiversite_causes_no_hrfp \
        ,number_of_biodiversite_consequences_no_hrfp \
        ,number_of_biodiversite_solutions_no_hrfp = get_themes_keywords_duration(plaintext, srt, start)

    conn.dispose()
    session.close()

    # theme
    assert set(new_theme) == set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(result_after_update_tf1.theme) == set(["adaptation_climatique_solutions", "changement_climatique_constat"])
        
    # keywords_with_timestamp
    assert len(result_after_update_tf1.keywords_with_timestamp) == len(new_keywords_with_timestamp)
   
    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update_tf1.number_of_keywords == new_value
    # assert result_before_update_tf1.number_of_keywords == wrong_value

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

    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
    un_jour_un_doc_m6_date = pd.to_datetime("2024-01-27 14:18:54", utc=True).tz_convert('Europe/Paris')
    df = pd.DataFrame([{
        "id" : primary_key_m6,
        "start": un_jour_un_doc_m6_date,
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
        ,"program_metadata_id": None
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    session = get_db_session(conn)
    result_before_update_m6 = get_keyword(primary_key_m6, session)
    
    # Should only update programs because program_only = True)
    update_keywords(session, batch_size=50, start_date="2024-01-01", program_only = True, end_date="2024-01-30")
 
    result_after_update_m6 = get_keyword(primary_key_m6, session)

    conn.dispose()
    session.close()
    assert result_after_update_m6.id == result_before_update_m6.id

    # theme - not updated because of program only
    assert set(result_after_update_m6.theme) == set(themes)
        
    # number_of_keywords - not updated because of program only
    assert result_after_update_m6.number_of_keywords == wrong_value
    assert result_before_update_m6.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat - not updated because of program only
    assert result_after_update_m6.number_of_changement_climatique_constat == wrong_value

    # number_of_adaptation_climatique_solutions_directes
    assert result_after_update_m6.number_of_adaptation_climatique_solutions_directes == wrong_value

    # program - only when UPDATE_PROGRAM_ONLY for speed issues
    assert result_after_update_m6.channel_program == "1245 le mag"
    assert result_after_update_m6.channel_program_type == "Information - Magazine"
    assert result_after_update_m6.id == "3fd350c088699dcdc6692e35fc030da2946ec44875319c9089a49c112312c64a"

    #channel_title
    assert result_after_update_m6.channel_title == "M6"

    # number_of_keywords_climat
    assert result_after_update_m6.number_of_keywords_climat == wrong_value

def test_update_only_program_with_only_one_channel():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
    primary_key_tf1 = "test_save_to_pg_keyword_only_program_tf1"
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }, {
        "id" : primary_key_tf1,
        "start": start_tf1,
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 2
    session = get_db_session(conn)
    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6, session)
    result_before_update_tf1 = get_keyword(primary_key_tf1, session)
    

    # Should only update programs because program_only = True and channel=tf1)
    update_keywords(session, batch_size=50, start_date="2024-01-01", program_only = True, end_date="2024-01-30", channel=tf1)
    result_after_update_m6 = get_keyword(primary_key_m6, session)
    result_after_update_tf1 = get_keyword(primary_key_tf1, session)

    conn.dispose()
    session.close()
    assert result_after_update_m6.id == result_before_update_m6.id
    assert result_after_update_tf1.id == result_before_update_tf1.id

    # theme - not updated because of program only
    assert set(result_after_update_m6.theme) == set(themes)
    assert set(result_after_update_tf1.theme) == set(themes)
        
    # number_of_keywords - not updated because of program only
    assert result_after_update_m6.number_of_keywords == wrong_value
    assert result_after_update_tf1.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat - not updated because of program only
    assert result_after_update_m6.number_of_changement_climatique_constat == wrong_value
    assert result_after_update_tf1.number_of_changement_climatique_constat == wrong_value

    # number_of_adaptation_climatique_solutions_directes
    assert result_after_update_m6.number_of_adaptation_climatique_solutions_directes == wrong_value
    assert result_after_update_tf1.number_of_adaptation_climatique_solutions_directes == wrong_value

    # program - only when UPDATE_PROGRAM_ONLY for speed issues
    assert result_after_update_m6.channel_program == "to change"
    assert result_before_update_m6.channel_program == "to change"
    assert result_after_update_m6.channel_program_type == "to change"
    assert result_before_update_m6.channel_program_type == "to change"
    ## TF1 should have changed because of channel=tf1
    assert result_after_update_tf1.channel_program == "JT 13h"
    assert result_after_update_tf1.channel_program_type == "Information - Journal"

    #channel_title
    assert result_after_update_m6.channel_title == None
    assert result_after_update_tf1.channel_title == "TF1"

    # number_of_keywords_climat
    assert result_after_update_m6.number_of_keywords_climat == wrong_value
    assert result_after_update_tf1.number_of_keywords_climat == wrong_value

def test_update_only_empty_program():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
   
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
    primary_key_tf1 = "test_save_to_pg_keyword_only_program_tf1"
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
        "channel_program_type": "",
        "channel_program":"" # Empty --> it's going to change
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }, {
        "id" : primary_key_tf1,
        "start": start_tf1,
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 2
    
    session = get_db_session(conn)
    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6, session)
    result_before_update_tf1 = get_keyword(primary_key_tf1, session)

    # Should only update programs because program_only = True)
    update_keywords(session, batch_size=50, start_date="2024-01-01", program_only = True, end_date="2024-01-30",\
                     empty_program_only=True
                   )
    result_after_update_m6 = get_keyword(primary_key_m6, session)
    result_after_update_tf1 = get_keyword(primary_key_tf1, session)
    conn.dispose()
    session.close()

    # program - only
    assert result_after_update_m6.channel_program == "1245 le mag"
    assert result_after_update_m6.channel_program_type == "Information - Magazine"


    ## TF1 should NOT changed because it has a value
    assert result_after_update_tf1.channel_program == "to change"
    assert result_before_update_tf1.channel_program == "to change"
    assert result_after_update_tf1.channel_program_type == "to change"
    assert result_before_update_tf1.channel_program_type == "to change"


def test_update_only_keywords_that_includes_some_keywords():
    conn = connect_to_db()
    plaintext_stop_word = " avait promis de lancer un plan de conditions de vie sur terre euh hélas pas pu climat tout s' est pa"
    # save some stop words
    stop_word_to_save = [
            {
                "keyword_id": "fake_id",
                "id": "test1",
                "keyword": "conditions de vie sur terre",
                "channel_title": "France 2",
                "context": plaintext_stop_word,
                "count": 20,
                "id" : get_consistent_hash(plaintext_stop_word),
            },
            {
                "keyword_id": "fake_id",
                "id": "test2",
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question",
                "count": 19,
                "id" : get_consistent_hash("lacieux selon les experts question climatique en fait elle dépasse la question"),
            }
        ]
    save_append_stop_word(conn, stop_word_to_save)

    wrong_value = 0
    # insert data
    primary_key = "test_save_to_pg_keyword"
   
    channel_name = "m6"
    df = pd.DataFrame([{
        "id" : primary_key,
        "start": start,
        "plaintext": plaintext + " " + plaintext_stop_word,
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
        "number_of_ressources":  2,
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    session = get_db_session(conn)
    result_before_update = get_keyword(primary_key, session)
    update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30", stop_word_keyword_only=True)
    result_after_update = get_keyword(primary_key, session)

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
        ,number_of_keywords_ressources \
        ,number_of_changement_climatique_constat_no_hrfp \
        ,number_of_changement_climatique_causes_no_hrfp \
        ,number_of_changement_climatique_consequences_no_hrfp \
        ,number_of_attenuation_climatique_solutions_no_hrfp \
        ,number_of_adaptation_climatique_solutions_no_hrfp \
        ,number_of_ressources_no_hrfp \
        ,number_of_ressources_solutions_no_hrfp \
        ,number_of_biodiversite_concepts_generaux_no_hrfp \
        ,number_of_biodiversite_causes_no_hrfp \
        ,number_of_biodiversite_consequences_no_hrfp \
        ,number_of_biodiversite_solutions_no_hrfp = get_themes_keywords_duration(plaintext, srt, start)
    
    assert result_after_update.id == result_before_update.id

    # theme
    expected_themes = set(["adaptation_climatique_solutions",  "changement_climatique_constat"])
    assert set(new_theme) == expected_themes
    assert set(result_after_update.theme) == expected_themes
        
    # keywords_with_timestamp
    assert len(result_after_update.keywords_with_timestamp) == len(new_keywords_with_timestamp)
    # Too hard to maintain for every new dict
    # assert compare_unordered_lists_of_dicts(expected_keywords_with_timestamp, new_keywords_with_timestamp)


    # number_of_keywords
    assert new_value == number_of_changement_climatique_constat + number_of_adaptation_climatique_solutions_directes
    assert result_after_update.number_of_keywords == new_value

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

    #channel_title
    assert result_after_update.channel_title == "M6"

    # number_of_keywords_climat
    conn.dispose()
    session.close()
    assert result_after_update.number_of_keywords_climat == number_of_keywords_climat


def test_get_top_keyword_of_stop_words_stop_word_keyword_only_True():
    sw1 = Stop_Word(
        id="test"
        ,keyword_id="test"
        ,channel_title="test"
        ,context="test_context"
        ,count=10
        ,keyword="keyword1"
        ,created_at="test"
        ,start_date="test"
        ,updated_at="test"
        ,validated=True
    )

    sw2 = Stop_Word(
        id="test"
        ,keyword_id="test"
        ,channel_title="test"
        ,context="test_context"
        ,count=10
        ,keyword="keyword2"
        ,created_at="test"
        ,start_date="test"
        ,updated_at="test"
        ,validated=True
    )

    sw3 = Stop_Word(
        id="test"
        ,keyword_id="test"
        ,channel_title="test"
        ,context="test_context"
        ,count=10
        # ,keyword="keyword2" empty keyword it should use context
        ,created_at="test"
        ,start_date="test"
        ,updated_at="test"
        ,validated=True
    )
    stop_words_objects = [sw1, sw1, sw1, sw2, sw3]

    output = get_top_keyword_of_stop_words(stop_word_keyword_only=True, stop_words_objects=stop_words_objects)
    expected = set(["keyword1", "keyword2", "test_context"])

    assert output == expected

def test_get_top_keyword_of_stop_words_stop_word_keyword_only_False():
    output = get_top_keyword_of_stop_words(stop_word_keyword_only=False, stop_words_objects=[])
    expected = []

    assert output == expected

def test_update_nothing_because_no_keywords_are_included():
    conn = connect_to_db()
    session = get_db_session(conn)
    empty_tables(session)
        # save some stop words
    stop_word_to_save = [
            {
                "keyword_id": "fake_id",
                "id": "test1",
                "keyword": "conditions de vie sur terre",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de conditions de vie sur terre euh hélas pas pu climat tout s' est pa",
                "count": 20,
                "id" : get_consistent_hash(" avait promis de lancer un plan de replantation euh hélas pas pu climat tout s' est pa"),
            },
            {
                "keyword_id": "fake_id",
                "id": "test2",
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question",
                "count": 19,
                "id" : get_consistent_hash("lacieux selon les experts question climatique en fait elle dépasse la question"),
            }
        ]
    save_append_stop_word(conn, stop_word_to_save)

    wrong_value = 0
    # insert data
    primary_key = "test_update_nothing_because_no_keywords_are_included"
    plaintext = "no keywords to match so the test should update nothing"
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
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  wrong_value,
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }])

    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    session = get_db_session(conn)
    result = update_keywords(session, batch_size=50, start_date="2024-01-01", end_date="2024-01-30", stop_word_keyword_only=True)
    conn.dispose()
    session.close()
    assert result == 0 # it means nothing to update
    

def test_update_only_biodiversity():
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
   
    primary_key_m6 = "test_save_to_pg_keyword_only_program_m6"
    primary_key_tf1 = "test_save_to_pg_keyword_only_program_tf1"
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
        "channel_program_type": "",
        "channel_program":"" # Empty --> it's going to change
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  wrong_value,
        "number_of_biodiversite_causes_no_hrfp":  5, # should update because of this
        "number_of_biodiversite_consequences_no_hrfp":  wrong_value,
        "number_of_biodiversite_solutions_no_hrfp" : wrong_value
    }, {
        "id" : primary_key_tf1,
        "start": start_tf1,
        "plaintext": plaintext,
        "channel_name": tf1,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp,
        "srt": srt,
        "number_of_keywords": 5,
        "number_of_changement_climatique_constat":  wrong_value,
        "number_of_changement_climatique_causes_directes":  wrong_value,
        "number_of_changement_climatique_consequences":  wrong_value,
        "number_of_attenuation_climatique_solutions_directes":  wrong_value,
        "number_of_adaptation_climatique_solutions_directes":  wrong_value,
        "number_of_ressources":  wrong_value,
        "number_of_ressources_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  0,
        "number_of_biodiversite_causes_directes":  0,
        "number_of_biodiversite_consequences":  0,
        "number_of_biodiversite_solutions_directes": 0,
        "channel_program_type": "to change",
        "channel_program":"to change"
        ,"channel_title":None
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
        ,"number_of_changement_climatique_constat_no_hrfp":  wrong_value,
        "number_of_changement_climatique_causes_no_hrfp":  wrong_value,
        "number_of_changement_climatique_consequences_no_hrfp":  wrong_value,
        "number_of_attenuation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_adaptation_climatique_solutions_no_hrfp":  wrong_value,
        "number_of_ressources_no_hrfp":  wrong_value,
        "number_of_ressources_solutions_no_hrfp":  wrong_value,
        "number_of_biodiversite_concepts_generaux_no_hrfp":  0,
        "number_of_biodiversite_causes_no_hrfp":  0,
        "number_of_biodiversite_consequences_no_hrfp":  0,
        "number_of_biodiversite_solutions_no_hrfp": 0
    }])

    assert save_to_pg(df, keywords_table, conn) == 2
    
    session = get_db_session(conn)
    # check the value is well existing
    result_before_update_m6 = get_keyword(primary_key_m6, session)
    result_before_update_tf1 = get_keyword(primary_key_tf1, session)

    # Should only biodiversity_only
    update_keywords(session, batch_size=50, start_date="2024-01-01", program_only = True, end_date="2024-01-30",\
                     biodiversity_only=True
                   )
    result_after_update_m6 = get_keyword(primary_key_m6, session)
    result_after_update_tf1 = get_keyword(primary_key_tf1, session)
    conn.dispose()
    session.close()

    # 6 has changed because of number_of_biodiversite > 0
    assert result_after_update_m6.channel_program == "1245 le mag"
    assert result_after_update_m6.channel_program_type == "Information - Magazine"


    ## TF1 should NOT changed because it has a value number_of_biodiversite at 0
    assert result_after_update_tf1.channel_program == "to change"
    assert result_before_update_tf1.channel_program == "to change"
    assert result_after_update_tf1.channel_program_type == "to change"
    assert result_before_update_tf1.channel_program_type == "to change"
