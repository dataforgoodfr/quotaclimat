import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)
from quotaclimat.data_ingestion.scrap_sitemap import (add_primary_key, get_consistent_hash)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *

original_timestamp = 1706437079004
start = datetime.utcfromtimestamp(original_timestamp / 1000)

def test_first_update_keywords():
    create_tables()
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key = "test_save_to_pg_keyword"
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
            "cts_in_ms": original_timestamp + 15500,
            "text": "de"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 16000,
            "text": "vie"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 17000,
            "text": "sur"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 18000,
            "text": "terre"
            },
            {
            "duration_ms": 34,
            "cts_in_ms": original_timestamp + 32000,
            "text": "digue"
            }
    ]

    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 6, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 15000,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète', # should not be there
                "timestamp": original_timestamp + 9,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre', # should not be there
                "timestamp": original_timestamp + 11,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'digue',
                "timestamp": original_timestamp + 32000,
                "theme":"wrong_theme",
            }
        ]
    themes = [
            "changement_climatique_constat",
            "ressources_naturelles_concepts_generaux",
            "adaptation_climatique_solutions_indirectes"
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
        "number_of_ressources_naturelles_concepts_generaux":  wrong_value,
        "number_of_ressources_naturelles_causes":  wrong_value,
        "number_of_ressources_naturelles_solutions":  wrong_value,
        "number_of_biodiversite_concepts_generaux":  wrong_value,
        "number_of_biodiversite_causes_directes":  wrong_value,
        "number_of_biodiversite_consequences":  wrong_value,
        "number_of_biodiversite_solutions_directes" : wrong_value
    }]) 
    df['start'] = pd.to_datetime(df['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')

    assert save_to_pg(df._to_pandas(), keywords_table, conn) == 1

    # check the value is well existing
    result_before_update = get_keyword(primary_key)
    session = get_db_session(conn)
    update_keywords(session, batch_size=50)
    result_after_update = get_keyword(primary_key)

    new_theme, new_keywords_with_timestamp, new_value \
        ,number_of_changement_climatique_constat \
        ,number_of_changement_climatique_causes_directes \
        ,number_of_changement_climatique_consequences \
        ,number_of_attenuation_climatique_solutions_directes \
        ,number_of_adaptation_climatique_solutions_directes \
        ,number_of_ressources_naturelles_concepts_generaux \
        ,number_of_ressources_naturelles_causes \
        ,number_of_ressources_naturelles_solutions \
        ,number_of_biodiversite_concepts_generaux \
        ,number_of_biodiversite_causes_directes \
        ,number_of_biodiversite_consequences \
        ,number_of_biodiversite_solutions_directes = get_themes_keywords_duration(plaintext, srt, start)

    expected_keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 6, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 15000,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'digue',
                "timestamp": original_timestamp + 32000,
                "theme":"adaptation_climatique_solutions_indirectes",
            }
    ]
    assert result_after_update.id == result_before_update.id

    # number_of_keywords
    assert new_value == 2
    assert result_after_update.number_of_keywords == new_value
    assert result_before_update.number_of_keywords == wrong_value

    # number_of_changement_climatique_constat
    assert number_of_changement_climatique_constat == new_value
    assert result_after_update.number_of_changement_climatique_constat == new_value

    # keywords_with_timestamp
    assert result_after_update.keywords_with_timestamp == new_keywords_with_timestamp
    assert expected_keywords_with_timestamp == new_keywords_with_timestamp

    # theme
    assert result_after_update.theme == ["changement_climatique_constat", "adaptation_climatique_solutions_indirectes"]
    assert new_theme == ["changement_climatique_constat", "adaptation_climatique_solutions_indirectes"]