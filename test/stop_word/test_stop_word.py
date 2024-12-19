import logging

import pandas as pd

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)
from quotaclimat.data_processing.mediatree.stop_word.main import (get_top_keywords_by_channel)
from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_existing_data_example import (
    parse_section, transformation_from_dumps_to_table_entry)
from postgres.schemas.models import create_tables, get_sitemap, connect_to_db
from postgres.insert_data import save_to_pg
from postgres.schemas.models import get_stop_word, stop_word_table, keywords_table
session = get_db_session()

original_timestamp = 1706271523 * 1000 # Sun Jan 28 2024 13:18:54 GMT+0100
wrong_value = 0
primary_key="m6_stop_word"
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
        "keyword": "accord de paris",
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

row = {
    "id" : primary_key,
    "start": original_timestamp,
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
}

df = pd.DataFrame([row | {'id': 1}, row| {'id': 2}, row| {'id': 3}, row| {'id': 4}, row| {'id': 5}, row| {'id': 6}])

save_to_pg(df, keywords_table, session)

def test_get_top_keywords_by_channel():
    df_to_save_2 = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name",
                "news_title": "title",
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
            }
        ]
    )
    
    
    top_keywords = get_top_keywords_by_channel(session, days=3000, top=5)

    pd.testing.assert_frame_equal(top_keywords, df_to_save_2)