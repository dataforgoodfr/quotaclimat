import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)
from quotaclimat.data_ingestion.scrap_sitemap import (add_primary_key, get_consistent_hash)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
def test_insert_data_in_sitemap_table():
    create_tables()
    session = get_db_session()
    conn = connect_to_db()
    wrong_value = 0
    # insezrt data
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
    channel_name = "m6"
    df = pd.DataFrame([{
        "id" : primary_key,
        "start": 1706437079006,
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": wrong_value # wrong data to reapply our custom logic for "new_value"
    }]) 
    df['start'] = pd.to_datetime(df['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')
   
    assert save_to_pg(df, keywords_table, conn) == 1

    # check the value is well existing
    result_before_update = get_keyword(primary_key)
    update_keywords(session)
    result_after_update = get_keyword(primary_key)

    new_value = count_keywords_duration_overlap(keywords_with_timestamp)
    assert result_after_update.id == result_before_update.id
    assert result_after_update.number_of_keywords == new_value
    assert result_before_update.number_of_keywords == wrong_value
