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

def test_insert_data_in_sitemap_table():
    create_tables()
    conn = connect_to_db()
    
    wrong_value = 0
    # insert data
    primary_key = "test_save_to_pg_keyword"
    keywords_with_timestamp = [{
                "keyword" : 'habitabilité de la planète',
                "timestamp": original_timestamp + 6, 
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'conditions de vie sur terre',
                "timestamp": original_timestamp + 10,
                "theme":"changement_climatique_constat",
            },
            {
                "keyword" : 'planète',
                "timestamp": original_timestamp + 9,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'terre',
                "timestamp": original_timestamp + 11,
                "theme":"ressources_naturelles_concepts_generaux",
            },
            {
                "keyword" : 'digue',
                "timestamp": original_timestamp + 12,
                "theme":"adaptation_climatique_solutions_indirectes",
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
        "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
        "channel_name": channel_name,
        "channel_radio": False,
        "theme": themes,
        "keywords_with_timestamp": keywords_with_timestamp
        ,"number_of_keywords": wrong_value # wrong data to reapply our custom logic for "new_value"
    }]) 
    df['start'] = pd.to_datetime(df['start'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Europe/Paris')
   
    assert save_to_pg(df._to_pandas(), keywords_table, conn) == 1

    # check the value is well existing
    result_before_update = get_keyword(primary_key)
    session = get_db_session(conn)
    update_keywords(session)
    result_after_update = get_keyword(primary_key)

    new_value = count_keywords_duration_overlap_without_indirect(keywords_with_timestamp, start)
    assert result_after_update.id == result_before_update.id
    assert result_after_update.number_of_keywords == new_value
    assert result_before_update.number_of_keywords == wrong_value
