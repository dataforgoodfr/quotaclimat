import logging

import pandas as pd

from quotaclimat.data_processing.mediatree.stop_word.main import *
from postgres.schemas.models import get_db_session, connect_to_db
from test_main_import_api import insert_mediatree_json



def test_get_top_keywords_by_channel():
    conn = connect_to_db()
    session = get_db_session(conn)
    insert_mediatree_json(conn, json_file_path='test/sitemap/short_mediatree.json')
    excepted_df = pd.DataFrame(
        [
            {
                "keyword": "agroécologie",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 8,
            },{
                "keyword": "replantation",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 4,
            },{
                "keyword": "végétation",
                "theme": "ressources",
                "channel_title": "France 2",
                "count": 4,
            },
            {
                "keyword": "agroécologie",
                "theme": "ressources_solutions",
                "channel_title": "TF1",
                "count": 8,
            },
            {
                "keyword": "climatique",
                "theme": "changement_climatique_constat",
                "channel_title": "TF1",
                "count": 2,
            }
        ]
    )
    
    
    top_keywords = get_top_keywords_by_channel(session, days=3000, top=5)
    assert len(top_keywords) != 0
    pd.testing.assert_frame_equal(top_keywords, excepted_df)

def test_get_repetitive_context_advertising():
        conn = connect_to_db()
        session = get_db_session(conn)
        top_keywords = pd.DataFrame(
        [
            {
                "keyword": "agroécologie",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 8,
            },{
                "keyword": "replantation",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 4,
            },{
                "keyword": "végétation",
                "theme": "ressources",
                "channel_title": "France 2",
                "count": 4,
            },
            {
                "keyword": "agroécologie",
                "theme": "ressources_solutions",
                "channel_title": "TF1",
                "count": 8,
            },
            {
                "keyword": "climatique",
                "theme": "changement_climatique_constat",
                "channel_title": "TF1",
                "count": 2,
            }
        ]
        )   
        
        get_repetitive_context_advertising(session, top_keywords, days=3000, top=5)