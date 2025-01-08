import logging

import pandas as pd

from quotaclimat.data_processing.mediatree.stop_word.main import *
from postgres.schemas.models import get_db_session, connect_to_db, drop_tables
from test_main_import_api import insert_mediatree_json

conn = connect_to_db()
session = get_db_session(conn)
drop_tables()
insert_mediatree_json(conn, json_file_path='test/sitemap/short_mediatree.json')

def test_get_top_keywords_by_channel():
    # conn = connect_to_db()
    # session = get_db_session(conn)
    # insert_mediatree_json(conn, json_file_path='test/sitemap/short_mediatree.json')
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

def test_get_all_repetitive_context_advertising_for_a_keyword():
        conn = connect_to_db()
        session = get_db_session(conn)
       
        excepted_df = [
            {
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                "count": 20 # min number of repetition
            }
        ]
        keyword1 =  "replantation"
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="France 2",days=3000, length_context=35)
        assert top_context == excepted_df


# TODO improve FOR LEAST(LENGTH("public"."keywords"."plaintext"), POSITION('{keyword}'
def test_get_all_repetitive_context_advertising_for_a_keyword_utf8_min_number_of_repeatition():
        conn = connect_to_db()
        session = get_db_session(conn)
        min_number_of_repeatition=1
        excepted_df = [
            {
                "keyword": "agroécologie",
                "channel_title": "TF1",
                "context": " climatique a",
                "count": 2 # min number of repetition
            }
        ]
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", days=3000,\
                                                                            length_context=35, min_number_of_repeatition=min_number_of_repeatition)
        assert top_context == excepted_df

def test_get_all_repetitive_context_advertising_for_a_keyword_not_enough_repetition():
        conn = connect_to_db()
        session = get_db_session(conn)
       
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", days=3000, length_context=35)
        assert len(top_context) == 0

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

        excepted = [
            {
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                "count": 20
            },
            {
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question ",
                "count": 20,
            }
        ]

        top_context = get_repetitive_context_advertising(session, top_keywords=top_keywords, days=3000, length_context_to_look_for_repetition=35)
        assert top_context == excepted