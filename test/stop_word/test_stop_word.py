import logging

import pandas as pd

from quotaclimat.data_processing.mediatree.stop_word.main import *
from postgres.schemas.models import get_db_session, connect_to_db, drop_tables
from test_main_import_api import insert_mediatree_json
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
conn = connect_to_db()
session = get_db_session(conn)
drop_tables()
create_tables()
insert_mediatree_json(conn, json_file_path='test/sitemap/short_mediatree.json')

def test_stop_word_get_top_keywords_by_channel():
    # conn = connect_to_db()
    # session = get_db_session(conn)
    # insert_mediatree_json(conn, json_file_path='test/sitemap/short_mediatree.json')
    excepted_df = pd.DataFrame(
        [
            {
                "keyword": "replantation",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 80,
            },{
                "keyword": "climatique",
                "theme": "changement_climatique_constat",
                "channel_title": "France 2",
                "count": 20,
            },{
                "keyword": "sortie des énergies fossiles",
                "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 20,
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
    
    
    top_keywords = get_top_keywords_by_channel(session, duration=3000, top=5, min_number_of_keywords=1)
    assert len(top_keywords) != 0
    pd.testing.assert_frame_equal(top_keywords, excepted_df)

def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_default():
        conn = connect_to_db()
        session = get_db_session(conn)
       
        excepted_df = [
            {
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                'id': '4bd208a8e3b14f2ac46e272647729f05fb7588e427ce12d99bde6d5698415970',
                "count": 20 # min number of repetition
            }
        ]
        keyword1 =  "replantation"
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="France 2",days=3000)
        assert top_context == excepted_df


def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_utf8_min_number_of_repeatition():
        conn = connect_to_db()
        session = get_db_session(conn)
        min_number_of_repeatition=1 # important for this test

        excepted_df = [
            {
                'channel_title': 'TF1',
                'context': "agroécologie végétation dans l' antre des las vegas raiders c' est ici que se j",
                'count': 1,
                'id': '06130961a8c4556edfd80084d9cf413819b8ba2d91dc8f90cca888585fac8adc',
                'keyword': 'agroécologie',
            },
            {
                'channel_title': 'TF1',
                'context': "agroécologie végétation hasard peter aussi mène contre sébastien à l' heure deu",
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash("agroécologie végétation hasard peter aussi mène contre sébastien à l' heure deu")
            },
            {
                'channel_title': 'TF1',
                'context': 'climatique agroécologie est le hameau de la cuisine pensez à ce sujet '
                'quinze an',
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash('climatique agroécologie est le hameau de la cuisine pensez à ce sujet quinze an')
            },
            {
                'channel_title': 'TF1',
                'context': "climatique agroécologie moment-là parce que l' éblouissement au "
                'balcon de bucki',
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash("climatique agroécologie moment-là parce que l' éblouissement au balcon de bucki")
            },
        ]
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", days=3000,\
                                                                            min_number_of_repeatition=min_number_of_repeatition)
        assert top_context == excepted_df

def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_not_enough_repetition():
        conn = connect_to_db()
        session = get_db_session(conn)
       
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json - only 1 repeat
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", days=3000)
        assert len(top_context) == 0

def test_stop_word_get_repetitive_context_advertising():
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
                "count": 20,
                "id" : get_consistent_hash(" avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa")
            },
            {
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question ",
                "count": 20,
                "id" : get_consistent_hash("lacieux selon les experts question climatique en fait elle dépasse la question "),
            }
        ]

        top_context = get_repetitive_context_advertising(session, top_keywords=top_keywords, days=3000)
        assert top_context == excepted


def test_stop_word_save_append_stop_word():
    conn = connect_to_db()

    to_save = [
            {
                "id": "test1",
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                "count": 20,
                "id" : get_consistent_hash(" avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"),
            },
            {
                "id": "test2",
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question ",
                "count": 19,
                "id" : get_consistent_hash("lacieux selon les experts question climatique en fait elle dépasse la question "),
            }
        ]
    save_append_stop_word(conn, to_save)

    # get all stop word from db
    stop_words = get_all_stop_word(session)
    
    assert len(stop_words) == 2
    assert stop_words[0].keyword == "replantation"
    assert stop_words[1].keyword == "climatique"
    assert stop_words[0].count == 20
    assert stop_words[1].count == 19
    assert stop_words[0].channel_title == "France 2"
    assert stop_words[1].channel_title == "TF1"
    assert stop_words[0].context == " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"
    assert stop_words[1].context == "lacieux selon les experts question climatique en fait elle dépasse la question "

def test_stop_word_main():
       conn = connect_to_db()
       manage_stop_word(conn=conn, duration=3000)
       # get all stop word from db
       stop_words = get_all_stop_word(session)
      
       assert len(stop_words) == 2
