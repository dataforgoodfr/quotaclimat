import logging

import pandas as pd

from quotaclimat.data_processing.mediatree.stop_word.main import *
from postgres.schemas.models import get_db_session, connect_to_db, drop_tables
from test_main_import_api import insert_mediatree_json
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
import zoneinfo
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
                # "theme": "ressources_solutions",
                "channel_title": "France 2",
                "count": 80,
            },{
                "keyword": "climatique",
                # "theme": "changement_climatique_constat",
                "channel_title": "France 2",
                "count": 20,
            },{
                "keyword": "sortie des énergies fossiles",
                # "theme": "attenuation_climatique_solutions",
                "channel_title": "France 2",
                "count": 20,
            },
            {
                "keyword": "agroécologie",
                # "theme": "ressources_solutions",
                "channel_title": "TF1",
                "count": 8,
            },
            {
                "keyword": "climatique",
                # "theme": "changement_climatique_constat",
                "channel_title": "TF1",
                "count": 2,
            }
        ]
    )
    
    
    top_keywords = get_top_keywords_by_channel(session, duration=3000, top=5, min_number_of_keywords=1)
    top_keywords.drop(columns=["theme"], inplace=True) # can be several themes, so dropping theme for tests

    assert len(top_keywords) != 0
    pd.testing.assert_frame_equal(top_keywords, excepted_df)

def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_default():
        conn = connect_to_db()
        session = get_db_session(conn)
        start_date = datetime(2025, 1, 14, 15, 18, 43, 807525, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))
        excepted_df = [
            {
                "keyword": "replantation",
                'keyword_id': 'f9761d34d1e9adfc44bab9ad220e216b1a9a6a0aca44c39c5fab5115fe098d79',
                'start_date': start_date,
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                'id': '4bd208a8e3b14f2ac46e272647729f05fb7588e427ce12d99bde6d5698415970',
                "count": 20 # min number of repetition
            }
        ]
        keyword1 =  "replantation"
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="France 2",duration=3000,from_date=start_date)
        assert top_context == excepted_df


def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_utf8_min_number_of_repetition():
        conn = connect_to_db()
        session = get_db_session(conn)
        start_date = datetime(2025, 1, 14, 15, 18, 43, 807525, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))
        min_number_of_repetition=1 # important for this test

        excepted_df = [
            {
                'channel_title': 'TF1',
                'keyword_id': 'eb0c12caa8a290c8e8a4a4d17e0ee5a943986fc29d14f54f0e1e30becd6aae32',
                'start_date': start_date,
                'context': "agroécologie végétation dans l' antre des las vegas raiders c' est ici que se j",
                'count': 1,
                'id': '06130961a8c4556edfd80084d9cf413819b8ba2d91dc8f90cca888585fac8adc',
                'keyword': 'agroécologie',
            },
            {
                'channel_title': 'TF1',
                'keyword_id': '0b4d8ca4c5d512acb1fac4731e8cb46dde85be7e875c7510c84cc6b6d4ada2f4',
                'start_date': start_date,
                'context': "agroécologie végétation hasard peter aussi mène contre sébastien à l' heure deu",
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash("agroécologie végétation hasard peter aussi mène contre sébastien à l' heure deu")
            },
            {
                'channel_title': 'TF1',
                'keyword_id': '94a088d85f338c5857e64c0ab8b4f3232db3a52d906264eea712701a2ad31cd4',
                'start_date': start_date,
                'context': 'climatique agroécologie est le hameau de la cuisine pensez à ce sujet '
                'quinze an',
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash('climatique agroécologie est le hameau de la cuisine pensez à ce sujet quinze an')
            },
            {
                'channel_title': 'TF1',
                'keyword_id': '1571457f2fb35ff37ca3cb9eaa9040606497baaf5e6ad5d6a42c69b12c596596',
                'start_date': start_date,
                'context': "climatique agroécologie moment-là parce que l' éblouissement au "
                'balcon de bucki',
                'count': 1,
                'keyword': 'agroécologie',
                "id" : get_consistent_hash("climatique agroécologie moment-là parce que l' éblouissement au balcon de bucki")
            },
        ]
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", duration=3000,\
                                                                            min_number_of_repetition=min_number_of_repetition, from_date=start_date)
        assert top_context == excepted_df


def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_with_quote():
        conn = connect_to_db()
        session = get_db_session(conn)
        min_number_of_repetition=1 # important for this test

        excepted_df = []
        keyword1 =  "manque d' eau" # not used enough in short_mediatree.json
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", duration=3000,\
                                                                            min_number_of_repetition=min_number_of_repetition)
        assert top_context == excepted_df



def test_stop_word_get_all_repetitive_context_advertising_for_a_keyword_not_enough_repetition():
        conn = connect_to_db()
        session = get_db_session(conn)
       
        keyword1 =  "agroécologie" # not used enough in short_mediatree.json - only 1 repeat
        top_context = get_all_repetitive_context_advertising_for_a_keyword(session, keyword1, channel_title="TF1", duration=3000)
        assert len(top_context) == 0

def test_stop_word_get_repetitive_context_advertising():
        conn = connect_to_db()
        session = get_db_session(conn)
        start_date = datetime(2025, 1, 14, 15, 29, 59, 329453, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))
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
                "keyword_id": "f9761d34d1e9adfc44bab9ad220e216b1a9a6a0aca44c39c5fab5115fe098d79",
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                "count": 20,
                "id" : get_consistent_hash(" avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"),
                'start_date': start_date,
            },
            {
                "keyword_id": "f9761d34d1e9adfc44bab9ad220e216b1a9a6a0aca44c39c5fab5115fe098d79",
                "keyword": "climatique",
                "channel_title": "TF1",
                "context": "lacieux selon les experts question climatique en fait elle dépasse la question ",
                "count": 20,
                "id" : get_consistent_hash("lacieux selon les experts question climatique en fait elle dépasse la question "),
                'start_date': start_date,
            }
        ]

        top_context = get_repetitive_context_advertising(session, top_keywords=top_keywords, days=3000, from_date=start_date)
        assert top_context == excepted


def test_stop_word_save_append_stop_word():
    conn = connect_to_db()

    to_save = [
            {
                "keyword_id": "fake_id",
                "id": "test1",
                "keyword": "replantation",
                "channel_title": "France 2",
                "context": " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa",
                "count": 20,
                "id" : get_consistent_hash(" avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"),
            },
            {
                "keyword_id": "fake_id",
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


def test_stop_word_is_already_known_stop_word():
       context1_avait= " avait promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"
       context2_avais= " avais promis de lancer un plan de replantation euh hélas pas pu tout s' est pa"
       assert is_already_known_stop_word(context1_avait, context2_avais) == True

def test_stop_word_save_append_stop_word_duplicate():
    conn = connect_to_db()
    save =  [{'context': 'ts historiques les bouleversements climatiques de très fortes chaleurs les vict', 'keyword_id': 'fca7a8626fb7762ba6849f1cbc3b2708099118352442025e9e99b8b451e698d3', 'count': 41, 'id': 'a9017d023deadedd24a915ea3ad2eac5c79a94a9dcd87214fc3a0378b0acf8c5', 'keyword': 'climatique', 'channel_title': 'Arte', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'tion des conflits les dérèglements climatiques et la crise économique aggrave l', 'keyword_id': 'd9e9290ef2fa15ab4d54e387bc3ef6204c5b34e0effa00f2479d9ed51442cbd6', 'count': 27, 'id': 'd2f44307d8057427f48e67c2400d9e3e8cb175d080f0f492a168c794178bb4dc', 'keyword': 'climatique', 'channel_title': 'Arte', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'aux pollutions et aux dérèglements climatiques olivier emond embarqué à bord de', 'keyword_id': 'e683a12b0260af1a9f22717165e869e30ed13dbb6b5da54b6c3c2adcd3b02308', 'count': 23, 'id': '5637bf04ae3d04d76a952d7d3864795fdb32d8d8f79a86b31115b5ab46cad9d9', 'keyword': 'climatique', 'channel_title': 'Arte', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': "nt de la lpo comment accueillir la biodiversité limiter l' érosion stocker le c", 'keyword_id': 'f2c11b05a19ad10e662c0669bbac7887c490b50b80cce9c3253ced24e5c7515c', 'count': 36, 'id': '09a1bb6f54bb34134d4010ce8c97991983ece3be1f598210596ff026612534fa', 'keyword': 'biodiversité', 'channel_title': 'Arte', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': "t s' engage dans le dispositif mon leasing électrique avec renault twingo hi te", 'keyword_id': 'ff68bad3169c0dfd7f382eb7ef25d2919f53a46fcdffd017c496eb239c96e151', 'count': 57, 'id': 'f6196da2b5d0eabe34fb08f4d9fc3e834b18f934298c3bc5b777194151909e22', 'keyword': 'leasing électrique', 'channel_title': 'Europe 1', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'ros par mois seulement grâce à mon leasing électrique comme ça tout le monde pe', 'keyword_id': 'fe506268294d1eae63bc7c40f2205436821824d5db78d095924e21e9103fce44', 'count': 47, 'id': '571bec95475147a5fc7b4d5497ae52a36fa570efe197865c2874ed531687814d', 'keyword': 'leasing électrique', 'channel_title': 'Europe 1', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': "t s' engage dans le dispositif mon leasing électrique avec renault mégane texte", 'keyword_id': 'fa6a852a31d9447978e886b4c621d20d69a9852082f0569e8e20475ba72a4a70', 'count': 38, 'id': '6cc34be846b6d9e39db9d7f514c0b54edb1e99d52053476c7fd0831e40287ac6', 'keyword': 'leasing électrique', 'channel_title': 'Europe 1', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': ' accessible fallait bien ça car la transition énergétique ça compte mais aussi ', 'keyword_id': 'f8d11f03feef88edbaf4646ddd2d29719825b3062683e0b54e4a377544b93c97', 'count': 42, 'id': 'b3f4ea8c4fd9ed3ef702faed1caccecca8d562a5bbf8c17705157517a322d35e', 'keyword': 'transition énergétique', 'channel_title': 'Europe 1', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'e a été formé pour créer une ferme agro écologique faites un don pour que son h', 'keyword_id': 'fff2e7844e4a926d6e744d36ef2ccfa6ecfb3f680ba139c5785a071c824d8159', 'count': 32, 'id': '2cbdb8b5d730926696abe523e953745266d33fd6a5198e1815394ba7d78daa8d', 'keyword': 'agro écologique', 'channel_title': 'France 2', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': "mérique favorisant l' émergence d' énergie renouvelable en afrique francophone ", 'keyword_id': 'fb1984e3c30e3f97840dcc0db9359b31c18acf08174c05be0532e371c99eeb0e', 'count': 24, 'id': '654d6946f00b27cd6d61dd051753fe7960eb2499be29a1b176e76cbf8acaed39', 'keyword': 'énergie renouvelable', 'channel_title': 'RFI', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'ts historiques les bouleversements climatiques de très fortes chaleurs les vict', 'keyword_id': 'fca7a8626fb7762ba6849f1cbc3b2708099118352442025e9e99b8b451e698d3', 'count': 41, 'id': 'a9017d023deadedd24a915ea3ad2eac5c79a94a9dcd87214fc3a0378b0acf8c5', 'keyword': 'climat', 'channel_title': 'RFI', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'tion des conflits les dérèglements climatiques et la crise économique aggrave l', 'keyword_id': 'd9e9290ef2fa15ab4d54e387bc3ef6204c5b34e0effa00f2479d9ed51442cbd6', 'count': 26, 'id': 'd2f44307d8057427f48e67c2400d9e3e8cb175d080f0f492a168c794178bb4dc', 'keyword': 'climat', 'channel_title': 'RFI', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}, {'context': 'aux pollutions et aux dérèglements climatiques olivier emond embarqué à bord de', 'keyword_id': 'e683a12b0260af1a9f22717165e869e30ed13dbb6b5da54b6c3c2adcd3b02308', 'count': 23, 'id': '5637bf04ae3d04d76a952d7d3864795fdb32d8d8f79a86b31115b5ab46cad9d9', 'keyword': 'climat', 'channel_title': 'RFI', 'start_date': datetime(2024, 1, 14, 10, 2, 21)}]
    save_append_stop_word(conn, save)
    stop_words = get_all_stop_word(session)
    assert len(stop_words) == 12