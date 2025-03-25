import logging

from quotaclimat.data_processing.mediatree.stop_word.main import *
from postgres.schemas.models import get_db_session, connect_to_db, drop_tables
from quotaclimat.data_processing.mediatree.api_import_utils.db import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db, drop_tables, empty_tables,keywords_table
from datetime import date
from quotaclimat.data_processing.mediatree.update_pg_keywords import *

conn = connect_to_db()
session = get_db_session(conn)



def test_mediatree_get_last_date_and_number_of_delay_saved_in_keywords():
        conn = connect_to_db()
        create_tables(conn)
        session = get_db_session(conn)
        start = pd.to_datetime("2025-01-26 12:18:54", utc=True).tz_convert('Europe/Paris')
        wrong_value = 1
        pk = "delete_me"
        df = pd.DataFrame([{
        "id" : pk,
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
        ,"program_metadata_id":"336643dc7fa09ac7335a4ceba43270ed3f553be3383a9b3b6e3cced101f2a87a"
        ,"channel_title":"channel_title"
        ,"number_of_keywords_climat": wrong_value
        ,"number_of_keywords_biodiversite": wrong_value
        ,"number_of_keywords_ressources": wrong_value
        }])

        save_to_pg(df, keywords_table, conn)

        keywordStats = get_last_date_and_number_of_delay_saved_in_keywords(session, days_filter=3000)
        expected_max_date = KeywordLastStats(date(2025, 1, 26), 2)
       
        assert expected_max_date.last_day_saved == keywordStats.last_day_saved
        assert keywordStats.number_of_previous_days_from_yesterday > 1
        delete_keywords_id(session, pk)


def test_get_delay_date():
        unixtimestamp_2025_01_26 = 1737849600
        expected_max_date = KeywordLastStats(date(2025, 1, 26), 2)
        default_start_date, default_number_of_previous_days = get_delay_date(expected_max_date, normal_delay_in_days=1)

        assert default_start_date == unixtimestamp_2025_01_26
        assert default_number_of_previous_days == 2