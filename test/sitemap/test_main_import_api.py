import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.api_import import *

import time as t


def test_main_api_import():
    create_tables()
    conn = connect_to_db()
    json_file_path = 'test/sitemap/mediatree.json'
    with open(json_file_path, 'r') as file:
        json_response = json.load(file)
        start_time = t.time()
        df = parse_reponse_subtitle(json_response)
        df = filter_and_tag_by_theme(df)
        df["id"] = add_primary_key(df)
        end_time = t.time()
        logging.info(f"Elapsed time for api import {end_time - start_time}")
        # must df._to_pandas() because to_sql does not handle modin dataframe
        save_to_pg(df._to_pandas(), keywords_table, conn)

        session = get_db_session(conn)
        saved_keywords = get_keywords_columns(session)
        assert len(saved_keywords) == len(df)

def test_first_row_api_import():
        primary_key = "71b8126a50c1ed2e5cb1eab00e4481c33587db478472c2c0e74325abb872bef6"
        specific_keyword = get_keyword(primary_key)
        assert specific_keyword.theme == [
        "changement_climatique_causes_indirectes"
        ]
        assert specific_keyword.keywords_with_timestamp == [
        {
            "keyword": "bâtiment",
            "timestamp": 1707675083088,
            "theme": "changement_climatique_causes_indirectes"
        }
        ]
        assert specific_keyword.number_of_keywords == 0

def test_second_row_api_import():
        
        primary_key = "67b9cc593516b40f55d6a3e89b377fccc8ab76d263c5fd6d4bfe379626190641"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
            "changement_climatique_consequences",
            "adaptation_climatique_solutions_indirectes",
            "changement_climatique_constat",
            "changement_climatique_causes",
        ])

        assert specific_keyword.keywords_with_timestamp == [ # from metabase to speedup check
            {
                "keyword": "écologique",
                "timestamp": 1707627623079,
                "theme": "changement_climatique_constat"
            },
            {
                "keyword": "écologiste",
                "timestamp": 1707627631076,
                "theme": "changement_climatique_constat"
            },
            {
                "keyword": "puit de pétrole",
                "timestamp": 1707627628054,
                "theme": "changement_climatique_causes" # was indirectes before
            },
            {
                "keyword": "submersion",
                "timestamp": 1707627611094,
                "theme": "changement_climatique_consequences"
            },
            {
                "keyword": "barrage",
                "timestamp": 1707627686004,
                "theme": "adaptation_climatique_solutions_indirectes"
            }
        ]
        assert specific_keyword.number_of_keywords == 3


def test_third_row_api_import():
        primary_key = "975b41e76d298711cf55113a282e7f11c28157d761233838bb700253d47be262"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
            "changement_climatique_consequences",
            "changement_climatique_constat",
        ])
        assert specific_keyword.keywords_with_timestamp == [
        {
            "keyword": "écologiste",
            "timestamp": 1707592768096,
            "theme": "changement_climatique_constat"
        },
        {
            "keyword": "submersion",
            "timestamp": 1707592792083,
            "theme": "changement_climatique_consequences"
        }
        ]
        assert specific_keyword.number_of_keywords == 2
        
