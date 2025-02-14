import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db, drop_tables, empty_tables
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.api_import import *
from quotaclimat.data_processing.mediatree.keyword.stop_words import STOP_WORDS
from quotaclimat.data_processing.mediatree.stop_word.main import save_append_stop_word
from quotaclimat.data_processing.mediatree.s3.api_to_s3 import parse_reponse_subtitle
from test_utils import get_localhost, debug_df, compare_unordered_lists_of_dicts

import time as t


def insert_mediatree_json(conn, json_file_path='test/sitemap/mediatree.json'):
#     create_tables()  
    empty_tables(get_db_session(conn), stop_word=False)
    logging.info(f"reading {json_file_path}")
    with open(json_file_path, 'r') as file:
        json_response = json.load(file)
        start_time = t.time()
        df = parse_reponse_subtitle(json_response)
        df = filter_and_tag_by_theme(df)
        df["id"] = df.apply(lambda x: add_primary_key(x), axis=1)
        end_time = t.time()
        logging.info(f"Elapsed time for api import {end_time - start_time}")
        
        # must df._to_pandas() because to_sql does not handle modin dataframe
        save_to_pg(df._to_pandas(), keywords_table, conn)
        
        return len(df)

def insert_stop_word(conn):
       logging.info("test saving stop words")
       to_save = []
       for stop in STOP_WORDS:
                stop_word  = dict()
                stop_word['id'] = stop
                stop_word['context'] = stop
                to_save.append(stop_word)

       save_append_stop_word(conn, to_save)

def test_main_api_import():
        conn = connect_to_db()
        drop_tables()
        create_tables()
        insert_stop_word(conn)
        len_df = insert_mediatree_json(conn, json_file_path="test/sitemap/light.json")

        session = get_db_session(conn)
        saved_keywords = get_keywords_columns(session, start_date="2024-02-01", end_date="2024-02-29")
        assert len(saved_keywords) != 0
        assert len(saved_keywords) == len_df

def test_first_row_api_import():
        primary_key = "29d2b1f8267b206cb62e475b960de3247e835273f396af012f5ce21bf3056472"
        
        specific_keyword = get_keyword(primary_key)
        logging.info(f"Getting {primary_key} :\n {specific_keyword}")
        assert set(specific_keyword.theme) == set([
              'biodiversite_concepts_generaux_indirectes',
              'changement_climatique_consequences_indirectes',
              'changement_climatique_constat_indirectes'
            ])
       
        assert specific_keyword.number_of_keywords == 0

def test_second_row_api_import():
        
        primary_key = "9f0fb1987371c1dc0b4a165a11feb7ca7ed9b6f9f40d3d6b4fc0748e2ca59c3f"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
                    'adaptation_climatique_solutions_indirectes',
                    'attenuation_climatique_solutions',
                    'attenuation_climatique_solutions_indirectes',
                    'biodiversite_causes_indirectes',
                    'biodiversite_solutions_indirectes',
                    'biodiversite_concepts_generaux_indirectes',
                    'changement_climatique_causes_indirectes',
                    'changement_climatique_consequences',
                    'changement_climatique_constat',
                    'ressources_indirectes',
                    'ressources_solutions',
        ])

        assert specific_keyword.number_of_keywords == 3


def test_third_row_api_import():
        primary_key = "32cb864fe56a4436151bcf78c385a7cc4226316e0563a298ac6988d1b8ee955b"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
        "biodiversite_solutions_indirectes",
        "attenuation_climatique_solutions_indirectes",
        "changement_climatique_causes_indirectes",
        "changement_climatique_constat"
        ,"changement_climatique_constat_indirectes"
        ,"ressources_solutions_indirectes"
        ])
        
        assert specific_keyword.number_of_keywords == 1

def test_get_api_stop():
        conn = connect_to_db()
        session = get_db_session(conn)
        stopwords = get_stop_words(session)      
        assert type(stopwords[0]) == str
