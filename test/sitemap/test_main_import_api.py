import logging

from quotaclimat.data_processing.mediatree.update_pg_keywords import *

from postgres.insert_data import (clean_data,
                                  insert_data_in_sitemap_table)

from postgres.schemas.models import create_tables, get_db_session, get_keyword, connect_to_db
from postgres.insert_data import save_to_pg
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.api_import import *
from test_utils import get_localhost, debug_df, compare_unordered_lists_of_dicts

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
        primary_key = "29d2b1f8267b206cb62e475b960de3247e835273f396af012f5ce21bf3056472"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
              'biodiversite_concepts_generaux_indirectes',
              'changement_climatique_consequences'
            ])
        assert specific_keyword.keywords_with_timestamp == [
              {
              'category': '',
              'keyword': 'eaux',
              'theme': 'biodiversite_concepts_generaux_indirectes',
              'timestamp': 1707627599055,
              },
              {
              'category': '',
              'keyword': 'mer',
              'theme': 'biodiversite_concepts_generaux_indirectes',
              'timestamp': 1707627596075,
              },
              {
              'category': 'Eau',
              "keyword": "submersion",
              "timestamp": 1707627569016,
              "theme": "changement_climatique_consequences"
          }
        ]
        assert specific_keyword.number_of_keywords == 1

def test_second_row_api_import():
        
        primary_key = "9f0fb1987371c1dc0b4a165a11feb7ca7ed9b6f9f40d3d6b4fc0748e2ca59c3f"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
                    'attenuation_climatique_solutions',
                    'attenuation_climatique_solutions_indirectes',
                    'biodiversite_concepts_generaux_indirectes',
                    'biodiversite_solutions',
                    'biodiversite_solutions_indirectes',
                    'changement_climatique_causes_indirectes',
                    'changement_climatique_constat',
                    'ressources',
                    'ressources_indirectes',
                    'ressources_solutions',

        ])

        assert compare_unordered_lists_of_dicts(specific_keyword.keywords_with_timestamp,[
  {
    "keyword": "puit de pétrole",
    "timestamp": 1707633159095,
    "theme": "changement_climatique_causes_indirectes",
    "category": "Energie"
  },
  {
    "keyword": "sortie des énergies fossiles",
    "timestamp": 1707633214077,
    "theme": "attenuation_climatique_solutions",
    "category": "Energie"
  },
  {
    "keyword": "militant écologiste",
    "timestamp": 1707633148016,
    "theme": "biodiversite_solutions_indirectes",
    "category": "General"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "biodiversite_solutions",
    "category": "Ecosystème"
  },
  {
    "keyword": "forêt",
    "timestamp": 1707633173076,
    "theme": "attenuation_climatique_solutions_indirectes",
    "category": "Ecosystème"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "attenuation_climatique_solutions",
    "category": "Ecosystème"
  },
  {
    "keyword": "forêt",
    "timestamp": 1707633173076,
    "theme": "biodiversite_concepts_generaux_indirectes",
    "category": ""
  },
  {
    "keyword": "climatique",
    "timestamp": 1707633231071,
    "theme": "changement_climatique_constat",
    "category": "General"
  },
  {
    "keyword": "écologiste",
    "timestamp": 1707633187053,
    "theme": "changement_climatique_constat",
    "category": "General"
  },
  {
    "keyword": "puit de pétrole",
    "timestamp": 1707633159095,
    "theme": "ressources_indirectes",
    "category": "Energie"
  },
  {
    "keyword": "forêt",
    "timestamp": 1707633173076,
    "theme": "ressources",
    "category": "Forêts"
  },
  {
    "keyword": "sortie des énergies fossiles",
    "timestamp": 1707633214077,
    "theme": "ressources_solutions",
    "category": "Energie"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "ressources_solutions",
    "category": "Air"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "ressources_solutions",
    "category": "Eau"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "ressources_solutions",
    "category": "Forêts"
  },
  {
    "keyword": "replantation",
    "timestamp": 1707633200076,
    "theme": "ressources_solutions",
    "category": "Sols"
  }
])
        assert specific_keyword.number_of_keywords == 5


def test_third_row_api_import():
        primary_key = "32cb864fe56a4436151bcf78c385a7cc4226316e0563a298ac6988d1b8ee955b"
        specific_keyword = get_keyword(primary_key)
        assert set(specific_keyword.theme) == set([
        "biodiversite_solutions_indirectes",
        "attenuation_climatique_solutions_indirectes",
        "changement_climatique_constat"
        ])
        assert compare_unordered_lists_of_dicts(specific_keyword.keywords_with_timestamp, [
          {
            "keyword": "militante écologiste",
            "timestamp": 1707566379025,
            "theme": "biodiversite_solutions_indirectes",
            "category": "General"
          },
          {
            "keyword": "militante écologiste",
            "timestamp": 1707566379025,
            "theme": "attenuation_climatique_solutions_indirectes",
            "category": "General"
          },
          {
            "keyword": "climatique",
            "timestamp": 1707566298018,
            "theme": "changement_climatique_constat",
            "category": "General"
          }
        ])
              
        assert specific_keyword.number_of_keywords == 1
        
