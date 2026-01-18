import logging
import time as t

from modin.pandas.dataframe import DataFrame
from test_utils import (compare_unordered_lists_of_dicts, debug_df,
                        get_localhost)

from postgres.insert_data import (clean_data, insert_data_in_sitemap_table,
                                  save_to_pg)
from postgres.schemas.models import (connect_to_db, create_tables, drop_tables,
                                     empty_tables, get_db_session, get_keyword)
from quotaclimat.data_processing.mediatree.api_import import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.keyword.stop_words import STOP_WORDS
from quotaclimat.data_processing.mediatree.s3.api_to_s3 import \
    parse_reponse_subtitle
from quotaclimat.data_processing.mediatree.stop_word.main import \
    save_append_stop_word
from quotaclimat.data_processing.mediatree.update_pg_keywords import *


def insert_mediatree_json(conn, json_file_path='test/sitemap/mediatree.json'):
    create_tables(conn)  
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
        drop_tables(conn)
        create_tables(conn)
        insert_stop_word(conn)
        len_df = insert_mediatree_json(conn, json_file_path="test/sitemap/light.json")

        session = get_db_session(conn)
        saved_keywords = get_keywords_columns(session, start_date="2024-02-01", end_date="2024-02-29")
        assert len(saved_keywords) != 0
        assert len(saved_keywords) == len_df

def test_first_row_api_import():
        primary_key = "29d2b1f8267b206cb62e475b960de3247e835273f396af012f5ce21bf3056472"
        
        specific_keyword =  get_keyword(primary_key)
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
        assert len(set(specific_keyword.theme)) > 0
        assert specific_keyword.number_of_keywords > 0


def test_third_row_api_import():
        primary_key = "32cb864fe56a4436151bcf78c385a7cc4226316e0563a298ac6988d1b8ee955b"

        specific_keyword = get_keyword(primary_key)
        assert len(set(specific_keyword.theme)) > 0
        
        assert specific_keyword.number_of_keywords == 1

def test_get_api_stop():
        conn = connect_to_db()
        session = get_db_session(conn)
        stopwords = get_stop_words(session, country=None)    
        assert type(stopwords[0]) == str

def test_transform_raw_keywords_srt_to_mediatree():
    conn = connect_to_db()

    channel = "LAUNE"
    primary_key = "df0d86983f0c4ed074800f5cdabbd577671b90845fb6208a5de1ae3802fb10e0"
    df: DataFrame= pd.read_parquet(path=f"i8n/mediatree_output/year=2024/month=10/day=1/channel={channel}")
    df_programs = get_programs()
    output = transform_raw_keywords(df, df_programs=df_programs,country=BELGIUM)

    output_dict = output.to_dict(orient='records')
    filtered = output[output["id"] == primary_key]
    row_dict = filtered.iloc[0].to_dict()
    assert row_dict["country"] == "belgium"
    assert row_dict["channel_name"] == channel

    assert len(output) == 30
    save_to_pg(df=output,conn=conn, table=keywords_table)
    specific_keyword = get_keyword(primary_key)
    assert set(specific_keyword.theme) == set([
        'changement_climatique_causes_indirectes',
    ])

    assert specific_keyword.number_of_keywords == 0