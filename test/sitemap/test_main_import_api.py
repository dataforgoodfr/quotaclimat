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
