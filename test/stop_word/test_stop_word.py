import logging

import pandas as pd

from quotaclimat.data_processing.mediatree.stop_word.main import (get_top_keywords_by_channel)
from postgres.schemas.models import get_db_session
from test_main_import_api import insert_mediatree_json

session = get_db_session()


insert_mediatree_json(session, json_file_path='test/sitemap/short_mediatree.json')

def test_get_top_keywords_by_channel():
    df_to_save_2 = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name",
                "news_title": "title",
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
            }
        ]
    )
    
    
    top_keywords = get_top_keywords_by_channel(session, days=3000, top=5)

    pd.testing.assert_frame_equal(top_keywords, df_to_save_2)