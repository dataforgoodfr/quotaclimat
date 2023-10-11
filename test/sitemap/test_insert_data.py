import pytest
import logging
import pandas as pd
import numpy as np
from postgres.insert_existing_data_example import transformation_from_dumps_to_table_entry
from postgres.insert_existing_data_example import parse_section
from postgres.insert_data import add_primary_key

def test_section():
    parse_section("test") == "test"
    parse_section("test, pizza") == "test,pizza"

def test_add_primary_key():
    df = pd.DataFrame([{
        "publication_name": "testpublication_name",
        "news_title": "testnews_title",
        "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
        }]
    )

    expected_output = pd.DataFrame([{
        "id": "testpublication_name"+"testnews_title"+"2023-10-11 13:10:00",
        "publication_name": "testpublication_name",
        "news_title": "testnews_title",
        "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
        }]
    )

    df['id'] = add_primary_key(df["publication_name"],df["news_title"],df["news_publication_date"])

    pd.testing.assert_frame_equal(expected_output,expected_output)

def test_transformation_from_dumps_to_table_entry():
    expected_result = pd.DataFrame([{
        "publication_name": "testpublication_name",
        "news_title": "testnews_title",
        "download_date": pd.Timestamp("2023-10-11 13:10:00"),
        "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
        "news_keywords": "testnews_keywords",
        "section": "testsection",
        "image_caption": "testimage_caption",
        "media_type": "testmedia_type"
        }]
    )

    ## From a sitemap.xml
    df = pd.DataFrame([{
        "url":"testurl",
        "news":"testnews",
        "news_publication":"testnews_publication",
        "publication_name":"testpublication_name",
        "publication_language":"testpublication_language",
        "news_publication_date":pd.Timestamp("2023-10-11 13:10:00"),
        "news_title":"testnews_title",
        "news_keywords":"testnews_keywords",
        "image":"testimage",
        "image_loc":"testimage_loc",
        "image_caption":"testimage_caption",
        "sitemap":"testsitemap",
        "etag":"testetag",
        "sitemap_last_modified":pd.Timestamp("2023-10-11 13:10:00"),
        "sitemap_size_mb":"testsitemap_size_mb",
        "download_date":pd.Timestamp("2023-10-11 13:10:00"),
        "section":"testsection",
        "media_type":"testmedia_type",
        "media":"testmedia",
        "lastmod":pd.Timestamp("2023-10-11 13:10:00")
    }])

    output = transformation_from_dumps_to_table_entry(df)

    pd.testing.assert_frame_equal(output, expected_result)

# def test_insert_data():
#     publication_name: "20minutes.fr"
#     news_title: "Coupe du monde de rugby EN DIRECT :"
#     download_date: pd.Timestamp("2023-10-11 13:10:00")
#     news_publication_date: pd.Timestamp("2023-10-11 13:10:00")
#     news_keywords pd.NaN
#     section "sport, rugby"
#     image_caption pd.NaN
#     media_type webpress
#     id "20minutes.fr" + "Coupe du monde de rugby EN DIRECT :" + "2023-10-11 13:10:00"
