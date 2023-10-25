import logging

import pandas as pd


from postgres.insert_data import (add_primary_key, clean_data,
                                  insert_data_in_sitemap_table,get_consistent_hash)


                                 
from postgres.insert_existing_data_example import (
    parse_section, transformation_from_dumps_to_table_entry)
from postgres.schemas.models import create_tables, get_sitemap, connect_to_db


def test_section():
    parse_section("test") == "test"
    parse_section("test, pizza") == "test,pizza"

def test_add_primary_key():
    df = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name",
                "news_title": "testnews_title",
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
            }
        ]
    )

    expected_output = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name",
                "news_title": "testnews_title",
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
                "id": get_consistent_hash("testpublication_name" + "testnews_title" + "2023-10-11 13:10:00"),
            }
        ]
    )

    df["id"] = add_primary_key(df)

    pd.testing.assert_frame_equal(expected_output, df)


    df_fake = pd.DataFrame(
    [
        {
            "fake_it": "testpublication_name",
        }
    ]
    )
    
    df_fake["id"] = add_primary_key(df_fake)
    assert df_fake['id'].values.take(0) == get_consistent_hash("empty")


def test_transformation_from_dumps_to_table_entry():
    expected_result = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name",
                "news_title": "testnews_title",
                "download_date": pd.Timestamp("2023-10-11 13:10:00"),
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
                "news_keywords": "testnews_keywords",
                "section": "testsection",
                "image_caption": "testimage_caption",
                "media_type": "testmedia_type",
                "url": "testurl",
            }
        ]
    )

    ## From a sitemap.xml
    df = pd.DataFrame(
        [
            {
                "url": "testurl",
                "news": "testnews",
                "news_publication": "testnews_publication",
                "publication_name": "testpublication_name",
                "publication_language": "testpublication_language",
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
                "news_title": "testnews_title",
                "news_keywords": "testnews_keywords",
                "image": "testimage",
                "image_loc": "testimage_loc",
                "image_caption": "testimage_caption",
                "sitemap": "testsitemap",
                "etag": "testetag",
                "sitemap_last_modified": pd.Timestamp("2023-10-11 13:10:00"),
                "sitemap_size_mb": "testsitemap_size_mb",
                "download_date": pd.Timestamp("2023-10-11 13:10:00"),
                "section": "testsection",
                "media_type": "testmedia_type",
                "media": "testmedia",
                "lastmod": pd.Timestamp("2023-10-11 13:10:00"),
            }
        ]
    )

    output = transformation_from_dumps_to_table_entry(df)

    pd.testing.assert_frame_equal(output, expected_result)


def test_insert_data_in_sitemap_table():
    create_tables()
    conn = connect_to_db()

    df = pd.DataFrame(
        [
            {
                "publication_name": "testpublication_name_new",
                "news_title": "testnews_title",
                "download_date": pd.Timestamp("2023-10-11 13:11:00"),
                "news_publication_date": pd.Timestamp("2023-10-11 13:10:00"),
                "news_keywords": "testnews_keywords",
                "section": "sport",
                "image_caption": "testimage_caption",
                "media_type": "testmedia_type",
                "url": "testurl",
            }
        ]
    )

    insert_data_in_sitemap_table(df, conn)

    # check the value is well existing
    primary_key = get_consistent_hash("testpublication_name_newtestnews_title2023-10-11 13:10:00")
    result = get_sitemap(primary_key)

    assert result.id == primary_key
    assert result.publication_name == "testpublication_name_new"
    assert result.news_title == "testnews_title"
    assert result.download_date == pd.Timestamp("2023-10-11 13:11:00")
    assert result.news_publication_date == pd.Timestamp("2023-10-11 13:10:00")
    assert result.news_keywords == "testnews_keywords"
    assert result.section == "sport"
    assert result.image_caption == "testimage_caption"
    assert result.media_type == "testmedia_type"
    assert result.url == "testurl"

def test_clean_data():
    df_wrong_format = pd.DataFrame([{"id": "empty"}])

    result = clean_data(df_wrong_format)
    assert result.empty == True

    df_right_format = pd.DataFrame([{"id": "proper_id"}])
    result = clean_data(df_right_format)
    assert result.empty == False
