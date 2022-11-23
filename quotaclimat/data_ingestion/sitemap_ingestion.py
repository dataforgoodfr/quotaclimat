import re
from typing import Dict, List

import advertools as adv
import pandas as pd

from quotaclimat.data_ingestion.config_sitmap import MEDIA_CONFIG


def get_sitemap(media_config: Dict) -> pd.DataFrame:
    """Scrap sitemap for each media and returns df"""

    df = pd.DataFrame(
        columns=[
            "loc",
            "lastmod",
            "news_publication_date",
            "news_title",
            "publication_name",
            "download_date",
        ]
    )

    for media, media_conf in media_config.items():

        temp_df = adv.sitemap_to_df(media_conf["sitemap_url"])
        temp_df["media"] = media
        df = pd.concat([df, temp_df], axis=0)

    return df


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean df from unused columns"""

    df = df.rename(columns={"loc": "url"})
    df = df.drop(
        [
            "image_loc",
            "image_caption",
            "sitemap",
            "sitemap_size_mb",
            "news",
            "news_publication",
            "news_keywords",
            "image",
            "news_genres",
            "etag",
            "sitemap_last_modified",
            "changefreq",
            "priority",
            "news_access",
            "publication_language",
            "lastmod",
            "publication_name",
        ],
        axis=1,
    )

    return df


def find_sections(media_config: Dict, url: str, media: str) -> List[str]:
    """Find and parse section with url"""

    clean_url_from_date = re.sub(r"\/[0-9]{2,4}\/[0-9]{2,4}\/[0-9]{2,4}", "", url)
    search_url = re.search(media_config[media]["regex_section"], clean_url_from_date)

    return search_url.group("section").split("/") if search_url else ["unknown"]


def get_sections(media_config: Dict, df: pd.DataFrame) -> pd.DataFrame:
    """Get sections and apply it to df"""

    df["section"] = df.apply(
        lambda x: find_sections(media_config, x.url, x.media), axis=1
    )

    return df


def change_datetime_format(df: pd.DataFrame) -> pd.DataFrame:
    """Changes the date format for BQ"""

    # WARNING: timezone information are deleted in this function.
    for column_name in ["news_publication_date", "download_date"]:
        df[column_name] = pd.to_datetime(df[column_name]).apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S")
        )
        df[column_name] = df[column_name].apply(pd.Timestamp)

    return df



def run() -> pd.DataFrame:

    df_raw = get_sitemap(MEDIA_CONFIG)
    df_cleaned = clean_df(df_raw)
    df = get_sections(MEDIA_CONFIG, df_cleaned)
    df = change_datetime_format(df)

    return df
