import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List

import advertools as adv
import pandas as pd

from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG)
from postgres.schemas.models import get_sitemap_cols

# TODO: silence advertools loggings
# TODO: add slack login
# TODO: add data models


def find_sections(url: str, media: str, sitemap_config=SITEMAP_CONFIG) -> List[str]:
    """Find and parse section with url"""
    if sitemap_config[media]["regex_section"] is not None:
        clean_url_from_date = re.sub(r"\/[0-9]{2,4}\/[0-9]{2,4}\/[0-9]{2,4}", "", url)
        search_url = re.search(
            sitemap_config[media]["regex_section"], clean_url_from_date
        )

        output = search_url.group("section").split("/") if search_url else ["unknown"]
        output = list(filter(lambda x: "article" not in x.lower() and not x.isdigit(), output))
        output = list(map(lambda item: item.replace("_", "-"), output))
        return output
    else:  # regex not defined
        return ["unknown"]


def get_sections(df: pd.DataFrame) -> pd.DataFrame:
    """Get sections and apply it to df"""

    df["section"] = df.apply(lambda x: find_sections(x.url, x.media), axis=1)

    return df

# TODO test me
def change_datetime_format(df: pd.DataFrame) -> pd.DataFrame:
    """Changes the date format for BQ"""

    # WARNING: timezone information are deleted in this function.
    for column_name in ["news_publication_date", "download_date", "lastmod"]:
        try:
            df[column_name] = pd.to_datetime(df[column_name]).apply(
                lambda x: x.strftime("%Y-%m-%d %H:%M:%S")
            )
            df[column_name] = df[column_name].apply(pd.Timestamp)
        except:  # column not found
            df[column_name] = df["download_date"]
            continue

    return df


def filter_on_date(
    df: pd.DataFrame, date_label="lastmod", days_duration=7
) -> pd.DataFrame:

    mask = df.loc[:, date_label] > datetime.now() - timedelta(days=days_duration)
    df_new = df[mask].copy()  # to avoid a warning.
    return df_new


def clean_surrounding_whitespaces_str(string: str) -> str:
    try:
        return str.strip(string)
    except TypeError:  # if not a string
        return string


def clean_surrounding_whitespaces_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove the useless characters in each cell"""
    return df.applymap(clean_surrounding_whitespaces_str)


def query_one_sitemap_and_transform(media: str, sitemap_conf: Dict) -> pd.DataFrame:
    """Query a site map url from media_conf and tranform it as pd.DataFrame

    Args:
        media_conf (Dict): from MEDIA_CONF

    Returns:
        pd.DataFrame
    """
    try:
        logging.info("Parsing %s with %s" % (media, sitemap_conf["sitemap_url"]))
        #@see https://advertools.readthedocs.io/en/master/advertools.sitemaps.html#news-sitemaps
        temp_df = adv.sitemap_to_df(sitemap_conf["sitemap_url"])
    except AttributeError:
        logging.error(
            "Sitemap query error for %s: %s does not match regexp."
            % (media, sitemap_conf["sitemap_url"])
        )
        return None
    cols = get_sitemap_cols()

    df_template_db = pd.DataFrame(columns=cols)
    temp_df = pd.concat([temp_df, df_template_db])
    temp_df.rename(columns={"loc": "url"}, inplace=True)
    temp_df["media"] = media
    df = get_sections(temp_df)
    df = change_datetime_format(df)

    date_label = sitemap_conf.get("filter_date_label", None)
    if date_label is not None:
        df = filter_on_date(df, date_label=date_label)

    df["media_type"] = MEDIA_CONFIG[media]["type"]
    df = clean_surrounding_whitespaces_df(df)
    df = df.drop(columns=["etag", "sitemap_size_mb", "news", "news_publication", "image"])

    return df

def run():
    for media, sitemap_conf in SITEMAP_CONFIG.items():
        logging.info("Reading for %s: with conf %s" % (media, sitemap_conf))
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            write_df(df, media)
        except Exception as err:
            logging.error("Could not write data for %s: %s" % (media, err))
            continue


if __name__ == "__main__":
    run()
