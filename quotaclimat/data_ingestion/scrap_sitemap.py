import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List

import advertools as adv
import pandas as pd

from quotaclimat.data_ingestion.config_sitemap import (SITEMAP_CONFIG, SITEMAP_TEST_CONFIG, SITEMAP_DOCKER_CONFIG, MEDIA_CONFIG)
from postgres.schemas.models import get_sitemap_cols
from quotaclimat.data_ingestion.scrap_html.scrap_description_article import get_meta_news
import asyncio
import hashlib

# TODO: silence advertools loggings
# TODO: add slack login
# TODO: add data models


def get_sitemap_list():
    dev_env=os.environ.get("ENV") == "dev" or os.environ.get("ENV") == "docker"
    
    if(dev_env):
        if(os.environ.get("ENV") == "docker"):
            logging.info("Testing locally - docker")
            return SITEMAP_DOCKER_CONFIG
        else:
            logging.info("Testing locally - without docker")
            return SITEMAP_TEST_CONFIG
    else:
        logging.info("Using all the websites (production config)")
        return SITEMAP_CONFIG

def get_sections_from_url(url, sitemap_config, default_output):
    logging.debug("regex %s", sitemap_config["regex_section"])
    clean_url_from_date = re.sub(r"\/[0-9]{2,4}\/[0-9]{2,4}\/[0-9]{2,4}", "", str(url))

    search_url = re.search(
        sitemap_config["regex_section"], clean_url_from_date
    )

    output = search_url.group("section").split("/") if search_url else default_output

    return output

def normalize_section(sections) -> List[str]:
    output = list(filter(lambda x: "article" not in x.lower() and not x.isdigit(), sections))
    output = list(map(lambda item: item.replace("_", "-"), output))

    return output

def find_sections(url: str, media: str, sitemap_config) -> List[str]:
    """Find and parse section with url"""

    default_output = ["unknown"]
    logging.debug("Url to parse %s from %s" % (url, media))

    if sitemap_config["regex_section"] is not None:
        try:
            output = get_sections_from_url(url, sitemap_config, default_output)
            return normalize_section(output)
        except Exception as err:
            logging.error(
                "Cannot find section for media %s with url %s:\n %s"
                % (media, url, err)
            )
            return default_output
    else:  # regex not defined
        return default_output


def get_sections(df: pd.DataFrame, sitemap_config) -> pd.DataFrame:
    """Get sections and apply it to df"""

    logging.debug("extract sections from url")
    df["section"] = df.apply(lambda x: find_sections(x.url, x.media, sitemap_config), axis=1)

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
    logging.info(f"Filtering {date_label} to keep only {days_duration} days")
    initial_len = len(df)
    
    mask = df.loc[:, date_label] > datetime.now() - timedelta(days=days_duration)
    df_new = df[mask].copy()  # to avoid a warning.
    remaining_len = len(df_new)

    if remaining_len != 0:
        logging.info(f"remaining rows: {remaining_len} out of {initial_len} rows")
    else:
        logging.warning(f"0 rows remaining after filter - is the sitemap not updated anymore ?")

    return df_new


def clean_surrounding_whitespaces_str(string: str) -> str:
    try:
        return str.strip(string)
    except TypeError:  # if not a string
        return string


def clean_surrounding_whitespaces_df(df: pd.DataFrame) -> pd.DataFrame:
    """Remove the useless characters in each cell"""
    return df.applymap(clean_surrounding_whitespaces_str)


# TODO be able to return other type of data than description if needed
async def add_news_meta(url: str, media:str, title:str):
    try:
        result = await get_meta_news(url, media)

        return result["description"]
    except (Exception) as error:
        logging.warning(error)
        return ""


def get_consistent_hash(my_pk):
    # Convert the object to a string representation
    obj_str = str(my_pk)
    sha256 = hashlib.sha256()
    # Update the hash object with the object's string representation
    sha256.update(obj_str.encode('utf-8'))
    hash_value = sha256.hexdigest()

    return hash_value


# hash of publication name + title 
def add_primary_key(df):
    try:
        return (
            df["publication_name"]
            + df["news_title"]
        ).apply(get_consistent_hash)
    except (Exception) as error:
        logging.warning(error)
        return get_consistent_hash("empty") #  TODO improve - should be a None ?
        
def get_diff_from_df(df, df_from_pg):
    initial_len = len(df)
    
    difference_df = df[~df['id'].isin(df_from_pg['id'])]
    logging.info(f"Number of new sitemap to save (compared to already saved in PG): {len(difference_df)} out of {initial_len}")
    return difference_df

async def query_one_sitemap_and_transform(media: str, sitemap_conf: Dict, df_from_pg) -> pd.DataFrame:
    """Query a site map url from media_conf and tranform it as pd.DataFrame

    Args:
        media_conf (Dict): from MEDIA_CONF

    Returns:
        pd.DataFrame
    """
    try:
        logging.info("\n\nParsing media %s with %s" % (media, sitemap_conf["sitemap_url"]))
        #@see https://advertools.readthedocs.io/en/master/advertools.sitemaps.html#news-sitemaps
        temp_df = adv.sitemap_to_df(sitemap_conf["sitemap_url"])
  
        temp_df.rename(columns={"loc": "url"}, inplace=True)
    
        cols = get_sitemap_cols()

        df_template_db = pd.DataFrame(columns=cols)
        temp_df = pd.concat([temp_df, df_template_db])
        temp_df["media"] = media
        
        df = get_sections(temp_df, sitemap_conf)

        df = change_datetime_format(df)
        # keep only one week of data to avoid massive sitemap.xml
        date_label = sitemap_conf.get("filter_date_label", None)
        if date_label is not None:
            df = filter_on_date(df, date_label=date_label, days_duration=7)
        else:
            logging.warning("No filter by date is done")

        df["media_type"] = MEDIA_CONFIG[media]["type"]

        df = clean_surrounding_whitespaces_df(df)
        df = df.drop(columns=["etag", "sitemap_size_mb", "news", "news_publication", "image"], errors='ignore')
        
        # primary key for the DB to avoid duplicate data
        df["id"] = add_primary_key(df)

        # empty for some medias such as JDD
        df["publication_name"] = df["publication_name"].fillna(media)

        #keep only unknown id to not parse every website for new_description
        difference_df = get_diff_from_df(df, df_from_pg)
        
        # concurrency : https://stackoverflow.com/a/67944888/3535853
        # https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
        difference_df['news_description'] = await asyncio.gather(*(add_news_meta(row["url"], media, row["news_title"]) for (_, row) in difference_df.iterrows()))

        return difference_df
    except Exception as err:
        logging.error(
            "Sitemap query error for %s: %s : %s"
            % (media, sitemap_conf["sitemap_url"], err)
        )
        return None