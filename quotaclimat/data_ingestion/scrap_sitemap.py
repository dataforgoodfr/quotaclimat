import json
import logging
import os
import re
import advertools as adv
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List

from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG)



# TODO: silence advertools loggings
# TODO: add slack login
# TODO: add data models


def cure_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean df from unused columns"""

    df = df.rename(columns={"loc": "url"})
    return df

def find_sections(url: str, media: str, sitemap_config=SITEMAP_CONFIG) -> List[str]:
    """Find and parse section with url"""
    if sitemap_config[media]["regex_section"] is not None:
        clean_url_from_date = re.sub(r"\/[0-9]{2,4}\/[0-9]{2,4}\/[0-9]{2,4}", "", url)
        search_url = re.search(
            sitemap_config[media]["regex_section"], clean_url_from_date
        )

        return search_url.group("section").split("/") if search_url else ["unknown"]
    else:  # regex not defined
        return "unknown"

def get_sections(df: pd.DataFrame) -> pd.DataFrame:
    """Get sections and apply it to df"""

    df["section"] = df.apply(lambda x: find_sections(x.url, x.media), axis=1)

    return df

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
        temp_df = adv.sitemap_to_df(sitemap_conf["sitemap_url"])
    except AttributeError:
        logging.error(
            "Sitemap query error for %s: %s does not match regexp."
            % (media, sitemap_conf["sitemap_url"])
        )
        return
    cols = [
        "publication_name",
        "news_title",
        "download_date",
        "news_publication_date",
        "news_keywords",
        "section",
        "image_caption",
        "media_type",
    ]
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
    sanity_check()
    return df


def sanity_check():
    """Checks if the data is correct, input a df"""
    # TODO data model here
    return


def write_df(df: pd.DataFrame, media: str):
    """Write the extracted dataframe to standardized path"""

    landing_path_media = "data_public/sitemap_dumps/media_type=%s/%s.json" % (
        MEDIA_CONFIG[media]["type"],
        media,
    )
    if not os.path.exists(landing_path_media):
        previous_entries = {}
        logging.info("Writing to %s for the first time" % landing_path_media)

    else:
        with open(landing_path_media) as f:
            previous_entries = json.load(f)
    previous_entries = insert_or_update_entry(df, previous_entries)
    with open(landing_path_media, "w+") as f:
        json.dump(previous_entries, f)


def insert_or_update_entry(df_one_media: pd.DataFrame, dict_previous_entries: dict):
    df_one_media.set_index("url", inplace=True)
    for row in df_one_media.sort_values("download_date").iterrows():
        if row[0] not in dict_previous_entries:  # new entry
            dict_previous_entries.update(
                {
                    row[0]: {
                        "news_title": row[1]["news_title"],
                        "image_caption": row[1]["image_caption"],
                        "download_date": row[1]["download_date"].strftime("%Y-%m-%d"),
                        "publication_name": row[1]["publication_name"],
                        "news_publication_date": row[1][
                            "news_publication_date"
                        ].strftime("%Y-%m-%d"),
                        "news_keywords": row[1]["news_keywords"],
                        "section": row[1]["section"],
                        "media_type": row[1]["media_type"],
                        "download_date_last": row[1]["download_date"].strftime(
                            "%Y-%m-%d"
                        ),
                    }
                }
            )
        else:  # this will update download_date_last, without updating download_date
            dict_previous_entries[row[0]].update(
                {
                    "download_date_last": row[1]["download_date"].strftime("%Y-%m-%d"),
                }
            )  # we update download_date_last with the current (new) download_date
    return dict_previous_entries


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
