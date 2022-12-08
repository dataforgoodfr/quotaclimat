import logging
import os
import re
from typing import Dict, List
import sys

import advertools as adv
import pandas as pd
from config_sitmap import MEDIA_CONFIG, SITEMAP_CONFIG

# TODO: silence advertools loggings
# TODO: add data models

dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(dir_path, '..', '..'))

from quotaclimat.logging import NoStacktraceFormatter, SlackerLogHandler

SLACK_TOKEN = "xoxb-4574857480-4452159914758-t0U00Q7HyfyIJz8sPTi3QDou"
SLACK_CHANNEL = "offseason_quotaclimat_logging"

slack_handler = SlackerLogHandler(
    SLACK_TOKEN, SLACK_CHANNEL, stack_trace=True, fail_silent=False
)
formatter = NoStacktraceFormatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
slack_handler.setFormatter(formatter)
logger = logging.getLogger("Quotaclimat Logger")
logger.addHandler(slack_handler)
logger.setLevel(logging.ERROR)


def cure_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean df from unused columns"""

    df = df.rename(columns={"loc": "url"})
    return df


def find_sections(url: str, media: str) -> List[str]:
    """Find and parse section with url"""

    clean_url_from_date = re.sub(r"\/[0-9]{2,4}\/[0-9]{2,4}\/[0-9]{2,4}", "", url)
    search_url = re.search(SITEMAP_CONFIG[media]["regex_section"], clean_url_from_date)

    return search_url.group("section").split("/") if search_url else ["unknown"]


def get_sections(df: pd.DataFrame) -> pd.DataFrame:
    """Get sections and apply it to df"""

    df["section"] = df.apply(lambda x: find_sections(x.url, x.media), axis=1)

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
    temp_df.rename(columns={"loc": "url"}, inplace=True)
    temp_df["media"] = media
    df = get_sections(temp_df)
    df = change_datetime_format(df)
    df["media_type"] = MEDIA_CONFIG[media]["type"]
    sanity_check()
    return df


def sanity_check():
    """Checks if the data is correct, input a df"""
    # TODO data model here
    return


def write_df(df: pd.DataFrame, media: str):
    """Write de extracted dataframe to standardized path"""
    download_date = df.download_date.min()
    landing_path = (
        "data_public/sitemap_dumps/media_type=%s/media=%s/year=%s/month=%s/"
        % (MEDIA_CONFIG[media]["type"], media, download_date.year, download_date.month)
    )
    if not os.path.exists(landing_path):
        os.makedirs(landing_path)
    df.to_parquet(landing_path + "/%s.parquet" % download_date.strftime("%Y%m%d"))


def run():
    for media, sitemap_conf in SITEMAP_CONFIG.items():
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            write_df(df, media)
        except Exception as err:
            logger.error("Could not write data for %s: %s" % (media, err))
            continue


if __name__ == "__main__":
    run()
