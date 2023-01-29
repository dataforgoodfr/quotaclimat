""" Scrap Le Monde's home page links by areas, position and type_home """

import logging
import re
from datetime import datetime
from typing import List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4.element import ResultSet, Tag

from quotaclimat.data_ingestion.scrap_homepage.config_homepage_website import \
    config_homepage_lemonde


def get_home_content():
    """Gets Le Monde's homepage content and parse it with Beautiful Soup"""

    soup = BeautifulSoup(
        requests.get(
            "https://www.lemonde.fr/", headers={"User-Agent": "Mozilla/5.0"}
        ).content,
        "html.parser",
    )

    return soup


def crawl_sections(section: str, config: dict) -> List[dict]:
    """Crawl sections by class"""

    homepage_content = get_home_content()
    find_all_classes = homepage_content.find_all(
        config["container"]["tag"], class_=config["container"]["class"]
    )

    link_by_class = []

    for position, unique_class in enumerate(find_all_classes, 1):

        link_by_class.extend(
            decomposer_class(
                section,
                config,
                unique_class,
                find_all_classes,
                homepage_content,
                position,
            )
        )

    return link_by_class


def decomposer_class(
    section: str,
    config: dict,
    unique_class: Tag,
    find_all_classes: List[ResultSet],
    homepage_content: BeautifulSoup,
    position: int,
) -> List[dict]:
    """Handles the different configurations areas and return a dictionnary with link, section and position"""

    if config["sub_container"]["tag"] is None:
        return [
            {
                "url": unique_class.find(href=True)[config["url"]],
                "section": section,
                "position": position,
            }
        ]

    elif config["sub_container"]["tag"] == "li":
        return [
            {
                "url": li.find("a", href=True)[config["url"]],
                "section": section,
                "position": count_li,
            }
            for count_li, li in enumerate(
                unique_class.findAll(config["sub_container"]["tag"]), 1
            )
        ]

    elif config["sub_container"]["tag"] == "a":
        find_all_classes_str = str(find_all_classes)
        regex_find_classes = re.findall(
            r"article--top old__top-article old__top-article-[0-9]+",
            find_all_classes_str,
        )
        return [
            {
                "url": homepage_content.findAll(
                    config["sub_container"]["tag"], class_=class_
                )[0]["href"],
                "section": section,
                "position": position_top,
            }
            for position_top, class_ in enumerate(regex_find_classes, 1)
        ]
    else:
        raise ValueError("Class is missing")


def get_df_article() -> pd.DataFrame:
    """Iterate through each section and extract information"""

    global_result = []

    for section, config in config_homepage_lemonde.items():
        try:
            crawl = crawl_sections(section, config)
            global_result.extend(crawl)
        except Exception as err:
            logging.error(
                "[CRAWL_HOME_LE_MONDE][ERROR] SECTION %s failed for reason: %s",
                section,
                err,
            )

    return pd.DataFrame(global_result)


def get_type_home(df):
    """Determines which home type is in production by selecting unique class with condition"""

    if (df["section"] == "main_title_related").any() and (
        df["section"] == "routine_main_title_headlines"
    ).any():
        df["home_type"] = "classic"
        return df
    elif (df["section"] == "main_title_related").any() and not (
        df["section"] == "routine_main_title_headlines"
    ).any():
        df["home_type"] = "event"
        return df
    elif (df["section"] == "event-old-article-related").any() and not (
        df["section"] == "classic_main_title_headlines"
    ).any():
        df["home_type"] = "special_event"
        return df
    else:
        df["home_type"] = "None"
        return df


def created_at_column(df: pd.DataFrame) -> pd.DataFrame:
    """Create new colomn with datetime now from Paris"""

    df["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["created_at"] = df["created_at"].apply(pd.Timestamp)

    return df


def run():
    """Runs all functions"""

    df = get_df_article()
    df = get_type_home(df)
    df = created_at_column(df)

    return df
