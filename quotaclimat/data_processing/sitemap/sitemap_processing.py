import glob
import json
import os
import re

import pandas as pd

from quotaclimat.data_ingestion.config_sitemap import MEDIA_CONFIG

LANDING_PATH_SITEMAP = "data_public/sitemap_dumps"
LANDING_PATH_SITEMAP = "data_public/sitemap_dumps"


def load_all(path: str = LANDING_PATH_SITEMAP):
    files = glob.glob(path + "/**/*.json")
    dfs = [pd.read_json(fp, orient="index") for fp in files]
    df_all = pd.concat(dfs)
    df_all.index.name = "url"
    return df_all


def load_webpress(path: LANDING_PATH_SITEMAP):
    files = glob.glob(path + "/media_type=webpress" + "/*.json")
    dfs = [pd.read_json(fp, orient="index") for fp in files]
    return pd.concat(dfs)


def feature_engineering_sitemap(df_origin: pd.DataFrame):
    df = df_origin.copy()
    # format date
    df["news_publication_date"] = df.news_publication_date.dt.strftime("%Y-%m-%d")
    df["download_date"] = df.download_date.dt.strftime("%Y-%m-%d")
    # filtering
    df = df[df.news_publication_date > "2022-11-24"]  # some article are very old

    # extract section
    # mlb = MultiLabelBinarizer()
    # df_sparse = pd.DataFrame(
    #    mlb.fit_transform(df.section), columns=mlb.classes_, index=df.index
    # )
    # df[df_sparse.columns] = df_sparse

    # news title processing
    df.news_title = df.news_title.str.lower()

    df["type"] = df["media"].apply(lambda m: MEDIA_CONFIG[m]["type"])
    return df


def filter_df(df, date_lower_bound, date_upper_bound, keywords):
    df_between_two_dates = df[
        (pd.to_datetime(df.download_date).dt.date >= date_lower_bound)
        & (pd.to_datetime(df.download_date).dt.date <= date_upper_bound)
    ]
    df_between_two_dates_kw = df_between_two_dates[
        df_between_two_dates.news_title.str.contains("|".join(keywords))
    ]
    return df_between_two_dates_kw


def preprocess(df):
    """Extraction de la section: cette colonne est sous forme de liste
    Retirer colonnes inutiles
    concaténation titre et texte d'image
    """
    df["section"] = df["section"].str[0]
    # colonnes inutiles
    col_to_drop = [
        "priority",
        "changefreq",
        "image",
        "image_title",
        "news",
        "news_access",
        "news_genres",
        "news_keywords",
        "news_publication",
        "publication_language",
        "image_loc",
        "sitemap",
        "media_type",
        "media_type",
        "sitemap_size_mb",
        "sitemap_last_modified",
        "publication_language",
        "sitemap",
        "publication_name",
        "download_date_last",
        "lastmod",
    ]
    df.drop(col_to_drop, axis=1, inplace=True)
    df = df.fillna("None")
    # concaténation titre et texte d'image
    df["image_caption"].fillna(" ", inplace=True)
    df["text"] = df["news_title"] + " " + df["image_caption"]
    return df


def search_words(text):
    """retirer les chiffres et caractères spéciaux"""
    result = re.findall(r"\b[^\d\W]+\b", text)

    return " ".join(result)
