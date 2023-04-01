import glob
import re
from pathlib import Path

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer

from quotaclimat.data_ingestion.config_sitmap import MEDIA_CONFIG
from quotaclimat.data_ingestion.scrap_sitemap import write_df


LANDING_PATH_SITEMAP = (
    
    "data_public/sitemap_dumps"
    
)


def load_all(path: str = LANDING_PATH_SITEMAP):
    # files = glob.glob(path + "**/**/**/**/*.parquet")
    files = glob.glob(path + "**/**/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return filter_on_first_ingestion_date(df)


def load_month(year: int, month: int, path: str = LANDING_PATH_SITEMAP):
    files = glob.glob(path + f"**/**/year={year}\month={month}/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return filter_on_first_ingestion_date(df)


def load_tv():
    files = glob.glob(LANDING_PATH_SITEMAP + "media_type=tv/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return df


def load_webpress():
    files = glob.glob(LANDING_PATH_SITEMAP + "/media_type=webpress/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return df


def filter_on_first_ingestion_date(df):
    return df[df.news_publication_date > "2022-11-24"]


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


def scan_for_duplicates_and_overwrite_the_history(
    overwrite: bool,
):  # there should be a more elegant way to do that
    # load all data #TODO refactor this to a window of n months
    df_archives = load_all("../data_public/sitemap_dumps/media_type")
    download_date_last = (
        df_archives.groupby("url")["download_date"]
        .apply(lambda x: x.max())
        .rename("download_date_last")
    )
    df_m = df_archives.merge(download_date_last, on=["url"])
    del df_archives
    df_m.sort_values("download_date", inplace=True)
    df_m.drop_duplicates(["url"], keep="first", inplace=True)

    # overwrite history without duplicates
    if overwrite:
        for mt in df_m.media_type.unique():
            df_mt = df_m[df_m.media_type == mt]
            for media in df_mt.media.unique():
                df_media = df_mt[df_mt.media == media]
                for download_date in df_media.download_date.unique():
                    df_per_day = df_media[df_media.download_date == download_date]
                    write_df(df_per_day, media)
    else:
        return df_m


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
