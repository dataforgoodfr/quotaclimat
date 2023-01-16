import glob
from pathlib import Path

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer

from quotaclimat.data_ingestion.config_sitmap import MEDIA_CONFIG

LANDING_PATH_SITEMAP = "data_public/sitemap_dumps/"


def load_all(path: str = LANDING_PATH_SITEMAP):
    files = glob.glob(path + "**/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return filter_on_first_ingestion_date(df)


def load_tv():
    files = glob.glob(LANDING_PATH_SITEMAP + "media_type=tv/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return df


def load_webpress():
    files = glob.glob(LANDING_PATH_SITEMAP + "media_type=webpress/**/**/**/*.parquet")
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
