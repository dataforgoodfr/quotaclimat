import glob

import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer

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
    mlb = MultiLabelBinarizer()
    df_sparse = pd.DataFrame(
        mlb.fit_transform(df.section), columns=mlb.classes_, index=df.index
    )
    df[df_sparse.columns] = df_sparse

    # news title processing
    df.news_title = df.news_title.str.lower()
    return df
