import glob

import pandas as pd

LANDING_PATH_SITEMAP = "data_public/sitemap_dumps/"


def load_all():
    files = glob.glob(LANDING_PATH_SITEMAP + "**/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
    return df


def load_tv():
    files = glob.glob(LANDING_PATH_SITEMAP + "media_type=tv/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)


def load_webpress():
    files = glob.glob(LANDING_PATH_SITEMAP + "media_type=webpress/**/**/**/*.parquet")
    dfs = [pd.read_parquet(fp) for fp in files]
    df = pd.concat(dfs)
