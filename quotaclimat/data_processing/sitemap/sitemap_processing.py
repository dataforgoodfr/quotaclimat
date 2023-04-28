import glob
import json
import os
import re

import pandas as pd

from quotaclimat.data_ingestion.config_sitmap import MEDIA_CONFIG
from quotaclimat.data_ingestion.scrap_sitemap import write_df

LANDING_PATH_SITEMAP = "data_public/sitemap_dumps"
LANDING_PATH_SITEMAP = "data_public/sitemap_dumps"


def load_all(path: str = LANDING_PATH_SITEMAP):
    files = glob.glob(path + "/**/*.json")
    dfs = [pd.read_json(fp, orient="index") for fp in files]
    return pd.concat(dfs)


def load_webpress(path: LANDING_PATH_SITEMAP):
    files = glob.glob(path + "/media_type=webpress"+ "/*.json")
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


def _migrate_from_pd_to_json(df_all: pd.DataFrame):
    """Writes json from a dataframe, url deduplicated . This was used to migrate the paradigm to a 'git scrapping'

    Args:
        df_all (pd.DataFrame): _description_
    """
    df_all.set_index("url", inplace=True)
    df_all["download_date_last"].fillna(
        df_all["download_date"], inplace=True
    )  # previous logic
    urls_not_deduplicated = df_all[
        df_all.index.duplicated()
    ].index.unique()  # some urls were already curred
    sanity = 0
    for df_per_media in df_all.groupby("media"):
        dict_i = {}
        df_per_media[1]
        for row in df_per_media[1].sort_values("download_date").iterrows():
            if row[0] not in dict_i:  # new entry
                dict_i.update(
                    {
                        row[0]: {
                            "news_title": row[1]["news_title"],
                            "image_caption": row[1]["image_caption"],
                            "download_date": row[1]["download_date"].strftime(
                                "%Y-%m-%d"
                            ),
                            "publication_name": row[1]["publication_name"],
                            "news_publication_date": row[1][
                                "news_publication_date"
                            ].strftime("%Y-%m-%d"),
                            "news_keywords": row[1]["news_keywords"],
                            "section": row[1]["section"].tolist(),
                            "media_type": row[1]["media_type"],
                            "download_date_last": row[1]["download_date_last"].strftime(
                                "%Y-%m-%d"
                            ),
                        }
                    }
                )
            else:  # this will update download_date_last, without duplicating or changing anything else
                if (row[0] in dict_i) and (row[0] in urls_not_deduplicated):
                    dict_i[row[0]].update(
                        {
                            "download_date_last": row[1]["download_date"].strftime(
                                "%Y-%m-%d"
                            ),
                        }
                    )
                else:
                    sanity += 1
        assert sanity == 0
        landing_path = "sitemap_dumps/media_type=%s" % row[1]["media_type"]
        if not os.path.exists(landing_path):
            os.makedirs(landing_path)
        with open(landing_path + "/%s.json" % row[1]["media"], "w") as f:
            json.dump(dict_i, f)


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
