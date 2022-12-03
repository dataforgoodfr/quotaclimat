import datetime
from os import listdir
from os.path import isfile, join

import pandas as pd

# TODO
# reconsiliate france3 with local fr3 code name


def columns_names_to_camel_case(df):
    df.columns = [x.replace(" ", "_").lower() for x in df.columns]
    return df


def read_and_format_all_data_dump(
    path_folder="../data/keywords/", path_channel_metadata="../data/channels.xlsx"
):
    df_all = pd.DataFrame()
    list_all_files = [f for f in listdir(path_folder) if isfile(join(path_folder, f))]
    for path_file in list_all_files:
        df_i = read_and_format_one(path_folder + path_file, path_channel_metadata)
        df_all = pd.concat([df_all, df_i])
    df_all_ = deduplicate_extracts(
        df_all
    )  # remove this deduplication as this is not relevant for the analysis of the media coverage
    return df_all_.reset_index()


def read_and_format_one(path_file=None, path_channels="", data=None, name=None):

    if data is None:
        data = pd.read_excel(path_file)
        if path_channels:
            channels = pd.read_excel(path_channels)
            data = data.merge(channels, on="CHANNEL")
    else:
        data = data.rename(columns={"CHANNEL": "CHANNEL_NAME"})

    data = (
        data.assign(
            date=lambda x: pd.to_datetime(x["DATE"], format="%Y-%m-%dT%H-%M-%S")
        )
        .assign(time=lambda x: x["date"].dt.time)
        .assign(
            time_of_the_day=lambda x: x["time"].map(
                lambda y: datetime.timedelta(
                    hours=y.hour, minutes=y.minute, seconds=y.second
                )
            )
        )
        .assign(media=lambda x: x["RADIO"].map(lambda y: "Radio" if y else "TV"))
        .assign(path_file=lambda x: path_file)
        .assign(count=lambda x: 1)
        .assign(duration=lambda x: 2)
        .assign(
            keyword=lambda x: x["path_file"].map(
                lambda y: y.rsplit("_", 1)[-1].replace(".xlsx", "")
                if name is None
                else name
            )
        )
        .drop(columns=["ORIGIN", "START CHUNK", "END CHUNK", "DATE"])
        .reset_index(drop=True)
    )
    data = columns_names_to_camel_case(data)
    data = hot_fix_columns_changing_over_time(data)

    # Remove duplicates within one keyword
    data = data.drop_duplicates(subset=["media", "date", "channel_name"])

    return data


def deduplicate_extracts(df: pd.DataFrame):
    deduplicate_ids = [
        x
        for x in df.columns.tolist()
        if x not in ["keyword", "text", "highlight", "index", "url", "path_file"]
    ]
    # the deduplication assumed all sample are floored at pair minutes. The following line check if it holds
    assert ~(df.date.dt.minute % 2).sum()
    # this groupby all ids but keyword and text, collecting the keywords present in duplicated extract
    df_dedup = df.groupby(deduplicate_ids).agg(
        {"keyword": [set, pd.Series.nunique], "text": "first", "url": "first"}
    )
    df_dedup.columns = ["keywords", "nb_keywords_in_extract", "text", "url"]
    # convert set in list for ease of use
    df_dedup.keywords = df_dedup.keywords.apply(list)
    # keep on of the keyword for easy indexing
    df_dedup["keyword"] = df_dedup.keywords.apply(lambda k: k[0])
    df_dedup.reset_index(inplace=True)
    # verify unicity per channel x date
    assert not df_dedup.duplicated(subset=["channel_name", "date"]).any()
    return df_dedup


def filter_data_only_keep_top_audiance(
    data: pd.DataFrame, path_channels: str = "../data/channels.xlsx"
):
    top_audiences = pd.read_excel(path_channels, sheet_name="top_audiences")
    top_audiences["channel_id"] = (
        top_audiences["channel_name"] + "_" + top_audiences["media"]
    )
    data["channel_id"] = data["channel_name"] + "_" + data["media"]
    data = data.merge(top_audiences[["channel_id"]], on=["channel_id"], how="inner")
    return data


def hot_fix_columns_changing_over_time(df):
    if "channel_name" not in df:
        df["channel_name"] = df["channel"]
    return df
