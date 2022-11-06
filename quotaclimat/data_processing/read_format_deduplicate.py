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
    path_folder="../data/keywords/", path_channel_metadata=None
):
    df_all = pd.DataFrame()
    list_all_files = [f for f in listdir(path_folder) if isfile(join(path_folder, f))]
    for path_file in list_all_files:
        df_i = read_and_format_one(path_folder + path_file, path_channel_metadata)
        df_all = pd.concat([df_all, df_i])
    return df_all.reset_index()


def read_and_format_one(path_file=None, path_channels=None, data=None, name=None):

    if data is None:
        data = pd.read_excel(path_file)

    if path_channels is not None:
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
    )
    data = columns_names_to_camel_case(data)
    data = deduplicate_extracts(data)
    return data


def deduplicate_extracts(df: pd.DataFrame):
    deduplicate_ids = [
        x for x in df.columns.tolist() if x not in ["keyword", "text", "highlight"]
    ]
    # the deduplication assumed all sample are floored at pair minutes. The following line check if it holds
    assert ~(df.date.dt.minute % 2).sum()
    # this groupby all ids but keyword and text, collecting the keywords present in duplicated extract
    df_dedup = df.groupby(deduplicate_ids).agg(
        {"keyword": [set, pd.Series.nunique], "text": "first"}
    )
    df_dedup.columns = ["keywords", "nb_keywords_in_extract", "text"]
    # convert set in list for ease of use
    df_dedup.keywords = df_dedup.keywords.apply(list)
    df_dedup.reset_index(inplace=True)

    # verify unicity per channel x date
    assert df_dedup.duplicated(subset=["channel", "date"]).any()
    return df_dedup
