from datetime import datetime

import plotly.graph_objects as go
from pandas import Grouper

from data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_all_data_dump)


def display_time_coverage_of_extracts(
    path_to_data_dumps: str, path_channel_metadata: str
):
    # read all files
    df_all = read_and_format_all_data_dump(
        path_folder=path_to_data_dumps, path_channel_metadata=path_channel_metadata
    )
    get_number_of_duplicates(df_all)
    # aggregate data daily
    data_all_daily = df_all.groupby(["keyword", Grouper(key="date", freq="d")]).count()
    data_all_daily.reset_index(inplace=True)
    # plot outcome
    fig = go.Figure()
    for key in data_all_daily.keyword.unique():
        data_per_key = data_all_daily[data_all_daily.keyword == key]
        df_coverage = data_per_key.copy()
        df_coverage["has_extract"] = key
        fig.add_trace(
            go.Scatter(x=df_coverage.date, y=df_coverage.has_extract, name=key)
        )
    fig.update_layout(
        title_text=f"Couvrance temporel des extractions %s"
        % datetime.today().strftime("%Y-%m-%d"),
        showlegend=False,
    )
    fig.show()


def get_number_of_duplicates(df):
    print("Number of duplicates in the data (keywords covering the same extract): ")
    print(len(df) - len(deduplicate_extracts(df)))
