from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_all_data_dump)
from utils.plotly_theme import SMALL_SEQUENCE2


def display_time_coverage_of_extracts(
    path_to_data_dumps: str, path_channel_metadata: str
):
    # read all files
    df_all = read_and_format_all_data_dump(
        path_folder=path_to_data_dumps, path_channel_metadata=path_channel_metadata
    )
    get_number_of_duplicates(df_all)
    # aggregate data daily
    data_all_daily = df_all.groupby(
        ["keyword", pd.Grouper(key="date", freq="d")]
    ).count()
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


def get_number_of_duplicates(df: pd.DataFrame):
    print("Number of duplicates in the data (keywords covering the same extract): ")
    print(len(df) - len(deduplicate_extracts(df)))


def fig_percentage_of_covered_time_by_keywords(
    df_all: pd.DataFrame, path_channel_metadata: str
):
    top_n = 15
    channels = pd.read_excel(path_channel_metadata)
    top_channels = channels.head(top_n)["CHANNEL_NAME"].tolist()

    df_principal_channel = df_all[df_all.channel_name.isin(top_channels)]

    minutes_covered_by_keywords_per_channel = (
        df_principal_channel.groupby(["channel_name", "media"], as_index=False)[
            "duration"
        ]
        .sum()
        .sort_values("duration", ascending=False)
    )
    # total minutes covered by sample (considering 12 h of relevant audiance a day)
    relevant_time_window = 12 / 24  # only considering 12h out of 24
    total_minutes_covered_by_sample_media = df_all.groupby(
        ["channel_name"], as_index=False
    ).apply(
        lambda grp: (grp.date.max() - grp.date.min())
        / pd.Timedelta(minutes=1)
        * relevant_time_window
    )
    total_minutes_covered_by_sample_media.columns = [
        "channel_name",
        "minutes_covered_by_sample_per_channel",
    ]
    percentage_of_keywords_coverage = minutes_covered_by_keywords_per_channel.merge(
        total_minutes_covered_by_sample_media, on=["channel_name"], how="left"
    )
    percentage_of_keywords_coverage["percentage_of_coverage"] = (
        percentage_of_keywords_coverage["duration"]
        / (percentage_of_keywords_coverage["minutes_covered_by_sample_per_channel"])
    ) * 100

    fig = px.bar(
        percentage_of_keywords_coverage,
        x="channel_name",
        y="percentage_of_coverage",
        color="media",
        color_discrete_sequence=SMALL_SEQUENCE2,
        text_auto=".2s",
        # category_orders={"channel_name": count["channel_name"].tolist()},
        height=500,
        title="Pourcentage de couverture par chaîne top %s principal channel entre %s et %s"
        % (top_n, df_all.date.min(), df_all.date.max()),
    )

    fig.update_xaxes(tickangle=-45, title=None)
    fig.update_yaxes(title=" % Pourcentage de couverture sur la chaine")
    fig.update_layout(margin={"b": 100})
    return fig
