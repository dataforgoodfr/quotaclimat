# %% Importation
from datetime import timedelta

import numpy as np
import pandas as pd
import plotly.express as px
import seaborn as sns

import quotaclimat.data_analytics.exploration as mt_exploration
from quotaclimat.data_processing.uncertainty_processing import (
    get_coverage_by_chunks, get_coverage_data_by_channel)
from quotaclimat.utils.channels import (TOP_CHANNELS_RADIO,
                                        TOP_CHANNELS_TV_CONTINU,
                                        TOP_CHANNELS_TV_GENERALISTE)
from quotaclimat.utils.plotly_theme import THEME, WARMING_STRIPES_SEQUENCE

# %% Initialisation
TOP_CHANNELS_TV = TOP_CHANNELS_TV_GENERALISTE + TOP_CHANNELS_TV_CONTINU
TOP_CHANNELS = TOP_CHANNELS_TV + TOP_CHANNELS_RADIO

COLOR_RADIO = WARMING_STRIPES_SEQUENCE[0]
COLOR_TV = WARMING_STRIPES_SEQUENCE[1]
COLOR_ECO = WARMING_STRIPES_SEQUENCE[3]


# %%
def make_ws_palette(n=10):
    return [
        f"rgb({int(x*255)},{int(y*255)},{int(z*255)})"
        for x, y, z in list(sns.color_palette("RdBu_r", n_colors=n))
    ]


def label_media_from_list(
    media,
):
    if media in TOP_CHANNELS_TV_GENERALISTE:
        return "TV - Généraliste"
    elif media in TOP_CHANNELS_TV_CONTINU:
        return "TV - Information en continu"
    elif media in TOP_CHANNELS_RADIO:
        return "Radio"
    else:
        return "Inconnu"


def data_extension(data):

    data_new = data.copy()
    data_new["media2"] = data_new.channel_name.apply(label_media_from_list)
    data_new["day_dt"] = data_new["date"].dt.date
    data_new["day_str"] = data_new["day_dt"].map(str)

    return data_new


def get_filtered_data(data):
    data_extracted = data.loc[np.isin(data.channel_name, TOP_CHANNELS), :]
    data_completed = data_extension(data_extracted)
    data_filtered = mt_exploration.filter_data_between_hours(
        data_completed, "06:00", "24:00"
    ).reset_index(drop=True)
    return data_filtered


def get_media_time(
    data_filtered,
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
    list_gen=TOP_CHANNELS_TV_GENERALISTE,
    list_continu=TOP_CHANNELS_TV_CONTINU,
    list_radio=TOP_CHANNELS_RADIO,
    heure_de_grande_ecoute: bool = False,
):

    n_days = (data_filtered["day_dt"].max() - data_filtered["day_dt"].min()).days + 1
    if heure_de_grande_ecoute:
        data_filtered_radio = data_filtered.loc[
            data_filtered.channel_name.isin(list_radio)
            & (data_filtered.date.dt.hour >= 6)
            & (data_filtered.date.dt.hour < 10)
        ]
        data_filtered_tv = data_filtered.loc[
            data_filtered.channel_name.isin(list_gen + list_continu)
            & (data_filtered.date.dt.hour >= 19)
            & (data_filtered.date.dt.hour < 21)
        ]
        data_filtered = pd.concat([data_filtered_radio, data_filtered_tv])
        hour_a_day = 2

    media_time = data_filtered.groupby(["media2"]).agg(
        {"count": "sum", "channel_name": "nunique"}
    )

    media_time.loc["Radio", "n_channels"] = len(list_radio)
    if heure_de_grande_ecoute:
        media_time.loc["Radio", "n_channels"] = (
            len(list_radio) * 2
        )  # fix for the difference of coverage between radio and TV
    media_time.loc["TV - Généraliste", "n_channels"] = len(list_gen)
    media_time.loc["TV - Information en continu", "n_channels"] = len(list_continu)
    # media_time = media_time.append(pd.DataFrame(media_time.sum(axis = 0).rename("Total")).T)
    media_time.loc["Total", :] = media_time.sum(axis=0)
    media_time["media_time"] = (
        media_time["count"] * minutes_by_chunk
    )  # media_time is in minutes
    media_time["total_time"] = (
        media_time["n_channels"] * n_days * hour_a_day * 60
    )  # 60 : from hours to minutes
    media_time["media_part"] = media_time["media_time"] / media_time["total_time"]

    return media_time


def get_ranking_evolution(
    data,
    method="first",  # average or first
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
):

    n_days = (data["day_dt"].max() - data["day_dt"].min()).days + 1
    ranking = (
        data.groupby(["channel_name", "media", "media2", "keyword", "day_dt"])["count"]
        .sum()
        .unstack("day_dt")
        .fillna(0.0)
        .stack()
        .unstack("keyword")
        .fillna(0.0)
        .stack()
        .reset_index(drop=False)
        .rename(columns={0: "count"})
    )

    ranking["minutes"] = ranking["count"] * minutes_by_chunk
    ranking["total_time"] = hour_a_day * 60 * n_days
    ranking["media_part"] = ranking["minutes"] / ranking["total_time"]

    ranking["rank"] = ranking.groupby(["media2", "day_dt", "keyword"])[
        "count"
    ].transform("rank", ascending=False, method=method)
    ranking["count_total"] = ranking.groupby(["channel_name", "media2", "day_dt"])[
        "count"
    ].transform("sum")
    ranking["rank_total"] = ranking.groupby(["media2", "day_dt", "keyword"])[
        "count_total"
    ].transform("rank", ascending=False, method=method)
    ranking["media_part_total"] = (
        ranking["count_total"] * minutes_by_chunk / ranking["total_time"]
    )
    ranking["day_str"] = ranking["day_dt"].map(str)

    return ranking


# %% plots
def plot_volume_mediatique(media_time, keyword):
    fig = px.bar(
        media_time.reset_index(),
        x="media2",
        y="media_part",
        height=400,
        text_auto=".1%",
    )
    fig.update_layout(
        yaxis_tickformat="0%",
        title="Volume médiatique total sur les 50 chaînes TV et Radio sur %s" % keyword,
        font_family="Poppins",
        yaxis_title="% du volume médiatique",
        xaxis_title="",
    )

    fig.update_traces(marker_color=[COLOR_RADIO, COLOR_TV, COLOR_TV])
    return fig


def plot_classement_volume_mediatique(
    data_filtered,
    top_channels_list,
    title_label,
    color_theme,
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
):
    n_days = (data_filtered["day_dt"].max() - data_filtered["day_dt"].min()).days + 1
    multiplier = minutes_by_chunk / (1 * 60 * hour_a_day * n_days)

    fig = mt_exploration.show_mentions_by_channel(
        data_filtered,
        list_of_channels=top_channels_list,
        n=25,
        split="keyword",
        method=multiplier,
        height=400,
        text_auto=".1%",
    )
    fig.update_layout(
        yaxis_tickformat="0%",
        font_family="Poppins",
        yaxis_title="% du volume médiatique",
        legend_title="",
        title=title_label,  # "Classement TV - Chaînes d'information en continu"
    )
    fig.update_traces(marker_color=color_theme)
    return fig


def plot_classement_volume_mediatique_tv_generique(
    data_filtered,
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
):
    fig = plot_classement_volume_mediatique(
        data_filtered,
        top_channels_list=TOP_CHANNELS_TV_GENERALISTE,
        title_label="Classement TV -  Chaînes généralistes",
        color_theme=COLOR_TV,
        minutes_by_chunk=minutes_by_chunk,
        hour_a_day=hour_a_day,
    )

    return fig


def plot_classement_volume_mediatique_tv_continue(
    data_filtered,
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
):
    fig = plot_classement_volume_mediatique(
        data_filtered,
        top_channels_list=TOP_CHANNELS_TV_CONTINU,
        title_label="Classement TV - Chaînes d'information en continu",
        color_theme=COLOR_TV,
        minutes_by_chunk=minutes_by_chunk,
        hour_a_day=hour_a_day,
    )

    return fig


def plot_classement_volume_mediatique_radio(
    data_filtered,
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
):
    fig = plot_classement_volume_mediatique(
        data_filtered,
        top_channels_list=TOP_CHANNELS_RADIO,
        title_label="Classement Radio",
        color_theme=COLOR_RADIO,
        minutes_by_chunk=minutes_by_chunk,
        hour_a_day=hour_a_day,
    )

    return fig


# %%


def show_ranking_chart(ranking_data, title="", height=500, total=False):

    rank_col = "rank" if not total else "rank_total"
    percent_col = "media_part" if not total else "media_part_total"

    annot_labels_data = ranking_data.loc[
        ranking_data["day_dt"] == ranking_data["day_dt"].max()
    ].sort_values(rank_col, ascending=True)

    fig = px.line(
        ranking_data,
        x="day_dt",
        y=rank_col,
        color="channel_name",
        text=rank_col,
        markers=True,
        color_discrete_sequence=make_ws_palette(len(annot_labels_data)),
        category_orders={"channel_name": annot_labels_data["channel_name"].tolist()},
    )

    # fig.update_traces(marker=dict(size=15),selector=dict(mode='markers'))
    fig.update_layout(
        xaxis_tickmode="linear",
        yaxis_autorange="reversed",
        xaxis_showgrid=False,
        yaxis_showgrid=False,
        xaxis_title="Date de la COP27",
        yaxis_title=None,
        yaxis_showticklabels=False,
    )
    fig.update_traces(
        marker_size=20,
        marker=dict(line=dict(width=2)),
        textposition="middle center",
        textfont_size=12,
        textfont_color="white",
    )

    annotations = []

    # Adding labels
    for i, row in annot_labels_data.iterrows():
        annotations.append(
            dict(
                xref="paper",
                x=0.95,
                y=row[rank_col],
                xanchor="left",
                yanchor="middle",
                text=row["channel_name"] + " (" + f"{row[percent_col]:.1%}" + ")",
                font=dict(family="Poppins", size=12),
                showarrow=False,
            )
        )
    #     # labeling the right_side of the plot
    #     annotations.append(dict(xref='paper', x=0.95, y=y_trace[11],
    #                                   xanchor='left', yanchor='middle',
    #                                   text='{}%'.format(y_trace[11]),
    #                                   font=dict(family='Arial',
    #                                             size=16),

    fig.update_layout(
        margin_r=200,
        height=height,
        annotations=annotations,
        showlegend=False,
        title=title,
        font_family="Poppins",
    )
    return fig


def media_volume_over_time(
    data,
    n_channels=len(TOP_CHANNELS),
    minutes_by_chunk=2,
    hour_a_day=18,  # link to the filter used
    title: str = "Evolution du volume médiatique",
):
    n_days = (data["day_dt"].max() - data["day_dt"].min()).days + 1

    if n_days == 1:
        PLOT_FREQUENCY = "H"
        multiplier = minutes_by_chunk / (
            n_channels * 60
        )  # as if only 1 hour for the evolution plot
    else:
        PLOT_FREQUENCY = "1D"
        multiplier = minutes_by_chunk / (
            n_channels * hour_a_day * 60
        )  # as if only 1 day for the evolution plot

    fig = mt_exploration.show_mentions_over_time(
        data,
        freq=PLOT_FREQUENCY,
        text_auto=".1%",
        method=multiplier,
        color_discrete_sequence=WARMING_STRIPES_SEQUENCE,
    )

    fig.update_layout(
        font_family="Poppins",
        xaxis_tickmode="linear",
        xaxis_title="Date",
        yaxis_title="% du volume médiatique",
        title=title,
        legend_title="",
        yaxis_tickformat="0%",
    )
    return fig
