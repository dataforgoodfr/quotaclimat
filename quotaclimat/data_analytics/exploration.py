from datetime import timedelta

import pandas as pd
import plotly.express as px

from ..utils.channels import TOP_25_CHANNELS, TOP_CHANNELS_TV


def filter_data_between_hours(data, min_hour="06:00", max_hour="24:00"):
    def parse_timedelta(x):
        hours, minutes = x.split(":")
        return timedelta(hours=int(hours), minutes=int(minutes))

    min_hour = parse_timedelta(min_hour)
    max_hour = parse_timedelta(max_hour)

    return data.loc[
        (data["time_of_the_day"] >= min_hour) & (data["time_of_the_day"] < max_hour)
    ]


def convert_number_of_mentions(x, method="count", n_channels=None):

    if isinstance(method, int) or isinstance(method, float):
        return x * method

    elif isinstance(method, str):

        if method == "count":
            return x
        elif method == "minutes":
            return x * 2
        elif method == "hours":
            return x * 2 / 60
        elif method == "media_time":
            assert n_channels is not None

            # Compute total minutes available on all channels during a day
            total_media_time = n_channels * 60 * 24

            # Compute how many minutes the mentions compare to the total media time
            part_media_time = x * 2 / total_media_time
            return part_media_time

    else:
        raise Exception(
            "Method argument should be count,minutes,hours,media_time or an integer or float multiplier"
        )


def show_mentions_by_channel(
    data, n=30, list_of_channels=None, split="media", method="count", **kwargs
):

    if list_of_channels is None:
        count = (
            data.groupby(["channel_name", split], as_index=False)["count"]
            .sum()
            .sort_values("count", ascending=False)
            .head(n)
        )
    else:
        count = (
            data.loc[data["channel_name"].isin(list_of_channels)]
            .groupby(["channel_name", split], as_index=False)["count"]
            .sum()
            .sort_values("count", ascending=False)
            .head(n)
        )

    # Convert number of mentions to minutes or media time percentage if method is provided
    n_channels = count["channel_name"].nunique()
    count["count"] = count["count"].map(
        lambda x: convert_number_of_mentions(x, method=method, n_channels=n_channels)
    )

    order = (
        count.groupby("channel_name")["count"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    fig = px.bar(
        count,
        x="channel_name",
        y="count",
        color=split,
        category_orders={"channel_name": order},
        **kwargs,
    )

    fig.update_xaxes(tickangle=-45, title=None)
    fig.update_yaxes(title=None)
    fig.update_layout(margin={"b": 100})
    return fig


def show_piechart_split_tv_radio(data):

    count = (
        data.groupby(["media"], as_index=False)["count"]
        .sum()
        .sort_values("count", ascending=False)
    )

    fig = px.pie(count, names="media", values="count", title="Split TV / Radio")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig


def show_mentions_over_time(
    data,
    freq="D",
    split=None,
    kind="bar",
    as_percent=False,
    list_of_channels=None,
    method="count",
    **kwargs,
):

    assert kind in ["area", "bar", "line"]

    # We take all channels if none is provided
    if list_of_channels is None:

        # Take TV + Radio
        if split is None:

            count = (
                data.set_index(["date"])
                .groupby([pd.Grouper(freq=freq)], as_index=True)["count"]
                .sum()
                .reset_index()
            )

            # Convert number of mentions to minutes or media time percentage if method is provided
            n_channels = data["channel_name"].nunique()
            count["count"] = count["count"].map(
                lambda x: convert_number_of_mentions(
                    x, method=method, n_channels=n_channels
                )
            )

            if kind == "bar":
                fig = px.bar(count, x="date", y="count", **kwargs)
            elif kind == "area":
                fig = px.area(count, x="date", y="count", **kwargs)
            else:
                raise Exception("kind argument should be 'area' or 'bar'")

        # Split TV / Radio
        else:

            count = (
                data.set_index(["date"])
                .groupby([pd.Grouper(freq=freq), split], as_index=True)["count"]
                .sum()
                .reset_index()
            )

            # Convert number of mentions to minutes or media time percentage if method is provided
            n_channels = data["channel_name"].nunique()
            count["count"] = count["count"].map(
                lambda x: convert_number_of_mentions(
                    x, method=method, n_channels=n_channels
                )
            )

            # Understand split between TV & Radio as a total percentage (ex: 55% radio / 45% TV)
            if as_percent:

                text_percentage = round(count["count"] / count["count"].sum() * 100, 1)

                fig = px.area(
                    count,
                    x="date",
                    y="count",
                    color=split,
                    groupnorm="fraction",
                    text=text_percentage.apply(lambda x: str(x) + " %"),
                    **kwargs,
                )
                fig.update_layout(yaxis_tickformat="0%")

            # Show Total mentions by TV or Radio
            else:

                if kind == "bar":
                    fig = px.bar(count, x="date", y="count", color=split, **kwargs)
                elif kind == "area":
                    text_percentage = round(count["count"], 1)

                    fig = px.area(
                        count,
                        x="date",
                        y="count",
                        color=split,
                        text=text_percentage.apply(lambda x: str(x)),
                        **kwargs,
                    )
                else:
                    raise Exception("kind argument should be 'area' or 'bar'")

    # Aggregate by channel if a list of channels is provided
    else:

        count = (
            data.set_index(["date"])
            .groupby([pd.Grouper(freq=freq), "channel_name"], as_index=True)["count"]
            .sum()
            .reset_index()
        )

        count = count.loc[count["channel_name"].isin(list_of_channels)]

        # Convert number of mentions to minutes or media time percentage if method is provided
        n_channels = count["channel_name"].nunique()
        count["count"] = count["count"].map(
            lambda x: convert_number_of_mentions(
                x, method=method, n_channels=n_channels
            )
        )

        if kind == "line":
            fig = px.line(count, x="date", y="count", color="channel_name", **kwargs)

        elif kind == "bar":
            fig = px.bar(count, x="date", y="count", color="channel_name", **kwargs)

        else:
            raise Exception("kind argument should be 'line' or 'bar'")

    return fig


def show_mentions_by_time_of_the_day(
    data,
    freq="1H",
    kind="bar",
    as_percent=False,
    split=None,
    list_of_channels=None,
    method="count",
    **kwargs,
):

    assert kind in ["area", "bar", "line"]

    if list_of_channels is None:

        if split is None:

            count = (
                data.set_index(["time_of_the_day"])
                .groupby([pd.Grouper(freq=freq)], as_index=True)["count"]
                .sum()
                .reset_index()
                .assign(
                    time_of_the_day=lambda x: x["time_of_the_day"].map(
                        lambda y: str(y)[7:12]
                    )
                )
            )

            # Convert number of mentions to minutes or media time percentage if method is provided
            n_channels = data["channel_name"].nunique()
            count["count"] = count["count"].map(
                lambda x: convert_number_of_mentions(
                    x, method=method, n_channels=n_channels
                )
            )

            if kind == "bar":
                fig = px.bar(
                    count,
                    x="time_of_the_day",
                    y="count",
                    **kwargs,
                )
            elif kind == "area":
                fig = px.area(count, x="time_of_the_day", y="count", **kwargs)

        # Split by TV or Radio
        else:

            count = (
                data.set_index(["time_of_the_day"])
                .groupby([pd.Grouper(freq=freq), split], as_index=True)["count"]
                .sum()
                .reset_index()
                .assign(
                    time_of_the_day=lambda x: x["time_of_the_day"].map(
                        lambda y: str(y)[7:12]
                    )
                )
                .sort_values("time_of_the_day", ascending=True)
            )

            # Convert number of mentions to minutes or media time percentage if method is provided
            n_channels = data["channel_name"].nunique()
            count["count"] = count["count"].map(
                lambda x: convert_number_of_mentions(
                    x, method=method, n_channels=n_channels
                )
            )

            if kind == "bar":

                fig = px.bar(
                    count,
                    text_auto="s",
                    x="time_of_the_day",
                    y="count",
                    color=split,
                    category_orders={
                        "time_of_the_day": count["time_of_the_day"].unique()
                    },
                    **kwargs,
                )

            elif kind == "area":

                fig = px.area(
                    count,
                    x="time_of_the_day",
                    y="count",
                    color=split,
                    groupnorm="fraction" if as_percent else None,
                    category_orders={
                        "time_of_the_day": count["time_of_the_day"].unique()
                    },
                    **kwargs,
                )
                if as_percent:
                    fig.update_layout(yaxis_tickformat="0%")

    else:

        count = (
            data.set_index(["time_of_the_day"])
            .groupby([pd.Grouper(freq=freq), "channel_name"], as_index=True)["count"]
            .sum()
            .reset_index()
            .assign(
                time_of_the_day=lambda x: x["time_of_the_day"].map(
                    lambda y: str(y)[7:12]
                )
            )
            .sort_values("time_of_the_day", ascending=True)
        )

        count = count.loc[count["channel_name"].isin(list_of_channels)]

        # Convert number of mentions to minutes or media time percentage if method is provided
        n_channels = count["channel_name"].nunique()
        count["count"] = count["count"].map(
            lambda x: convert_number_of_mentions(
                x, method=method, n_channels=n_channels
            )
        )

        if kind == "bar":
            fig = px.bar(
                count,
                x="time_of_the_day",
                y="count",
                color="channel_name",
                category_orders={"time_of_the_day": count["time_of_the_day"].unique()},
                **kwargs,
            )

        elif kind == "area":

            fig = px.area(
                count,
                x="time_of_the_day",
                y="count",
                color="channel_name",
                groupnorm="fraction" if as_percent else None,
                category_orders={"time_of_the_day": count["time_of_the_day"].unique()},
                **kwargs,
            )
            if as_percent:
                fig.update_layout(yaxis_tickformat="0%")

        elif kind == "line":

            fig = px.line(
                count,
                x="time_of_the_day",
                y="count",
                color="channel_name",
                category_orders={"time_of_the_day": count["time_of_the_day"].unique()},
                **kwargs,
            )

    return fig


def show_mentions_treemap(
    data, list_of_channels, freq="4H", path=["channel_name", "time_of_the_day"]
):
    def parse_period(y, freq):
        hours = int(str(y)[7:9])
        return f"{hours}-{hours+int(freq.replace('H',''))}h"

    # Process path to add grouper on time of the day
    group = []

    if "time_of_the_day" in path:
        group.append(pd.Grouper(freq=freq))
        group.extend([x for x in path if x != "time_of_the_day"])

        count = (
            data.set_index(["time_of_the_day"])
            .groupby(group, as_index=True)["count"]
            .sum()
            .reset_index()
            .assign(
                time_of_the_day=lambda x: x["time_of_the_day"].map(
                    lambda y: parse_period(y, freq)
                )
            )
            .sort_values("time_of_the_day", ascending=True)
        )

    else:

        count = data.groupby(path, as_index=True)["count"].sum().reset_index()

    count = count.loc[count["channel_name"].isin(list_of_channels)]

    fig = px.treemap(
        count,
        path=path,
        values="count",
    )
    return fig
