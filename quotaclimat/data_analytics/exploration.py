import pandas as pd
import plotly.express as px

from ..utils.channels import TOP_25_CHANNELS,TOP_CHANNELS_TV


def show_mentions_by_channel(data,n = 30,list_of_channels = None,split = "media",title = "Nombre de mentions par chaîne"):

    if list_of_channels is None:
        count = data.groupby(["channel_name",split],as_index = False)["count"].sum().sort_values("count",ascending = False).head(n)
    else:
        count = (data
                .loc[data["channel_name"].isin(list_of_channels)]
                .groupby(["channel_name",split],as_index = False)["count"].sum().sort_values("count",ascending = False)
        )

    fig = px.bar(
        count,
        x = "channel_name",
        y = "count",
        color = split,
        text_auto = ".2s",
        category_orders={"channel_name": count["channel_name"].tolist()},
        height = 500,
        title = title,
    )

    fig.update_xaxes(tickangle=-45,title=None)
    fig.update_yaxes(title=None)
    fig.update_layout(margin={"b":100})
    return fig


def show_piechart_split_tv_radio(data):

    count = data.groupby(["media"],as_index = False)["count"].sum().sort_values("count",ascending = False)

    fig = px.pie(
        count,
        names = "media",
        values = "count",
        title = "Split TV / Radio"
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig



def show_mentions_over_time(data,freq = "D",split = None,kind = "bar",as_percent = False,list_of_channels = None):

    assert kind in ["area","bar","line"]

    # We take all channels if none is provided
    if list_of_channels is None:

        # Take TV + Radio
        if split is None:

            count = (
                data.set_index(["date"])
                .groupby([pd.Grouper(freq = freq)],as_index = True)
                ["count"].sum()
                .reset_index()
            )

            if kind == "bar":
                fig = px.bar(count,x = "date",y = "count",title = "Evolution du nombre de mentions au cours du temps",height = 400)
            elif kind == "area":
                fig = px.area(count,x = "date",y = "count",title = "Evolution du nombre de mentions au cours du temps",height = 400)
            else:
                raise Exception("kind argument should be 'area' or 'bar'")


        # Split TV / Radio
        else:

            count = (
                data.set_index(["date"])
                .groupby([pd.Grouper(freq = freq),split],as_index = True)
                ["count"].sum()
                .reset_index()
            )

            # Understand split between TV & Radio as a total percentage (ex: 55% radio / 45% TV)
            if as_percent:

                fig = px.area(count,
                            x = "date",y = "count",color = split,groupnorm='fraction',
                            title = "Evolution du nombre de mentions au cours du temps par type de média en %",height = 400,
                )
                fig.update_layout(yaxis_tickformat='0%')
            
            # Show Total mentions by TV or Radio
            else:

                if kind == "bar":
                    fig = px.bar(count,
                                x = "date",y = "count",color = split,
                                title = "Evolution du nombre de mentions au cours du temps",
                                height = 400,
                    )
                elif kind == "area":
                    fig = px.area(count,
                                x = "date",y = "count",color = split,
                                title = "Evolution du nombre de mentions au cours du temps",height = 400,
                    )
                else:
                    raise Exception("kind argument should be 'area' or 'bar'")


    # Aggregate by channel if a list of channels is provided
    else:

        count = (
            data.set_index(["date"])
            .groupby([pd.Grouper(freq = freq),"channel_name"],as_index = True)
            ["count"].sum()
            .reset_index()
        )

        count = count.loc[count["channel_name"].isin(list_of_channels)]

        if kind == "line":
            fig = px.line(count,
                        x = "date",y = "count",color = "channel_name",
                        title = "Evolution du nombre de mention au cours du temps par chaîne",height = 400
            )

        elif kind == "bar":
            fig = px.bar(count,
                        x = "date",y = "count",color = "channel_name",
                        title = "Evolution du nombre de mention au cours du temps par chaîne",height = 400
            )

        else:
            raise Exception("kind argument should be 'line' or 'bar'")

    return fig


def show_mentions_by_time_of_the_day(data,freq = "1H",kind = "bar",as_percent = False,split = None,list_of_channels = None):

    assert kind in ["area","bar","line"]


    if list_of_channels is None:

        if split is None:

            count = (
                data
                .set_index(["time_of_the_day"])
                .groupby([pd.Grouper(freq = freq)],as_index = True)
                ["count"].sum()
                .reset_index()
                .assign(time_of_the_day = lambda x: x["time_of_the_day"].map(lambda y : str(y)[7:12]))
            )

            if kind == "bar":
                fig = px.bar(
                    count,
                    x = "time_of_the_day",y = "count",
                )
            elif kind == "area":
                fig = px.area(
                    count,
                    x = "time_of_the_day",y = "count",
                )       

        # Split by TV or Radio
        else:

            count = (
                data
                .set_index(["time_of_the_day"])
                .groupby([pd.Grouper(freq = freq),split],as_index = True)
                ["count"].sum()
                .reset_index()
                .assign(time_of_the_day = lambda x: x["time_of_the_day"].map(lambda y : str(y)[7:12]))
                .sort_values("time_of_the_day",ascending = True)
            )


            if kind == "bar":

                fig = px.bar(
                    count,
                    text_auto = "s",
                    x = "time_of_the_day",y = "count",color=split,
                    height = 400,
                    category_orders={"time_of_the_day":count["time_of_the_day"].unique()},
                    title = "Répartition des mentions par heure de la journée"
                )

            elif kind == "area":

                fig = px.area(
                    count,
                    x = "time_of_the_day",y = "count",color = split,groupnorm='fraction' if as_percent else None,
                    category_orders={"time_of_the_day":count["time_of_the_day"].unique()},
                    title = "Répartition des mentions par heure de la journée en %",height = 400,
                )
                if as_percent: fig.update_layout(yaxis_tickformat='0%') 

    else:

        count = (
            data
            .set_index(["time_of_the_day"])
            .groupby([pd.Grouper(freq = freq),"channel_name"],as_index = True)
            ["count"].sum()
            .reset_index()
            .assign(time_of_the_day = lambda x: x["time_of_the_day"].map(lambda y : str(y)[7:12]))
            .sort_values("time_of_the_day",ascending = True)
        )

        count = count.loc[count["channel_name"].isin(list_of_channels)]

        if kind == "bar":
            fig = px.bar(
                count,
                x = "time_of_the_day",y = "count",color="channel_name",
                height = 400,
                category_orders={"time_of_the_day":count["time_of_the_day"].unique()},
                title = "Répartition des mentions par heure de la journée"
            )

        elif kind == "area":

            fig = px.area(
                count,
                x = "time_of_the_day",y = "count",color = "channel_name",groupnorm='fraction' if as_percent else None,
                category_orders={"time_of_the_day":count["time_of_the_day"].unique()},
                title = "Répartition des mentions par heure de la journée en %",height = 400,
            )
            if as_percent: fig.update_layout(yaxis_tickformat='0%') 
        
        elif kind == "line":

            fig = px.line(
                count,
                x = "time_of_the_day",y = "count",color="channel_name",
                height = 400,
                category_orders={"time_of_the_day":count["time_of_the_day"].unique()},
                title = "Répartition des mentions par heure de la journée"
            )

    return fig



def show_mentions_treemap(data,list_of_channels,freq = "4H",path = ["channel_name","time_of_the_day"]):

    def parse_period(y,freq):
        hours = int(str(y)[7:9])
        return f"{hours}-{hours+int(freq.replace('H',''))}h"

    # Process path to add grouper on time of the day
    group = []

    if "time_of_the_day" in path:
        group.append(pd.Grouper(freq = freq))
        group.extend([x for x in path if x != "time_of_the_day"])

        count = (
            data
            .set_index(["time_of_the_day"])
            .groupby(group,as_index = True)
            ["count"].sum()
            .reset_index()
            .assign(time_of_the_day = lambda x: x["time_of_the_day"].map(lambda y : parse_period(y,freq)))
            .sort_values("time_of_the_day",ascending = True)
        )

    else:

        count = (
            data
            .groupby(path,as_index = True)
            ["count"].sum()
            .reset_index()
        )

    count = count.loc[count["channel_name"].isin(list_of_channels)]

    fig = px.treemap(
        count,
        path = path,
        values = "count",
    )
    return fig