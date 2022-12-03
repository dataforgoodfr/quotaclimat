import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def plot_media_count_comparison(df, keywords: list, keywords_comp: list):
    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]

    df_gb_1 = df_keywords_1.groupby("media").count().reset_index()
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_gb_2 = df_keywords_2.groupby("media").count().reset_index()
    df_gb_2["keyword list"] = ", ".join(keywords_comp)[:25] + "..."
    df_to_plot = pd.concat([df_gb_1, df_gb_2])
    fig = px.bar(
        df_to_plot,
        x="media",
        y="news_title",
        color="keyword list",
        title="Comparaison du nombre de titre d'articles comprenant mot des deux listes",
    )
    return fig


def plot_comparison_of_temporal_total_count(df, keywords: list, keywords_comp: list):

    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]
    df_keywords_1[
        "news_publication_date_as_str"
    ] = df_keywords_1.news_publication_date.dt.strftime("%Y-%m-%d")
    df_keywords_2[
        "news_publication_date_as_str"
    ] = df_keywords_2.news_publication_date.dt.strftime("%Y-%m-%d")

    df_gb_1 = (
        df_keywords_1.groupby(["news_publication_date_as_str"]).count().reset_index()
    )
    df_gb_1["keyword list"] = ", ".join(keywords)
    df_gb_2 = (
        df_keywords_2.groupby(["news_publication_date_as_str"]).count().reset_index()
    )
    df_gb_2["keyword list"] = ", ".join(keywords_comp)

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            y=df_gb_1["news_title"],
            x=df_gb_1["news_publication_date_as_str"],
            name=", ".join(keywords)[:25] + "...",
        )
    )
    fig.add_trace(
        go.Scatter(
            y=df_gb_2["news_title"],
            x=df_gb_2["news_publication_date_as_str"],
            name=", ".join(keywords_comp)[:25] + "...",
        )
    )
    fig.update_layout(
        title_text="Comparaison de l évolution temporelle du nombre d apparition des mots clé",
        xaxis_title="Date de publication des articles",
        yaxis_title="Nombre total d articles",
    )
    return fig
