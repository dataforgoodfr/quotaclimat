import datetime as dt

import matplotlib.pyplot as plt
import nltk
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from wordcloud import WordCloud

from quotaclimat.utils.plotly_theme import THEME, WARMING_STRIPES_SEQUENCE

COLOR_RADIO = WARMING_STRIPES_SEQUENCE[0]
COLOR_TV = WARMING_STRIPES_SEQUENCE[1]
COLOR_ECO = WARMING_STRIPES_SEQUENCE[3]

SECTION_CLIMAT = ["planete", "environnement", "crise-climatique"]

nltk.download("stopwords")

stopwords_fr = nltk.corpus.stopwords.words("french")


def plot_media_count_comparison(
    df_keywords_ref,
    df_keywords_to_compare,
    df_total,
    keywords: list,
    keywords_comp: list,
):

    df_gb_1 = (
        df_keywords_ref.groupby("media").count() / df_total.groupby(["media"]).count()
    ).reset_index()
    df_gb_1 = round(
        (
            df_keywords_ref.groupby(["media"]).count()
            / df_total.groupby(["media"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_gb_1 = df_gb_1.sort_values("news_title", ascending=False)
    df_gb_2 = round(
        (
            df_keywords_to_compare.groupby(["media"]).count()
            / df_total.groupby(["media"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()
    df_gb_2["keyword list"] = ", ".join(keywords_comp)[:25] + "..."
    df_to_plot = pd.concat([df_gb_1, df_gb_2])
    fig = px.bar(
        df_to_plot,
        x="media",
        y="news_title",
        color="keyword list",
        title="Comparaison du % de titre d'articles comprenant un mot d'une des deux listes",
    )
    return fig


def plot_media_count_comparison_all_kw(df, keywords: list):

    dict_data = {}
    for kw in keywords:
        dict_data[kw] = df[df.news_title.str.contains(kw)]
        dict_data[kw]["keyword"] = kw

    df_keywords_1 = pd.concat([value for value in dict_data.values()])
    # Count the data for the plot
    df_to_plot = df_keywords_1.groupby(["media", "keyword"]).count().reset_index()
    # Count the proportion of a keyword in a media
    total_media = df_keywords_1.groupby(["media"]).count().news_title
    for media in total_media.items():
        mask = df_to_plot.media.values == media[0]
        df_to_plot.loc[mask, "keyword_percent_in_media"] = (
            df_to_plot.loc[mask, "news_title"] / media[1] * 100
        )
    df_to_plot.keyword_percent_in_media = df_to_plot.keyword_percent_in_media.apply(
        lambda x: f"{x:.2f}"
    )

    fig = px.bar(
        df_to_plot,
        x="media",
        y="news_title",
        color="keyword",
        hover_data=["keyword_percent_in_media"],
        title="Comparaison du nombre de titre d'articles comprenant un mot d'une des deux listes (avec doublons)",
    )

    return fig


def plot_comparison_of_temporal_total_count(df, keywords: list, keywords_comp: list):
    """
    Comparaison de l évolution temporelle de l'apparition des mots clé
    """
    df["news_publication_date_as_str"] = df.news_publication_date.dt.strftime(
        "%Y-%m-%d"
    )
    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]
    df_gb_1 = round(
        (
            df_keywords_1.groupby(["news_publication_date_as_str"]).count()
            / df.groupby(["news_publication_date_as_str"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()
    df_gb_2 = round(
        (
            df_keywords_2.groupby(["news_publication_date_as_str"]).count()
            / df.groupby(["news_publication_date_as_str"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()

    df_gb_1["keyword list"] = ", ".join(keywords)
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
        title_text="Comparaison de l évolution temporelle du % d apparition des mots clé",
        xaxis_title="Date de publication des articles",
        yaxis_title="% du total d articles",
    )
    return fig


def plot_articles_total_count_evolution(df, keywords: list, keywords_comp: list):
    """
    Comparaison de l évolution temporelle de la présence des mots clé
    """
    duration = (
        df["download_date_last"] - df["news_publication_date"] + dt.timedelta(days=1)
    )
    df["duration_in_days"] = duration.dt.days

    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]

    label1 = ", ".join(keywords)[:25] + "..."
    label2 = ", ".join(keywords_comp)[:25] + "..."

    day_min = df["news_publication_date"].dt.date.min()
    day_max = df["news_publication_date"].dt.date.max()

    article_count = pd.DataFrame(index=pd.date_range(day_min, day_max, freq="D"))

    for idx in article_count.index:
        temp_min1 = df_keywords_1.news_publication_date.dt.date <= idx.date()
        temp_max1 = idx.date() <= df_keywords_1.download_date_last.dt.date
        article_count.loc[idx, label1] = sum(np.logical_and(temp_min1, temp_max1))

        temp_min2 = df_keywords_2.news_publication_date.dt.date <= idx.date()
        temp_max2 = idx.date() <= df_keywords_2.download_date_last.dt.date
        article_count.loc[idx, label2] = sum(np.logical_and(temp_min2, temp_max2))

    fig = px.line(
        article_count.reset_index(),
        x="index",
        y=[label1, label2],
        title="Evolution du nombre d'articles comprenant un mot d'une des deux listes au cours du temps",
    )

    return fig


def filter_df_section_and_keyword(
    df_origin: pd.DataFrame, keywords: list, sections: list
):
    df = df_origin.copy()
    df_positive_topic = df[
        df.news_title.str.contains("|".join(keywords)) | (df[sections] == 1).any(axis=1)
    ]
    return df_positive_topic


def make_word_cloud(df_origin: pd.DataFrame):

    vectorizer = TfidfVectorizer(max_df=0.1, min_df=0.01, stop_words=stopwords_fr)
    tfidf_positive_topic = vectorizer.fit_transform(df_origin.news_title)
    tfidf_positive_topic_sum = pd.DataFrame(
        tfidf_positive_topic.T.sum(axis=1),
        index=vectorizer.get_feature_names_out(),
        columns=["tfidf_sum"],
    )

    wordcloud = WordCloud()
    wordcloud.generate_from_frequencies(
        frequencies=tfidf_positive_topic_sum.to_dict()["tfidf_sum"]
    )
    fig, _ = plt.subplots(figsize=(13, 8), facecolor="k")

    _ = plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    return fig


def draw_figures_coverage_percentage(
    df_gb_1, df_gb_per_media, date_lower_bound, date_upper_bound, keywords
):
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    figs_percentage = []
    figs_percentage.append(
        px.bar(
            df_gb_1,
            x="download_date",
            y="news_title",
            color="type",
            title="Pourcentage de couverture total entre %s et %s"
            % (
                date_lower_bound.strftime("%Y-%m-%d"),
                date_upper_bound.strftime("%Y-%m-%d"),
            ),
            labels={"news_title": "Pourcentage", "download_date": "Jour d indexing"},
        )
    )

    figs_percentage.append(
        px.bar(
            df_gb_per_media,
            x="publication_name",
            y="news_title",
            color="type",
            title="Classement des médias entre %s et %s"
            % (
                date_lower_bound.strftime("%Y-%m-%d"),
                date_upper_bound.strftime("%Y-%m-%d"),
            ),
            labels={"news_title": "Pourcentage", "publication_name": "Media"},
        )
    )
    return figs_percentage


def fig_percentage_between_two_dates_per_day_and_leaderboard_per_media(
    df, date_lower_bound, date_upper_bound, keywords
):

    df_between_two_dates = df[
        (pd.to_datetime(df.download_date).dt.date >= date_lower_bound)
        & (pd.to_datetime(df.download_date).dt.date <= date_upper_bound)
    ]
    df_between_two_dates_kw = df_between_two_dates[
        df_between_two_dates.news_title.str.contains("|".join(keywords))
    ]

    df_gb_1 = round(
        (
            df_between_two_dates_kw.groupby(["type", "download_date"]).count()
            / df_between_two_dates.groupby(["type", "download_date"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()

    df_gb_per_media = round(
        (
            df_between_two_dates_kw.groupby(["publication_name", "type"]).count()
            / df_between_two_dates.groupby(["publication_name", "type"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()
    df_gb_per_media["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_gb_per_media = df_gb_per_media.sort_values("news_title", ascending=False)
    figs_percentage = draw_figures_coverage_percentage(
        df_gb_1, df_gb_per_media, date_lower_bound, date_upper_bound, keywords
    )

    return figs_percentage


def plot_articles_lifespan_comparison(df, keywords: list, keywords_comp: list):

    duration = (
        df["download_date_last"] - df["news_publication_date"] + dt.timedelta(days=1)
    )
    df["duration_in_days"] = duration.dt.days

    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]

    df_keywords_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_keywords_2["keyword list"] = ", ".join(keywords_comp)[:25] + "..."

    df_to_plot = pd.concat([df_keywords_1, df_keywords_2])

    fig = px.box(
        df_to_plot,
        x="media",
        y="duration_in_days",
        color="keyword list",
        title="Répartition du nombre de jours de présences des articles comprenant un mot d'une des deux listes",
        hover_data=["news_title"],
        log_y=True,
    )

    return fig


def plot_bar_volume_mediatique(percentages_per_type, title="Volume médiatique total"):
    fig = px.bar(
        percentages_per_type,
        height=400,
        text_auto=".1%",
    )
    fig.update_layout(
        yaxis_tickformat="0%",
        title=title,
        font_family="Poppins",
        yaxis_title="% du volume médiatique",
        xaxis_title="",
    )
    fig.update_layout(xaxis={"categoryorder": "total descending"}, showlegend=False)

    # fig.update_traces(marker_color=[COLOR_RADIO, COLOR_TV, COLOR_TV])
    return fig
