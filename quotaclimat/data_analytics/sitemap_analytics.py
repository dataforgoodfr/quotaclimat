import matplotlib.pyplot as plt
import nltk
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from wordcloud import WordCloud

SECTION_CLIMAT = ["planete", "environnement", "crise-climatique"]
CLIMATE_KW = [
    " cop27",
    "  cop ",
    "climatique",
    "écologie",
    "CO2",
    "effet de serre",
    "transition énergétique",
    "carbone",
    "sécheresse" "transition énergétique",
    "méthane",
    "GIEC",
    "zéro émission",
]
nltk.download("stopwords")

stopwords_fr = nltk.corpus.stopwords.words("french")


def plot_media_count_comparison(df, keywords: list, keywords_comp: list):
    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]

    df_gb_1 = (
        df_keywords_1.groupby("media").count() / df.groupby(["media"]).count()
    ).reset_index()
    df_gb_1 = round(
        (
            df_keywords_1.groupby(["media"]).count() / df.groupby(["media"]).count()
        ).news_title
        * 100,
        2,
    ).reset_index()
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_gb_1 = df_gb_1.sort_values("news_title", ascending=False)
    df_gb_2 = round(
        (
            df_keywords_2.groupby(["media"]).count() / df.groupby(["media"]).count()
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
        title="Comparaison du % de titre d'articles comprenant mot des deux listes",
    )
    return fig


def plot_comparison_of_temporal_total_count(df, keywords: list, keywords_comp: list):

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
        index=vectorizer.get_feature_names(),
        columns=["tfidf_sum"],
    )

    wordcloud = WordCloud()
    wordcloud.generate_from_frequencies(
        frequencies=tfidf_positive_topic_sum.to_dict()["tfidf_sum"]
    )
    fig, ax = plt.subplots(figsize=(13, 8), facecolor="k")

    ax = plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    return fig


def fig_percentage_between_two_dates_per_day_and_leaderboard_per_media(
    df, date_lower_bound, date_upper_bound, keywords
):
    figs_percentage = []

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
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
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
