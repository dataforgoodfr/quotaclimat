import matplotlib.pyplot as plt
import nltk
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import MultiLabelBinarizer
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
        title="Comparaison du nombre de titre d'articles comprenant un mot d'une des deux listes",
    )
    return fig

def plot_media_count_comparison_all_kw(df, keywords: list):

    dict_data = {}
    for kw in keywords:
        dict_data[kw] = df[df.news_title.str.contains(kw)]
        dict_data[kw]["keyword"] = kw

    df_keywords_1 = pd.concat([value for value in dict_data.values()])
    # Count the data for the plot 
    df_to_plot = df_keywords_1.groupby(["media","keyword"]).count().reset_index()
    # Count the proportion of a keyword in a media
    total_media = df_keywords_1.groupby(["media"]).count().news_title
    for media in total_media.items():
        mask = df_to_plot.media.values == media[0]
        df_to_plot.loc[mask,"keyword_percent_in_media"] = df_to_plot.loc[mask,"news_title"]/media[1]*100
    df_to_plot.keyword_percent_in_media = df_to_plot.keyword_percent_in_media.apply(lambda x : f'{x:.2f}')

    fig = px.bar(
        df_to_plot,
        x="media",
        y="news_title",
        color="keyword",
        hover_data =["keyword_percent_in_media"],
        title="Comparaison du nombre de titre d'articles comprenant un mot d'une des deux listes (avec doublons)",
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
    fig, ax = plt.subplots(figsize=(13, 8), facecolor="k")

    ax = plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.tight_layout(pad=0)

    return fig
