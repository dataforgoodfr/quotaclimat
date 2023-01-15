import os

import pandas as pd
import spacy
import plotly.express as px

PATH_SIGNING_PARTNERS = "data_public/signataire_charte/"


def load_signing_partners_data(latest=True):
    latest_data_path = (
        PATH_SIGNING_PARTNERS + sorted(os.listdir(PATH_SIGNING_PARTNERS))[0]
    )
    df = pd.read_csv(latest_data_path + "/" + os.listdir(latest_data_path)[0])
    return df


def get_summary_statistics(df):
    nb_unique_signataire = len(df.drop_duplicates(subset=["Nom", "Prénom"]))
    nb_unique_organisation = len(df.drop_duplicates(subset=["Média ou organisation"]))

    return nb_unique_signataire, nb_unique_organisation


def apply_lemmatizer_media(df, column_name="Média ou organisation") -> pd.DataFrame:
    """
    Lemmatize the given column of a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to be transformed.
    column_name : str
        The name of the column to apply the lemmatization.

    Returns
    -------
    The entire dataframe, with cleaned column.
    """
    # Load spacy model
    nlp = spacy.load('fr_core_news_md')

    # New column name
    new_column_name = f"{column_name}"

    # Apply porter stemmer
    df[new_column_name] = [[token.lemma_ for token in nlp(sentence)]
                           if pd.notna(sentence) else sentence for sentence in df[column_name]]
    # Join stemmed words
    df[new_column_name] = [" ".join(sentence).capitalize()
                           if type(sentence) == list else sentence for sentence in df[new_column_name]]

    # Clean media with REGEX
    df[new_column_name] = df[new_column_name] \
        .str.replace(r"[Ff]rance.?3.*", "France 3") \
        .str.replace(r"[Ff]rance.?télévisions?|[Ff]rance.?[Tt][Vv].?", "France TV") \
        .str.replace(r"[Ff]rance.?info.*", "France info") \
        .str.replace(r".*[Ii]nd[ée]pend.*", "Indépendant") \
        .str.replace(r"Free - lancer", "Freelance") \
        .str.replace(r"Aucun", "Indépendant")

    return df


def apply_lemmatizer_job(df, column_name="Fonction") -> pd.DataFrame:
    """
    Lemmatize the Fonction column of a dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to be transformed.
    column_name : str
        The name of the column to apply the lemmatization.

    Returns
    -------
    The entire dataframe, with cleaned column.
    """
    # Load spacy model
    nlp = spacy.load('fr_core_news_md')

    # New column name
    new_column_name = f"{column_name}"

    # Apply porter stemmer
    df[new_column_name] = [[token.lemma_ for token in nlp(sentence)]
                           if pd.notna(sentence) else sentence for sentence in df[column_name]]
    # Join stemmed words
    df[new_column_name] = [" ".join(sentence).capitalize()
                           if type(sentence) == list else sentence for sentence in df[new_column_name]]

    # Clean media with REGEX
    df[new_column_name] = df[new_column_name] \
        .str.replace(r".*[Ii]nd[ée]pend.*", "Indépendant") \
        .str.replace(r"Journaliste pigist", "Pigiste") \
        .str.replace(r"Journaliste reporter", "Reporter") \
        .str.replace(r"Journaliste r[ée]alisateur", "Réalisateur") \
        .str.replace(r"Photojournaliste", "Photographe") \
        .str.replace(r"R[ée]alisatrice", "Réalisateur") \
        .str.replace(r"R[ée]dactrice", "Rédacteur") \
        .str.replace(r"Journaliste r[ée]dacteur", "Rédacteur") \
        .str.replace(r"Directrice", "Directeur") \
        .str.replace(r"de le", "de") \
        .str.replace(r'(^.*[ÉEée]tudiant.*$)', 'Étudiant')

    return df


def bar_plot_nb_of_signataire_per_media(df):
    """

    """
    count_medias = (
        df.groupby("Média ou organisation").count().sort_values("Nom", ascending=False)
    )
    top_n = 50
    fig = px.bar(
        count_medias.reset_index().head(top_n),
        x="Média ou organisation",
        y="Nom",
    )

    fig.update_xaxes(tickangle=-45, title=None)
    fig.update_yaxes(title="Nombre de personne signataire")
    fig.update_layout(
        margin={"b": 100},
        title="Nombre de signataires de la charte journalisme écologie par média, top %s"
        % top_n,
    )
    return fig


def bar_plot_nb_of_signataire_per_job(df):
    """

    """
    count_medias = (
        df.groupby("Fonction").count().sort_values("Nom", ascending=False)
    )
    top_n = 20
    fig = px.bar(
        count_medias.reset_index().head(top_n),
        x="Nom",
        y="Fonction",
        orientation='h',
        color='Fonction'
    )

    fig.update_xaxes(tickangle=-45, title=None)
    fig.update_yaxes(title="Nombre de personne signataire")
    fig.update_layout(
        margin={"b": 100},
        title="Nombre de signataires de la charte journalisme écologie par fonction, top %s"
        % top_n,
        showlegend=False
    )
    return fig
