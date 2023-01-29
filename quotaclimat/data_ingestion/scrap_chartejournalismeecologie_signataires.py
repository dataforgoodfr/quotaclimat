import os
from datetime import datetime

import pandas as pd
import requests
import spacy
from bs4 import BeautifulSoup

# TODO: add log in and catch expections
# TODO: enforce field type with pandera

TARGET_PATH = (
    "data_public/signataire_charte/generated_on=%s"
    % datetime.today().strftime("%Y%m%d")
)


def capitalize(name):
    """Capitalize a first name or a last name. Even the hyphenated first names.

    Examples:
     - elsa -> Elsa
     - jean-pierre or Jean-pierre -> Jean-Pierre
    """
    if name:
        if name.find("-") != -1:
            list_names = name.split("-")
            list_capitalized_names = [name.capitalize() for name in list_names]
            return "-".join(list_capitalized_names)
        else:
            list_names = name.split()
            list_capitalized_names = [name.capitalize() for name in list_names]
            return " ".join(list_capitalized_names)


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
    nlp = spacy.load("fr_core_news_md")

    # New column name
    new_column_name = f"{column_name}"

    # Apply porter stemmer
    df[column_name] = [
        [token.lemma_ for token in nlp(sentence)] if pd.notna(sentence) else sentence
        for sentence in df[column_name]
    ]
    # Join stemmed words
    df[column_name] = [
        " ".join(sentence).capitalize() if type(sentence) == list else sentence
        for sentence in df[column_name]
    ]

    # Clean media with REGEX
    df[column_name] = (
        df[column_name]
        .str.replace(r"[Ff]rance.?3.*", "France 3")
        .str.replace(r"[Ff]rance.?télévisions?|[Ff]rance.?[Tt][Vv].?", "France TV")
        .str.replace(r"[Ff]rance.?info.*", "France info")
        .str.replace(r".*[Ii]nd[ée]pend.*", "Indépendant")
        .str.replace(r"Free - lancer", "Freelance")
        .str.replace(r"Aucun", "Indépendant")
    )

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
    nlp = spacy.load("fr_core_news_md")

    # Apply porter stemmer
    df[column_name] = [
        [token.lemma_ for token in nlp(sentence)] if pd.notna(sentence) else sentence
        for sentence in df[column_name]
    ]
    # Join stemmed words
    df[column_name] = [
        " ".join(sentence).capitalize() if type(sentence) == list else sentence
        for sentence in df[column_name]
    ]

    # Clean media with REGEX
    df[column_name] = (
        df[column_name]
        .str.replace(r".*[Ii]nd[ée]pend.*", "Indépendant")
        .str.replace(r"Journaliste pigist", "Pigiste")
        .str.replace(r"Journaliste reporter", "Reporter")
        .str.replace(r"Journaliste r[ée]alisateur", "Réalisateur")
        .str.replace(r"Photojournaliste", "Photographe")
        .str.replace(r"R[ée]alisatrice", "Réalisateur")
        .str.replace(r"R[ée]dactrice", "Rédacteur")
        .str.replace(r"Journaliste r[ée]dacteur", "Rédacteur")
        .str.replace(r"Directrice", "Directeur")
        .str.replace(r"de le", "de")
        .str.replace(r"(^.*[ÉEée]tudiant.*$)", "Étudiant")
    )

    return df


def scrap_chartejournalismeecologie_dot_fr():

    URL = "https://chartejournalismeecologie.fr/les-signataires/"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "html.parser")
    html_table = soup.table
    df_signing_journalists = pd.read_html(str(html_table), header=0)[0]

    df_signing_journalists.dropna(axis=0, how="all", inplace=True)
    df_signing_journalists.reset_index(drop=True, inplace=True)
    # Capitalize first name and last name
    df_signing_journalists["Nom"] = (
        df_signing_journalists["Nom"].astype(str).apply(capitalize)
    )
    df_signing_journalists["Prénom"] = (
        df_signing_journalists["Prénom"].astype(str).apply(capitalize)
    )
    # Clean Media column
    df_signing_journalists = apply_lemmatizer_media(df_signing_journalists)
    # Clean Job column
    df_signing_journalists = apply_lemmatizer_job(df_signing_journalists)
    return df_signing_journalists


def write_signataire_charte(df):

    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH)

    df.to_csv(TARGET_PATH + "/signataire_chartejournalismeecologie.csv", index=False)


def run():
    df = scrap_chartejournalismeecologie_dot_fr()
    write_signataire_charte(df)


if __name__ == "__main__":
    run()
