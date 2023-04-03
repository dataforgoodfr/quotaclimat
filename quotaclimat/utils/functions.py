# Import des librairies

import glob
import math
import re
# Pour les warnings
import warnings
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from numpy import arange

warnings.filterwarnings("ignore")
from collections import Counter

# Pour le tritement de texte
import nltk
from nltk.corpus import stopwords
from wordcloud import STOPWORDS, ImageColorGenerator, WordCloud


def colonne(df):
    """Itération sur les colonnes du dataframe pour afficher le nombre unique des valeurs dans chaque colonne
    Exploration des colonnes"""
    for col in df.columns:
        print("La colonne ", col, " : contient", df[col].nunique(), "valeur unique")


def format_pourcentage(value):
    """
    Format a percentage with 1 digit after comma
    """
    return "{0:.4f}%".format(value * 100)


def missing_data(df):
    """fonction qui retourne le nombre de nan total dans un df"""
    return df.isna().sum().sum()


def missing_percent(df):
    """fonction qui retourne le nombre de nan total dans un df en pourcentage"""
    return df.isna().sum().sum() / (df.size)


def summary(df):
    """ "Fonction summary du dataframe elle affciche la taille du df,nbre unique de la variable,nana et valeur minimale"""
    obs = df.shape[0]
    types = df.dtypes
    counts = df.apply(lambda x: x.count())
    # min = df.min()
    uniques = df.apply(lambda x: x.unique().shape[0])
    nulls = df.apply(lambda x: x.isnull().sum())
    print("Data shape:", df.shape)
    # cols = ["types", "counts", "uniques", "nulls","min","max"]
    cols = ["types", "counts", "uniques", "nulls"]
    str = pd.concat([types, counts, uniques, nulls], axis=1, sort=True)

    str.columns = cols
    dtypes = str.types.value_counts()
    print("___________________________\nData types:")
    print(str.types.value_counts())
    print("___________________________")
    return str


def missing_values(df):
    """Fonction qui retourne un df avec nombre de nan et pourcentage"""
    nan = pd.DataFrame(columns=["Variable", "nan", "%nan"])
    nan["Variable"] = df.columns
    missing = []
    percent_missing = []
    for col in df.columns:
        nb_missing = missing_data(df[col])
        pc_missing = format_pourcentage(missing_percent(df[col]))
        missing.append(nb_missing)
        percent_missing.append(pc_missing)
    nan["nan"] = missing
    nan["%nan"] = percent_missing
    return nan.sort_values(by="%nan", ascending=False)


def filter_columns(df, cutoff=0.9):
    tot_rows = df.shape[0]
    removed_cols = []
    print("original number of columns: ", df.shape[1])
    for col in df.columns:
        num_na = df[col].isna().sum()
        if (num_na / tot_rows) > cutoff:
            removed_cols.append(col)
    print("number of columns removed: ", len(removed_cols))
    return df.drop(removed_cols, axis=1)


def filter_rows(df, cutoff=0.9):
    tot_cols = df.shape[1]
    print("original number of rows: ", df.shape[0])
    df = df[df.isnull().sum(axis=1) < tot_cols * cutoff]
    print("remaining rows: ", df.shape[0])
    return df


def plot_nan(df):
    """Fonction nan et plot"""
    fig = plt.figure(figsize=(22, 10))

    nan_p = df.isnull().sum().sum() / len(df) / len(df.columns) * 100
    plt.axhline(y=nan_p, linestyle="--", lw=2)
    plt.legend(["{:.2f}% Taux global de nan".format(nan_p)], fontsize=14)

    null = df.isnull().sum(axis=0).sort_values() / len(df) * 100
    sns.barplot(x=null.index, y=null.values)

    plt.ylabel("%")
    plt.title("Pourcentage de NAN pour chaque variable")
    plt.xticks(rotation=70)
    plt.show()


def plot_remp(df):
    """Fonction remplissage et plot"""
    remplissage_df = df.count().sort_values(ascending=True)
    ax = remplissage_df.plot(kind="bar", figsize=(15, 15))
    ax.set_title("Remplissage des données")
    ax.set_ylabel("Nombre de données")
    ax.set_xticklabels(ax.get_xticklabels(), rotation=40, ha="right", fontsize=14)
    plt.tight_layout()


def missing_rows(df):
    """Fonction de nan par lignes"""
    lines_nan_info = []
    for index, row in df.iterrows():
        lines_nan_info.append((row.isna().sum().sum() / df.shape[1]) * 100)
        df_lines_nan_info = pd.DataFrame(np.array(lines_nan_info), columns=["nan %"])
    return df_lines_nan_info.sort_values(by=["nan %"], ascending=False)


def plot_cnt(df, col, title):
    countplt, ax = plt.subplots(figsize=(15, 15))
    ax = sns.countplot(
        x=col,
        data=df,
        ax=ax,
        order=df[col].value_counts().index,
    )

    ax.set_title(title, fontsize=18, color="b", fontweight="bold")
    plt.xticks(rotation=60)
    for rect in ax.patches:
        ax.text(
            rect.get_x() + rect.get_width() / 2,
            rect.get_height() + 0.75,
            rect.get_height(),
            horizontalalignment="center",
            fontsize=11,
        )
        countplt


def plot_stat(data, feature, title):

    ax, fig = plt.subplots(figsize=(20, 10))
    ax = sns.countplot(
        y=feature, data=data, order=data[feature].value_counts(ascending=False).index
    )
    ax.set_title(title)

    for p in ax.patches:
        percentage = "{:.1f}%".format(100 * p.get_width() / len(data["url"]))
        x = p.get_x() + p.get_width()
        y = p.get_y() + p.get_height() / 2
        ax.annotate(percentage, (x, y), fontsize=20, fontweight="bold")


def plot_perc(data, feature, title):

    ax, fig = plt.subplots(figsize=(20, 8))
    ax = sns.countplot(
        y=feature, data=data, order=data[feature].value_counts(ascending=False).index
    )
    ax.set_title(title)

    for p in ax.patches:
        percentage = "{:.1f}%".format(100 * p.get_width() / len(df["url_clean"]))
        x = p.get_x() + p.get_width()
        y = p.get_y() + p.get_height() / 2
        ax.annotate(percentage, (x, y), fontsize=20, fontweight="bold")


def wordCloudFunction(df, column, numWords):
    topic_words = [
        z.lower()
        for y in [x.split() for x in df[column] if isinstance(x, str)]
        for z in y
    ]
    word_count_dict = dict(Counter(topic_words))
    popular_words = sorted(word_count_dict, key=word_count_dict.get, reverse=True)
    popular_words_nonstop = [
        w for w in popular_words if w not in stopwords.words("french")
    ]
    word_string = str(popular_words_nonstop)
    wordcloud = WordCloud(
        stopwords=STOPWORDS,
        background_color="white",
        max_words=numWords,
        width=1000,
        height=1000,
    ).generate(word_string)
    plt.clf()
    plt.imshow(wordcloud)
    plt.axis("off")
    plt.show()


def wordBarGraphFunction(df, column, title):
    topic_words = [
        z.lower()
        for y in [x.split() for x in df[column] if isinstance(x, str)]
        for z in y
    ]
    word_count_dict = dict(Counter(topic_words))
    popular_words = sorted(word_count_dict, key=word_count_dict.get, reverse=True)
    popular_words_nonstop = [
        w for w in popular_words if w not in stopwords.words("french")
    ]
    plt.barh(
        range(50), [word_count_dict[w] for w in reversed(popular_words_nonstop[0:50])]
    )
    plt.yticks([x + 0.5 for x in range(50)], reversed(popular_words_nonstop[0:50]))
    plt.title(title)
    plt.show()
