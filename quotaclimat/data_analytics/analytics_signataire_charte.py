import os

import pandas as pd
import plotly.express as px
import spacy

PATH_SIGNING_PARTNERS = "data_public/signataire_charte/"


def load_signing_partners_data(latest=True):
    latest_data_path = (
        PATH_SIGNING_PARTNERS + sorted(os.listdir(PATH_SIGNING_PARTNERS))[-1]
    )
    df = pd.read_csv(latest_data_path + "/" + os.listdir(latest_data_path)[0])
    return df


def get_summary_statistics(df):
    nb_unique_signataire = len(df.drop_duplicates(subset=["Nom", "Prénom"]))
    nb_unique_organisation = len(df.drop_duplicates(subset=["Média ou organisation"]))

    return nb_unique_signataire, nb_unique_organisation


def bar_plot_nb_of_signataire_per_media(df):
    """ """
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
    """ """
    count_medias = df.groupby("Fonction").count().sort_values("Nom", ascending=False)
    top_n = 20
    fig = px.bar(
        count_medias.reset_index().head(top_n),
        x="Nom",
        y="Fonction",
        orientation="h",
        color="Fonction",
    )

    fig.update_xaxes(tickangle=-45, title=None)
    fig.update_yaxes(title="Nombre de personne signataire")
    fig.update_layout(
        margin={"b": 100},
        title="Nombre de signataires de la charte journalisme écologie par fonction, top %s"
        % top_n,
        showlegend=False,
    )
    return fig
