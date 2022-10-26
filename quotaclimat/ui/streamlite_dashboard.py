import streamlit as st

from quotaclimat.data_analytics.data_coverage import (
    fig_time_coverage_of_extracts,
    fig_percentage_of_covered_time_by_keywords)
from quotaclimat.data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_all_data_dump)

PATH_EXTRACTS_DATA_DUMPS = "data/keywords/"
PATH_CHANNEL_METADATA = "data/channels.xlsx"


def build_dashboard():

    # streamlit interface
    st.image('coverquotaclimat.png')
    st.title("Quota climat x DataForGood ")
    st.header("Introduction")
    st.markdown(
        "Dans le paysage médiatique aujourd’hui, entre 2 et 5% du temps est consacré aux enjeux écologiques. Et c’est à peu près le seul chiffre que nous avons grâce aux travaux de l’Affaire du siècle et de ClimatMédias. Entrainé par l’élan des médias indépendants, de plus en plus de médias et journalistes s’engagent et c’est un tournant majeur (Radio France, TF1, chartes des journalistes à la hauteur de l’urgence écologique). Mais qu’en est-il en réalité ?"
    )
    df_all = read_and_format_all_data_dump(
        PATH_EXTRACTS_DATA_DUMPS, PATH_CHANNEL_METADATA
    )
    # figures answering the questions
    fig_percentage_of_covered_time_by_keywords_ = (
        fig_percentage_of_covered_time_by_keywords(df_all, PATH_CHANNEL_METADATA)
    )

    fig_coverage_extracts = fig_time_coverage_of_extracts(df_all)
    st.plotly_chart(fig_percentage_of_covered_time_by_keywords_)
    st.plotly_chart(fig_coverage_extracts)
