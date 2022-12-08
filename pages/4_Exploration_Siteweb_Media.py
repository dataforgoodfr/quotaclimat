import datetime

import pandas as pd
import streamlit as st
from streamlit_tags import st_tags

from quotaclimat.data_analytics.sitemap_analytics import (
    CLIMATE_KW, SECTION_CLIMAT, filter_df_section_and_keyword, make_word_cloud,
    plot_comparison_of_temporal_total_count, plot_media_count_comparison)
from quotaclimat.data_processing.sitemap_processing import (
    feature_engineering_sitemap, load_all)

# TODO: seperate processing from plotting!


st.sidebar.markdown("# Exploration des titres d'article des site web")


df_all = load_all()

date_min = df_all.news_publication_date.min().date()
date_max = df_all.news_publication_date.max().date()


tab1, tab2, tab3 = st.tabs(
    [
        "Evolution au cours du temps",
        "Comparer appartition de mot cléfs",
        "Les mots de la semaine",
    ]
)

with tab1:
    st.markdown("Onglet en cours de constuction") #TODO

with tab2:
    st.markdown("## Exploration des titres d'article sur les siteweb des medias")
    st.markdown(f"Données disponibles du {date_min} au {date_max}.")
    keywords = st_tags(
        label="Entrez des mots clé:",
        text="Pressez entrez pour ajouter",
        value=[
            "COP",
            "climatique",
            "écologie",
            "CO2",
            "effet de serre",
            "transition énergétique",
            "carbone",
        ],
        suggestions=["environnement"],
        maxtags=30,
        key="1",
    )
    keywords_compare = st_tags(
        label="Pour les comparer aux mots clé suivants:",
        text="Pressez entrez pour ajouter",
        value=[
            "migrants",
            "immigrés",
            "sans-papiers",
            "immigration",
            "migration",
            "émigration",
            "émigrés",
            "ocean viking",
        ],
        suggestions=["politique"],
        maxtags=30,
        key="2",
    )
    btn = st.button("Lancer l'analyse")
    if btn:
        fig = plot_media_count_comparison(df_all, keywords, keywords_compare)
        st.plotly_chart(fig)

        fig_temporal_count = plot_comparison_of_temporal_total_count(
            df_all, keywords, keywords_compare
        )
        st.plotly_chart(fig_temporal_count)


with tab3:
    st.markdown("Les mots les plus apparus cette semaine:")
    a_week_ago = datetime.datetime.today() - datetime.timedelta(weeks=1)

    df_last_week = df_all[pd.to_datetime(df_all.download_date) > a_week_ago]
    df_featured = feature_engineering_sitemap(df_all)
    st.pyplot(make_word_cloud(df_featured))
    st.markdown("Les mots les plus apparus cette semaine dans les sections climats:")
    df_climate = filter_df_section_and_keyword(df_featured, CLIMATE_KW, SECTION_CLIMAT)
    print(df_featured.shape)

    print(df_climate.shape)
    st.pyplot(make_word_cloud(df_climate))
