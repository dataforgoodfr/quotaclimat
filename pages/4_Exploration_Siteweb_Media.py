import datetime

import pandas as pd
import streamlit as st
from streamlit_tags import st_tags

from quotaclimat.data_analytics.sitemap_analytics import (
    fig_percentage_between_two_dates_per_day_and_leaderboard_per_media,
    plot_articles_lifespan_comparison, plot_articles_total_count_evolution,
    plot_comparison_of_temporal_total_count, plot_media_count_comparison,
    plot_media_count_comparison_all_kw)
from quotaclimat.data_processing.sitemap_processing import (
    feature_engineering_sitemap, filter_df, load_all)
from quotaclimat.utils.climate_keywords import CLIMATE_KW

# TODO: seperate processing from plotting!


@st.cache_data(ttl=7200)
def cached_load_all():
    return load_all()


st.sidebar.markdown("# Exploration des titres d'article des site web")

df_all = cached_load_all()
df_featured = feature_engineering_sitemap(df_all)

date_min = df_all.news_publication_date.min().date()
date_max = df_all.news_publication_date.max().date()


tab1, tab2, tab3 = st.tabs(
    [
        "Evolution au cours du temps",
        "Comparaison de mots clé",
        "Qualité de couvertures WIP",
    ]
)

with tab1:
    with st.expander("Les mots les plus apparus cette semaine", expanded=False):
        a_week_ago = datetime.datetime.today() - datetime.timedelta(weeks=1)

        # df_last_week = df_all[pd.to_datetime(df_all.download_date) > a_week_ago]
        # df_lw_featured = feature_engineering_sitemap(df_last_week)
        # st.pyplot(make_word_cloud(df_lw_featured))

    with st.expander("Analyse de mot clé", expanded=False):
        keywords = st_tags(
            label="Entrez des mots clé en minuscule:",
            text="Pressez entrez pour ajouter",
            value=CLIMATE_KW,
            suggestions=["environment"],
            maxtags=30,
            key="0",
        )
        d_lower = st.date_input(
            "Entrez date à laquel commencer le traitement", datetime.date(2022, 12, 1)
        )
        d_upper = st.date_input(
            "Entrez date à laquel terminer le traitement", datetime.date(2022, 12, 8)
        )
        df_between_two_dates = filter_df(df_featured, d_lower, d_upper, keywords)
        (
            fig_time_series,
            fig_leaderboard,
        ) = fig_percentage_between_two_dates_per_day_and_leaderboard_per_media(
            df_featured, d_lower, d_upper, keywords
        )
        st.plotly_chart(fig_time_series)
        st.plotly_chart(fig_leaderboard)
        # st.pyplot(make_word_cloud(df_between_two_dates))

with tab2:
    st.markdown("## Exploration des titres d'article sur les siteweb des medias")
    st.markdown(f"**Données disponibles du {date_min} au {date_max}.**")
    keywords = st_tags(
        label="Entrez des mots clé:",
        text="Pressez entrez pour ajouter",
        value=CLIMATE_KW,
        suggestions=["environnement"],
        maxtags=30,
        key="1",
    )
    keywords_compare = st_tags(
        label="Pour les comparer aux mots clé suivants:",
        text="Pressez entrez pour ajouter",
        value=[
            "macron",
            "retraites",
            "retraite",
            "49.3",
            "Corée du Nord",
            "Ukraine",
        ],
        suggestions=["politique"],
        maxtags=30,
        key="2",
    )
    d_lower_ = st.date_input(
        "Entrez date à laquel commencer le traitement",
        datetime.date(2022, 12, 1),
        key="3",
    )
    d_upper_ = st.date_input(
        "Entrez date à laquel terminer le traitement",
        datetime.date(2023, 3, 19),
        key="4",
    )
    df_between_two_dates = df_all[
        (pd.to_datetime(df_all.download_date).dt.date >= d_lower_)
        & (pd.to_datetime(df_all.download_date).dt.date <= d_upper_)
    ]
    btn = st.button("Lancer l'analyse")
    if btn:
        fig = plot_media_count_comparison(
            df_between_two_dates, keywords, keywords_compare
        )
        st.plotly_chart(fig)

        fig_all_kw = plot_media_count_comparison_all_kw(
            df_all, keywords + keywords_compare
        )
        st.plotly_chart(fig_all_kw)

        fig_temporal_count = plot_comparison_of_temporal_total_count(
            df_between_two_dates, keywords, keywords_compare
        )
        st.plotly_chart(fig_temporal_count)

        fig_temporal_total_count = plot_articles_total_count_evolution(
            df_between_two_dates, keywords, keywords_compare
        )
        st.plotly_chart(fig_temporal_total_count)

        fig_lifespan = plot_articles_lifespan_comparison(
            df_between_two_dates, keywords, keywords_compare
        )
        st.plotly_chart(fig_lifespan)


with tab3:
    st.markdown("WORK IN PROGRESS ")
