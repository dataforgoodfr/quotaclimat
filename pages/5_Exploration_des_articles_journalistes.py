import datetime

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_tags import st_tags

from quotaclimat.data_analytics.sitemap_analytics import (
    plot_articles_lifespan_comparison, plot_articles_total_count_evolution,
    plot_bar_volumne_mediatique, plot_comparison_of_temporal_total_count,
    plot_media_count_comparison, plot_media_count_comparison_all_kw)
from quotaclimat.data_processing.sitemap.queries import (
    query_all_articles_titles_between_two_dates, query_data_coverage,
    query_matching_keywords_articles_titles_between_two_dates)
from quotaclimat.data_processing.sitemap.sitemap_processing import (
    feature_engineering_sitemap, filter_df, load_all)
from quotaclimat.utils.climate_keywords import CLIMATE_KW

st.set_page_config(
    layout="wide",
)
# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


# Initialize session
if "key" not in st.session_state:
    st.session_state.key = "value"

conn = init_connection()


st.sidebar.markdown("# Exploration des titres d'article des site web")

date_min, date_max = query_data_coverage(conn)


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
        figs = []
        keywords = st_tags(
            label="Entrez des mots clé en minuscule:",
            text="Pressez entrez pour ajouter",
            value=CLIMATE_KW,
            suggestions=["environment"],
            maxtags=30,
            key="0",
        )
        d_lower = st.date_input(
            "Entrez date à laquel commencer le traitement", datetime.date(2023, 1, 1)
        )
        d_upper = st.date_input("Entrez date à laquel terminer le traitement", date_max)

        df_matching_keywords_articles_titles_between_two_dates = (
            query_matching_keywords_articles_titles_between_two_dates(
                conn, keywords, d_lower, d_upper
            )
        )
        df_all_articles_titles_between_two_dates = (
            query_all_articles_titles_between_two_dates(conn, d_lower, d_upper)
        )

        # percentage
        count_kw_per_type = (
            df_matching_keywords_articles_titles_between_two_dates.groupby(
                "type"
            ).count()
        )
        count_total_per_type = df_all_articles_titles_between_two_dates.groupby(
            "type"
        ).count()

        figs.append(
            plot_bar_volumne_mediatique(
                count_kw_per_type["news_title"] / count_total_per_type["news_title"]
            )
        )

        # percentage per day
        count_kw_per_day = (
            df_matching_keywords_articles_titles_between_two_dates.groupby(
                "news_publication_date"
            ).count()
        )
        count_total_per_day = df_all_articles_titles_between_two_dates.groupby(
            "news_publication_date"
        ).count()
        figs.append(
            plot_bar_volumne_mediatique(
                count_kw_per_day["news_title"] / count_total_per_day["news_title"],
                title="Evolution du volume médiatique par jour",
            )
        )

        # percentage per media
        count_kw_per_day = (
            df_matching_keywords_articles_titles_between_two_dates.groupby(
                "publication_name"
            ).count()
        )
        count_total_per_day = df_all_articles_titles_between_two_dates.groupby(
            "publication_name"
        ).count()
        figs.append(
            plot_bar_volumne_mediatique(
                count_kw_per_day["news_title"] / count_total_per_day["news_title"],
                title="Classement des médias",
            )
        )

        for fig in figs:
            st.plotly_chart(fig)
