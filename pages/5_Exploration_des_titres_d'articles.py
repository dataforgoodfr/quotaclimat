import datetime

import pandas as pd
import psycopg2
import streamlit as st
from streamlit_tags import st_tags

from quotaclimat.data_analytics.sitemap_analytics import (
    plot_articles_lifespan_comparison, plot_articles_total_count_evolution,
    plot_bar_volume_mediatique, plot_comparison_of_temporal_total_count,
    plot_media_count_comparison, plot_media_count_comparison_all_kw)
from quotaclimat.data_processing.sitemap.queries import (
    query_all_articles_titles_between_two_dates, query_data_coverage,
    query_matching_keywords_articles_titles_between_two_dates)
from quotaclimat.data_processing.sitemap.sitemap_processing import (
    feature_engineering_sitemap, filter_df, load_all)
from quotaclimat.utils.climate_keywords import CLIMATE_KW

from postgres.database_connection import get_db_session

st.set_page_config(
    layout="wide",
)

# Initialize session
if "key" not in st.session_state:
    st.session_state.key = "value"

conn = get_db_session()
date_min, date_max = query_data_coverage(conn)


st.sidebar.markdown("# Présence de mot dans les titres d'articles")
st.markdown(
    " Cette page donne un apperçu du contenu des titres d'articles publiés de %s à %s, pour cette liste de média."
    % (date_min.strftime("%Y-%m-%d"), date_max.strftime("%Y-%m-%d"))
)


tab1, tab2 = st.tabs(
    [
        "Couverture et évolution temporelle",
        "Comparaison de mots clé",
    ]
)

with tab1:
    st.markdown(" Cette page permet de répondre aux questions suivantes:")
    st.markdown(
        "- Quel est le pourcentage de présence dans le titre d'articles de ces mots ?"
    )
    st.markdown("- Quel est l'évolution au cours du temps de cette présence ?")
    st.markdown("- Comment les média se compare dans cette couverture ?")

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
            plot_bar_volume_mediatique(
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
            plot_bar_volume_mediatique(
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
            plot_bar_volume_mediatique(
                (count_kw_per_day["news_title"] / count_total_per_day["news_title"]),
                title="Classement des médias",
            )
        )

        for fig in figs:
            st.plotly_chart(fig)


with tab2:
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
        "Entrez date à laquelle commencer le traitement",
        datetime.date(2023, 4, 1),
        key="3",
    )
    d_upper_ = st.date_input(
        "Entrez date à laquelle terminer le traitement",
        date_max,
        key="4",
    )

    btn = st.button("Lancer l'analyse")
    if btn:
        df_all_articles_titles_between_two_dates = (
            query_all_articles_titles_between_two_dates(conn, d_lower, d_upper)
        )
        df_matching_keywords_articles_titles_between_two_dates_ref = (
            query_matching_keywords_articles_titles_between_two_dates(
                conn, keywords, d_lower_, d_upper_
            )
        )
        df_matching_keywords_articles_titles_between_two_dates_to_compare = (
            query_matching_keywords_articles_titles_between_two_dates(
                conn, keywords_compare, d_lower_, d_upper_
            )
        )
        fig = plot_media_count_comparison(
            df_matching_keywords_articles_titles_between_two_dates_ref,
            df_matching_keywords_articles_titles_between_two_dates_to_compare,
            df_all_articles_titles_between_two_dates,
            keywords,
            keywords_compare,
        )
        st.plotly_chart(fig)
