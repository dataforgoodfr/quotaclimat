import streamlit as st
from streamlit_tags import st_tags

from quotaclimat.data_analytics.sitemap_analytics import \
    plot_media_count_comparison
from quotaclimat.data_processing.sitemap_processing import *

st.markdown("Exploration des contenue des site web")
st.sidebar.markdown("# Exploration des contenue des site web")


df_all = load_all()


keywords = st_tags(
    label="Entrez des mots clé:",
    text="Pressez entrez pour ajouter",
    value=[
        " COP",
        "climatique",
        "écologie",
        "CO2",
        "effet de serre",
        "transition énergétique",
        "carbone",
    ],
    suggestions=["five"],
    maxtags=30,
    key="1",
)
keywords_compare = st_tags(
    label="Pour les comparer aux mots clé suivants:",
    text="Pressez entrez pour ajouter",
    value=[
        "migrants",
        " immigrés",
        "sans-papiers",
        "immigration",
        "migration",
        "émigration",
        "émigrés",
        "ocean viking",
    ],
    suggestions=["five"],
    maxtags=30,
    key="2",
)

fig = plot_media_count_comparison(df_all, keywords, keywords_compare)
st.plotly_chart(fig)
