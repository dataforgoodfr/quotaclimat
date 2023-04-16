from pathlib import Path

import streamlit as st

st.set_page_config(
    layout="wide",
)
MD_COP_BAROMETRE = (
    "quotaclimat/content/Méthodologie du baromètre COP27 QuotaClimat x Data For Good.md"
)
st.markdown("Cette page motive la validité des resultats presentés ici.")
st.sidebar.markdown("# 👩‍🔬 Methodologie")
tab1, tab2 = st.tabs(["Baromètre COP", "Titre d'article"])

with tab1:
    st.markdown(Path(MD_COP_BAROMETRE).read_text(), unsafe_allow_html=True)

with tab2:
    st.markdown("La donnée derrière les analyses des titres d'articles est mis à disposition du grand public à partir des référencement pour moteur de recherche. Cet outils utilise l'ingestion journalière dans une base de donnée à partir des [sitemap](https://www.xml-sitemaps.com/).")


