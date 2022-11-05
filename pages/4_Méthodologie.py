import streamlit as st
from pathlib import Path
MD_COP_BAROMETRE = "quotaclimat/content/Méthodologie du baromètre COP27 QuotaClimat x Data For Good.md"
st.markdown("Cette page motive la validité des resultats presentés ici.")
st.sidebar.markdown("# 👩‍🔬 Methodologie")
tab1, tab2, tab3 = st.tabs(["Baromètre COP", "Traitement et qualité", "Stress Test"])

with tab1: 
    st.markdown(Path(MD_COP_BAROMETRE).read_text(), unsafe_allow_html=True)

btn = st.button("Celebrate!")
if btn:
    st.balloons()
