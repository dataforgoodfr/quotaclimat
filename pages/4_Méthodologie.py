import streamlit as st

st.markdown("Cette page motive la validité des resultats presentés ici.")
st.sidebar.markdown("# 👩‍🔬 Methodologie")
st.header("Collections des données")
st.markdown("Cette page motive la validité des resultats presentés ici.")
st.markdown(
    """
Les données ont été recuperées sur la plateforme Mediatree:
- récupérer extract depuis mediatree par mot clef
- trouver plus de mots clefs potentiel
"""
)

st.markdown(
    """
    <style>
    [data-testid="stMarkdownContainer"] ul{
        padding-left:40px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.header("Filtrer")
st.markdown(
    """
Les données sont ensuite passé au crible:
- dedoublonner
- vérifier la pertinence de l'extraction
"""
)
st.header("Enrichir l'information")
st.markdown(
    """
Topic modeling et analyse de qualité:
- tbd
"""
)
st.markdown(
    """
-    
"""
)

btn = st.button("Celebrate!")
if btn:
    st.balloons()
