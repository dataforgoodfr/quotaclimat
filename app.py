# from quotaclimat import build_dashboard

import streamlit as st
from pathlib import Path

st.set_page_config(
    page_title="QuotaClimat x Data For Good",
    page_icon="👋",
    layout="wide",
    initial_sidebar_state="expanded",
)

if __name__ == "__main__":

st.image("quotaclimat/utils/coverquotaclimat.png")

    st.image(str(image_file))

    st.title("Quota Climat & Data4Good")
    st.markdown(
        "Cet outil, conçu par les associations QuotaClimat et Data For Good, permet de quantifier et\
                 de qualifier le traitement médiatique des enjeux écologiques. À partir de mots clés identifiés\
                  au préalable, il permet d’obtenir de façon automatisée les proportions globales que leur\
                       consacrent les médias, et de comparer ces proportions avec celles consacrées à d’autres \
                           thématiques. Des classements sont également fournis, ainsi que l’évolution dans le temps \
                               du traitement des sujets recherchés. Cet outil a pour vocation d’objectiver le traitement \
                                   médiatique des enjeux écologiques dans un contexte où les médias y accordent une\
                                        faible attention en comparaison de la gravité de la crise."
    )

    st.header("Structure du site web")
st.markdown("Work in progress")
