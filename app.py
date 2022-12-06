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

    LOCAL_PATH = Path(__file__).parent

    image_file = LOCAL_PATH / Path("quotaclimat/utils/coverquotaclimat.png")

    st.image(str(image_file))

    st.title("Quota Climat & Data4Good")
    st.markdown(
        "Dans le paysage médiatique aujourd’hui, entre 2 et 5% du temps est consacré aux enjeux \
             écologiques. Et c’est à peu près le seul chiffre que nous avons grâce aux travaux de \
                 l’Affaire du siècle et de ClimatMédias. Entrainé par l’élan des médias indépendants,\
                      de plus en plus de médias et journalistes s’engagent et c’est un tournant \
                           majeur (Radio France, TF1, chartes des journalistes à la hauteur de \
                               l’urgence écologique). Mais qu’en est-il en réalité ?"
    )

    st.header("Structure du site web")
