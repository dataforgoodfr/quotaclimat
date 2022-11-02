# from quotaclimat import build_dashboard

import streamlit as st

st.set_page_config(
    page_title="QuotaClimat x Data For Good",
    page_icon="👋",
    layout="wide",
    initial_sidebar_state="expanded",
)

if __name__ == "__main__":

    st.image("quotaclimat/utils/coverquotaclimat.png")

    st.title("Quota Climat & Data4Good")
    st.header("Mais qui est Quota Climat ?")
    st.markdown("Présentation de l'association Quota Climat")
    st.header("La collaboration")
    st.markdown(
        "Dans le paysage médiatique aujourd’hui, entre 2 et 5% du temps est consacré aux enjeux \
             écologiques. Et c’est à peu près le seul chiffre que nous avons grâce aux travaux de \
                 l’Affaire du siècle et de ClimatMédias. Entrainé par l’élan des médias indépendants,\
                      de plus en plus de médias et journalistes s’engagent et c’est un tournant \
                           majeur (Radio France, TF1, chartes des journalistes à la hauteur de \
                               l’urgence écologique). Mais qu’en est-il en réalité ?"
    )

    st.header("Structure du site web")
    st.markdown(
        "Le site web est divisé en différentes pages qui analysent la représentation des \
        sujets environnementaux dans les médias :"
    )
    st.markdown(
        f"- Résultats principaux : Indicateurs aggrégés permettant d'avoir une vue globale \
        du sujet"
    )
    st.markdown(
        f"- Mots clés : Explication des mots clés considérés et sur quelle période"
    )
    st.markdown(
        f"- Méthodologie : Détails sur la methodologie permettant ces resultats"
    )
