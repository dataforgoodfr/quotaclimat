import streamlit as st

from quotaclimat.utils.multipage import MultiPage
from quotaclimat.ui.pages import main_results
from quotaclimat.ui.pages import keywords
from quotaclimat.ui.pages import methodology

PAGE_1 = "👋 Introduction"
PAGE_2 = "🎯 Résultat principaux"
PAGE_3 = "📊 Mots clés"
PAGE_4 = "👩‍🔬 Methodologie"


def main():
    app = MultiPage()

    st.sidebar.header("Quota Climat & Data4Good")
    st.sidebar.image("quotaclimat/utils/coverquotaclimat.png")

    app.add_page(PAGE_1, first_page)
    app.add_page(PAGE_2, main_results.app)
    app.add_page(PAGE_3, keywords.app)
    app.add_page(PAGE_4, methodology.app)

    app.run()


def first_page():
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
    st.markdown("Le site web est divisé en différentes pages qui analysent la représentation des \
        sujets environnementaux dans les médias :")
    st.markdown(f"- {PAGE_2} : Indicateurs aggrégés permettant d'avoir une vue globale \
        du sujet")
    st.markdown(f"- {PAGE_3} : Explication des mots clés considérés et sur quelle période")
    st.markdown(f"- {PAGE_4} : Détails sur la methodologie permettant ces resultats")
