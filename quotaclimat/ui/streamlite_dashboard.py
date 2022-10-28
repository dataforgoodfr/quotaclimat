import streamlit as st

from quotaclimat.utils.multipage import MultiPage
from quotaclimat.ui.pages import main_results
from quotaclimat.ui.pages import keywords

PAGE_1 = "Introduction"
PAGE_2 = "Résultat principaux"
PAGE_3 = "Mots clés"


def main():
    app = MultiPage()

    st.sidebar.header("Quota Climat & Data4Good")
    st.sidebar.image("quotaclimat/utils/coverquotaclimat.png")

    app.add_page(PAGE_1, first_page)
    app.add_page(PAGE_2, main_results.app)
    app.add_page(PAGE_3, keywords.app)
    app.run()


def first_page():
    st.title("Quota Climat & Data4Good")
    st.header("Mais qui est Quota Climat ?")
    st.markdown("Présentation de l'association Quota Climat")

    st.header("Structure du site web")
    st.markdown("Le site web est divisé en différentes pages qui analysent la représentation des \
        sujets environnementaux dans les médias :")
    st.markdown(f"- {PAGE_2} : Indicateurs aggrégés permettant d'avoir une vue globale \
        du sujet")
    st.markdown(f"- {PAGE_3} : Explication des mots clés considérés et sur quelle période")
