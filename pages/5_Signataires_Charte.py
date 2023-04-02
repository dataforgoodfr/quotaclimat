import streamlit as st

from quotaclimat.data_analytics.analytics_signataire_charte import (
    bar_plot_nb_of_signataire_per_job, bar_plot_nb_of_signataire_per_media,
    get_summary_statistics, load_signing_partners_data)
from quotaclimat.data_ingestion.scrap_chartejournalismeecologie_signataires import \
    run as scrap_charte_journalisme

st.set_page_config(
    layout="wide",
)
st.markdown("Statistiques signature de la charte d'écologie")
st.sidebar.markdown("# Charte Journaliste d'écologie")

# Import raw data
df = load_signing_partners_data()

# Get statistics
nb_unique_signataire, nb_unique_organisation = get_summary_statistics(df)

st.markdown(
    "A ce jour %s personnes ont signé la charte, parmis %s organisations."
    % (nb_unique_signataire, nb_unique_organisation)
)

# Media trace
fig_nb_of_signataire_per_media = bar_plot_nb_of_signataire_per_media(df)
st.plotly_chart(fig_nb_of_signataire_per_media, use_container_width=True)

# Job trace
fig_nb_of_signataire_per_job = bar_plot_nb_of_signataire_per_job(df)
st.plotly_chart(fig_nb_of_signataire_per_job, use_container_width=True)

# Configure refresh
btn = st.button("Rafraîchir les résultats")
if btn:
    scrap_charte_journalisme()
