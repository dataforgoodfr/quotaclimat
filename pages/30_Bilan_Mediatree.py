from io import StringIO

import pandas as pd
import streamlit as st

import quotaclimat.data_analytics.bilan as mt_bilan
import quotaclimat.data_analytics.exploration as mt_exploration
from quotaclimat.data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_one)
from quotaclimat.utils.channels import TOP_25_CHANNELS, TOP_CHANNELS_TV
from quotaclimat.utils.plotly_theme import THEME

st.write("### Bilan d'un événement sur les données Mediatree - page en construction")

uploaded_files = st.sidebar.file_uploader(
    "Uploader des fichiers excel mediatree", accept_multiple_files=True, type=["xlsx"]
)


# button_analysis = st.sidebar.button("Lancer l'analyse")


@st.cache(allow_output_mutation=True, suppress_st_warning=True)
def load_data(uploaded_files):
    # TODO add deduplication

    data = []

    progress_bar = st.sidebar.progress(0)

    if len(uploaded_files) > 0:

        for i, uploaded_file in enumerate(uploaded_files):

            uploaded_file.seek(0)
            df_imported = pd.read_excel(uploaded_file)
            # st.sidebar.write(uploaded_file.name)
            df_i = read_and_format_one(
                data=df_imported,
                path_file=uploaded_file.name,
                path_channels="data/channels.xlsx",
            )

            data.append(df_i)

            progress_bar.progress(float((i + 1) / len(uploaded_files)))

        data = pd.concat(data, axis=0, ignore_index=True)
        data_unique = deduplicate_extracts(data)
        return data_unique

    else:
        return None


data = load_data(uploaded_files)


if data is not None:
    data_filtered = mt_bilan.get_filtered_data(data)
    media_time = mt_bilan.get_media_time(data_filtered)
    st.sidebar.metric("Extraits trouvés", len(data))

    with st.expander("Vue d'ensemble", expanded=False):
        st.markdown("## Analyse sur le total")
        fig_volume = mt_bilan.plot_volume_mediatique(media_time)
        st.plotly_chart(fig_volume, use_container_width=True)

        st.markdown("## Classement")
        fig_clsmt_tv_c = mt_bilan.plot_classement_volume_mediatique_tv_continue(
            data_filtered
        )
        st.plotly_chart(fig_clsmt_tv_c, use_container_width=True)
        fig_clsmt_tv_g = mt_bilan.plot_classement_volume_mediatique_tv_generique(
            data_filtered
        )
        st.plotly_chart(fig_clsmt_tv_g, use_container_width=True)
        fig_clsmt_radio = mt_bilan.plot_classement_volume_mediatique_radio(
            data_filtered
        )
        st.plotly_chart(fig_clsmt_radio, use_container_width=True)

    with st.expander("Comparaison entre les sujets", expanded=False):
        st.markdown("## Classement")
        st.write("En cours de construction")

    with st.expander("Evolutions au cours du temps", expanded=False):
        st.markdown("## Volumes médiatiques")
        fig_time_volume = mt_bilan.media_volume_over_time(data_filtered)
        st.plotly_chart(fig_time_volume)

        st.markdown("## Classement")
        ranking = mt_bilan.get_ranking_evolution(data_filtered)

        st.markdown("### TV d'info en continu")
        fig_ranking_tv_c = mt_bilan.show_ranking_chart(
            ranking.query("media2=='TV - Information en continu'"),
            "Evolution du classement des chaînes TV d'information en continu",
            height=600,
        )
        st.plotly_chart(fig_ranking_tv_c)

        st.markdown("### TV généraliste")
        fig_ranking_tv_g = mt_bilan.show_ranking_chart(
            ranking.query("media2=='TV - Généraliste'"),
            "Evolution du classement des chaînes TV généralistes",
            height=600,
        )
        st.plotly_chart(fig_ranking_tv_g)

        st.markdown("### Radio")
        fig_ranking_radio = mt_bilan.show_ranking_chart(
            ranking.query("media2=='Radio'"),
            "Evolution du classement des chaînes Radio",
            height=800,
        )
        st.plotly_chart(fig_ranking_radio)
else:

    st.info("Chargez un ou plusieurs fichiers Mediatree à droite pour lancer l'analyse")
