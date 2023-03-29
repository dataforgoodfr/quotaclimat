import datetime
from io import StringIO

import pandas as pd
import streamlit as st

import quotaclimat.data_analytics.exploration as mt_exploration
from quotaclimat.data_processing.mediatree.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_one)
from quotaclimat.utils.channels import TOP_25_CHANNELS, TOP_CHANNELS_TV
from quotaclimat.utils.plotly_theme import THEME

st.write("### Outil d'exploration des fichiers Mediatree")

uploaded_files = st.sidebar.file_uploader(
    "Uploader des fichiers excel mediatree", accept_multiple_files=True, type=["xlsx"]
)


# button_analysis = st.sidebar.button("Lancer l'analyse")


@st.cache_data()
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

    PLOT_FREQUENCY = (
        "D" if data.date.max() - data.date.min() > datetime.timedelta(days=1) else "H"
    )

    st.sidebar.metric("Extraits trouvés", len(data))

    st.markdown("### Liste des mots-clés présents")
    st.markdown(list(set(data.keyword)))

    with st.expander("📺 Répartition par chaîne", expanded=False):

        st.markdown("### Nombre de mentions par chaîne")
        fig = mt_exploration.show_mentions_by_channel(data, n=30)

        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)

        col1.markdown("### Nombre de mentions dans les chaînes les plus écoutés")
        fig = mt_exploration.show_mentions_by_channel(
            data, list_of_channels=TOP_25_CHANNELS
        )
        col1.plotly_chart(fig, use_container_width=True)

        fig = mt_exploration.show_piechart_split_tv_radio(data)

        col2.plotly_chart(fig, use_container_width=True)

    with st.expander("📅 Evolution au cours du temps", expanded=False):

        fig = mt_exploration.show_mentions_over_time(
            data, freq=PLOT_FREQUENCY, split="media", kind="area"
        )
        st.plotly_chart(fig, use_container_width=True)

        fig = mt_exploration.show_mentions_over_time(
            data, freq=PLOT_FREQUENCY, list_of_channels=TOP_CHANNELS_TV, kind="bar"
        )
        st.plotly_chart(fig, use_container_width=True)

        selection_channels = st.multiselect(
            "Choisir les chaînes à étudier", TOP_25_CHANNELS, default=["CNEWS", "BFMTV"]
        )

        fig = mt_exploration.show_mentions_over_time(
            data, freq=PLOT_FREQUENCY, list_of_channels=selection_channels, kind="bar"
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("⏲ Répartition par heure de la journée", expanded=False):

        fig = mt_exploration.show_mentions_by_time_of_the_day(
            data, freq="1H", split="media"
        )
        st.plotly_chart(fig, use_container_width=True)

        fig = mt_exploration.show_mentions_by_time_of_the_day(
            data, freq="1H", list_of_channels=TOP_CHANNELS_TV, kind="area"
        )
        st.plotly_chart(fig, use_container_width=True)

    with st.expander("🔎 Répartition par mot clé", expanded=False):

        fig = mt_exploration.show_mentions_over_time(
            data, freq=PLOT_FREQUENCY, split="keyword", kind="area"
        )

        st.plotly_chart(fig, use_container_width=True)

        fig = mt_exploration.show_mentions_treemap(
            data,
            TOP_25_CHANNELS,
            freq="4H",
            path=["channel_name", "keyword", "time_of_the_day"],
        )
        st.plotly_chart(fig, use_container_width=True)

else:

    st.info("Chargez un ou plusieurs fichiers Mediatree à droite pour lancer l'analyse")
