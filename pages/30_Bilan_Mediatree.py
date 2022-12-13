from io import StringIO

import pandas as pd
import streamlit as st

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

    st.sidebar.metric("Extraits trouvés", len(data))

    with st.expander("Vue d'ensemble", expanded=False):
        st.markdown("## Analyse sur le total")

        st.markdown("## Classement")


    with st.expander("Comparaison entre les sujets", expanded=False):
        st.markdown("## Classement")

    with st.expander("Evolutions au cours du temps", expanded=False):
        st.markdown("## Volumes médiatiques")

        st.markdown("## Classement")
        st.markdown("### TV d'info en continu")

        st.markdown("### TV généraliste")

        st.markdown("### Radio")

else:

    st.info("Chargez un ou plusieurs fichiers Mediatree à droite pour lancer l'analyse")