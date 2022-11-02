import streamlit as st

from quotaclimat.data_analytics.data_coverage import (
    fig_percentage_of_covered_time_by_keywords, fig_time_coverage_of_extracts)
from quotaclimat.data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_all_data_dump)

PATH_EXTRACTS_DATA_DUMPS = "data/keywords/"
PATH_CHANNEL_METADATA = "data/channels.xlsx"


@st.cache(allow_output_mutation=True)
def figures_computation():

    df_all = read_and_format_all_data_dump(
        PATH_EXTRACTS_DATA_DUMPS, PATH_CHANNEL_METADATA
    )
    # figures answering the questions
    fig_percentage_of_covered_time_by_keywords_ = (
        fig_percentage_of_covered_time_by_keywords(df_all, PATH_CHANNEL_METADATA)
    )

    fig_time_coverage_of_extracts_ = fig_time_coverage_of_extracts(df_all)
    fig_percentage_of_covered_time_by_keywords_, fig_time_coverage_of_extracts_
    return fig_percentage_of_covered_time_by_keywords_, fig_time_coverage_of_extracts_


st.header("Couverture mediatique")
st.sidebar.markdown("# Resultat principaux")
figs = figures_computation()
for fig_ in figs:
    st.plotly_chart(fig_)
