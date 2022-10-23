import streamlit as st

from quotaclimat.data_analytics.data_coverage import \
    fig_percentage_of_covered_time_by_keywords
from quotaclimat.data_processing.read_format_deduplicate import (
    deduplicate_extracts, read_and_format_all_data_dump)

PATH_EXTRACTS_DATA_DUMPS = "data/keywords/"
PATH_CHANNEL_METADATA = "data/channels.xlsx"


def build_dashboard():
    df_all = read_and_format_all_data_dump(
        PATH_EXTRACTS_DATA_DUMPS, PATH_CHANNEL_METADATA
    )
    # figures answering the questions
    fig_percentage_of_covered_time_by_keywords_ = (
        fig_percentage_of_covered_time_by_keywords(df_all, PATH_CHANNEL_METADATA)
    )
    # streamlit interface
    st.title("Quota climat")
    st.markdown("Welcome")
    st.header("to fill")
    st.plotly_chart(fig_percentage_of_covered_time_by_keywords_)
