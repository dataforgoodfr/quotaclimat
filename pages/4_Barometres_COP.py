import streamlit as st

from quotaclimat.data_processing.read_format_deduplicate import (
    filter_data_only_keep_top_audiance, read_and_format_all_data_dump)

PATH_EXTRACTS_DATA_COP26 = "data/cop26/"
PATH_CHANNEL_METADATA = "data/channels.xlsx"

from quotaclimat.data_analytics.exploration import (
    filter_data_between_hours, show_mentions_by_channel,
    show_mentions_by_time_of_the_day, show_mentions_over_time,
    show_mentions_treemap, show_piechart_split_tv_radio)
from quotaclimat.utils.channels import (TOP_25_CHANNELS, TOP_CHANNELS_TV,
                                        TOP_CHANNELS_TV_8)


@st.cache(allow_output_mutation=True)
def figures_computation_cop26():
    # get data
    df_all = read_and_format_all_data_dump(
        path_folder=PATH_EXTRACTS_DATA_COP26, path_channel_metadata=None
    )
    df = filter_data_only_keep_top_audiance(df_all, PATH_CHANNEL_METADATA)
    # FIGURES
    # over time
    multiplier = 2 / (
        df["channel_name"].nunique() * 60 * 18
    )  # multiplier = n_mentions * 2 min / (n_channels * 60 minutes * 20h)
    fig_mentions_over_time = show_mentions_over_time(
        df, freq="D", method=multiplier, height=500, text_auto=".1%"
    )
    fig_mentions_over_time.update_layout(yaxis_tickformat="0%")
    fig_mentions_by_time_of_the_day = show_mentions_by_time_of_the_day(
        df, split="channel_name", kind="bar", height=500, method="minutes"
    )

    return fig_mentions_over_time, fig_mentions_by_time_of_the_day


st.sidebar.markdown("# Baromètres COP")
tab1, tab2 = st.tabs(["COP 26", "COP 27"])
with tab1:
    st.header("Retour sur la COP 26.")
    figs = figures_computation_cop26()
    for fig_ in figs:
        st.plotly_chart(fig_)
