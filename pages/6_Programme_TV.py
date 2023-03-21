# %%
# from pathlib import Path
import os
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from quotaclimat.data_processing.extract_and_process_tv_program import \
    get_tv_programs_next_days

# TODO: make dynamic!
PATH_TV_PROGRAMS = "data_public/tv_program/20221125_20221202_Programme_TV.csv"


@st.cache_data(ttl=7200)
def get_data():
    df = pd.read_csv(PATH_TV_PROGRAMS)
    # df = get_tv_programs_next_days(number_of_days=4, save=False)
    return df


st.header("Programme télévision pour les prochains jours")
st.sidebar.markdown("# Programme TV")
df = get_data()

bool_print = st.checkbox("Afficher la donnée sous forme de tableau")
if bool_print:
    st.write(df)

channels = st.multiselect(
    "De quelle chaine de télévision voulez vous visualiser le programme TV ?",
    df.channel_name.unique(),
    default="TF1",
)


col1, col2 = st.columns(2)

with col1:
    date_beginning = st.date_input(
        "Quelle est la date initiale ?",
        value=datetime.strptime(df.start.min().split(" ")[0], "%Y-%m-%d"),
        min_value=datetime.strptime(df.start.min().split(" ")[0], "%Y-%m-%d"),
        max_value=datetime.strptime(df.start.max().split(" ")[0], "%Y-%m-%d"),
    )
with col2:
    date_end = st.date_input(
        "Quelle est la date finale ?",
        value=datetime.strptime(df.stop.max().split(" ")[0], "%Y-%m-%d"),
        min_value=date_beginning,
        max_value=datetime.strptime(df.stop.max().split(" ")[0], "%Y-%m-%d"),
    )

fig = px.timeline(
    df[
        (df.channel_name.isin(channels))
        & (df.start >= date_beginning.strftime("%Y-%m-%d"))
        & (df.stop <= date_end.strftime("%Y-%m-%d"))
    ],
    x_start="start",
    x_end="stop",
    y="channel_name",
    color="category_text",
    hover_data=["title", "subtitle", "start", "stop", "category_text"],
)
st.plotly_chart(fig)


# %%
