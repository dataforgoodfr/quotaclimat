import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

from quotaclimat.data_processing.sitemap.queries import \
    percentage_article_in_section_list_per_day
from quotaclimat.data_processing.sitemap.settings import CLIMATE_SECTION
from postgres.database_connection import get_db_session

st.set_page_config(
    layout="wide",
    initial_sidebar_state="expanded",
)

# Fetch datas


conn = get_db_session()


df_climate_per_day = percentage_article_in_section_list_per_day(conn, CLIMATE_SECTION)
df_sport_per_day = percentage_article_in_section_list_per_day(
    conn, ["sport", "football"]
)
df_politics_per_day = percentage_article_in_section_list_per_day(conn, ["politique"])

fig = px.bar(
    df_climate_per_day,
    x="date",
    y="Pourcentage",
    height=400,
    text_auto=".1%",
)
fig.update_layout(
    yaxis_tickformat="0%",
    title="% articles dans les sections climats",
    font_family="Poppins",
    yaxis_title="% du volume médiatique",
    xaxis_title="",
)
fig.update_traces(
    marker=dict(color="darkseagreen"),
    selector=dict(name=df_climate_per_day["date"].iloc[-1]),
)


# Page Layout
# Create a container for the centered tile
container = st.container()

# Center the tile using columns
col1, col2, col3 = st.columns(3)

# Set the width of col1 and col3 to center the tile
col1.write("")
col3.write("")

# Add CSS styling to create a solid square around the tile
col2.markdown(
    """
    <style>
    .square {
        border-radius: 5px;
        border: 2px solid green;
        padding: 20px;
        text-align: center;
        display: inline-block;
        background-color: darkseagreen;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Add the centered tile inside the styled square
col2.markdown(
    """
    <div class="square">
        <h1>%s %% </h1>
        hier, d'articles en section climat.
    </div>
    """
    % (round(df_climate_per_day["Pourcentage"].iloc[-1] * 100, 2)),
    unsafe_allow_html=True,
)
st.plotly_chart(fig)
# Add content to the container
with container:
    st.write("Voici un brouillon du baromètre")


import streamlit as st

# Create a container for the centered tile and two squares below
container = st.container()

# Center the tile and squares using columns
col1, col2, col3 = st.columns(spec=[3, 1, 3])

# Set the width of col1 and col3 to center the tile and squares
col1.write("")
col3.write("")

# Add CSS styling to create a solid square
col2.markdown(
    """
    <style>
    .square {
        border: 2px solid black;
        padding: 20px;
        text-align: center;
        display: inline-block;
    }
    
    .tile {
        background-color: darkseagreen;
    }

    .square1 {
        background-color: skyblue;
    }
    
    .square2 {
        background-color: salmon;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# Add two squares below the centered tile
col1.markdown(
    """
    <div class="square square1">
        <h1>%s %%</h1>
        hier, d'articles en section politique.

    </div>
    """
    % (round(df_politics_per_day["Pourcentage"].iloc[-1] * 100, 2)),
    unsafe_allow_html=True,
)

col3.markdown(
    """
    <div class="square square2">
        <h1>%s %%</h1>
        hier, d'articles en section sport.

    </div>
    """
    % (round(df_sport_per_day["Pourcentage"].iloc[-1] * 100, 2)),
    unsafe_allow_html=True,
)
