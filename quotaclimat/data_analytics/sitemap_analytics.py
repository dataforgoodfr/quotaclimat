import pandas as pd
import plotly.express as px


def plot_media_count_comparison(df, keywords: list, keywords_comp: list):
    df_keywords_1 = df[df.news_title.str.contains("|".join(keywords))]
    df_keywords_2 = df[df.news_title.str.contains("|".join(keywords_comp))]

    df_gb_1 = df_keywords_1.groupby("media").count().reset_index()
    df_gb_1["keyword list"] = ", ".join(keywords)[:25] + "..."
    df_gb_2 = df_keywords_2.groupby("media").count().reset_index()
    df_gb_2["keyword list"] = ", ".join(keywords_comp)[:25] + "..."
    df_to_plot = pd.concat([df_gb_1, df_gb_2])
    fig = px.bar(
        df_to_plot,
        x="media",
        y="news_title",
        color="keyword list",
        title="Comparaison du nombre de titre d'articles comprenant mot des deux listes",
    )
    return fig
