import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# %% Parameters
CHUNK_SIZE = datetime.timedelta(minutes=2)

SCORE_EVOLUTION = np.array(
    [
        ["00:00:00", 1000.0],
        ["06:00:00", 1000.0],
        ["06:30:00", 1700.0],
        ["07:00:00", 5600.0],
        ["07:30:00", 8900.0],
        ["07:50:00", 7900.0],
        ["08:13:00", 4000.0],
        ["09:20:00", 3300.0],
        ["10:00:00", 3300.0],
        ["10:55:00", 1700.0],
        ["12:05:00", 2100.0],
        ["15:35:00", 2800.0],
        ["16:05:00", 2700.0],
        ["16:45:00", 3500.0],
        ["17:05:00", 2700.0],
        ["18:05:00", 5100.0],
        ["19:10:00", 5100.0],
        ["19:15:00", 13900.0],
        ["19:35:00", 28200.0],
        ["20:00:00", 35000.0],
        ["23:00:00", 35000.0],
        ["23:15:00", 5000.0],
        ["23:30:00", 2000.0],
    ]
)

# %% Fonction generale
def compute_media_time(chunks_data, n_days=16, hour_a_day=18):
    total_time = np.sum(chunks_data.date_end - chunks_data.date)
    return total_time / datetime.timedelta(hours=hour_a_day * n_days)


def get_moment_of_the_day(date_test):

    return datetime.timedelta(
        hours=date_test.hour, minutes=date_test.minute, seconds=date_test.second
    ) / datetime.timedelta(hours=24)


# %% Fonction de score
def get_formatted_score_evolution(score_evol=SCORE_EVOLUTION):

    score = pd.DataFrame(score_evol, columns=["heure", "score"])
    score.heure = pd.to_timedelta(score.heure) / datetime.timedelta(hours=24)
    score.score = score.score.astype(float)

    score = pd.concat(
        (
            score,
            pd.DataFrame(data=[[1, score.score.iloc[0]]], columns=["heure", "score"]),
        ),
        ignore_index=True,
    )

    total_score = np.trapz(score.score.astype(float), score.heure)

    return score.set_index("heure").squeeze() / total_score


def get_score_time(date_test, formatted_score):
    # formatting score
    score = formatted_score.reset_index()

    # get the time as a fraction
    moment_of_the_day = get_moment_of_the_day(date_test)

    # score is a ponderation of this period on the full day
    return np.interp(moment_of_the_day, score.heure, score.score)


def get_score_range(range_test, score_evol=SCORE_EVOLUTION):

    score = get_formatted_score_evolution(score_evol)

    mask_time = score.reset_index().heure.between(
        get_moment_of_the_day(range_test[0]),
        get_moment_of_the_day(range_test[1]),
        inclusive="neither",
    )

    start_score = pd.Series(
        data=[get_score_time(range_test[0], score)],
        index=[get_moment_of_the_day(range_test[0])],
    )

    end_score = pd.Series(
        data=[get_score_time(range_test[1], score)],
        index=[get_moment_of_the_day(range_test[1])],
    )

    if mask_time.sum() > 0:  # il faut ajouter au moins un point
        score_time = pd.concat(
            (start_score, score[score.index[mask_time]], end_score), ignore_index=False
        )
    else:  # seul les points du range sont importants
        score_time = pd.concat((start_score, end_score), ignore_index=False)

    return np.trapz(score_time.values, score_time.index.values)


def get_score_by_chunk(unitary_chunk):
    return get_score_range([unitary_chunk.date, unitary_chunk.date_end])


def get_chunk_score(chunk_data):
    return chunk_data.apply(get_score_by_chunk, axis=1)


def compute_chunk_score(chunk_data, n_days=16):
    return get_chunk_score(chunk_data).sum() / n_days


# %% extraction depuis les données de chunks
def get_coverage_data(data, chunk_size=CHUNK_SIZE):
    coverage_data = data[
        ["index", "channel", "date", "keyword", "mentions", "n_mentions"]
    ].copy()
    coverage_data["date_end"] = coverage_data["date"] + chunk_size
    return coverage_data


def mention_date_from_serie(serie, chunk_size=CHUNK_SIZE):

    return [serie.date + x * chunk_size for x in serie.mentions]


def get_mentions_date(data, chunk_size=CHUNK_SIZE):
    return data.apply(lambda x: mention_date_from_serie(x, chunk_size), axis=1)


# %%
def get_coverage_data_by_channel(data, channel_name):
    data_channel = data[data.channel == channel_name]
    cov_data_channel = get_coverage_data(data_channel)

    cov_data_channel["mentions_date"] = get_mentions_date(cov_data_channel)

    return cov_data_channel


def get_mention_timestamp_by_channel(cov_data_channel, channel_name):

    mention_timestamp = sorted(
        list(
            set(
                [
                    x
                    for date_list in cov_data_channel["mentions_date"].values
                    for x in date_list
                ]
            )
        )
    )  # get in a unique list
    return mention_timestamp


def get_mention_df_for_plot(mention_timestamp, channel_name):
    df_event = pd.DataFrame(mention_timestamp, columns=["x"])
    df_event["y"] = channel_name
    df_event["color"] = np.array(df_event.y, dtype=str)
    return df_event


def get_coverage_by_chunks(cov_data_channel, channel_name):
    data_ranges = [
        (row[1].date, row[1].date_end) for row in cov_data_channel.iterrows()
    ]
    sorted_unique_ranges = sorted(list(set(data_ranges)))
    chunks_method_initiale = pd.DataFrame(
        merge_date_ranges(sorted_unique_ranges), columns=["date", "date_end"]
    )
    chunks_method_initiale["channel"] = channel_name
    return chunks_method_initiale


def get_coverage_by_window(
    cov_data_channel, channel_name, continuity_window_minutes, mention_extension_seconds
):

    mention_timestamp = get_mention_timestamp_by_channel(cov_data_channel, channel_name)

    continuity_window = datetime.timedelta(minutes=continuity_window_minutes)
    mention_extension = datetime.timedelta(seconds=mention_extension_seconds)

    chunks_data = join_timestamps_within_rolling_window(
        mention_timestamp, continuity_window, mention_extension
    )

    chunks_data["channel"] = channel_name

    return chunks_data


# %% Fusion des mentions
def merge_date_ranges(data):
    # data is a list of tuples (start,end) for each ranges
    result = []
    t_old = data[0]
    for t in data[1:]:
        if t_old[1] >= t[0]:  # I assume that the data is sorted already
            t_old = (min(t_old[0], t[0]), max(t_old[1], t[1]))
        else:
            result.append(t_old)
            t_old = t
    else:
        result.append(t_old)
    return result


def join_timestamps_within_rolling_window(
    mention_timestamp, continuity_window, mention_extension
):
    """
    mention_timestamp -> [datetime.timedelta] list
        - moment des matchs des keywords
    continuity_window -> datetime.timedelta
        - delta de temps en dessous duquel deux mentions sont considérées comme un continuum
        e.g. : continuity_window = datetime.timedelta(minutes=2)
    mention_extension -> datetime.timedelta
        - entension des mentions, independemment de la continuité (et donc durée d'une mention isolée)
        e.g. : mention_extension = datetime.timedelta(seconds=30.8)
    """
    # safety : continuity_window must be larger than mention_extension
    if continuity_window < mention_extension:  # if yes, exchange the values
        continuity_window, mention_extension = mention_extension, continuity_window

    cleaned_timestamp = sorted(list(set(mention_timestamp)))

    df = pd.DataFrame(cleaned_timestamp, columns=["mention_timestamp"])
    df["mention_windows_start"] = df.mention_timestamp - continuity_window / 2
    df["mention_windows_end"] = df.mention_timestamp + continuity_window / 2

    data_ranges = [
        (row[1].mention_windows_start, row[1].mention_windows_end)
        for row in df.iterrows()
    ]

    chunks_df = pd.DataFrame(
        merge_date_ranges(data_ranges),
        columns=["mention_windows_start", "mention_windows_end"],
    )

    chunks_df["date"] = (
        chunks_df.mention_windows_start + continuity_window / 2 - mention_extension / 2
    )
    chunks_df["date_end"] = (
        chunks_df.mention_windows_end - continuity_window / 2 + mention_extension / 2
    )

    return chunks_df[["date", "date_end"]]


# %% Plots
def plot_grouped_chunks_and_mentions(chunks_data, df_event):
    total_time = compute_media_time(chunks_data)

    fig1 = px.timeline(chunks_data, x_start="date", x_end="date_end", y="channel")

    fig2 = px.scatter(
        df_event, x="x", y="y", color_discrete_sequence=["#DC3912"]  # forced to red
    )  # marginal_x="histogram" ne fonctionne pas avec la fusion

    fig = go.Figure(data=fig2.data + fig1.data)
    fig.update_xaxes(type="date")

    fig.update_layout(
        {
            "title": {
                "text": f"Mentions et temps comptabilisé pendant la COP27 | total = {total_time*100:.2f}%"
            }
        }
    )

    return fig
