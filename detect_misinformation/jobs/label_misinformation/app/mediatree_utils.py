from io import BytesIO
import os
import logging
import openai
import requests
import modin.pandas as pd
from secret_utils import get_secret_docker
from whisper_utils import WHISPER_COLUMN_NAME, transform_mp4_to_mp3
from typing import Optional

mediatree_password = get_secret_docker("MEDIATREE_PASSWORD")
AUTH_URL: str = os.environ.get("MEDIATREE_AUTH_URL")
mediatree_user = get_secret_docker("MEDIATREE_USER")
# https://keywords.mediatree.fr/api/docs/#/paths/~1api~1export~1single/get
API_BASE_URL = os.environ.get("KEYWORDS_URL")


def mediatree_check_secrets():
    if (
        mediatree_user is None
        or mediatree_password is None
        or AUTH_URL is None
        or API_BASE_URL is None
    ):
        logging.error(
            "Config missing: MEDIATREE_USER/MEDIATREE_PASSWORD/MEDIATREE_AUTH_URL/KEYWORDS_URL missing"
        )
        raise Exception("Config missing: user/password/auth url missing")


def get_auth_token(user=mediatree_user, password=mediatree_password):
    logging.debug("Getting a token")
    try:
        post_arguments = {
            "grant_type": "password",
            "username": user,
            "password": password,
        }
        response = requests.post(AUTH_URL, data=post_arguments)
        output = response.json()
        token = output["data"]["access_token"]
        logging.debug("got a token")
        return token
    except Exception as err:
        raise Exception(f"Could not get token {err}")


def get_start_and_end_of_chunk(start):
    logging.info(f"get_start_and_end_of_chunk - datetime: {start}")
    start = pd.to_datetime(start)
    two_minutes = 120
    timestamp = str(int(start.timestamp()))
    timestamp_end = str(int(start.timestamp() + two_minutes))
    return timestamp, timestamp_end


def get_url_mediatree(date, channel) -> str:
    # https://keywords.mediatree.fr/player/?fifo=france-inter&start_cts=1729447079&end_cts=1729447201&position_cts=1729447080
    timestamp, timestamp_end = get_start_and_end_of_chunk(date)
    channel = channel if channel != "sud-radio" else "sudradio"
    return f"https://keywords.mediatree.fr/player/?fifo={channel}&start_cts={timestamp}&end_cts={timestamp_end}&position_cts={timestamp}"


def get_mediatree_single_export_url(token, channel_name, start, end):
    channel_name = channel_name if channel_name != "sud-radio" else "sudradio"
    return f"{API_BASE_URL}?token={token}&channel={channel_name}&cts_in={start}&cts_out={end}&format=mp4"


# to mock tests
def get_response_single_export_api(single_export_api):
    return requests.get(single_export_api)


def fetch_video_url(row, token):
    """Fetches a single video URL based on a DataFrame row."""
    try:
        if not pd.isna(row["start"]) and not pd.isna(row["channel_name"]):
            logging.info(f"fetch_video_url for : {row}")
            start, end = get_start_and_end_of_chunk(row["start"])
            channel_name = row["channel_name"]
            logging.info(f"Fetching URL for {channel_name} {start} {end}...")

            single_export_api = get_mediatree_single_export_url(
                token, channel_name, start, end
            )

            logging.info(
                f"Fetching URL for {channel_name} [{start}-{end}]: {single_export_api}"
            )

            response = get_response_single_export_api(single_export_api)
            if response.status_code != 200:
                logging.error(
                    f"Failed to fetch URL for {channel_name} [{start}-{end}]: {response.status_code}"
                )
                return None  # Keep column as None for failed cases

            output = response.json()
            url = output.get("src", "")

            logging.info(f"Got response URL: {url}")

            if not url:
                logging.warning(
                    f"No video URL found for {channel_name} [{start}-{end}]"
                )
                return None
            logging.debug(f"url is {url}")
            return url
        else:
            return None

    except Exception as e:
        logging.error(f"Error fetching video URL {row}: {e}")
        return None


def get_video_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Retrieves video URLs and adds them directly to the DataFrame using apply().

    :param df: DataFrame with columns ["start", "channel_name"]
    :return: Updated DataFrame with a new "media_url" column
    """
    logging.info("Fetching video URLs for downloading...")

    try:
        token = token = get_auth_token()
        df["media_url"] = df.apply(lambda row: fetch_video_url(row, token), axis=1)

        return df
    except Exception as e:
        logging.error(f"get_video_urls {df}: {e}")
        return None


def download_media(row) -> Optional[bytes]:
    """
    Downloads a media file, converts it to mp3 and returns its binary content.
    """
    try:
        url = row["media_url"]
        if not url:
            logging.warning(
                f"Skipping empty URL for {row['channel_name']} at {row['start']}"
            )
            return None
        logging.info(f"Downloading: {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad responses (4xx, 5xx)

        media: bytes = response.content
        if url.endswith(".mp4"):
            media = transform_mp4_to_mp3(media)
            return media
        else:
            return media  # Return binary MP3 data

    except Exception as e:
        logging.error(f"Error downloading {url}: {e}")
        return None


def add_medias_to_df(df: pd.DataFrame):
    """
    Downloads videos from URLs and saves them to dataframe
    """

    try:
        # add  "media_url" column
        df = get_video_urls(df)
        df["media"] = df.apply(lambda row: download_media(row), axis=1)

        return df
    except Exception as e:
        logging.error(f"Error with add_medias_to_df: {e}")
        raise e


"""
TODO: move this entire function in an abstracted client
From a dataframe with the mp3 information
"""


def get_whispered_transcript(audio_bytes: Optional[bytes]) -> str:
    if audio_bytes is None:
        logging.warning("get_whispered_transcript - audio bytes is None")
        return ""
    try:
        buffer = BytesIO(audio_bytes)
        buffer.name = "audio.mp3"
        openai.api_key = get_secret_docker("OPENAI_API_KEY")
        transcript = openai.audio.transcriptions.create(
            model="whisper-1",
            file=buffer,
            response_format="text",
        )
        # It looks weird but this is the whisper response to an empty audio track
        # Probably better ways to detect the empty audio and avoid the api call TODO
        if transcript == "you you you you you\n":
            logging.error("get_whispered_transcript - Empty audio track")
            return None
        logging.info(f"Whisper sample: {transcript[:100]}...")
        return transcript
    except Exception as e:
        logging.error(f"Error with whisper client: {e}")
        raise e


def get_whispered_transcript_from_row(row: pd.Series):
    media = row.get("media")
    plaintext = row.get("plaintext", "")
    if media is not None:
        return get_whispered_transcript(media)
    else:
        logging.info("Media not present, returning plaintext.")
        return plaintext


def add_new_plaintext_column_from_whister(df: pd.DataFrame) -> pd.DataFrame:
    df[WHISPER_COLUMN_NAME] = df.apply(
        lambda row: get_whispered_transcript_from_row(row),
        axis=1,
    )

    return df


def get_new_plaintext_from_whisper(df: pd.DataFrame) -> pd.DataFrame:
    df = add_medias_to_df(df)

    df = add_new_plaintext_column_from_whister(df)

    return df
