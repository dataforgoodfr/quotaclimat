import datetime
from datetime import date

import pandas as pd
import requests
import xmltodict
import traceback
import logging
import sys
import os
from categorization_program_type import MAPPING_PROGRAMMES_CATEGORIES

dir_path = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(dir_path, '..', '..'))

from quotaclimat.logging import NoStacktraceFormatter, SlackerLogHandler


SLACK_TOKEN = "xoxb-4574857480-4452159914758-t0U00Q7HyfyIJz8sPTi3QDou"
SLACK_CHANNEL = "offseason_quotaclimat_logging"

slack_handler = SlackerLogHandler(
    SLACK_TOKEN, SLACK_CHANNEL, stack_trace=True, fail_silent=False
)
formatter = NoStacktraceFormatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
slack_handler.setFormatter(formatter)
logger = logging.getLogger("Quotaclimat Logger")
logger.addHandler(slack_handler)
logger.setLevel(logging.ERROR)


def extract_tv_program(
    headers={"User-Agent": "Mozilla"}, URL="https://xmltv.ch/xmltv/xmltv-tnt.xml"
):
    """Extract the next 2 weeks of the TV program for the TNT channels
    NB: Arte does not have 2 weeks of TV programmation in this website, only 6 days


    Parameters
    ----------
    headers : dict, optional
        Where to find the header, by default {'User-Agent': 'Mozilla'}
    URL : str, optional
        HTTP adress of the website, by default "https://xmltv.ch/xmltv/xmltv-tnt.xml"

    Returns
    -------
    dict
        Table with the TV programs for the next 2 weeks
    """
    response = requests.get(URL, headers=headers)
    with open("data_public/tv_program/temp/xmltv-tnt.xml", "wb") as outfile:
        _ = outfile.write(response.content)
    with open("data_public/tv_program/temp/xmltv-tnt.xml", "r", encoding="utf-8") as f:
        data = f.read()
    data = xmltodict.parse(data)
    return data


def create_channels_df(data):
    df_channels = pd.DataFrame(data["tv"]["channel"])
    df_channels = pd.json_normalize(df_channels.to_dict(orient="records"))
    df_channels.rename(
        columns={
            "@id": "channel_id",
            "icon.@src": "channel_icon",
            "display-name": "channel_name",
        },
        inplace=True,
    )
    return df_channels


def create_programs_df(data):
    df_programs = pd.DataFrame(data["tv"]["programme"])
    df_programs = pd.json_normalize(df_programs.to_dict(orient="records"), sep="_")
    # Clean column names
    df_programs.columns = [
        col.replace("@", "").replace("#", "").replace("-", "")
        for col in df_programs.columns
    ]
    return df_programs


def add_channels_info_to_programs(df_channels, df_programs):
    df_programs = df_programs.join(df_channels.set_index("channel_id"), on="channel")
    return df_programs


def process_programs(df_programs):
    # Drop empty columns
    df_programs.dropna(axis=1, how="all", inplace=True)
    # Convert some columns to datetime
    df_programs["start"] = pd.to_datetime(
        df_programs["start"], infer_datetime_format=True
    )
    df_programs["stop"] = pd.to_datetime(
        df_programs["stop"], infer_datetime_format=True
    )
    df_programs["day"] = df_programs["start"].dt.floor("d")
    df_programs["length_minutes"] = (df_programs["stop"] - df_programs["start"]).apply(
        lambda x: int(x.total_seconds() / 60)
    )
    df_programs["macro_category"] = df_programs.category_text.map(
        MAPPING_PROGRAMMES_CATEGORIES
    )
    df_programs["%_of_channel"] = (
        100
        * df_programs["length_minutes"]
        / df_programs.groupby("channel_name")["length_minutes"].transform("sum")
    )
    return df_programs


def create_clean_programs(df_programs):
    columns = [
        "start",
        "stop",
        "day",
        "length_minutes",
        "%_of_channel",
        "channel_name",
        "title",
        "subtitle",
        "date",
        "desc_text",
        "category_text",
        "macro_category",
    ]
    df = df_programs[columns]
    return df


def create_clean_programs_with_channels(data):
    df_channels = create_channels_df(data)
    df_programs = create_programs_df(data)
    df_programs_joined = add_channels_info_to_programs(
        df_channels=df_channels, df_programs=df_programs
    )
    df_processed = process_programs(df_programs_joined)
    df_clean = create_clean_programs(df_processed)
    return df_clean


def get_tv_programs_next_days(number_of_days: int, save: bool = True):
    data = extract_tv_program()
    df = create_clean_programs_with_channels(data)

    # Filter with only the next "number_of_days" days
    today = date.today()
    end_date = today + datetime.timedelta(days=number_of_days + 1)
    end_date = end_date.strftime("%Y-%m-%d")
    today_str = today.strftime("%Y-%m-%d")
    df = df[(df.start >= today_str) & (df.stop <= end_date)]

    if save:
        date_beginning_str = today_str.replace("-", "")
        date_end_str = (
            (today + datetime.timedelta(days=number_of_days))
            .strftime("%Y-%m-%d")
            .replace("-", "")
        )
        df.to_csv(
            f"data_public/tv_program/{date_beginning_str}_{date_end_str}_Programme_TV.csv",
            index=False,
        )
    return df


if __name__ == "__main__":
    try:
        df = get_tv_programs_next_days(number_of_days=7, save=True)
    except:
        logger.error(traceback.format_exc())
