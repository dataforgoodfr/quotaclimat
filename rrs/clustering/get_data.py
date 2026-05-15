import os
from datetime import date, datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
import psycopg
import requests
from datasets import Dataset, DatasetDict, load_dataset
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def fetch_articles(columns: List[str] = None) -> pd.DataFrame:
    conninfo = (
        f"host={os.getenv('PG_HOST', 'localhost')} port={os.getenv('PG_PORT', 5432)} "
        f"dbname={os.getenv('PG_DATABASE', 'postgres')} user={os.getenv('PG_USER', 'user')} "
        f"password={os.getenv('PG_PASSWORD', 'supersecret')}"
    )
    columns_str = ", ".join(columns) if columns else "*"
    query = f"""
        SELECT {columns_str}
        FROM analytics.task_global_completion
    """
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            columns = [desc.name for desc in cur.description]

    return pd.DataFrame(rows, columns=columns)


def get_week_number(record):
    format_str = "%Y-%m-%dT%H:%M:%S"
    if len(record["data_item_start"]) > 19:
        format_str = format_str + "%z"
    week_number = (
        datetime.strptime(record["data_item_start"], format_str).isocalendar().week
    )
    return week_number

def cast_to_int_with_nan(col: pd.Series, int_type: str='uint8'):
    col = col.fillna(-1)
    col = col.astype(int_type)
    col[col==-1] = np.nan
    return col


def format_dtypes(df: pd.DataFrame):
    df.data_item_day = df.data_item_day.astype('f2')
    df.data_item_month = df.data_item_month.astype('f2')
    df.data_item_year = df.data_item_year.astype('f2')
    df.data_item_model_result = df.data_item_model_result.astype('f2')
    df.mesinfo_correct = df.mesinfo_correct.astype('f2')
    df.mesinfo_incorrect = df.mesinfo_incorrect.astype('f2')
    df.speaker_journalist = df.speaker_journalist.astype('f2')
    df.speaker_commentator = df.speaker_commentator.astype('f2')
    df.speaker_guest = df.speaker_guest.astype('f2')
    df.speaker_politician = df.speaker_politician.astype('f2')
    df.speaker_audience = df.speaker_audience.astype('f2')
    df.speaker_unknown = df.speaker_unknown.astype('f2')
    df.mesinfo_corrected_bool = df.mesinfo_corrected_bool.astype('f2')
    df.mesinfo_corrected = df.mesinfo_corrected.fillna("")
    return df

def generate_hf_dataset(df):
    df.data_item_start = df.data_item_start.dt.strftime(
        "%Y-%m-%dT%H:%M:%S%z"
    )
    dataset = Dataset.from_pandas(df)
    return dataset

DATASET_COLUMNS = [
    "task_completion_aggregate_id",
    "task_aggregate_id",
    "created_at",
    "updated_at",
    "is_labeled",
    "project_id",
    "country",
    "data_item_id",
    "data_item_channel",
    "data_item_channel_name",
    "data_item_channel_title",
    "data_item_channel_program",
    "data_item_channel_program_type",
    "data_item_day",
    "data_item_month",
    "data_item_year",
    "data_item_start",
    "data_item_model_name",
    "data_item_model_reason",
    "data_item_model_result",
    "data_item_plaintext",
    "data_item_plaintext_whisper",
    "data_item_url_mediatree",
    "mesinfo_choice",
    "locuteur_choice",
    "mesinfo_correct",
    "mesinfo_incorrect",
    "speaker_journalist",
    "speaker_commentator",
    "speaker_guest",
    "speaker_politician",
    "speaker_audience",
    "speaker_unknown",
    "mesinfo_corrected",
    "mesinfo_corrected_bool",
    "debunk_references",
    "claims",
    "explanations",
    "other_comments",
]


def get_data_from_db():
    df = fetch_articles(DATASET_COLUMNS)
    df = format_dtypes(df)
    dataset = generate_hf_dataset(df)
    return dataset

if __name__ == "__main__":
    get_data_from_db()
