import json
import os
import sys

import pandas as pd
import pytest

sys.path.append(os.path.abspath("/app"))
import logging
from datetime import datetime

from app.country import (
    ALL_COUNTRIES,
    BELGIUM_COUNTRY,
    BRAZIL_COUNTRY,
    FRANCE_COUNTRY,
    Country,
    CountryCollection,
    get_country_or_collection_from_name,
)

from app.pg_utils import *


def save_to_pg(df, table, conn):
    number_of_elements = len(df)
    logging.info(f"Saving {number_of_elements} elements to PG table '{table}'")
    try:
        logging.debug("Schema before saving\n%s", df.dtypes)
        df.to_sql(
            table,
            index=False,
            con=conn,
            if_exists="append",
            chunksize=1000,
            # method=insert_or_do_nothing_on_conflict,  # pandas does not handle conflict natively
            # dtype={"keywords_with_timestamp": JSON, "theme": JSON, "srt": JSON}, # only for keywords
        )
        logging.info("Saved dataframe to PG")
        return len(df)
    except Exception as err:
        logging.error("Could not save : \n %s" % (err))
        return 0


def empty_tables(session=None, table=Keywords):
    if (
        os.environ.get("POSTGRES_HOST") == "postgres_db"
        or os.environ.get("POSTGRES_HOST") == "localhost"
    ):
        logging.warning("""Doing: Empty table """)
        session.query(table).delete()
        session.commit()
        logging.warning("""Done: Empty table """)


def test_get_keywords_for_a_day_and_channel():
    create_tables()
    session = get_db_session()
    empty_tables(session)
    conn = connect_to_db()
    start = pd.to_datetime("2024-12-12 10:10:10")
    channel_name = "itele"
    channel_title = "Cnews"
    primary_key1 = "id1"
    primary_key2 = "id2"
    channel_program = "program"
    channel_program_type = "program_type"

    list = [
        {
            "id": primary_key1,
            "start": start,
            "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
            "channel_name": channel_name,
            "channel_title": channel_title,
            "number_of_keywords_climat": 1,
            "channel_program_type": channel_program_type,
            "channel_program": channel_program,
            "country": "france",
        },
        {
            "id": primary_key2,
            "start": start,
            "plaintext": "hello world",
            "channel_name": channel_name,
            "channel_title": channel_title,
            "number_of_keywords_climat": 3,
            "channel_program_type": channel_program_type,
            "channel_program": channel_program,
            "country": "france",
        },
        {  # should be ignored as not the right channel_name
            "id": "id3",
            "start": start,
            "plaintext": "hello world",
            "channel_name": "fake tv news we should ignore",
            "channel_title": channel_title,
            "number_of_keywords_climat": 3,
            "channel_program_type": channel_program_type,
            "channel_program": channel_program,
            "country": "france",
        },
    ]
    dataframe_to_save = pd.DataFrame(list)
    save_to_pg(dataframe_to_save, table=keywords_table, conn=conn)
    dataframe_to_save.drop(columns=["number_of_keywords_climat"], inplace=True)
    dataframe_to_save.drop(dataframe_to_save.index[-1], inplace=True)
    output = get_keywords_for_a_day_and_channel(
        session, date=start, country=FRANCE_COUNTRY, channel_name=channel_name
    )
    output = output._to_pandas()
    dataframe_to_save = dataframe_to_save._to_pandas()
    pd.testing.assert_frame_equal(dataframe_to_save, output, check_like=True)


def test_get_keywords_for_a_day_and_channel_ignore_id():
    create_tables()
    session = get_db_session()
    empty_tables(session)
    conn = connect_to_db()

    list = [
        {
            "id": "id1",
            "start": pd.to_datetime("2024-12-12 10:10:10"),
            "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
            "channel_name": "itele",
            "channel_title": "Cnews",
            "number_of_keywords_climat": 1,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
        {
            "id": "id2",
            "start": pd.to_datetime("2024-12-12 10:10:10"),
            "plaintext": "hello world",
            "channel_name": "itele",
            "channel_title": "Cnews",
            "number_of_keywords_climat": 3,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
        {  # should be ignored as id3 needs to be avoided
            "id": "id3",
            "start": pd.to_datetime("2024-12-12 10:10:10"),
            "plaintext": "hello world",
            "channel_name": "itele",
            "channel_title": "Cnews",
            "number_of_keywords_climat": 3,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
    ]
    dataframe_to_save = pd.DataFrame(list)
    save_to_pg(dataframe_to_save, table=keywords_table, conn=conn)
    dataframe_to_save.drop(columns=["number_of_keywords_climat"], inplace=True)
    dataframe_to_save.drop(dataframe_to_save.index[-1], inplace=True)
    output = get_keywords_for_a_day_and_channel(
        session,
        date=pd.to_datetime("2024-12-12 10:10:10"),
        country=FRANCE_COUNTRY,
        channel_name="itele",
        ids_to_avoid=["id3"],
    )
    output = output._to_pandas()
    dataframe_to_save = dataframe_to_save._to_pandas()
    pd.testing.assert_frame_equal(dataframe_to_save, output, check_like=True)


def test_get_labelstudio_ids():
    create_tables(
        conn=connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")),
        label_studio=True,
    )
    session = get_db_session(
        engine=connect_to_db(
            db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")
        )
    )
    empty_tables(session, table=LabelStudioTask)
    conn = connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio"))
    start = pd.to_datetime("2024-12-12 10:10:10")
    channel_name = "itele"
    channel_title = "Cnews"
    primary_key1 = "id1"
    primary_key2 = "id2"
    channel_program = "program"
    channel_program_type = "program_type"
    test_country = Country(
        code="fra",
        name="france",
        language="french",
        bucket="test",
        model="gpt-4o-mini",
        prompt_version="0.0.0",
        label_studio_id=0,
        label_studio_project=1,
        channels=[
            "itele",
        ],
    )

    list = [
        dict(
            id=1142,  # (Integer, nullable=False, primary_key=True)
            data=json.dumps(
                dict(
                    item=dict(
                        start="2024-12-12 10:10:10", id="id1", channel_name=channel_name
                    )
                )
            ),  # (JSON, nullable=False)
            created_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            updated_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            is_labeled=False,  # (Boolean, nullable=False)
            project_id=1,  # (Integer, nullable=True)
            overlap=1,  # (Integer, nullable=False)
            updated_by_id=1,  # (Integer, nullable=True)
            total_annotations=0,  # (Integer, nullable=False)
            cancelled_annotations=0,  # (Integer, nullable=False)
            total_predictions=0,  # (Integer, nullable=False)
            comment_count=0,  # (Integer, nullable=False)
            unresolved_comment_count=0,  # (Integer, nullable=False)
        ),
        dict(
            id=1143,  # (Integer, nullable=False, primary_key=True)
            data=json.dumps(
                dict(
                    item=dict(
                        start="2024-12-12 10:10:10", id="id1", channel_name=channel_name
                    )
                )
            ),  # (JSON, nullable=False)
            created_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            updated_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            is_labeled=False,  # (Boolean, nullable=False)
            project_id=2,  # (Integer, nullable=True)
            overlap=1,  # (Integer, nullable=False)
            updated_by_id=1,  # (Integer, nullable=True)
            total_annotations=0,  # (Integer, nullable=False)
            cancelled_annotations=0,  # (Integer, nullable=False)
            total_predictions=0,  # (Integer, nullable=False)
            comment_count=0,  # (Integer, nullable=False)
            unresolved_comment_count=0,  # (Integer, nullable=False)
        ),
    ]
    dataframe_to_save = pd.DataFrame(list)
    save_to_pg(dataframe_to_save, table=labelstudio_task_table, conn=conn)
    output = get_labelstudio_ids(
        session, date=start, country=test_country, channel_name=channel_name
    )
    assert len(output) == 1


def test_pg_insert_data():
    session = get_db_session()
    conn = connect_to_db()
    create_tables(conn=conn)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_files = os.listdir(os.path.join(script_dir, "test_data"))
    result = 0
    for file in data_files:
        if not file.startswith("."):
            try:
                with open(os.path.join(script_dir, f"test_data/{file}")) as jsonfile:
                    data_sample = json.load(jsonfile)
            except FileNotFoundError:
                raise FileNotFoundError(
                    "Cannot find " + os.path.join(script_dir, f"test_data/{file}")
                )
            dataframe_to_save = pd.DataFrame(data_sample)
            dataframe_to_save["start"] = pd.to_datetime(
                dataframe_to_save["start"], utc=True
            )
            result = save_to_pg(dataframe_to_save, table=keywords_table, conn=conn)
            assert result != 0
    logging.info("Data inserted into PG")


def test_is_there_data_for_this_day_safe_guard():
    date = pd.to_datetime("2024-12-12 10:10:10")
    session = get_db_session()
    result = is_there_data_for_this_day_safe_guard(session, date, country=ALL_COUNTRIES)
    assert result == True


def test_is_there_data_for_this_day_safe_guard_future():
    date = pd.to_datetime("2100-12-12 10:10:10")
    session = get_db_session()
    result = is_there_data_for_this_day_safe_guard(session, date, country=ALL_COUNTRIES)
    assert result == False


def test_get_keywords_for_period_and_channels():
    create_tables()
    session = get_db_session()
    empty_tables(session)
    conn = connect_to_db()

    list = [
        {
            "id": "id1",
            "start": pd.to_datetime("2024-12-12 10:10:10"),
            "plaintext": "cheese pizza habitabilité de la planète conditions de vie sur terre animal",
            "channel_name": "itele",
            "channel_title": "Cnews",
            "number_of_keywords_climat": 1,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
        {
            "id": "id2",
            "start": pd.to_datetime("2024-12-13 10:10:10"),
            "plaintext": "hello world",
            "channel_name": "itele",
            "channel_title": "Cnews",
            "number_of_keywords_climat": 3,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
        {
            "id": "id3",
            "start": pd.to_datetime("2024-12-14 10:10:10"),
            "plaintext": "hello world",
            "channel_name": "wrong_channel",
            "channel_title": "Wrong Channel",
            "number_of_keywords_climat": 3,
            "channel_program_type": "program_type",
            "channel_program": "program",
            "country": "france",
        },
    ]
    dataframe_to_save = pd.DataFrame(list)
    save_to_pg(dataframe_to_save, table=keywords_table, conn=conn)
    dataframe_to_save.drop(columns=["number_of_keywords_climat"], inplace=True)
    dataframe_to_save.drop(dataframe_to_save.index[-1], inplace=True)
    output = get_keywords_for_period_and_channels(
        session=session,
        date_start=pd.to_datetime("2024-12-11 10:10:10"),
        date_end=pd.to_datetime("2024-12-13 10:10:10"),
        channels=["itele"],
        country=FRANCE_COUNTRY,
    )
    output = output._to_pandas()
    dataframe_to_save = dataframe_to_save._to_pandas()
    pd.testing.assert_frame_equal(dataframe_to_save, output, check_like=True)


def test_get_labelstudio_records_period():
    create_tables(
        conn=connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")),
        label_studio=True,
    )
    session = get_db_session(
        engine=connect_to_db(
            db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")
        )
    )
    empty_tables(session, table=LabelStudioTask)
    conn = connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio"))
    channel_name = "itele"
    list = [
        dict(
            id=1142,  # (Integer, nullable=False, primary_key=True)
            data=json.dumps(
                dict(
                    item=dict(
                        start="2024-12-12 10:10:10", id="id1", channel_name=channel_name
                    )
                )
            ),  # (JSON, nullable=False)
            created_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            updated_at="2024-11-12 10:10:10",  # (DateTime, nullable=False)
            is_labeled=False,  # (Boolean, nullable=False)
            project_id=FRANCE_COUNTRY.label_studio_id,  # (Integer, nullable=True)
            overlap=1,  # (Integer, nullable=False)
            updated_by_id=1,  # (Integer, nullable=True)
            total_annotations=0,  # (Integer, nullable=False)
            cancelled_annotations=0,  # (Integer, nullable=False)
            total_predictions=0,  # (Integer, nullable=False)
            comment_count=0,  # (Integer, nullable=False)
            unresolved_comment_count=0,  # (Integer, nullable=False)
        ),
        dict(
            id=1143,  # (Integer, nullable=False, primary_key=True)
            data=json.dumps(
                dict(
                    item=dict(
                        start="2024-12-15 10:10:10", id="id1", channel_name=channel_name
                    )
                )
            ),  # (JSON, nullable=False)
            created_at="2024-11-15 10:10:10",  # (DateTime, nullable=False)
            updated_at="2024-11-15 10:10:10",  # (DateTime, nullable=False)
            is_labeled=False,  # (Boolean, nullable=False)
            project_id=FRANCE_COUNTRY.label_studio_id,  # (Integer, nullable=True)
            overlap=1,  # (Integer, nullable=False)
            updated_by_id=1,  # (Integer, nullable=True)
            total_annotations=0,  # (Integer, nullable=False)
            cancelled_annotations=0,  # (Integer, nullable=False)
            total_predictions=0,  # (Integer, nullable=False)
            comment_count=0,  # (Integer, nullable=False)
            unresolved_comment_count=0,  # (Integer, nullable=False)
        ),
    ]
    dataframe_to_save = pd.DataFrame(list)
    save_to_pg(dataframe_to_save, table=labelstudio_task_table, conn=conn)
    output = get_labelstudio_records_period(
        session=session,
        date_start=pd.to_datetime("2024-12-11 10:10:10"),
        date_end=pd.to_datetime("2024-12-13 10:10:10"),
        channels=["itele"],
        country=FRANCE_COUNTRY,
    )
    assert output.shape == (1, 17)


def test_get_labelstudio_annotations():
    create_tables(
        conn=connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")),
        label_studio=True,
    )
    session = get_db_session(
        engine=connect_to_db(
            db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio")
        )
    )
    empty_tables(session, table=LabelStudioTaskCompletion)
    conn = connect_to_db(db_database=os.environ.get("POSTGRES_DB_LS", "labelstudio"))
    annotations = [
        dict(
            id=1237,
            result=json.dumps(
                [
                    {
                        "id": "Dxcd6WtsvJ",
                        "type": "choices",
                        "value": {"choices": ["Incorrect"]},
                        "origin": "manual",
                        "to_name": "table",
                        "from_name": "choice",
                    },
                ]
            ),
            was_cancelled=False,
            ground_truth=False,
            created_at=pd.to_datetime("19-03-2025 09:44:00"),
            updated_at=pd.to_datetime("19-03-2025 09:44:00"),
            task_id=1880,
            prediction=json.dumps({}),
            lead_time=590.11,
            result_count=2,
            completed_by_id=7,
            project_id=6,
            updated_by_id=7,
            unique_id="353f813c-b6d4-4ce5-aa62-7ead83f4f49f",
            draft_created_at=pd.to_datetime("19-03-2025 09:44:00"),
            bulk_created=False,
        ),
        dict(
            id=3760,
            result=json.dumps(
                [
                    {
                        "id": "Dxcd6WtsvJ",
                        "type": "choices",
                        "value": {"choices": ["Incorrect"]},
                        "origin": "manual",
                        "to_name": "table",
                        "from_name": "choice",
                    },
                ]
            ),
            was_cancelled=False,
            ground_truth=False,
            created_at=pd.to_datetime("19-01-2025 09:44:00"),
            updated_at=pd.to_datetime("19-01-2025 09:44:00"),
            task_id=1882,
            prediction=json.dumps({}),
            lead_time=590.11,
            result_count=2,
            completed_by_id=7,
            project_id=6,
            updated_by_id=7,
            unique_id="353f813c-b6d4-4ce5-aa62-7ead83f4f45f",
            draft_created_at=pd.to_datetime("19-01-2025 09:44:00"),
            bulk_created=False,
        ),
    ]
    dataframe_to_save = pd.DataFrame(annotations)
    save_to_pg(dataframe_to_save, table=labelstudio_task_completion_table, conn=conn)
    output = get_labelstudio_annotations(
        session=session,
        task_ids=[1880],
    )
    assert output.shape == (1, 21)
