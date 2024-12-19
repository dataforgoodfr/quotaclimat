### Library imports
import requests
import json

import logging
import asyncio
from time import sleep
import sys
import os
import gc
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, connect_to_db, get_db_session
from postgres.schemas.models import keywords_table

from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from typing import List, Optional
from tenacity import *
import sentry_sdk
from sentry_sdk.crons import monitor
import modin.pandas as pd
from modin.pandas import json_normalize
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import func, case, extract, and_, desc
from sqlalchemy.orm import sessionmaker, aliased
from sqlalchemy.sql import select
from sqlalchemy import func, case, extract, and_, text, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.expression import cast
from sqlalchemy.types import String
import pandas as pd
from datetime import datetime, timedelta

from datetime import datetime, timedelta
import ray
from quotaclimat.utils.sentry import sentry_init
from postgres.schemas.models import Keywords, Program_Metadata, stop_word_table

logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
logging.getLogger('distributed').setLevel(logging.ERROR)
logging.getLogger('worker').setLevel(logging.ERROR)
sentry_init()

#read whole file to a string
password = get_password()
AUTH_URL = get_auth_url()
USER = get_user()
KEYWORDS_URL = get_keywords_url()

# TODO
# def apply_cosine_similarity_on_keywords(keywords: pd.DataFrame) -> pd.DataFrame:

def save_append_stop_word(session, stop_word_list: pd.DataFrame):
    logging.info(f"Saving stop word {stop_word_list} list to the database")
    try:
        # Save the stop word list to the database
        stop_word_list.to_sql(
            stop_word_table,
            session.bind,
            if_exists="overwrite",
            index=False,
        )
    finally:
        session.close()

def get_repetitive_context_advertising(
    session, top_keywords: pd.DataFrame, days=int
) -> pd.DataFrame:
    # we are going to look before and after the keyword to look for repetition
    length_context_to_look_for_repetition = 35
    
    # get unique keywords
    top_unique_keywords = top_keywords["keyword"].unique()
    logging.info(f"Top unique keywords: {top_unique_keywords}")

    # for each top_unique_keywords call get_all_repetitive_context_advertising_for_a_keyword
    for keyword in top_unique_keywords.itertuples(index=False):
                            logging.info(f"Keyword: {keyword}")
                            # TODO what's inside ?
                            advertising_context = get_all_repetitive_context_advertising_for_a_keyword(
                                     session, keyword, length_context_to_look_for_repetition, days
                                 )
                            logging.info(f"Advertising context: {advertising_context}")
                            save_append_stop_word(session, advertising_context) 

    return advertising_context
    
def get_all_repetitive_context_advertising_for_a_keyword(
    session, keyword: str, length_context: int, days=int
) -> pd.DataFrame:
    """
    Fetches the repetitive context around a specified keyword in plaintext for advertising analysis.

    Parameters:
        db_url (str): The database connection URL.
        keyword (str): The keyword to search in plaintext.
        length_context (int): The maximum length of the extracted context.

    Returns:
        pd.DataFrame: A DataFrame containing the contexts and their repetition counts.
    """
    # Calculate the date range for the last 7 days
    end_date = get_now()
    start_date = get_last_X_days(days)

    try:
        # Calculate the start position for the substring based on the keyword position
        context_start = func.greatest(
            1, func.position(keyword, Keywords.plaintext) - 35
        )

        # Calculate the substring context around the keyword
        context_keyword = func.substring(
            Keywords.plaintext, context_start, length_context
        ).label("context_keyword")

        # Weekday logic for filtering
        weekday_case = case(
            [
                (
                    ((extract("dow", Keywords.start) + 1 + 6) % 7) == 0,
                    7,
                )
            ],
            else_=((extract("dow", Keywords.start) + 1 + 6) % 7),
        )

        # Subquery to calculate context_keyword
        subquery = (
            session.query(
                context_keyword,
                func.count().label("repetition_count"),
            )
            .join(
                Program_Metadata,
                and_(
                    Keywords.channel_name == Program_Metadata.channel_name,
                    Keywords.channel_program == Program_Metadata.channel_program,
                    weekday_case == Program_Metadata.weekday,
                    Keywords.start.between(
                        Program_Metadata.program_grid_start,
                        Program_Metadata.program_grid_end,
                    ),
                ),
            )
            .filter(
                Keywords.start >= start_date,
                Keywords.start < end_date,
                Keywords.number_of_keywords > 0,
                cast(Keywords.keywords_with_timestamp, String).ilike(f"%{keyword}%"),
            )
            .group_by(context_keyword)
            .subquery()
        )

        # Query to aggregate and order the contexts
        query = (
            session.query(
                subquery.c.context_keyword.label("context"),
                subquery.c.repetition_count.label("repetition_count"),
            )
            .order_by(desc(subquery.c.repetition_count))
        )

        # Execute query and convert to Pandas DataFrame
        result = pd.read_sql(query.statement, session.bind)
        return result

    finally:
        session.close()

def get_top_keywords_by_channel(session, days: int, top: int) -> pd.DataFrame:
    """
    Fetches the top 5 keywords by channel for the last 7 days using SQLAlchemy ORM.
    Returns:
        pd.DataFrame: A DataFrame containing the top keywords by channel.
    """
    logging.info(f"Getting top {top} keywords by channel for the last {days} days")

    end_date = get_now()
    start_date = get_last_X_days(days)

    try:
        # TODO use date
        start_date =  "'2020-12-12 00:00:00.000 +01:00'"
        end_date = "'2024-12-19 00:00:00.000 +01:00'"
        sql_query = f"""
        WITH ranked_keywords AS (
            SELECT 
                "keyword" AS "keyword",
                "theme" AS "theme",
                "Program Metadata - Channel Program__channel_title" AS "chaine",
                COUNT(*) AS "Nombre",
                ROW_NUMBER() OVER (PARTITION BY "Program Metadata - Channel Program__channel_title"
                ORDER BY COUNT(*) DESC) AS rank,
                ROW_NUMBER() OVER (PARTITION BY "Program Metadata - Channel Program__channel_title", "keyword" 
                ORDER BY COUNT(*) DESC) AS rank_keyword
            FROM (
                SELECT
                    "public"."program_metadata"."channel_title" AS "Program Metadata - Channel Program__channel_title",
                    "public"."program_metadata"."channel_program" AS "Program Metadata - Channel Program__channel_program",
                    "json_keywords_with_timestamp" ->> 'theme' AS "theme",
                    "json_keywords_with_timestamp" ->> 'keyword' AS "keyword",
                    "json_keywords_with_timestamp" ->> 'category' AS "category"
                FROM
                    "public"."keywords"
                INNER JOIN "public"."program_metadata" 
                    ON "public"."keywords"."channel_program" = "public"."program_metadata"."channel_program"
                    AND "public"."keywords"."channel_name" = "public"."program_metadata"."channel_name"
                    AND (
                        CASE
                            WHEN (
                                (CAST(extract(dow FROM "public"."keywords"."start") AS integer) + 1 + 6) % 7
                            ) = 0 THEN 7
                            ELSE (CAST(extract(dow FROM "public"."keywords"."start") AS integer) + 1 + 6) % 7
                        END
                    ) = "public"."program_metadata"."weekday"
                    AND CAST("public"."keywords"."start" AS date) >= CAST("public"."program_metadata"."program_grid_start" AS date)
                    AND CAST("public"."keywords"."start" AS date) <= CAST("public"."program_metadata"."program_grid_end" AS date)
                CROSS JOIN LATERAL json_array_elements("public"."keywords"."keywords_with_timestamp") AS "json_keywords_with_timestamp"
                WHERE
                    "json_keywords_with_timestamp" ->> 'theme' NOT LIKE '%indirect%'
                    AND "public"."keywords"."start" >= timestamp with time zone {start_date}
                    AND "public"."keywords"."start" < timestamp with time zone {end_date}
            ) tmp
            GROUP BY "keyword", "theme", "Program Metadata - Channel Program__channel_title"
            HAVING count(*) > 1
        )

        SELECT 
            keyword, 
            theme, 
            chaine, 
            rank, 
            "Nombre"
        FROM ranked_keywords
        WHERE rank <= {top} AND rank_keyword = 1
        ORDER BY chaine, "Nombre" DESC
        LIMIT 100;
        """

        result = session.execute(
            text(sql_query),
            {'start_date': '2020-12-12 00:00:00.000 +01:00', 'end_date': '2024-12-19 00:00:00.000 +01:00'}
        )
        logging.warning(f"result: {result}")
        logging.debug(f"Query: {sql_query}")
        # Execute and convert to Pandas DataFrame
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.warning(f"result: {result}")
        return result

    finally:
        session.close()


async def manage_stop_word(exit_event, session):
    # get 5 top keywords for each channel for the last 7 days
    top_keywords = get_top_keywords_by_channel(session, duration=7, top=5)
    logging.info(f"Top keywords: {top_keywords}")
    # for each top keyword, check if the plaintext contexte (35 words before and after) is repetitive > 5 occurences
    stop_word_list = get_repetitive_context_advertising(session, top_keywords)

    # save the stop word list to the database
    save_append_stop_word(stop_word_list)

    exit_event.set()



async def main(exit_event):
    with monitor(monitor_slug='stopword'): #https://docs.sentry.io/platforms/python/crons/
        try:
            logging.info("Start stop word")
            create_tables()
            session = get_db_session()

            event_finish = asyncio.Event()
            # Start the health check server in the background
            health_check_task = asyncio.create_task(run_health_check_server())

            context = ray.init(
                dashboard_host="0.0.0.0", # for docker dashboard
                # runtime_env=dict(worker_process_setup_hook=sentry_init),
            )
            logging.info(f"Ray context dahsboard available at : {context.dashboard_url}")
            logging.warning(f"Ray Information about the env: {ray.available_resources()}")

            # Start batch job
            asyncio.create_task(manage_stop_word(event_finish, session))

            # Wait for both tasks to complete
            await event_finish.wait()

            res=health_check_task.cancel()
        except Exception as err:
            logging.fatal("Main crash (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)
    logging.info("Exiting with success")
    sys.exit(0)

if __name__ == "__main__":
    getLogger()
    asyncio.run(main())
    sys.exit(0)

