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
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, get_db_session

from tenacity import *
from sentry_sdk.crons import monitor
import modin.pandas as pd
import pandas as pd
from sqlalchemy import text
import pandas as pd
import ray
from quotaclimat.utils.sentry import sentry_init
from postgres.schemas.models import stop_word_table

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
    session, top_keywords: pd.DataFrame, days: int, length_context_to_look_for_repetition: int = 35
) -> pd.DataFrame:
    # get unique keywords
    top_unique_keywords = top_keywords["keyword"].unique() # some keyword can be listed in different channels
    logging.info(f"Top unique keywords: {top_unique_keywords}")

    # for each top_unique_keywords call get_all_repetitive_context_advertising_for_a_keyword
    for keyword in top_unique_keywords.itertuples(index=False):
                            logging.info(f"Keyword: {keyword}")  # TODO what's inside ?

                            advertising_context = get_all_repetitive_context_advertising_for_a_keyword(
                                     session, keyword["keyword"], length_context_to_look_for_repetition, days
                            )
                            logging.info(f"Advertising context: {advertising_context}")
                            save_append_stop_word(session, advertising_context) 

    return advertising_context
    
def get_all_repetitive_context_advertising_for_a_keyword(
    session, keyword: str, length_context:  int = 35, days:int = 7, from_date : datetime = None
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
    logging.info(f"Getting context for keyword {keyword} for last {days} days from {from_date}")
    start_date = get_last_X_days(days)
    if from_date is None:
        logging.info(f"From date default to today")
        end_date = get_now()
    else:
        end_date = from_date
    # TODO fix UTF8 keyword issue (végétalisation)
    # TODO difference between {length_context} and 80
    try:
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        sql_query = f"""
            SELECT SUBSTRING("context_keyword",0,80) AS "context", COUNT(*) AS "count"
            FROM (
                SELECT
                "public"."keywords"."number_of_keywords",
                "public"."keywords"."channel_title" AS "channel_title",
                ("public"."keywords"."start" at time zone 'UTC') AS "start_UTC_time",
                ("public"."keywords"."start") AS "start_default_time",
                "public"."keywords"."theme" AS "theme",
                SUBSTRING("public"."keywords"."plaintext" FROM 
                    GREATEST(1, POSITION('{keyword}' IN "public"."keywords"."plaintext") - {length_context}) 
                    FOR LEAST(LENGTH("public"."keywords"."plaintext"), POSITION('{keyword}' IN "public"."keywords"."plaintext"))
                ) AS "context_keyword",
                "public"."keywords"."keywords_with_timestamp" AS "keywords_with_timestamp",
                "public"."keywords"."number_of_keywords" AS "number_of_keywords",
                "public"."keywords"."channel_program" AS "channel_program",
                "public"."keywords"."plaintext" AS "plaintext"
                FROM
                "public"."keywords"
                WHERE "public"."keywords"."start" >= timestamp with time zone {start_date}
                AND "public"."keywords"."start" < timestamp with time zone {end_date}
                AND "public"."keywords"."number_of_keywords" > 0
                AND "public"."keywords"."keywords_with_timestamp"::text LIKE CONCAT('%', '{keyword}', '%') 
                ORDER BY "public"."keywords"."number_of_keywords" DESC
            ) tmp
            GROUP BY 1
            ORDER BY count DESC
        """
        result = session.execute(
            text(sql_query)
        )
        logging.debug(f"Query: {sql_query}")
        # Execute and convert to Pandas DataFrame
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.info(f"result: {result}")
        return result
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
    finally:
        session.close()

def get_top_keywords_by_channel(session, days: int = 7, top: int = 5, from_date : datetime = None) -> pd.DataFrame:
    """
    Fetches the top 5 keywords by channel for the last 7 days using SQLAlchemy ORM.
    Returns:
        pd.DataFrame: A DataFrame containing the top keywords by channel.
    """
    
    logging.info(f"Getting top {top} keywords by channel for the last {days} days")
    start_date = get_last_X_days(days)
    if from_date is None:
        logging.info(f"From date default to today")
        end_date = get_now()
    else:
        end_date = from_date

    try:
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        sql_query = f"""
        WITH ranked_keywords AS (
            SELECT 
                "keyword" AS "keyword",
                "theme" AS "theme",
                "channel_title" AS "channel_title",
                COUNT(*) AS "count",
                ROW_NUMBER() OVER (PARTITION BY "channel_title"
                ORDER BY COUNT(*) DESC) AS rank,
                ROW_NUMBER() OVER (PARTITION BY "channel_title", "keyword" 
                ORDER BY COUNT(*) DESC) AS rank_keyword
            FROM (
                SELECT
                    "public"."keywords"."channel_title" AS "channel_title",
                    "json_keywords_with_timestamp" ->> 'theme' AS "theme",
                    "json_keywords_with_timestamp" ->> 'keyword' AS "keyword",
                    "json_keywords_with_timestamp" ->> 'category' AS "category"
                FROM
                    "public"."keywords"
                CROSS JOIN LATERAL json_array_elements("public"."keywords"."keywords_with_timestamp") AS "json_keywords_with_timestamp"
                WHERE
                    "json_keywords_with_timestamp" ->> 'theme' NOT LIKE '%indirect%'
                    AND "public"."keywords"."start" >= timestamp with time zone {start_date}
                    AND "public"."keywords"."start" < timestamp with time zone {end_date}
            ) tmp
            GROUP BY "keyword", "theme", "channel_title"
            HAVING count(*) > 1
        )

        SELECT 
            keyword,
            theme, 
            channel_title,
            count
        FROM ranked_keywords
        WHERE rank <= {top} AND rank_keyword = 1
        ORDER BY channel_title, "count" DESC
        LIMIT 100;
        """

        result = session.execute(
            text(sql_query)
        )
        logging.debug(f"Query: {sql_query}")
        # Execute and convert to Pandas DataFrame
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.info(f"result: {result}")
        return result
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
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

