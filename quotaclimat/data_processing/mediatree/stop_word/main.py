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
from postgres.schemas.models import create_tables, connect_to_db, Stop_Word, get_db_session
from sqlalchemy.orm import Session
from sqlalchemy import func
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash

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

def get_all_stop_word(session: Session, offset: int = 0, batch_size: int = 50000) -> list:
    logging.debug(f"Getting {batch_size} elements from offset {offset}")
    query = session.query(
            Stop_Word.id,
            Stop_Word.context,
            Stop_Word.keyword,
            Stop_Word.channel_title,
            Stop_Word.count,
            func.timezone('UTC', Stop_Word.created_at).label('created_at'),
            func.timezone('UTC', Stop_Word.updated_at).label('updated_at')
        ).order_by(Stop_Word.created_at)

    return query.offset(offset) \
        .limit(batch_size) \
        .all()

def save_append_stop_word(conn, stop_word_list: list):
    logging.info(f"Saving stop word {stop_word_list} list to the database")
    try:
        if len(stop_word_list) == 0:
            logging.warning("No stop word to save")
        else:
            stop_word_list_df = pd.DataFrame(stop_word_list)
            save_to_pg(
                stop_word_list_df,
                Stop_Word.__tablename__,
                conn
            )
    finally:
        conn.dispose()

def get_repetitive_context_advertising(
    session, top_keywords: pd.DataFrame, days: int, from_date: datetime = None
) -> pd.DataFrame:
    # get unique keywords and one of their channel
    filtered_keywords = top_keywords.drop_duplicates(subset='keyword', keep='first')
    top_unique_keyword_with_channel = filtered_keywords.to_dict(orient='records')
    logging.info(f"Top unique keywords: {top_unique_keyword_with_channel}")

    result = []
    for keyword_channel in top_unique_keyword_with_channel:
                            logging.info(f"Keyword: {keyword_channel["keyword"]}")

                            advertising_context = get_all_repetitive_context_advertising_for_a_keyword(
                                     session, keyword_channel["keyword"], channel_title=keyword_channel["channel_title"], \
                                     days=days,from_date=from_date
                            )
                            
                            if(len(advertising_context) > 0):
                                logging.info(f"Advertising context for {keyword_channel["keyword"]}: {advertising_context}")
                                result.extend(advertising_context)
                            else:
                                logging.debug(f"Advertising context empty for {keyword_channel["keyword"]}")

    return result
    
def get_all_repetitive_context_advertising_for_a_keyword(
    session, keyword: str, channel_title: str, days:int = 7, from_date : datetime = None, min_number_of_repeatition: int = 20\
    ,min_length_context:int = 20
    ,after_context:int = 140
    ,before_context:int = 35
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
    min_length_context += len(keyword)# security nets to not add short generic context
    after_context += len(keyword) # should include the keyword length

    start_date = get_last_X_days(days)
    if from_date is None:
        end_date = get_now()
    else:
        end_date = from_date

    try:
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        sql_query = f"""
            SELECT SUBSTRING("context_keyword",0,80) AS "context",
                   COUNT(*) AS "count"
            FROM (
                SELECT
                "public"."keywords"."number_of_keywords",
                "public"."keywords"."channel_title" AS "channel_title",
                "public"."keywords"."start" AS "start_default_time",
                "public"."keywords"."theme" AS "theme",
                SUBSTRING(
                    "public"."keywords"."plaintext", 
                    GREATEST(POSITION('{keyword}' IN "public"."keywords"."plaintext") - {before_context}, 1), -- start position
                    LEAST({after_context}, LENGTH( "public"."keywords"."plaintext")) -- length of the context
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
                AND jsonb_pretty("keywords_with_timestamp"::jsonb) LIKE CONCAT('%', '{keyword}', '%') 
                ORDER BY "public"."keywords"."number_of_keywords" DESC
            ) tmp
            WHERE LENGTH(SUBSTRING("context_keyword",0,80)) > {min_length_context} -- safety net to not add small generic context
            GROUP BY 1
            HAVING COUNT(*) >= {min_number_of_repeatition}
            ORDER BY count DESC
        """

        result = session.execute(
            text(sql_query)
        )
        logging.debug(f"Query: {sql_query}")
        # Execute and convert to Pandas DataFrame
        result = [dict(row) for row in result.mappings()]
        # add metadata to result for all rows
        for row in result:
            row["id"] = get_consistent_hash(row["context"]) # to avoid adding duplicates
            row["keyword"] = keyword
            row["channel_title"] = channel_title
        
        logging.debug(f"result: {result}")
        return result
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
    finally:
        session.close()

def get_top_keywords_by_channel(session, duration: int = 7, top: int = 5, from_date : datetime = None\
                                ,min_number_of_keywords:int = 10) -> pd.DataFrame:
    """
    Fetches the top 5 keywords by channel for the last 7 days using SQLAlchemy ORM.
    Returns:
        pd.DataFrame: A DataFrame containing the top keywords by channel.
    """
    
    logging.info(f"Getting top {top} keywords by channel for the last {duration} days")
    start_date = get_last_X_days(duration)
    if from_date is None:
        end_date = get_now()
    else:
        end_date = from_date

    try:
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        logging.info(f"From start_date {start_date} to end_date {end_date}")

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
            HAVING count(*) >= {min_number_of_keywords}
        )

        SELECT 
            keyword,
            theme, 
            channel_title,
            count
        FROM ranked_keywords
        WHERE rank <= {top} AND rank_keyword = 1
        ORDER BY channel_title, "count" DESC, keyword 
        LIMIT 100;
        """

        result = session.execute(
            text(sql_query)
        )
        logging.debug(f"Query: {sql_query}")
        # Execute and convert to Pandas DataFrame
        result = pd.DataFrame(result.fetchall(), columns=result.keys())
        logging.debug(f"result: {result}")
        return result
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
    finally:
        session.close()


async def manage_stop_word(exit_event = None, conn = None, duration: int = 7, from_date = None):
    try:
        if from_date is not None:
            from_date = datetime.fromtimestamp(int(from_date))
        session = get_db_session(conn)
        # get 5 top keywords for each channel for the last 7 days
        top_keywords = get_top_keywords_by_channel(session, duration=duration, top=5, from_date=from_date)
        logging.info(f"Top keywords: {top_keywords}")
        # for each top keyword, check if the plaintext contexte (35 words before and after) is repetitive > 5 occurences
        stop_word_list = get_repetitive_context_advertising(session, top_keywords, days=duration, from_date=from_date)

        # save the stop word list to the database
        save_append_stop_word(conn, stop_word_list=stop_word_list)
        if exit_event is not None:
            exit_event.set()
    except Exception as err:
        logging.fatal("manage_stop_word (%s) %s" % (type(err).__name__, err))
        ray.shutdown()
        sys.exit(1)


# TODO : increase context length (due to repeated ad words)
# TODO : test with several ad words said in the same plaintext
async def main():
    with monitor(monitor_slug='stopword'): #https://docs.sentry.io/platforms/python/crons/
        try:
            logging.info("Start stop word")
            create_tables()
            conn = connect_to_db()

            event_finish = asyncio.Event()
            # Start the health check server in the background
            health_check_task = asyncio.create_task(run_health_check_server())

            context = ray.init(
                dashboard_host="0.0.0.0", # for docker dashboard
                # runtime_env=dict(worker_process_setup_hook=sentry_init),
            )
            logging.info(f"Ray context dahsboard available at : {context.dashboard_url}")
            logging.warning(f"Ray Information about the env: {ray.available_resources()}")

            number_of_previous_days = int(os.environ.get("NUMBER_OF_PREVIOUS_DAYS", 7))
            logging.info(f"Number of previous days (NUMBER_OF_PREVIOUS_DAYS): {number_of_previous_days}")

            from_date = os.environ.get("START_DATE", None)
            logging.info(f"Start date (START_DATE): {from_date}")
            # Start batch job
            asyncio.create_task(manage_stop_word(exit_event=event_finish, conn=conn, duration=number_of_previous_days, from_date=from_date))

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