import logging
import asyncio
import sys
import os
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.detect_keywords import MEDIATREE_TRANSCRIPTION_PROBLEM
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, connect_to_db, Stop_Word, get_db_session
from sqlalchemy.orm import Session
from sqlalchemy import func, select
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
from thefuzz import fuzz

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

# use to not add (almost) duplicates context to PG  - TODO: not useful for now
def is_already_known_stop_word(stop_words: list, context: str) -> bool:
    for stop_word in stop_words:
        if fuzz.partial_ratio(stop_word, context) :
            return True
        
    return False

def get_all_stop_word(session: Session, offset: int = 0, batch_size: int = 50000, validated_only = True, filter_days: int = None) -> list:
    logging.debug(f"Getting {batch_size} elements from offset {offset}")

    statement = select(
                Stop_Word.id,
                Stop_Word.context,
                Stop_Word.keyword,
                Stop_Word.channel_title,
                Stop_Word.count,
                func.timezone('UTC', Stop_Word.created_at).label('created_at'),
                func.timezone('UTC', Stop_Word.updated_at).label('updated_at')
            ).select_from(Stop_Word) \
    .order_by(Stop_Word.count.desc(),
              func.length(Stop_Word.context).desc(), 
              Stop_Word.created_at
              ) \
    .limit(batch_size).offset(offset)       

    if validated_only:
            statement = statement.filter(Stop_Word.validated.is_not(False))

    if filter_days is not None:
        logging.info(f"Getting last {filter_days} days of stop words only for performance execution")
        date_threshold = datetime.utcnow() - timedelta(days=filter_days)
        statement = statement.filter(Stop_Word.created_at >= date_threshold)

    return session.execute(statement).fetchall()

def save_append_stop_word(conn, stop_word_list: list):
    logging.info(f"Saving stop word {stop_word_list} list to the database")
    try:
        if len(stop_word_list) == 0:
            logging.warning("No stop word to save")
        else:
            stop_word_list_df = pd.DataFrame(stop_word_list)
            # remove duplicates from df
            stop_word_list_df = stop_word_list_df.drop_duplicates(subset="context")
            save_to_pg(
                stop_word_list_df,
                Stop_Word.__tablename__,
                conn
            )
    finally:
        conn.dispose()

def get_repetitive_context_advertising(
    session, top_keywords: pd.DataFrame, days: int, from_date: datetime = None, total_length: int = 80, min_number_of_repetition: int = 20
) -> pd.DataFrame:
    # get unique keywords and one of their channel
    filtered_keywords = top_keywords.drop_duplicates(subset='keyword', keep='first')
    top_unique_keyword_with_channel = filtered_keywords.to_dict(orient='records')
    logging.info(f"Top unique {len(top_unique_keyword_with_channel)} keywords:\n {top_unique_keyword_with_channel}")

    result = []
    for keyword_channel in top_unique_keyword_with_channel:
                            logging.info(f"Keyword: {keyword_channel["keyword"]}")

                            advertising_context = get_all_repetitive_context_advertising_for_a_keyword(
                                     session, keyword_channel["keyword"], channel_title=keyword_channel["channel_title"], \
                                     duration=days,from_date=from_date,\
                                     total_length=total_length,min_number_of_repetition=min_number_of_repetition
                            )
                            
                            if(len(advertising_context) > 0):
                                logging.info(f"Advertising context for {keyword_channel["keyword"]}:\n {advertising_context}")
                                result.extend(advertising_context)
                            else:
                                logging.info(f"Advertising context empty for {keyword_channel["keyword"]}")
    logging.info(f"{len(result)} stop words found")
    return result
    
def get_all_repetitive_context_advertising_for_a_keyword(
    session, keyword: str, channel_title: str, duration: int = 7, from_date: datetime = None, min_number_of_repetition: int = 15\
    ,min_length_context:int = 20
    ,after_context:int = 140
    ,before_context:int = 35
    ,total_length: int = 80
) -> pd.DataFrame:
    
    escaped_keyword = keyword.replace("'", "''")
    min_length_context += len(keyword)# security nets to not add short generic context
    after_context += len(keyword) # should include the keyword length

    if from_date is None:
        start_date = get_last_X_days(duration)
        end_date = get_now()
    else:
        start_date = get_last_X_days(duration, from_date)
        end_date = from_date
    try:
        logging.info(f"Getting context for keyword {keyword} for last {duration} days from {end_date}")
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date_sql = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        sql_query = f"""
            SELECT SUBSTRING("context_keyword",0,{total_length}) AS "context",
                   MAX("keyword_id") AS "keyword_id", -- enable to check directly on mediatree service
                   COUNT(*) AS "count"
            FROM (
                SELECT
                "public"."keywords"."number_of_keywords",
                "public"."keywords"."channel_title" AS "channel_title",
                "public"."keywords"."start" AS "start_default_time",
                "public"."keywords"."theme" AS "theme",
                "public"."keywords"."id" AS "keyword_id",
                TRIM(
                    REGEXP_REPLACE(
                        SUBSTRING(
                            REPLACE("public"."keywords"."plaintext",'{MEDIATREE_TRANSCRIPTION_PROBLEM}',''), -- mediatree transcription pollution
                            GREATEST(POSITION('{escaped_keyword}' IN REPLACE("public"."keywords"."plaintext",'{MEDIATREE_TRANSCRIPTION_PROBLEM}','')) - {before_context}, 1), -- start position
                            LEAST({after_context}, LENGTH( REPLACE("public"."keywords"."plaintext",'<unk> ',''))) -- length of the context
                        )
                    ,'^\w{{1,2}}\s+|\s+\w{{1,2}}\s*$', '', 'g') -- removes 1-2 letter words at boundaries
                ) AS "context_keyword",
                "public"."keywords"."keywords_with_timestamp" AS "keywords_with_timestamp",
                "public"."keywords"."number_of_keywords" AS "number_of_keywords",
                "public"."keywords"."channel_program" AS "channel_program",
                "public"."keywords"."plaintext" AS "plaintext"
                FROM
                "public"."keywords"
                WHERE "public"."keywords"."start" >= timestamp with time zone {start_date}
                AND "public"."keywords"."start" < timestamp with time zone {end_date_sql}
                AND "public"."keywords"."number_of_keywords" > 0
                AND jsonb_pretty("keywords_with_timestamp"::jsonb) LIKE CONCAT('%"keyword": "', '{escaped_keyword}', '",%')
                ORDER BY "public"."keywords"."number_of_keywords" DESC
            ) tmp
            WHERE LENGTH(SUBSTRING("context_keyword",0,80)) > {min_length_context} -- safety net to not add small generic context
            GROUP BY 1
            HAVING COUNT(*) >= {min_number_of_repetition}
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
            row["context"] = row["context"].strip()
            row["id"] = get_consistent_hash(row["context"]) # to avoid adding duplicates
            row["keyword"] = keyword
            row["channel_title"] = channel_title
            row["start_date"] = end_date
        
        logging.debug(f"result: {result}")
        return result
    except Exception as err:
            logging.error("get_top_keywords_by_channel crash (%s) %s" % (type(err).__name__, err))
    finally:
        session.close()

def get_top_keywords_by_channel(session, duration: int = 7, top: int = 5, from_date : datetime = None\
                                ,min_number_of_keywords:int = 10) -> pd.DataFrame:
    start_date = get_last_X_days(duration)
    if from_date is None:
        start_date = get_last_X_days(duration)
        end_date = get_now()
    else:
        start_date = get_last_X_days(duration, from_date)
        end_date = from_date

    try:
        start_date = get_date_sql_query(start_date) # "'2020-12-12 00:00:00.000 +01:00'"
        end_date = get_date_sql_query(end_date) #"'2024-12-19 00:00:00.000 +01:00'"
        logging.info(f"Getting top {top} keywords by channel for the last {duration} days from start_date {start_date} to end_date {end_date}")

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


async def manage_stop_word(exit_event = None, conn = None, duration: int = 7, from_date = None, min_number_of_repetition: int = 20 \
                           ,stop_word_context_total_length = 80):
    try:
        if from_date is not None:
            from_date = datetime.fromtimestamp(int(from_date))
        session = get_db_session(conn)
        # get 5 top keywords for each channel for the last 7 days
        top_keywords = get_top_keywords_by_channel(session, duration=duration, top=5, from_date=from_date)
        logging.info(f"Top keywords: {top_keywords}")
        
        stop_word_list = get_repetitive_context_advertising(session, top_keywords, days=duration,\
                                                            from_date=from_date, total_length=stop_word_context_total_length,\
                                                            min_number_of_repetition=min_number_of_repetition)

        # TODO is it useful when comparing a whole text compared to a small context ?
        # already_saved_stop_words = get_all_stop_word(session)

        # stop_word_list["similarity"] = [stop_word for stop_word in stop_word_list if not is_already_known_stop_word(already_saved_stop_words, stop_word["context"])]
        
        # # remove all rows with already_known = True
        # # stop_word_list = stop_word_list[stop_word_list["already_known"] == False]

        # save the stop word list to the database
        save_append_stop_word(conn, stop_word_list=stop_word_list)
        if exit_event is not None:
            exit_event.set()
    except Exception as err:
        logging.fatal("manage_stop_word (%s) %s" % (type(err).__name__, err))
        ray.shutdown()
        sys.exit(1)


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

            stop_word_context_total_length = int(os.environ.get("CONTEXT_TOTAL_LENGTH", 80))
            logging.info(f"Ad context total length (CONTEXT_TOTAL_LENGTH): {stop_word_context_total_length}")

            min_number_of_repetition = int(os.environ.get("MIN_REPETITION", 15))
            logging.info(f"Number of minimum repetition of a stop word (MIN_REPETITION): {min_number_of_repetition}")

            from_date = os.environ.get("START_DATE", None)
            logging.info(f"Start date (START_DATE): {from_date}")
            # Start batch job
            asyncio.create_task(manage_stop_word(exit_event=event_finish, conn=conn, duration=number_of_previous_days,\
                                                  from_date=from_date, \
                                                  min_number_of_repetition=min_number_of_repetition, \
                                                  stop_word_context_total_length = stop_word_context_total_length))

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