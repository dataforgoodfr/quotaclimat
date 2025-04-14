### Library imports
import logging
import asyncio
import sys
import os
from quotaclimat.data_processing.mediatree.i8n.country import CountryMediaTree
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import getLogger
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.config import *
from quotaclimat.data_processing.mediatree.update_pg_keywords import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.channel_program import *
from quotaclimat.data_processing.mediatree.stop_word.main import get_all_stop_word
from quotaclimat.data_processing.mediatree.api_import_utils.db import get_last_date_and_number_of_delay_saved_in_keywords, KeywordLastStats, get_delay_date
from quotaclimat.data_processing.mediatree.s3.s3_utils import read_folder_from_s3, transform_raw_keywords
from quotaclimat.data_processing.mediatree.i8n.country import *
from postgres.insert_data import save_to_pg
from postgres.schemas.models import create_tables, connect_to_db, get_db_session
from postgres.schemas.models import keywords_table

from typing import List, Optional
from tenacity import *
import sentry_sdk
from sentry_sdk.crons import monitor
import modin.pandas as pd
import ray
from quotaclimat.utils.sentry import sentry_init
logging.getLogger('modin.logger.default').setLevel(logging.ERROR)
logging.getLogger('distributed.scheduler').setLevel(logging.ERROR)
logging.getLogger('distributed').setLevel(logging.ERROR)
logging.getLogger('worker').setLevel(logging.ERROR)


# reapply word detector logic to all saved keywords
# use when word detection is changed
@monitor(monitor_slug='update')
async def update_pg_data(exit_event):
    try:
        start_date = os.environ.get("START_DATE_UPDATE", None)
        if start_date is None:
            number_of_days_to_update = int(os.environ.get("NUMBER_OF_DAYS", 7))
            tmp_start_date = get_date_now_minus_days(start=get_now(), minus_days=number_of_days_to_update)
            logging.info(f"START_DATE_UPDATE is None, using today minus NUMBER_OF_DAYS : {number_of_days_to_update}")
            start_date = tmp_start_date
            end_date = get_now()
        else:
            logging.info(f"START_DATE_UPDATE is {start_date}")
            tmp_end_date = get_end_of_month(start_date)
            end_date = os.environ.get("END_DATE", tmp_end_date)

        batch_size = int(os.environ.get("BATCH_SIZE", 50000))
        stop_word_keyword_only = os.environ.get("STOP_WORD_KEYWORD_ONLY", "false") == "true"
        if stop_word_keyword_only:
            logging.warning(f"Update - STOP_WORD_KEYWORD_ONLY to true : Only updating rows whose plaintext match top stop words' keyword. It uses to speed up update.")

        biodiversity_only = os.environ.get("BIODIVERSITY_ONLY", "false") == "true"
        if biodiversity_only:
            logging.warning(f"Update - BIODIVERSITY_ONLY to true : Only updating rows whose have at least one number_of_biodiversity_* > 0. It uses to speed up update.")
        
        program_only = os.environ.get("UPDATE_PROGRAM_ONLY", "false") == "true"
        empty_program_only = os.environ.get("UPDATE_PROGRAM_CHANNEL_EMPTY_ONLY", "false") == "true"
        channel = os.environ.get("CHANNEL", "")
        if(program_only):
            logging.warning(f"Update : Program only mode activated - UPDATE_PROGRAM_ONLY with UPDATE_PROGRAM_CHANNEL_EMPTY_ONLY set to {empty_program_only}")
        else:
            logging.warning("Update : programs will not be updated for performance issue - use UPDATE_PROGRAM_ONLY to true for this")

        logging.warning(f"Updating already saved data for channel {channel} from Postgresql from date {start_date} - env variable START_DATE_UPDATE until {end_date} - you can use END_DATE to set it (optional)")
        
        session = get_db_session()
        update_keywords(session, batch_size=batch_size, start_date=start_date, program_only=program_only, end_date=end_date,\
                        channel=channel, empty_program_only=empty_program_only,stop_word_keyword_only=stop_word_keyword_only, biodiversity_only=biodiversity_only)
        exit_event.set()
    except Exception as err:
        logging.fatal("Could not update_pg_data %s:(%s)" % (type(err).__name__, err))
        ray.shutdown()
        sys.exit(1)

def get_stop_words(session, validated_only=True, context_only=True, filter_days: int = None, country=FRANCE) -> list[Stop_Word]:
    logging.info(f"Getting Stop words for {country} with days filter of {filter_days} days...")
    try:
        stop_words = get_all_stop_word(session, validated_only=validated_only, filter_days=filter_days, country=country)
        if(context_only):
            result = list(map(lambda stop: stop.context, stop_words))
        else:
            result = stop_words
        
        result_len = len(result)
        if result_len > 0:
            logging.info(f"Got {len(result)} stop words")
        else:
            logging.error("No stop words from sql tables")

        return result
    except Exception as err:
        logging.error(f"Stop word error {err}")
        raise Exception

def get_start_time_to_query_from(session, country=FRANCE)      :
    normal_delay_in_days = 1
    lastSavedKeywordsDate: KeywordLastStats = get_last_date_and_number_of_delay_saved_in_keywords(session,country=country)
    logging.info(f"last saved date for keywords for {country.name} is {lastSavedKeywordsDate.last_day_saved}, with a delay of  \
                 {lastSavedKeywordsDate.number_of_previous_days_from_yesterday} days compared to yesterday")
    
    start_date = int(os.environ.get("START_DATE", 0))
    default_number_of_previous_days = 1
    if start_date != 0:
        logging.info(f"Using START_DATE/NUMBER_OF_PREVIOUS_DAYS {start_date}")
        number_of_previous_days = int(os.environ.get("NUMBER_OF_PREVIOUS_DAYS", default_number_of_previous_days))
        return start_date, number_of_previous_days

    if(lastSavedKeywordsDate.last_day_saved is None or lastSavedKeywordsDate.number_of_previous_days_from_yesterday == normal_delay_in_days):
        logging.info("No delay (nice!), going with default dates yesterday")
        default_start_date = 0
        default_number_of_previous_days = 1
        return default_start_date, default_number_of_previous_days
    else:
        last_saved_date, default_number_of_previous_days = get_delay_date(lastSavedKeywordsDate, normal_delay_in_days=normal_delay_in_days)
        default_start_date = 0
        return default_start_date, default_number_of_previous_days

async def get_and_save_s3_data_to_pg(exit_event):
    with sentry_sdk.start_transaction(op="task", name="get_and_save_s3_data_to_pg"):
        try:
            logging.warning(f"Available CPUS {os.cpu_count()} - MODIN_CPUS config : {os.environ.get('MODIN_CPUS', 3)}")

            conn = connect_to_db()
            session = get_db_session(conn)
            
            country_code: str = os.environ.get("COUNTRY", FRANCE_CODE)
            logging.info(f"Country used is (default {FRANCE_CODE}) : {country_code}")
            countries: List[CountryMediaTree] = get_countries_array(country_code=country_code)

            for country in countries:
                logging.info(f"Getting info for country : {country}...")

                # TODO : should we only trust data from S3 ?
                df_programs = get_programs(country=country) # memory bumps ? should be lazy instead of being copied on each worker
                channels = country.channels

            
                stop_words = get_stop_words(session, validated_only=True, country=country)
                
                (start_date, number_of_previous_days) = get_start_time_to_query_from(session, country=country)
                (start_date_to_query, end_date) = get_start_end_date_env_variable_with_default(start_date, minus_days=number_of_previous_days)
                day_range = get_date_range(start_date_to_query, end_date, number_of_previous_days)
                logging.info(f"Number of days to query : {len(day_range)} - day_range : {day_range}")
                for day in day_range:
                    for channel in channels:
                        try:
                            logging.info("Querying day %s for channel %s" % (day, channel))
                            df_channel_for_a_day = read_folder_from_s3(date=day, channel=channel, country=country)

                            df = transform_raw_keywords(df=df_channel_for_a_day, stop_words=stop_words,\
                                                         df_programs=df_programs, country=country)

                            if(df is not None):
                                logging.debug(f"Memory df {df.memory_usage()}")
                                save_to_pg(df, keywords_table, conn)
                                del df
                            else:
                                logging.info("Nothing to save to Postgresql")
                        except Exception as err:
                            logging.error(f"continuing loop fpr but met error with {channel} - day {day}: {err}")
                            continue
            exit_event.set()
        except Exception as err:
            logging.fatal("get_and_save_s3_data (%s) %s" % (type(err).__name__, err))
            ray.shutdown()
            sys.exit(1)

async def main():
    with monitor(monitor_slug='mediatree'): #https://docs.sentry.io/platforms/python/crons/
        try:
            logging.info("Start api mediatree import from S3")
            sentry_init()
            create_tables()

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
            if(os.environ.get("UPDATE") == "true"):
                asyncio.create_task(update_pg_data(event_finish))
            else:
                asyncio.create_task(get_and_save_s3_data_to_pg(event_finish))

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

