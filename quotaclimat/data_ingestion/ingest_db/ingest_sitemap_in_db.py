import logging
from argparse import ArgumentParser
import sys,time
import os
from postgres.insert_data import insert_data_in_sitemap_table
from postgres.insert_existing_data_example import \
    transformation_from_dumps_to_table_entry
from postgres.schemas.models import create_tables, connect_to_db, get_last_month_sitemap_id
from quotaclimat.utils.healthcheck_config import run_health_check_server
from quotaclimat.utils.logger import CustomFormatter
import sentry_sdk
from sentry_sdk.crons import monitor
from quotaclimat.utils.sentry import sentry_init
import asyncio
from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform, get_sitemap_list



async def batch_sitemap(exit_event):
    create_tables()
    
    conn = connect_to_db()
    sitemap_list = get_sitemap_list().items() 
    logging.info("Going to parse %s" % (sitemap_list))
    df_from_pg = get_last_month_sitemap_id(conn)
    for media, sitemap_conf in sitemap_list:
        try:
            df = await query_one_sitemap_and_transform(media, sitemap_conf, df_from_pg)
            df_to_insert = transformation_from_dumps_to_table_entry(df)
            await asyncio.to_thread(insert_data_in_sitemap_table(df_to_insert, conn))
        except TypeError as err:
            logging.debug("Asyncio error %s" % (err))
            continue
        except Exception as err:
            logging.error("Could not ingest data in db for media %s:(%s) %s" % (media,type(err).__name__, err))
            continue

    logging.info("finished")
    conn.dispose()
    exit_event.set()
    return

async def main():
    with monitor(monitor_slug='sitemap'): #https://docs.sentry.io/platforms/python/crons/
        event_finish = asyncio.Event()
        # Start the health check server in the background
        health_check_task = asyncio.create_task(run_health_check_server())

        # Start batch job
        asyncio.create_task(batch_sitemap(event_finish))

        # Wait for both tasks to complete
        await event_finish.wait()

        res=health_check_task.cancel()
    logging.info("Exiting with success")
    sys.exit(0)

if __name__ == "__main__":
    # create logger with 'spam_application'
    logger = logging.getLogger()
    logger.setLevel(level=os.getenv('LOGLEVEL', 'INFO').upper())
    sentry_init()
    # create console handler with a higher log level
    if (logger.hasHandlers()):
        logger.handlers.clear()
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)

    asyncio.run(main())
    sys.exit(0)
