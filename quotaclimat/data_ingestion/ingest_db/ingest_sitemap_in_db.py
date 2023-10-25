import logging
from argparse import ArgumentParser
import sys

from postgres.insert_data import insert_data_in_sitemap_table
from postgres.insert_existing_data_example import \
    transformation_from_dumps_to_table_entry
from postgres.schemas.models import create_tables, connect_to_db
from quotaclimat.utils.healthcheck_config import run_health_check_server

import asyncio

from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform, get_sitemap_list


async def batch_sitemap(exit_event):
    create_tables()
    
    conn = connect_to_db()
    sitemap_list = get_sitemap_list().items()   
    logging.info("Going to parse %s" % (sitemap_list))
    for media, sitemap_conf in sitemap_list:
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
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
    asyncio.run(main())
    sys.exit(0)
