import logging
import os
from argparse import ArgumentParser
import sys

from postgres.insert_data import insert_data_in_sitemap_table
from postgres.insert_existing_data_example import \
    transformation_from_dumps_to_table_entry
from postgres.schemas.models import create_tables, connect_to_db


from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform, get_sitemap_list

# parser = ArgumentParser()
# parser.add_argument("-p", "--dbpwd")
# args = parser.parse_args()

def run():
    logging.info("start")

    create_tables()
    
    conn = connect_to_db()
    sitemap_list = get_sitemap_list().items()   
    logging.info("Going to parse %s" % (sitemap_list))
    for media, sitemap_conf in sitemap_list:
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            df_to_insert = transformation_from_dumps_to_table_entry(df)
            insert_data_in_sitemap_table(df_to_insert, conn)
        except Exception as err:
            logging.error("Could not ingest data in db for media %s: %s" % (media, err))
            continue

    logging.info("finished")
    sys.exit(0)

if __name__ == "__main__":
    run()
