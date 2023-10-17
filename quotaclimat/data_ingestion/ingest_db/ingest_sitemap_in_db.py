import logging
import os
from argparse import ArgumentParser
import sys

from postgres.insert_data import insert_data_in_sitemap_table
from postgres.insert_existing_data_example import \
    transformation_from_dumps_to_table_entry
from postgres.schemas.models import create_tables


from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform, get_sitemap_list

# parser = ArgumentParser()
# parser.add_argument("-p", "--dbpwd")
# args = parser.parse_args()

def run():
    logging.info("start")
    create_tables()
    sitemap_list = get_sitemap_list().items()   
    for media, sitemap_conf in sitemap_list:
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            df_to_insert = transformation_from_dumps_to_table_entry(df)
            insert_data_in_sitemap_table(df_to_insert)
            logging.info("finished")
            sys.exit(0)
        except Exception as err:
            logging.error("Could not ingest data in db for media %s: %s" % (media, err))
            sys.exit(1)
            continue


if __name__ == "__main__":
    run()
