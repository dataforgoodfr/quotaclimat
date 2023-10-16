import logging
import os
from argparse import ArgumentParser

from postgres.insert_data import insert_data_in_sitemap_table
from postgres.insert_existing_data_example import \
    transformation_from_dumps_to_table_entry
from postgres.schemas.models import create_tables
from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG,
                                                      SITEMAP_TEST_CONFIG)
from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform

# parser = ArgumentParser()
# parser.add_argument("-p", "--dbpwd")
# args = parser.parse_args()

def get_sitemap_list():
    if(os.environ.get("ENV") == "dev" or os.environ.get("ENV") == "docker"):
        logging.info("Testing locally")
        return SITEMAP_TEST_CONFIG.items()
    else:
        return SITEMAP_CONFIG.items()

def run():
    create_tables()
    sitemap_list = get_sitemap_list()   
    for media, sitemap_conf in sitemap_list:
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            df_to_insert = transformation_from_dumps_to_table_entry(df)
            insert_data_in_sitemap_table(df_to_insert)

        except Exception as err:
            logging.error("Could not ingest data in db for %s: %s" % (media, err))
            continue


if __name__ == "__main__":
    run()
