import logging
from argparse import ArgumentParser

from postgres.insert_existing_data_example import (
    insert_data_in_sitemap_table, transformation_from_dumps_to_table_entry)
from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG)
from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform

parser = ArgumentParser()
parser.add_argument("-p", "--dbpwd")
args = parser.parse_args()
DB_PWD = args.dbpwd


def run():
    for media, sitemap_conf in SITEMAP_CONFIG.items():
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            df_to_insert = transformation_from_dumps_to_table_entry(df)
            insert_data_in_sitemap_table(df_to_insert, DB_PWD)
        except Exception as err:
            logging.error("Could not ingest data in db for %s: %s" % (media, err))
            continue


if __name__ == "__main__":
    run()
