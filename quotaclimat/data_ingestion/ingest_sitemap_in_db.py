import logging
from argparse import ArgumentParser

from postgres.insert_existing_data_example import insert_data_in_sitemap_table
from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG)
from quotaclimat.data_ingestion.scrap_sitemap import \
    query_one_sitemap_and_transform

parser = ArgumentParser()
parser.add_argument("-p", "--dbpsw")
args = parser.parse_args()
DB_PWD = args.dbpsw


def run():
    for media, sitemap_conf in SITEMAP_CONFIG.items():
        if media != "bfmtv":  # TO REMOVE for dev: quick fail
            continue
        try:
            df = query_one_sitemap_and_transform(media, sitemap_conf)
            insert_data_in_sitemap_table(df, DB_PWD)
        except Exception as err:
            logging.error("Could not ingest data in db for %s: %s" % (media, err))
            continue


if __name__ == "__main__":
    run()
