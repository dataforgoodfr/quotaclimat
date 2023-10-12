import logging
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta

import psycopg2

from quotaclimat.data_processing.sitemap.queries import (
    query_all_articles_titles_between_two_dates, query_data_coverage)

parser = ArgumentParser()
parser.add_argument("-p", "--dbpwd")
parser.add_argument("-k", "--keyid")
parser.add_argument("-a", "--accesskey")

args = parser.parse_args()
DB_DATABASE = os.environ.get("POSTGRES_DB", "quotaclimat")
DB_USER = os.environ.get("POSTGRES_USER", "root")
DB_PWD = os.environ.get("POSTGRES_PASSWORD", args.dbpwd)
DB_HOST = os.environ.get("POSTGRES_HOST", "212.47.253.253")
DB_PORT = os.environ.get("POSTGRES_PORT", "49154")
KEY_ID = args.keyid
ACCESS_KEY = args.accesskey

CONN = psycopg2.connect(
    database=DB_DATABASE,
    user=DB_USER,
    password=DB_PWD,
    host=DB_HOST,
    port=DB_PORT,
)


def write_backup_between_dates(date_min: datetime, date_max: datetime):
    """Fetch the data from the db and write a flat file to S3
    Later, to read the data:
    pd.read_parquet('s3://quotaclimat-s3/backups/v1/from_..._to_....parquet', storage_options=dict_ecs)
    Args:
        date_min (datetime): date of the first element to backup
        date_max (datetime): date of the last element to backup
    """
    df_to_backup = query_all_articles_titles_between_two_dates(CONN, date_min, date_max)
    dict_ecs = {
        "anon": False,
        "key": KEY_ID,
        "s3_additional_kwargs": {"ACL": "private"},
        "secret": ACCESS_KEY,
        "use_ssl": False,
        "client_kwargs": {
            "endpoint_url": "https://s3.fr-par.scw.cloud",
            "region_name": "fr-par",
        },
    }
    path_back_up = "s3://quotaclimat-s3/backups/v1/from_%s_to_%s.parquet" % (
        date_min.strftime("%Y%m%d"),
        date_max.strftime("%Y%m%d"),
    )
    df_to_backup.to_parquet(path_back_up, storage_options=dict_ecs)
    logging.info("DB Data was backup to %s" % path_back_up)


def write_init_backup():
    date_min, date_max = query_data_coverage(CONN)
    write_backup_between_dates(date_min, date_max)


def write_past_week_backup():
    today = datetime.today()
    a_week_ago = today - timedelta(days=7)
    write_backup_between_dates(a_week_ago, today)


if __name__ == "__main__":
    write_past_week_backup()
