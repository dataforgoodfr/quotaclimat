import pandas as pd
import psycopg2
import psycopg2.extras
import os
import logging

from quotaclimat.data_processing.sitemap.sitemap_processing import load_all
from postgres.schemas.models import get_sitemap_cols

#@TODO remove me
DB_DATABASE = os.environ.get('POSTGRES_DB', "quotaclimat")
DB_USER = os.environ.get('POSTGRES_USER', "root")
DB_HOST = os.environ.get('POSTGRES_HOST', "212.47.253.253")
DB_PORT = os.environ.get('POSTGRES_PORT', "49154")


def parse_section(section: str):
    logging.debug(section)
    if("," not in section):
        return section
    else:
        return ",".join(map(str, section))

def transformation_from_dumps_to_table_entry(df: pd.DataFrame):
    cols = get_sitemap_cols()
    df_template_db = pd.DataFrame(columns=cols)
    df_consistent = pd.concat([df, df_template_db])

    df_consistent.section = df_consistent.section.apply(parse_section)

    return df_consistent[cols]

def insert_data_in_sitemap_table(df_to_insert: pd.DataFrame):
    #@TODO use postgres utils
    table = "sitemap_table"

    if len(df_to_insert) > 0:
        df_columns = list(df_to_insert)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)

        #@TODO create a utils to connect to the DB
        conn = psycopg2.connect(
            database=DB_DATABASE,
            user=DB_USER,
            password=os.environ.get('POSTGRES_PASSWORD'),
            host=DB_HOST,
            port=DB_PORT,
        )
        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df_to_insert.values)
        conn.commit()
        cur.close()


def run():

    df_archives = load_all()
    download_date_last = (
        df_archives.groupby("url")["download_date"]
        .apply(lambda x: x.max())
        .rename("download_date_last")
    )
    df_m = df_archives.merge(download_date_last, on=["url"])
    # del df_archives
    df_m.sort_values("download_date", inplace=True)
    df_m.drop_duplicates(["url"], keep="first", inplace=True)
    del df_archives
    # insert by batches
    CHUNK_SIZE = 1000
    for chunk_num in range(len(df_m) // CHUNK_SIZE + 1):
        start_index = chunk_num * CHUNK_SIZE
        end_index = min(chunk_num * CHUNK_SIZE + CHUNK_SIZE, len(df_m))
        df_m_batch = df_m[start_index:end_index]
        df_to_insert = transformation_from_dumps_to_table_entry(df_m_batch)
        db_password = "dummy_password_it_wont_work"
        insert_data_in_sitemap_table(df_to_insert, db_password)


if __name__ == "__main__":
    run()
