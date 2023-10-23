import logging
import time

import pandas as pd
from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import insert
import hashlib

from postgres.database_connection import connect_to_db
from postgres.schemas.models import sitemap_table

def get_consistent_hash(my_pk):
    # Convert the object to a string representation
    obj_str = str(my_pk)
    sha256 = hashlib.sha256()
    # Update the hash object with the object's string representation
    sha256.update(obj_str.encode('utf-8'))
    hash_value = sha256.hexdigest()

    return hash_value

# hash of publication name + title 
def add_primary_key(df):
    try:
        return (
            df["publication_name"]
            + df["news_title"]
            + pd.to_datetime(df["news_publication_date"]).dt.strftime("%Y-%m-%d %X")
        ).apply(get_consistent_hash)
    except (Exception) as error:
        logging.warning(error)
        return hash("empty") #  TODO improve - should be a None ?


def clean_data(df: pd.DataFrame):
    df = df.drop_duplicates(subset="id")
    return df.query("id != 'empty'")  #  TODO improve - should be a None ?


# do not save when primary key already exist - ignore duplicate key
# from https://stackoverflow.com/a/69421596/3535853
def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    logging.debug("data_iter %s", data)
    insert_statement = insert(table.table).values(data)

    on_duplicate_key_stmt = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )

    logging.debug("insert_statement %s", on_duplicate_key_stmt)
    return conn.execute(on_duplicate_key_stmt)


def show_sitemaps_dataframe(df: pd.DataFrame):
    
    df_tmp = df.groupby(by="id").size().reset_index(name="count").nlargest(5, "count")
    df_final = df_tmp[df_tmp['count'] > 1]
    if df_final.empty:
        logging.info("No duplicates detected")
    else:
        logging.warning("Duplicates to remove : %s out of %s" % (len(df_final), len(df)))

def insert_data_in_sitemap_table(df: pd.DataFrame, conn):
    logging.info("Received %s elements", df.size)

    # primary key for the DB to avoid duplicate data
    df["id"] = add_primary_key(df)
    show_sitemaps_dataframe(df)

    df = clean_data(df)
    logging.debug("Could  save%s" % (df.head(1).to_string()))
    
    try:
        logging.debug("Schema before saving\n%s", df.dtypes)
        df.to_sql(
            sitemap_table,
            index=False,
            con=conn,
            if_exists="append",
            chunksize=1000,
            method=insert_or_do_nothing_on_conflict,  # pandas does not handle conflict natively
        )
        logging.info("Saved dataframe to PG")
    except Exception as err:
        logging.error("Could not save : \n %s \n %s" % (err, df.head(1).to_string()))
