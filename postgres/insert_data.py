import logging
import time

import pandas as pd
from sqlalchemy import DateTime
from sqlalchemy.dialects.postgresql import insert

from postgres.schemas.models import sitemap_table

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
    try:
        df_tmp = df.groupby(by="id").size().reset_index(name="count").nlargest(5, "count")
        df_final = df_tmp[df_tmp['count'] > 1]
        if df_final.empty:
            logging.debug("No duplicates detected")
        else:
            logging.warning("Duplicates to remove : %s out of %s" % (len(df_final), len(df)))
    except Exception as err:
            logging.warning("Could show sitemap before saving : \n %s \n %s" % (err, df.head(1).to_string()))


def insert_data_in_sitemap_table(df: pd.DataFrame, conn):
    number_of_rows = len(df)
    if(number_of_rows == 0):
        logging.warning("0 elements to parse")
    else:
        logging.info("Received %s elements", number_of_rows)

    show_sitemaps_dataframe(df)

    df = clean_data(df)
    logging.debug("Saving %s" % (df.head(1).to_string()))
    
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
        return number_of_rows
    except Exception as err:
        logging.error("Could not save : \n %s \n %s" % (err, df.head(1).to_string()))
        return 0