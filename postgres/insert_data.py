import logging
import pandas as pd
from sqlalchemy import DateTime
from postgres.database_connection import connect_to_db
from postgres.schemas.models import sitemap_table
import time
from sqlalchemy.dialects.postgresql import insert

def add_primary_key(df):
    try:
        return df['publication_name'] + df['news_title'] + df['news_publication_date'].dt.strftime('%Y-%m-%d %X')
    except (Exception) as error:
        logging.warning(error)
        return "empty" #  TODO improve - should be a df value

def clean_data(df: pd.DataFrame):
    return df.query("id != 'empty'") #  TODO improve
    
#do not save when primary key already exist - ignore duplicate key
# from https://stackoverflow.com/a/69421596/3535853
def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
        data = [dict(zip(keys, row)) for row in data_iter]
        logging.info("data_iter %s",data)
        insert_statement = insert(table.table).values(data)
        
        on_duplicate_key_stmt = insert_statement.on_conflict_do_update(
            constraint=f"{table.table.name}_pkey",
            set_={c.key: c for c in insert_statement.excluded},
        )
        #insert_statement.on_conflict_do_update(index_elements=['id'])
        logging.info("insert_statement %s", on_duplicate_key_stmt)
        return conn.execute(on_duplicate_key_stmt)


def insert_data_in_sitemap_table(df: pd.DataFrame):
    logging.info("Received %s elements", df.size)

    #primary key for the DB to avoid duplicate data
    df['id'] = add_primary_key(df)
    
    df = clean_data(df)

    conn = connect_to_db()
    try:
        logging.debug("Schema before saving\n%s", df.dtypes)
        df.to_sql(sitemap_table,
            index=False,
            con=conn,
            if_exists='append',
            chunksize=1000,
            method=insert_or_do_nothing_on_conflict # pandas does not handle conflict natively
        )
    except Exception as err:
            logging.error("Could not save : \n %s \n %s" % (err, df.head(1).to_string()))