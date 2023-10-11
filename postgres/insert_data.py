import logging
import pandas as pd
from sqlalchemy import DateTime
from postgres.database_connection import connect_to_db
from postgres.schemas.models import get_schema_sitemap
import time
from sqlalchemy.dialects.postgresql import insert

def add_primary_key(publication_name, news_title, date):
    try:
        return publication_name + news_title + date.dt.strftime('%Y-%m-%d %X')
    except (Exception) as error:
        logging.warning(error)
        return "empty" #  TODO improve - should be a df value

def clean_data(df: pd.DataFrame):
    return df.query("id != 'empty'") #  TODO improve
    
#TODO debug me
def insert_or_do_nothing_on_conflict(table, conn, keys, data_iter):
        insert_stmt = insert(table.table).values(list(data_iter))
        on_duplicate_key_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['id'])
        conn.execute(on_duplicate_key_stmt)

def insert_data_in_sitemap_table(df: pd.DataFrame):
    logging.info("Received %s elements", df.size)

    # logging.info("Saving %s elements (removed NaN values) with schema :\n %s", df.size, df.dtypes)
    schema = get_schema_sitemap()

    df['id'] = add_primary_key(df['publication_name'], df['news_title'], df['news_publication_date'])
    
    df = clean_data(df)

    conn = connect_to_db()
    try:
        logging.info("Schema before saving %s elements", df.dtypes)
        result = df.to_sql("sitemap_table",
         index=False,
         con=conn,
         if_exists='append',
         chunksize=1000,
         method=insert_or_do_nothing_on_conflict #TODO debug me
        )
        logging.info("Saved %s elements", result)
    except Exception as err:
            logging.error("Could not save : \n %s \n %s" % (err, df.head(1).to_string()))