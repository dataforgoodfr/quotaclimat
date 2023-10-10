import logging
import pandas as pd
from sqlalchemy import DateTime
from postgres.database_connection import connect_to_db
from postgres.schemas.models import get_schema_sitemap
import time

def add_primary_key(publication_name: str,  news_title: str, date: DateTime):
    try:
        return publication_name + "-" + news_title + "-" + time.strftime('%Y%m%d:%H%M:%S')
    except (Exception) as error:
        logging.warning(error)
        return None
    return 

def clean_data(df: pd.DataFrame):
    return df[df["id"] != None]

def insert_data_in_sitemap_table(df: pd.DataFrame):
    logging.info("Saving %s elements with schema :\n %s", df.size, df.dtypes)
    schema = get_schema_sitemap()

    df['id'] = add_primary_key(df['publication_name'], df['news_title'], df['news_publication_date'])
    
    df = clean_data(df)

    conn = connect_to_db()
    try:
        result = df.to_sql("sitemap_table", index=False, schema=schema,con=conn, if_exists='append', chunksize=1000)
        logging.info("Saved %s elements", result)
    except Exception as err:
            logging.error("Could not save : \n %s \n %s" % (df.head(1).to_string(), err))