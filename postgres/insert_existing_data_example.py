import pandas as pd
import psycopg2
import psycopg2.extras

from quotaclimat.data_processing.sitemap_processing import load_all


def transformation_from_dumps_to_table_entry(df):
    cols = [
        "publication_name",
        "news_title",
        "download_date",
        "news_publication_date",
        "news_keywords",
        "section",
        "image_caption",
        "media_type",
    ]
    # convert section to str
    df.section = df.section.apply(lambda x: ",".join(map(str, x)))
    df = df[cols]
    return df


def insert_data_in_sitemap_table(df_to_insert: pd.DataFrame):
    table = "sitemap_table"
    if len(df_to_insert) > 0:
        df_columns = list(df_to_insert)
        # create (col1,col2,...)
        columns = ",".join(df_columns)

        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

        # create INSERT INTO table (columns) VALUES('%s',...)
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        conn = psycopg2.connect(
            database="quotaclimat",
            user="root",
            password="quotaclimat",
            host="212.47.253.253",
            port="49155",
        )

        cur = conn.cursor()
        psycopg2.extras.execute_batch(cur, insert_stmt, df_to_insert.values)
        conn.commit()
        cur.close()


def run():

    df = load_all("../../data_public/sitemap_dumps/")
    df_sample = df.head(5)  # for the example
    del df
    df_to_insert = transformation_from_dumps_to_table_entry(df_sample)
    insert_data_in_sitemap_table(df_to_insert)


if __name__ == "__main__":
    run()
