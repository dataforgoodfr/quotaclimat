import pandas as pd
import psycopg2
import psycopg2.extras

from quotaclimat.data_processing.sitemap.sitemap_processing import load_all


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
    df_template_db = pd.DataFrame(columns=cols)
    df_consistent = pd.concat([df, df_template_db])

    # convert section to str
    df_consistent.section = df_consistent.section.apply(lambda x: ",".join(map(str, x)))
    return df_consistent[cols]


def insert_data_in_sitemap_table(df_to_insert: pd.DataFrame, password: str):
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
            password=password,
            host="212.47.253.253",
            port="49155",
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
