import pandas as pd
from sqlalchemy.sql import text

def run_query(query, conn):
    return list(conn.execute(text(query)))


# queries TODO migrate
def query_data_coverage(conn):
    dates_min_and_max = run_query(
        "SELECT MIN(download_date), MAX(download_date) from sitemap_table;",
        conn,
    )

    return dates_min_and_max[0][0], dates_min_and_max[0][1]


def query_matching_keywords_articles_titles_between_two_dates(
    conn, keywords, start_date, end_date
):
    query = """ SELECT news_title, news_publication_date, media_type, publication_name, download_date, publication_name FROM sitemap_table
        WHERE ({keywords})
        AND (news_publication_date BETWEEN '{start_date}' and '{end_date}') """.format(
        keywords=" OR ".join("news_title LIKE '%%%s%%' \n" % w for w in keywords),
        start_date=start_date,
        end_date=end_date,
    )
    return pd.DataFrame(
        run_query(query, conn),
        columns=[
            "news_title",
            "news_publication_date",
            "type",
            "media",
            "download_date",
            "publication_name",
        ],
    )


def query_all_articles_titles_between_two_dates(conn, start_date, end_date):
    query = """ SELECT news_title, news_publication_date, media_type, publication_name, download_date, publication_name FROM sitemap_table
        WHERE (news_publication_date BETWEEN '{start_date}' and '{end_date}') """.format(
        start_date=start_date,
        end_date=end_date,
    )
    return pd.DataFrame(
        run_query(query, conn),
        columns=[
            "news_title",
            "news_publication_date",
            "type",
            "media",
            "download_date",
            "publication_name",
        ],
    )


def percentage_article_in_section_list_per_day(conn, keywords_list: list):
    query_over_days = """SELECT download_date,
              ROUND((COUNT(CASE WHEN ({keywords}) THEN 1 ELSE NULL END) * 100.0 / COUNT(*)), 3)/100 AS rounded_percentage
       FROM sitemap_table
       GROUP BY download_date""".format(
        keywords=" OR ".join("section LIKE '%%%s%%' \n" % w for w in keywords_list),
    )

    return pd.DataFrame(
        run_query(query_over_days, conn), columns=["date", "Pourcentage"]
    )
