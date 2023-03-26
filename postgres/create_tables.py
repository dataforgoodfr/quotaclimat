import psycopg2


def config_tables():
    """List of table schemas files"""
    return ["postgres/schemas/sitemap.pgsql"]


def create_tables():
    """Create tables in the PostgreSQL database"""

    try:
        # establishing the connection
        conn = psycopg2.connect(
            database="quotaclimat",
            user="root",
            password="quotaclimat",
            host="212.47.253.253",
            port="49155",
        )
        cursor = conn.cursor()
        for schema_table in config_tables():
            cursor.execute(open(schema_table, "r").read())
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def delete_table(table):
    conn = psycopg2.connect(
        database="quotaclimat",
        user="root",
        password="quotaclimat",
        host="212.47.253.253",
        port="49155",
    )
    cursor = conn.cursor()

    query = "DROP TABLE %s;" % table

    cursor.execute(query)


if __name__ == "__main__":
    create_tables()
    # delete_table('sitemap_table')
