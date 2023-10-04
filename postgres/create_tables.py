import psycopg2, os, logging
from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-p", "--dbpwd")
args = parser.parse_args()
DB_DATABASE = os.environ.get('POSTGRES_DB', "quotaclimat")
DB_USER = os.environ.get('POSTGRES_USER', "root")
DB_PWD = os.environ.get('POSTGRES_PASSWORD', args.dbpwd)
DB_HOST = os.environ.get('POSTGRES_HOST', "212.47.253.253")
DB_PORT = os.environ.get('POSTGRES_PORT', "49154")

def config_tables():
    """List of table schemas files"""
    return ["postgres/schemas/sitemap.pgsql"]


def create_tables():
    """Create tables in the PostgreSQL database"""

    try:
        # establishing the connection
        conn = psycopg2.connect(
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PWD,
            host=DB_HOST,
            port=DB_PORT,
        )
        cursor = conn.cursor()
        for schema_table in config_tables():
            logging.info("creating '%s' table" % schema_table)
            cursor.execute(open(schema_table, "r").read())
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()


def delete_table(table):
    logging.info("delete tables")
    conn = psycopg2.connect(
        database=DB_DATABASE,
        user=DB_USER,
        password=DB_PWD,
        host=DB_HOST,
        port=DB_PORT,
    )
    cursor = conn.cursor()

    query = "DROP TABLE %s;" % table

    cursor.execute(query)


if __name__ == "__main__":
    create_tables()
    # delete_table('sitemap_table')
