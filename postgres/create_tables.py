import logging
from postgres.database_connection import connect_to_db
from postgres.schemas.models import Sitemap

def config_tables():
    """List of table schemas files"""
    return ["postgres/schemas/sitemap.pgsql"]

def create_tables():
    """Create tables in the PostgreSQL database"""

    logging.info("create sitemap table")
    try:
        engine = connect_to_db()

        Sitemap.__table__.create(engine, checkfirst=True)
    except (Exception) as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()
   
if __name__ == "__main__":
    create_tables()