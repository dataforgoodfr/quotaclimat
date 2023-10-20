import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, String, Text
from sqlalchemy.orm import declarative_base

from postgres.database_connection import connect_to_db, get_db_session

Base = declarative_base()


def get_sitemap_cols():

    cols = [
        "publication_name",
        "news_title",
        "download_date",
        "news_publication_date",
        "news_keywords",
        "section",
        "image_caption",
        "media_type",
        "url"
    ]
    return cols


sitemap_table = "sitemap_table"


class Sitemap(Base):
    __tablename__ = sitemap_table

    id = Column(Text, primary_key=True)
    publication_name = Column(String, nullable=False)
    news_title = Column(Text, nullable=False)
    download_date = Column(DateTime(), default=datetime.now)
    news_publication_date = Column(DateTime(), default=datetime.now)
    news_keywords = Column(Text)
    section = Column(Text)
    image_caption = Column(Text)
    media_type = Column(Text)
    url = Column(Text)
    updated_on = Column(DateTime(), default=datetime.now, onupdate=datetime.now)


def get_sitemap(id: str):
    session = get_db_session()
    return session.get(Sitemap, id)


def create_tables():
    """Create tables in the PostgreSQL database"""

    logging.info("create sitemap table")
    try:
        engine = connect_to_db()

        Base.metadata.create_all(engine, checkfirst=True)
        logging.info("Table creation done, if not already done.")
    except (Exception) as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()

def drop_tables():
    """Drop tables in the PostgreSQL database"""

    logging.warning("drop tables")
    try:
        engine = connect_to_db()

        Base.metadata.drop_all(engine, checkfirst=True)
        logging.info("Table deletion done")
    except (Exception) as error:
        logging.error(error)
    finally:
        if engine is not None:
            engine.dispose()