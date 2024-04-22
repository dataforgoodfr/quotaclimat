import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker


def connect_to_db():
    DB_DATABASE = os.environ.get("POSTGRES_DB", "barometre")
    DB_USER = os.environ.get("POSTGRES_USER", "user")
    DB_HOST = os.environ.get("POSTGRES_HOST", "localhost")
    DB_PORT = os.environ.get("POSTGRES_PORT", 5432)
    DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "password")

    logging.info("Connect to the host %s for DB %s" % (DB_HOST, DB_DATABASE))

    url = URL.create(
        drivername="postgresql",
        username=DB_USER,
        host=DB_HOST,
        database=DB_DATABASE,
        port=DB_PORT,
        password=DB_PASSWORD,
    )

    engine = create_engine(url)

    return engine


def get_db_session(engine = None):
    if engine is None:
        engine = connect_to_db()
    Session = sessionmaker(bind=engine)
    return Session()
