import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
import logging

def connect_to_db():
    DB_DATABASE = os.environ.get('POSTGRES_DB')
    DB_USER = os.environ.get('POSTGRES_USER')
    DB_HOST = os.environ.get('POSTGRES_HOST')
    DB_PORT = os.environ.get('POSTGRES_PORT')
    DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD')

    logging.debug("Connect to the host %s for DB %s" % (DB_HOST, DB_DATABASE))

    url = URL.create(
        drivername="postgresql",
        username=DB_USER,
        host=DB_HOST,
        database=DB_DATABASE,
        port=DB_PORT,
        password=DB_PASSWORD
    )


    engine = create_engine(url)
    Session = sessionmaker(bind=engine)
    session = Session()

    return engine