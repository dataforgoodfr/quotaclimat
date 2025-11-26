import json
import logging
import os

from sqlalchemy import URL, Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

logging.basicConfig(level=logging.INFO)


def _json_serializer(obj):
    """Custom JSON serializer that preserves Unicode characters."""
    return json.dumps(obj, ensure_ascii=False)


def connect_to_db(
    database: str = os.environ.get("POSTGRES_DB", "barometre"),
    user: str = os.environ.get("POSTGRES_USER", "user"),
    host: str = os.environ.get("POSTGRES_HOST", "localhost"),
    port: int = os.environ.get("POSTGRES_PORT", 5432),
    password: str = os.environ.get("POSTGRES_PASSWORD", "password"),
    use_custom_json_serializer: bool = False,
):
    """
    Connect to the PostgreSQL database using environment variables or provided parameters.

    Parameters:
    - database (str, optional): The name of the database. Defaults to 'barometre'.
    - user (str, optional): The username for accessing the database. Defaults to 'user'.
    - localhost (str, optional): The hostname of the database server. Defaults to 'localhost'.
    - port (int, optional): The port number on which the database server is listening. Defaults to 5432.
    - password (str, optional): The password for accessing the database. Defaults to 'password'.

    Returns:
    - Engine: The SQLAlchemy engine object representing the connection to the database.
    """

    logging.info("Connect to the host %s for DB %s" % (host, database))

    url = URL.create(
        drivername="postgresql",
        username=user,
        host=host,
        database=database,
        port=port,
        password=password,
    )

    # Create engine, optionally with a custom JSON serializer that preserves Unicode characters
    if use_custom_json_serializer:
        engine = create_engine(url, json_serializer=_json_serializer)
    else:
        engine = create_engine(url)

    return engine


def get_db_session(engine: Engine = None) -> Session:
    """
    Create a session for interacting with the database using the provided engine.

    Parameters:
    - engine (Engine, optional): The SQLAlchemy engine object. If not provided, it calls `connect_to_db()` to obtain one.

    Returns:
    - Session: A SQLAlchemy session bound to the provided engine or created by calling `connect_to_db()`.
    """
    if engine is None:
        engine = connect_to_db()

    Session = sessionmaker(bind=engine)
    return Session()
