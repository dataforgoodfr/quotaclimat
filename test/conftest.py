import pytest
from postgres.schemas.models import connect_to_db, get_db_session


@pytest.fixture(scope="session")
def db_conn():
    """Single DB connection reused across the entire test session."""
    conn = connect_to_db()
    yield conn
    conn.dispose()


@pytest.fixture(scope="session")
def db_session(db_conn):
    """Single DB session reused across the entire test session."""
    return get_db_session(db_conn)
