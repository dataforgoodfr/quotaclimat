import os
import re
from logging.config import fileConfig

from sqlalchemy import create_engine

from alembic import context
from rrs.schemas.base import RRSBase
from rrs.schemas.models import (  # noqa: F401 — import models so metadata is populated
    Case,
    CaseToCluster,
    Cluster,
    DictionaryEntry,
    Segment,
    Subject,
)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = RRSBase.metadata


def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and reflected and compare_to is None:
        return False
    return True


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url_tokens = {
        "RRS_PG_USER": os.getenv("RRS_PG_USER", "user"),
        "RRS_PG_DATABASE": os.getenv("RRS_PG_DATABASE", "rrs_db"),
        "RRS_PG_PASSWORD": os.getenv("RRS_PG_PASSWORD", "password"),
        "RRS_PG_HOST": os.getenv("RRS_PG_HOST", "localhost"),
        "RRS_PG_PORT": os.getenv("RRS_PG_PORT", "5432"),
    }

    url = config.get_main_option("sqlalchemy.url")
    url = re.sub(r"\${(.+?)}", lambda m: url_tokens[m.group(1)], url)

    connectable = create_engine(url)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
