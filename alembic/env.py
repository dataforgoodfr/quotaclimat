from logging.config import fileConfig

from sqlalchemy import create_engine
from postgres.schemas.base import Base
from quotaclimat.data_ingestion.labelstudio.models import TargetBase
from alembic import context

import re
import os

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = [Base.metadata, TargetBase.metadata]

# from https://stackoverflow.com/a/63672522/3535853
# https://alembic.sqlalchemy.org/en/latest/cookbook.html#don-t-generate-any-drop-table-directives-with-autogenerate
def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and reflected and compare_to is None:
        return False
    else:
        return True
    
# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_object=include_object
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url_tokens = {
      "POSTGRES_USER": os.getenv("POSTGRES_USER",""),
      "POSTGRES_DB": os.getenv("POSTGRES_DB",""),
      "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD",""),
      "POSTGRES_HOST": os.getenv("POSTGRES_HOST",""),
      "POSTGRES_PORT": os.getenv("POSTGRES_PORT","")
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
            include_object=include_object
        )

        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
