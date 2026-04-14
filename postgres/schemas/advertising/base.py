from sqlalchemy import DDL, MetaData, event
from sqlalchemy.orm import declarative_base

metadata = MetaData(schema="advertising")

AdvertisingBase = declarative_base(metadata=metadata)

# Mainly useful for tests
event.listen(
    AdvertisingBase.metadata,
    "before_create",
    DDL(
        "CREATE SCHEMA IF NOT EXISTS %(schema)s",
        {"schema": AdvertisingBase.metadata.schema},
    ),
)
