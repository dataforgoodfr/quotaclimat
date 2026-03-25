from sqlalchemy import MetaData
from sqlalchemy.orm import declarative_base

metadata = MetaData(schema="advertising")

AdvertisingBase = declarative_base(metadata=metadata)
