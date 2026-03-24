from sqlalchemy.orm import MetaData, declarative_base

metadata = MetaData(schema="advertising")

AdvertisingBase = declarative_base(metadata=metadata)
