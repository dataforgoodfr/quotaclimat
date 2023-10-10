from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base
from datetime import datetime

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
    ]
    return cols

## Used for Pandas to_sql
def get_schema_sitemap():
    return {"publication_name": "TEXT",
        "news_title": "TEXT",
        "download_date": "TIMESTAMP",
        "news_publication_date": "TIMESTAMP",
        "news_keywords": "TEXT",
        "section": "TEXT",
        "image_caption": "TEXT",
        "media_type": "TEXT"
    }

class Sitemap(Base):
    __tablename__ = 'sitemap_table'

    id = Column(String, primary_key=True)
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
    