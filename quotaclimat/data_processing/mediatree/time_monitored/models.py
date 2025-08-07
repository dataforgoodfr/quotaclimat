import logging
from datetime import datetime

from sqlalchemy import Column, DateTime, String, Text, Boolean, ARRAY, JSON, Integer, Table, MetaData, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from sqlalchemy import text
from postgres.database_connection import connect_to_db, get_db_session
from postgres.schemas.base import Base
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_processing.mediatree.i8n.country import FRANCE
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
import os
import json
from json import JSONDecodeError


import traceback

# The duration in minutes of media monitoring based on number of chunks of 2 minutes saved in S3
class Time_Monitored(Base):
    __tablename__ = "time_monitored"
    id = Column(Text, primary_key=True)
    channel_name = Column(String, nullable=False)
    start = Column(DateTime(), nullable=False)
    duration_minutes= Column(Integer)
    country = Column(String, nullable=False)

def get_time_monitored(id: str):
    session = get_db_session()
    return session.get(Time_Monitored, id)

# count how many rows are in the dataframe and save it to postgresql inside a new table called time_monitor
def save_time_monitored(number_of_rows : int, day: datetime, channel :str, country : str,session=None):
    """
    Save the number of rows (chunk) to the time_monitor table in PostgreSQL.
    
    Args:
        number_of_rows (int): The number of rows (2 minute chunk) to save.
        day (datetime): The date of the monitoring.
        channel (str): The name of the channel.
        country (str): The country name.
    """
    try:
        duration_minutes = number_of_rows * 2 # 2 minutes per chunk
        logging.info(f"Saving time monitored of {duration_minutes} minutes ({number_of_rows} chunks of 2 minutes) for {day} - {channel} - {country}")
        max_hours = 23
        if duration_minutes / 60 > max_hours:
            logging.error(f"Duration of {duration_minutes / 60} hours is above {max_hours} hours. Please check the data.")
        
        if session is None:
            session = get_db_session()
        
        stmt = insert(Time_Monitored).values(
            id=get_consistent_hash(f"{channel}_{day}_{country}"),
            channel_name=channel,
            start=day,
            duration_minutes=duration_minutes,
            country=country
        )
        # upsert
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],  # Use the 'id' column as the conflict target
            set_={
                'channel_name': stmt.excluded.channel_name,
                'start': stmt.excluded.start,
                'duration_minutes': stmt.excluded.duration_minutes,
                'country': stmt.excluded.country
            }
        )

        # Execute the statement
        session.execute(stmt)
        
        session.commit()
        logging.info("Saved time monitored")
    except SQLAlchemyError as e:
        logging.error(f"Error saving time monitored data: {e}")
        logging.error(traceback.format_exc())
    finally:
        session.close()