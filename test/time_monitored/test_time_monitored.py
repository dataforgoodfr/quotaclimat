import logging
import pytest
import pandas as pd

from postgres.schemas.models import get_db_session, connect_to_db, create_tables
from quotaclimat.data_processing.mediatree.time_monitored.models import *
import zoneinfo

@pytest.fixture(scope="module", autouse=True)
def init_tables(): 
    create_tables()

def test_save_time_monitored():
    start = datetime(2025, 1, 14, 15, 18, 43, 807525, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))
    channel_name = "test_channel"
    country = "france"
    id = get_consistent_hash(f"{channel_name}_{start}_{country}")
    duration_minutes = 30

    time_monitored = Time_Monitored(
        id=id,
        channel_name=channel_name,
        start=start,
        duration_minutes=duration_minutes,
        country=country
    )
    save_time_monitored(number_of_rows=int(duration_minutes/2), day=start, channel=channel_name, country=country)
    
    output = get_time_monitored(id)
    assert output.duration_minutes == time_monitored.duration_minutes