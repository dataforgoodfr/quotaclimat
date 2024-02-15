import datetime
import pytz
from datetime import datetime, timedelta, time
import logging
from zoneinfo import ZoneInfo
import pandas as pd
import os 

timezone='Europe/Paris'


def get_keyword_time_separation_ms():
    return 15000

def is_time_distance_between_keyword_enough(overlap):
    return overlap >= get_keyword_time_separation_ms()

def get_exact_days_from_week_day_name(
        start_date
        , end_date
        , target_weekdays
        , timestamp_hour_start
        , timestamp_minute_start
        , timestamp_hour_end
        , timestamp_minute_end
    ):
    weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

    timestamps = []

    timestamp_hour_start = f"{timestamp_hour_start:02d}"
    timestamp_minute_start = f"{timestamp_minute_start:02d}"
    timestamp_hour_end = f"{timestamp_hour_end:02d}"
    timestamp_minute_end = f"{timestamp_minute_end:02d}"
    
    for target_weekday in target_weekdays:
        target_weekday = target_weekday.lower()

        if target_weekday not in weekdays:
            raise ValueError("Invalid weekday. Please provide a valid weekday string.")

        target_weekday_index = weekdays.index(target_weekday)
        current_date = start_date + timedelta(days=(target_weekday_index - start_date.weekday() + 7) % 7)

        while current_date <= end_date:
            start_timestamp = current_date.strftime(f"%Y-%m-%d {timestamp_hour_start}:{timestamp_minute_start}:00")
            end_timestamp = current_date.strftime(f"%Y-%m-%d {timestamp_hour_end}:{timestamp_minute_end}:00")
            timestamps.append([start_timestamp, end_timestamp])
            current_date += timedelta(days=7)

    return timestamps

def get_epoch_from_datetime(date: datetime):
    return int(date.timestamp())

def get_now():
    return datetime.now(ZoneInfo(timezone))

def get_datetime_yesterday():
    midnight_today = datetime.combine(get_now(), time.min)
    return midnight_today - timedelta(days=1)

def get_yesterday():
    yesterday = get_datetime_yesterday()
    yesterday_timestamp = yesterday.timestamp()

    return int(yesterday_timestamp)

def get_start_end_date_env_variable_with_default():
    start_date = os.environ.get("START_DATE")

    if start_date is not None:
        logging.info(f"Using START_DATE env var {start_date}")
        return (int(start_date), get_yesterday())
    else:
        logging.info(f"Getting data from yesterday - you can use START_DATE env variable to provide another starting date")
        return (get_yesterday(), None)

# to query the API every X hour
def get_hour_frequency():
    hour_frequency=20
    return hour_frequency

# Get range of 2 date by week from start to end
def get_date_range(start_date_to_query, end_epoch):
    if end_epoch is not None:
        range = pd.date_range(pd.to_datetime(start_date_to_query, unit='s'), pd.to_datetime(end_epoch, unit='s'), freq=f"{get_hour_frequency()}h") # every X hour

        logging.info(f"Date range: {range} \ {start_date_to_query} until {end_epoch}")
        return range
    else:
        logging.info("Empty range using default from yesterday")
        range = pd.date_range(start=get_datetime_yesterday(), periods=4, freq=f"{get_hour_frequency()}h")
        return range

def is_it_tuesday(date):
    weekday = date.weekday()
    logging.debug(f"weekday : {weekday}")
    return weekday