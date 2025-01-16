import datetime

from datetime import datetime, timedelta, time
import logging
from zoneinfo import ZoneInfo
import modin.pandas as pd
import os 
from pandas.tseries.offsets import MonthEnd

timezone='Europe/Paris'
EPOCH__5MIN_MARGIN = 300
EPOCH__1MIN_MARGIN = 60 # to add margin for program

def get_keyword_time_separation_ms(duration_seconds: int = 20):
    return duration_seconds * 1000

def get_chunk_duration_api():
    return 2 * 60 * 1000

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

def get_min_hour(date: datetime):
    return datetime.combine(date, time.min)

def get_max_hour(date: datetime):
    return datetime.combine(date, time.max)

def get_datetime_yesterday(days=1):
    midnight_today = get_min_hour(get_now())
    return midnight_today - timedelta(days=days)

def get_yesterday(days=1):
    yesterday = get_datetime_yesterday(days=1)
    yesterday_timestamp = yesterday.timestamp()

    return int(yesterday_timestamp)

    
 # should format date to "'2020-12-12 00:00:00.000 +01:00'"
def get_date_sql_query(date: datetime):
    date = date.strftime('%Y-%m-%d %H:%M:%S.000 +00:00')

    return f"'{date}'" 

def get_end_of_month(start_date: str) -> str:
    date = pd.Timestamp(start_date) + MonthEnd(n=1)
    date = pd.to_datetime(date, format='%Y%m%d')
    return date.strftime('%Y-%m-%d')

def get_first_of_month(start_date: datetime) -> str:
    # Get the first day of the current month
    first_day = start_date.replace(day=1)

    # Format as DD-MM-YYYY
    return first_day.strftime("%Y-%m-%d")

def get_start_end_date_env_variable_with_default(start_date:int, minus_days:int=1):
    if start_date != 0:
        start_date_minus_days = int(int(start_date) - (minus_days * 24 * 60 * 60))
        logging.info(f"Using START_DATE env var {start_date} - to get {minus_days} day(s) before (env var NUMBER_OF_PREVIOUS_DAYS) : {start_date_minus_days}")
        return (int(start_date), start_date_minus_days)
    else:
        logging.info(f"Getting data from yesterday - you can use START_DATE env variable to provide another starting date")
        return (get_yesterday(), None)

# Get range of 2 date by week from start to end
def get_date_range(start_date_to_query, end_epoch, minus_days:int=1):
    if end_epoch is not None:
        logging.info(f"Getting date range from {pd.to_datetime(start_date_to_query, unit='s').normalize()} - {pd.to_datetime(end_epoch, unit='s').normalize()}")
        range = pd.date_range( pd.to_datetime(end_epoch, unit='s').normalize(),
                              pd.to_datetime(start_date_to_query, unit='s').normalize(),
                              freq="D")

        logging.info(f"Date range: {range} \n {start_date_to_query} until {end_epoch}")
        return range
    else:
        logging.info(f"Default date range from yesterday to {minus_days} day(s) - (env var NUMBER_OF_PREVIOUS_DAYS)")
        range = pd.date_range(start=get_datetime_yesterday(), periods=minus_days, freq="D")
        return range

def is_it_tuesday(date):
    weekday = date.weekday()
    logging.debug(f"weekday : {weekday}")
    return weekday

def format_hour_minute(time: str) -> pd.Timestamp:
    date_str = "1970-01-01"
    logging.debug(f"format_hour_minute with : {time}")
    return pd.to_datetime(date_str + " " + time)

def get_timestamp_from_yyyymmdd(time: str) -> pd.Timestamp:
    if(time == ""):
        return (pd.Timestamp.now() + pd.DateOffset(years=100)).tz_localize("Europe/Paris")
    else:
        return pd.Timestamp(pd.to_datetime(time)).tz_localize("Europe/Paris")
    

def get_last_X_days(days, from_date = None) -> datetime:
    if from_date is None:
        end_date = get_now()
    else:
        end_date = from_date

    start_date = end_date - timedelta(days=days)
    logging.debug(f"start_date: {start_date} end_date: {end_date}")
    return start_date