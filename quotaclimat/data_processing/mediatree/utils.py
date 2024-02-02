import datetime
import pytz
from datetime import datetime, timedelta, time
import logging
from zoneinfo import ZoneInfo
timezone='Europe/Paris'

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

def date_to_epoch(date_string):
    # Define the timezone
    tz = pytz.timezone(timezone)

    # Create a datetime object from the date string
    date = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')

    # Localize the datetime object to the specified timezone
    date = tz.localize(date)

    # Convert the datetime to epoch time in seconds
    epoch_time = int(date.timestamp())
    return epoch_time

def get_yesterday():
    midnight_today = datetime.combine(datetime.now(ZoneInfo(timezone)), time.min)
    yesterday = midnight_today - timedelta(days=1)
    yesterday_timestamp = yesterday.timestamp()
    logging.info(f"From date: {yesterday.strftime('%Y-%m-%d')}")
    return int(yesterday_timestamp)