from datetime import datetime, timedelta, time
import modin.pandas as pd
import logging


def set_date(date: str = "") -> datetime:
    if date == "":
        logging.info(f"Using yesterday's date")
        return get_datetime_minus_x_days(days=1)
    else:
        logging.info(f"Using date {date}")
        clean_date = date.split(" ")[0]  # Keep only "YYYY-MM-DD"
        return datetime.strptime(clean_date, "%Y-%m-%d")


def get_min_hour(date: datetime):
    return datetime.combine(date, time.min)


def get_datetime_minus_x_days(days=1):
    midnight_today = get_min_hour(datetime.now())
    return midnight_today - timedelta(days=days)


# Get range of 2 date
def get_date_range(date: str = "", minus_days: int = 1):
    date = set_date(date)
    logging.info(
        f"Default date minus {minus_days} day(s) - (env var NUMBER_OF_PREVIOUS_DAYS)"
    )
    range = pd.date_range(end=date, periods=minus_days, freq="D")
    return range
