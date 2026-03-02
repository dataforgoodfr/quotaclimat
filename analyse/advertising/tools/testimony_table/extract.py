import csv
import os
from datetime import datetime
from typing import Literal
from zoneinfo import ZoneInfo

zone_paris = ZoneInfo("Europe/Paris")

EXPORT_FILE = os.path.join(os.path.dirname(__file__), "export.csv")
DATE_FORMAT = "%d/%m/%Y %H:%M:%S"
CHANNELS = Literal["TF1", "M6", "France 2", "France 3", "C8"]


def get_testimony_data(
    channel: CHANNELS, from_date: datetime, to_date: datetime
) -> list[dict]:
    """
    Get testimony data for a given channel and date range.

    Args:
        channel (str): The channel to get testimony data for.
        from_date (datetime): The start date of the date range (inclusive).
        to_date (datetime): The end date of the date range (inclusive).

    Returns:
        list[dict]: A list of dictionaries containing testimony data.
    """
    output = []

    with open(EXPORT_FILE, "r") as file:
        csv_file = csv.reader(file, delimiter=",")
        next(csv_file)  # Skip header

        for row in csv_file:
            channel_name, type, start_time, end_time = row
            start_date = datetime.strptime(start_time, DATE_FORMAT).replace(
                tzinfo=zone_paris
            )
            end_date = datetime.strptime(end_time, DATE_FORMAT).replace(
                tzinfo=zone_paris
            )

            if channel_name == channel and (
                (from_date <= start_date <= to_date)
                or (from_date <= end_date <= to_date)
            ):
                output.append(
                    {
                        "type": type,
                        "start": start_date,
                        "end": end_date,
                    }
                )

    return output


if __name__ == "__main__":
    get_testimony_data(
        channel="TF1",
        from_date=datetime(2025, 3, 4, 11, 47, 0),
        to_date=datetime(2025, 3, 4, 13, 0, 0),
    )
