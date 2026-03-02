import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

from quotaclimat.data_processing.mediatree.config import (
    get_auth_url,
    get_password,
    get_user,
)

load_dotenv()

PASSWORD = get_password()
AUTH_URL = get_auth_url()
USER = get_user()
MEDIATREE_API_URL = os.environ.get("MEDIATREE_API_URL")


class MediatreeAPI:
    def __init__(self):
        self.token = self._get_auth_token()

    def _get_auth_token(self):
        post_arguments = {
            "grant_type": "password",
            "username": USER,
            "password": PASSWORD,
        }
        assert AUTH_URL is not None, "AUTH_URL is not set"
        response = requests.post(AUTH_URL, data=post_arguments)
        output = response.json()
        token = output["data"]["access_token"]
        return token

    def get_single_export_url(
        self, channel: str, from_date: datetime, to_date: datetime, media_format: str
    ):
        response = requests.get(
            f"{MEDIATREE_API_URL}/export/single",
            params={
                "token": self.token,
                "channel": channel,
                "cts_in": int(from_date.timestamp()),
                "cts_out": int(to_date.timestamp()),
                "media_format": media_format,
            },
        )
        return response.json()["src"]

    def download_export(
        self, file_name, channel: str, from_date: datetime, to_date: datetime
    ):
        single_export_url = self.get_single_export_url(
            channel, from_date, to_date, "mp3"
        )

        response = requests.get(single_export_url)

        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, "wb") as f:
            f.write(response.content)


class CachedMediatreeAPI:
    def __init__(self, export_folder="../exports", prefix=""):
        self.api = MediatreeAPI()
        self.export_folder = export_folder
        self.prefix = prefix

    def _file_name(self, channel: str, from_date: datetime, to_date: datetime):
        from_date_utc = from_date.astimezone(tz=ZoneInfo("UTC"))
        to_date_utc = to_date.astimezone(tz=ZoneInfo("UTC"))
        return f"{self.prefix}{channel}_{from_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}_{to_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}.mp3"

    def download_export(self, channel: str, from_date: datetime, to_date: datetime, file_name: str | None = None):
        if file_name is None:
            file_name = self._file_name(channel, from_date, to_date)

        file_path = os.path.join(self.export_folder, file_name)

        if not os.path.isfile(file_path):
            print(f"Downloading export for {channel} from {from_date} to {to_date}...")
            self.api.download_export(file_path, channel, from_date, to_date)

        return file_path

    def export_channel_whole_week(self, channel: str, week_start_date: datetime):
        for start_date, end_date in all_intervals_between(
            week_start_date, week_start_date + timedelta(days=7), timedelta(hours=1)
        ):
            self.download_export(channel, start_date, end_date)


def all_intervals_between(
    start_date: datetime, end_date: datetime, interval: timedelta
):
    intervals = []
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + interval, end_date)
        intervals.append((current_start, current_end))
        current_start = current_end
    return intervals
