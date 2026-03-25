import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
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
        self._token = None

    @property
    def token(self):
        if self._token is None:
            self._token = self._get_auth_token()
        return self._token

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

    async def _get_single_export_url(
        self,
        client: httpx.AsyncClient,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
    ):
        response = await client.get(
            f"{MEDIATREE_API_URL}/export/single/",
            params={
                "token": self.token,
                "channel": channel,
                "cts_in": int(from_date.timestamp()),
                "cts_out": int(to_date.timestamp()),
                "media_format": media_format,
            },
        )
        if response.status_code != 200:
            raise Exception(
                f"Unexpected response status code: {response.status_code} ({response.url})\nResponse text: {response.text}"
            )

        json_response = response.json()
        if "src" not in json_response:
            raise Exception(
                f"Unexpected response format ({response.url}): {json_response}",
            )

        return json_response["src"]

    async def download_export(
        self,
        file_name,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
    ):
        # Opening a new client for each download. It would be better to open and close a single client for whole execution.
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=120.0)
        ) as client:
            single_export_url = await self._get_single_export_url(
                client, channel, from_date, to_date, media_format
            )

            response = await client.get(single_export_url)

            os.makedirs(os.path.dirname(file_name), exist_ok=True)
            with open(file_name, "wb") as f:
                f.write(response.content)

    async def get_src_url(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
    ):
        # Opening a new client for each call. It would be better to open and close a single client for whole execution.
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=120.0)
        ) as client:
            url = await self._get_single_export_url(
                client, channel, from_date, to_date, media_format
            )

            return url

    async def get_api_subtitle(
        self,
        channel,
        start_gte,
        start_lte,
    ):
        # Opening a new client for each call. It would be better to open and close a single client for whole execution.
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=120.0)
        ) as client:
            response = await client.get(
                f"{MEDIATREE_API_URL}/v2/subtitle/",
                params={
                    "token": self.token,
                    "channel": channel,
                    "start_gte": int(start_gte.timestamp()),
                    "start_lte": int(start_lte.timestamp()),
                },
            )
            if response.status_code != 200:
                raise Exception(
                    f"Unexpected response status code: {response.status_code} ({response.url})\nResponse text: {response.text}"
                )

            return response.json()


class CachedMediatreeAPI:
    def __init__(self, export_folder="./.cache/mediatree", prefix=""):
        self.api = MediatreeAPI()
        self.export_folder = export_folder
        self.prefix = prefix

    def _file_name(
        self, channel: str, from_date: datetime, to_date: datetime, media_format: str
    ):
        from_date_utc = from_date.astimezone(tz=ZoneInfo("UTC"))
        to_date_utc = to_date.astimezone(tz=ZoneInfo("UTC"))
        return f"{self.prefix}{channel}_{from_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}Z_{to_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}Z.{media_format}"

    async def download_export(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
        file_path: str | Path | None = None,
    ):
        if file_path is None:
            file_name = self._file_name(channel, from_date, to_date, media_format)

            file_path = os.path.join(self.export_folder, file_name)

        if not os.path.isfile(file_path):
            # print(f"Downloading export for {channel} from {from_date} to {to_date}...")
            await self.api.download_export(
                file_path, channel, from_date, to_date, media_format
            )

        return file_path

    async def generate_src_url(
        self, channel: str, from_date: datetime, to_date: datetime, media_format: str
    ):
        file_name = self._file_name(
            channel, from_date, to_date, media_format + "-source"
        )
        file_path = os.path.join(self.export_folder, file_name)

        if os.path.isfile(file_path):
            with open(file_path, "r") as f:
                return f.read().strip()

        url = await self.api.get_src_url(channel, from_date, to_date, media_format)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(url)

        return url

    async def get_subtitle(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
    ):
        raw_subtitle = await self.api.get_api_subtitle(
            channel,
            from_date
            - timedelta(
                minutes=2
            ),  # subtitle are computed every 2 minutes, we need the batch the starts in the last 2 minutes before start
            to_date + timedelta(minutes=2),  # same after end date
        )

        output = []
        for parts in raw_subtitle["data"]:
            for srt in parts["srt"]:
                timestamp = srt["cts_in_ms"] / 1000
                if from_date.timestamp() <= timestamp <= to_date.timestamp():
                    output.append(srt["text"])

        return " ".join(output)


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


if __name__ == "__main__":
    import asyncio

    # Example usage
    api = CachedMediatreeAPI()

    # These are the assets used by the tests
    chunk1 = (
        "tf1_1",
        "tf1",
        datetime(2025, 5, 5, 9, 19, tzinfo=ZoneInfo("Europe/Paris")),
        datetime(2025, 5, 5, 9, 21, tzinfo=ZoneInfo("Europe/Paris")),
    )
    chunk2 = (
        "tf1_2",
        "tf1",
        datetime(2025, 5, 5, 13, 47, tzinfo=ZoneInfo("Europe/Paris")),
        datetime(2025, 5, 5, 13, 49, tzinfo=ZoneInfo("Europe/Paris")),
    )

    path = (
        Path(__file__).parent
        / ".."
        / ".."
        / ".."
        / ".."
        / ".."
        / "test"
        / "advertising_detection"
        / "assets"
    )

    for name, channel, from_date, to_date in [chunk1, chunk2]:
        file_url = asyncio.run(
            api.download_export(
                channel, from_date, to_date, "mp3", path / f"{name}.mp3"
            )
        )
        file_url = asyncio.run(
            api.download_export(
                channel, from_date, to_date, "mp4", path / f"{name}.mp4"
            )
        )
