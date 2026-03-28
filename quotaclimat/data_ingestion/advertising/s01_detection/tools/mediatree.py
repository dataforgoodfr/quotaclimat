import asyncio
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv

from quotaclimat.data_processing.mediatree.config import (
    get_auth_url,
    get_password,
    get_user,
)

load_dotenv()

logger = logging.getLogger(__name__)

PASSWORD = get_password()
AUTH_URL = get_auth_url()
USER = get_user()
MEDIATREE_API_URL = os.environ.get("MEDIATREE_API_URL")

DEFAULT_TIMEOUT = httpx.Timeout(120.0, connect=120.0)


class MediatreeAPI:
    """Async Mediatree API client.

    Usage::

        async with MediatreeAPI() as api:
            await api.download_export(...)

    Features:
    - Lazy authentication (token fetched on first request)
    - Shared httpx.AsyncClient for connection pooling
    - Semaphore to limit concurrent requests to the API
    - Automatic retry with exponential backoff
    """

    def __init__(
        self,
        max_concurrent_requests: int = 5,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
    ):
        self._token: str | None = None
        self._client: httpx.AsyncClient | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay

    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def token(self) -> str:
        if self._token is None:
            self._token = self._fetch_auth_token()
        return self._token

    def _fetch_auth_token(self) -> str:
        assert AUTH_URL is not None, "AUTH_URL is not set"
        response = httpx.post(
            AUTH_URL,
            data={
                "grant_type": "password",
                "username": USER,
                "password": PASSWORD,
            },
        )
        response.raise_for_status()
        return response.json()["data"]["access_token"]

    @property
    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "MediatreeAPI must be used as an async context manager: "
                "async with MediatreeAPI() as api: ..."
            )
        return self._client

    async def _request_with_retry(self, method: str, url: str, **kwargs) -> httpx.Response:
        async with self._semaphore:
            last_exc: Exception | None = None
            for attempt in range(1, self._max_retries + 1):
                try:
                    response = await self._http.request(method, url, **kwargs)
                    response.raise_for_status()
                    return response
                except Exception as exc:
                    last_exc = exc
                    if attempt == self._max_retries:
                        raise
                    delay = self._retry_base_delay * (2 ** (attempt - 1))
                    logger.warning(
                        "Request %s %s attempt %d/%d failed, retrying in %.1fs: %s",
                        method,
                        url,
                        attempt,
                        self._max_retries,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
            raise last_exc  # unreachable, but satisfies type checkers

    async def get_single_export_url(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
    ) -> str:
        response = await self._request_with_retry(
            "GET",
            f"{MEDIATREE_API_URL}/export/single/",
            params={
                "token": self.token,
                "channel": channel,
                "cts_in": int(from_date.timestamp()),
                "cts_out": int(to_date.timestamp()),
                "media_format": media_format,
            },
        )
        json_response = response.json()
        if "src" not in json_response:
            raise ValueError(f"Unexpected response format ({response.url}): {json_response}")
        return json_response["src"]

    async def download_export(
        self,
        file_path: str | Path,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
    ) -> None:
        src_url = await self.get_single_export_url(channel, from_date, to_date, media_format)
        response = await self._request_with_retry("GET", src_url)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "wb") as f:
            f.write(response.content)

    async def get_subtitle(
        self,
        channel: str,
        start_gte: datetime,
        start_lte: datetime,
    ) -> dict:
        response = await self._request_with_retry(
            "GET",
            f"{MEDIATREE_API_URL}/v2/subtitle/",
            params={
                "token": self.token,
                "channel": channel,
                "start_gte": int(start_gte.timestamp()),
                "start_lte": int(start_lte.timestamp()),
            },
        )
        return response.json()


class CachedMediatreeAPI:
    """Mediatree API with local file caching.

    Usage::

        async with CachedMediatreeAPI() as api:
            path = await api.download_export("tf1", from_date, to_date, "mp3")
    """

    def __init__(self, export_folder: str = "./.cache/mediatree", prefix: str = "", **api_kwargs):
        self.api = MediatreeAPI(**api_kwargs)
        self.export_folder = export_folder
        self.prefix = prefix

    async def __aenter__(self):
        await self.api.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.api.__aexit__(exc_type, exc_val, exc_tb)

    def _file_name(
        self, channel: str, from_date: datetime, to_date: datetime, media_format: str
    ) -> str:
        from_date_utc = from_date.astimezone(tz=ZoneInfo("UTC"))
        to_date_utc = to_date.astimezone(tz=ZoneInfo("UTC"))
        return (
            f"{self.prefix}{channel}"
            f"_{from_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}Z"
            f"_{to_date_utc.strftime('%Y-%m-%d_%H-%M-%S')}Z"
            f".{media_format}"
        )

    async def download_export(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
        media_format: str,
        file_path: str | Path | None = None,
    ) -> str:
        if file_path is None:
            file_name = self._file_name(channel, from_date, to_date, media_format)
            file_path = os.path.join(self.export_folder, file_name)

        file_path = str(file_path)
        if not os.path.isfile(file_path):
            await self.api.download_export(file_path, channel, from_date, to_date, media_format)

        return file_path

    async def generate_src_url(
        self, channel: str, from_date: datetime, to_date: datetime, media_format: str
    ) -> str:
        file_name = self._file_name(channel, from_date, to_date, media_format + "-source")
        file_path = os.path.join(self.export_folder, file_name)

        if os.path.isfile(file_path):
            with open(file_path, "r") as f:
                return f.read().strip()

        url = await self.api.get_single_export_url(channel, from_date, to_date, media_format)

        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            f.write(url)

        return url

    async def get_subtitle(
        self,
        channel: str,
        from_date: datetime,
        to_date: datetime,
    ) -> str:
        raw_subtitle = await self.api.get_subtitle(
            channel,
            from_date - timedelta(minutes=2),
            to_date + timedelta(minutes=2),
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

    async def main():
        async with CachedMediatreeAPI() as api:
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
                await api.download_export(
                    channel, from_date, to_date, "mp3", path / f"{name}.mp3"
                )
                await api.download_export(
                    channel, from_date, to_date, "mp4", path / f"{name}.mp4"
                )

    asyncio.run(main())
