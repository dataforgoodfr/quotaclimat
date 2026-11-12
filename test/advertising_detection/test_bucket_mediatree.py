import asyncio
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from quotaclimat.data_ingestion.advertising.tools.mediatree import bucket_mediatree
from quotaclimat.data_ingestion.advertising.tools.mediatree.bucket_mediatree import (
    _get_s3_archive_keys_for_part,
    list_available_part_starts,
    list_channels,
    list_days_parts,
)

BUCKET = f"/{bucket_mediatree.BUCKET_NAME}/output"
DAY = date(2026, 10, 5)


def archive(mediatree_channel, day, hour):
    folder = f"{BUCKET}/{mediatree_channel}/{day.strftime('%Y/%m/%d')}"
    start = f"{day.isoformat()}T{hour:02d}-00-00Z"
    end = f"{day.isoformat()}T{hour:02d}-02-00Z"
    return f"{folder}/{mediatree_channel}_{start}_{end}.tar"


class FakeS3:
    """Only the listing part of s3fs used by bucket_mediatree."""

    def __init__(self, keys):
        self.keys = keys

    async def _ls(self, path, refresh=False):
        path = path.rstrip("/")
        children = {
            path + "/" + key[len(path) + 1 :].split("/")[0]
            for key in self.keys
            if key.startswith(path + "/")
        }
        if not children:
            raise FileNotFoundError(path)
        return sorted(children)

    async def _exists(self, path):
        return path in self.keys


def starts(parts):
    return [start.hour for start, _ in parts]


class TestFallbackFolder:
    def test_fr3_idf_is_read_in_its_own_folder(self):
        fs = FakeS3([archive("fr3-idf", DAY, 6), archive("france3", DAY, 7)])
        parts = asyncio.run(list_days_parts(fs, "fr3-idf", [DAY]))
        assert starts(parts) == [6]

    def test_fr3_idf_day_without_its_folder_is_read_in_france3(self):
        fs = FakeS3([archive("france3", DAY, 7)])
        parts = asyncio.run(list_days_parts(fs, "fr3-idf", [DAY]))
        assert starts(parts) == [7]

    def test_france3_is_not_read_in_fr3_idf(self):
        fs = FakeS3([archive("fr3-idf", DAY, 6)])
        assert asyncio.run(list_days_parts(fs, "france3", [DAY])) == []

    def test_day_without_any_folder(self):
        fs = FakeS3([])
        assert asyncio.run(list_days_parts(fs, "fr3-idf", [DAY])) == []
        assert asyncio.run(list_available_part_starts(fs, "fr3-idf", DAY)) == set()

    def test_available_part_starts_use_the_fallback(self):
        fs = FakeS3([archive("france3", DAY, 7)])
        result = asyncio.run(list_available_part_starts(fs, "fr3-idf", DAY))
        assert result == {datetime(2026, 10, 5, 7, tzinfo=ZoneInfo("UTC"))}

    def test_archive_keys_of_a_part(self):
        start = datetime(2026, 10, 5, 7, tzinfo=ZoneInfo("UTC"))
        keys = _get_s3_archive_keys_for_part(
            "fr3-idf", start, start + timedelta(minutes=2)
        )
        assert keys == [archive("fr3-idf", DAY, 7), archive("france3", DAY, 7)]


class TestChannelNames:
    def test_sud_radio_is_read_in_sudradio(self):
        fs = FakeS3([archive("sudradio", DAY, 6)])
        parts = asyncio.run(list_days_parts(fs, "sud-radio", [DAY]))
        assert starts(parts) == [6]

    @pytest.mark.parametrize(
        "folders,expected",
        [
            (["fr3-idf", "france3"], ["fr3-idf", "france3"]),
            (["sudradio", "tf1"], ["sud-radio", "tf1"]),
        ],
    )
    def test_list_channels_with_our_names(self, folders, expected):
        fs = FakeS3([archive(folder, DAY, 6) for folder in folders])
        assert asyncio.run(list_channels(fs)) == expected
