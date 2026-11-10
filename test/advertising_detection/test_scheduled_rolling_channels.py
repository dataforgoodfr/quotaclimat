from datetime import datetime
from unittest.mock import patch

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.tools import \
    scheduled_rolling_channels

MODULE = "quotaclimat.data_ingestion.advertising.s01_detection.tools.scheduled_rolling_channels"


def channel_at(src, now, monkeypatch):
    monkeypatch.setenv("SCHEDULED_ROLLING_CHANNELS", src)
    with patch(f"{MODULE}.datetime") as mocked:
        mocked.now.return_value = now
        return scheduled_rolling_channels.get_scheduled_rolling_channel()


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 11, 10, 1, 0), "tf1"),  # Tuesday
        (datetime(2026, 11, 14, 1, 31), "france2"),  # Saturday
        (datetime(2026, 11, 14, 3, 0), None),  # After the last channel
    ],
)
def test_every_day_cron(now, expected, monkeypatch):
    assert channel_at("*/30 1-6 * * *:tf1,france2", now, monkeypatch) == expected


def test_weekday_cron_only_runs_that_day(monkeypatch):
    src = "*/30 18-23 * * 1:tf1,france2"
    assert channel_at(src, datetime(2026, 11, 9, 18, 0), monkeypatch) == "tf1"
    assert channel_at(src, datetime(2026, 11, 10, 18, 0), monkeypatch) is None
