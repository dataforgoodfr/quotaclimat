from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.tools import (
    scheduled_rolling_channels,
)

MODULE = "quotaclimat.data_ingestion.advertising.s01_detection.tools.scheduled_rolling_channels"


def channel_at(cron, channels, now, monkeypatch, timezone=None):
    monkeypatch.setenv("SCHEDULED_ROLLING_CHANNELS", channels)
    monkeypatch.setenv("SCHEDULED_ROLLING_CHANNELS_CRON", cron)
    if timezone is None:
        monkeypatch.delenv("SCHEDULED_ROLLING_CHANNELS_TIMEZONE", raising=False)
    else:
        monkeypatch.setenv("SCHEDULED_ROLLING_CHANNELS_TIMEZONE", timezone)
    with patch(f"{MODULE}.datetime") as mocked:
        mocked.now.return_value = now
        channel = scheduled_rolling_channels.get_scheduled_rolling_channel()
        if mocked.now.called:
            channel_at.tz = mocked.now.call_args.kwargs["tz"]
        return channel


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 11, 10, 1, 0), "tf1"),  # Tuesday
        (datetime(2026, 11, 14, 1, 31), "france2"),  # Saturday
        (datetime(2026, 11, 14, 3, 0), None),  # After the last channel
    ],
)
def test_every_day_cron(now, expected, monkeypatch):
    assert channel_at("*/30 1-6 * * *", "tf1,france2", now, monkeypatch) == expected


def test_weekday_cron_only_runs_that_day(monkeypatch):
    cron = "*/30 18-23 * * 1"
    assert (
        channel_at(cron, "tf1,france2", datetime(2026, 11, 9, 18, 0), monkeypatch)
        == "tf1"
    )
    assert (
        channel_at(cron, "tf1,france2", datetime(2026, 11, 10, 18, 0), monkeypatch)
        is None
    )


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 11, 16, 7, 55), "tf1"),
        (datetime(2026, 11, 16, 7, 54), "tf1"),  # Cron started a little early
        (datetime(2026, 11, 16, 8, 56), "france2"),
        (datetime(2026, 11, 16, 9, 55), "m6"),
        (datetime(2026, 11, 16, 7, 30), None),  # Before the first run
        (datetime(2026, 11, 16, 10, 55), None),  # After the last channel
        (datetime(2026, 11, 16, 19, 55), None),  # After the last hour
    ],
)
def test_fixed_minute_cron(now, expected, monkeypatch):
    assert channel_at("55 7-18 * * *", "tf1,france2,m6", now, monkeypatch) == expected


@pytest.mark.parametrize(
    "now,expected",
    [
        (datetime(2026, 11, 16, 8, 15), "tf1"),
        (datetime(2026, 11, 16, 10, 15), "arte"),
        (
            datetime(2026, 11, 16, 11, 15),
            None,
        ),  # After the last channel of the first cron
        (
            datetime(2026, 11, 16, 20, 15),
            "tf1",
        ),  # The second cron starts the list again
        (datetime(2026, 11, 16, 22, 14), "arte"),
    ],
)
def test_several_crons(now, expected, monkeypatch):
    cron = "15 8-12 * * *; 15 20-23 * * *"
    assert channel_at(cron, "tf1,itele,arte", now, monkeypatch) == expected


def test_cron_hours_default_to_paris_time(monkeypatch):
    now = datetime(2026, 11, 16, 10, 15)
    assert channel_at("15 8-19 * * *", "tf1,itele,arte", now, monkeypatch) == "arte"
    assert channel_at.tz == ZoneInfo("Europe/Paris")


def test_cron_timezone(monkeypatch):
    now = datetime(2026, 11, 16, 10, 15)
    assert (
        channel_at("15 8-19 * * *", "tf1,itele,arte", now, monkeypatch, "Etc/UTC")
        == "arte"
    )
    assert channel_at.tz == ZoneInfo("Etc/UTC")


def test_unknown_timezone(monkeypatch):
    now = datetime(2026, 11, 16, 10, 15)
    assert channel_at("15 8-19 * * *", "tf1", now, monkeypatch, "Mars/Olympus") is None


def test_old_format_is_not_read_as_channels(monkeypatch):
    now = datetime(2026, 11, 16, 8, 15)
    assert (
        channel_at("15 8-19 * * *", "15 8-19 * * *:tf1,arte", now, monkeypatch) is None
    )


def test_disabled_cron(monkeypatch):
    now = datetime(2026, 11, 16, 8, 15)
    assert channel_at("-15 8-19 * * *", "tf1", now, monkeypatch) is None
