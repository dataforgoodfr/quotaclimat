from datetime import date, datetime, timezone

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.tools.rolling_channels import (
    channel_statuses,
    format_summary,
    is_week_finished,
    next_monday_after,
    parse_channels,
    plan_week,
    tz_paris,
    week_available_from,
)


def test_parse_channels():
    assert parse_channels(" tf1, france2,,tf1 ") == ["tf1", "france2"]
    assert parse_channels("") == []
    assert parse_channels(None) == []


@pytest.mark.parametrize(
    "last_occurrence,expected",
    [
        (datetime(2026, 11, 4, 12, 0), date(2026, 11, 9)),  # Wednesday
        (datetime(2026, 11, 8, 22, 30), date(2026, 11, 9)),  # Sunday evening
        # Sunday show crossing midnight: Monday 00:40 in Paris still belongs to its week
        (datetime(2026, 11, 8, 23, 40), date(2026, 11, 9)),
        (datetime(2026, 11, 9, 12, 0), date(2026, 11, 16)),  # Monday noon
    ],
)
def test_next_monday_after(last_occurrence, expected):
    assert next_monday_after(last_occurrence) == expected


def statuses_of(last_occurrences, channels=("tf1", "france2", "arte"), minimal=None):
    return channel_statuses(list(channels), last_occurrences, minimal)


def test_plan_takes_the_oldest_week_and_its_channels():
    statuses = statuses_of(
        {
            "tf1": datetime(2026, 11, 4, 12, 0),
            "france2": datetime(2026, 10, 28, 12, 0),
            "arte": datetime(2026, 10, 30, 12, 0),
        }
    )
    assert plan_week(statuses) == (date(2026, 11, 2), ["france2", "arte"])


def test_plan_runs_every_channel_when_they_are_on_the_same_week():
    statuses = statuses_of(
        {
            channel: datetime(2026, 11, 4, 12, 0)
            for channel in ("tf1", "france2", "arte")
        }
    )
    assert plan_week(statuses) == (date(2026, 11, 9), ["tf1", "france2", "arte"])


def test_channel_without_occurrence_starts_from_minimal_start_date():
    statuses = statuses_of(
        {"tf1": datetime(2026, 11, 4, 12, 0)},
        channels=("tf1", "arte"),
        minimal=date(2025, 4, 28),
    )
    assert plan_week(statuses) == (date(2025, 4, 28), ["arte"])


def test_channel_without_occurrence_nor_minimal_start_date_fails():
    statuses = statuses_of({"tf1": datetime(2026, 11, 4, 12, 0)})
    with pytest.raises(AssertionError, match="france2, arte"):
        plan_week(statuses)


def test_forced_start_date_runs_every_channel():
    statuses = statuses_of({"tf1": datetime(2026, 11, 4, 12, 0)})
    assert plan_week(statuses, date(2026, 6, 1)) == (
        date(2026, 6, 1),
        ["tf1", "france2", "arte"],
    )


def test_is_week_finished_waits_for_the_last_day_delivery():
    week = date(2026, 11, 9)  # Monday
    assert week_available_from(week) == datetime(2026, 11, 17, 6, 0, tzinfo=tz_paris)
    # Sunday evening and the Monday after: Sunday is not delivered yet
    assert not is_week_finished(week, datetime(2026, 11, 15, 23, 0, tzinfo=tz_paris))
    assert not is_week_finished(week, datetime(2026, 11, 16, 12, 0, tzinfo=tz_paris))
    assert not is_week_finished(week, datetime(2026, 11, 17, 5, 59, tzinfo=tz_paris))
    # Tuesday 6 a.m. Paris time
    assert is_week_finished(week, datetime(2026, 11, 17, 6, 0, tzinfo=tz_paris))
    # UTC datetimes are compared on the same instant
    assert is_week_finished(week, datetime(2026, 11, 17, 5, 0, tzinfo=timezone.utc))


def test_summary_lists_every_channel():
    statuses = statuses_of(
        {"tf1": datetime(2026, 11, 4, 12, 0)},
        channels=("tf1", "arte"),
        minimal=date(2025, 4, 28),
    )
    summary = format_summary(statuses, date(2025, 4, 28), ["arte"])
    assert "tf1   last occurrence: 2026-11-04 12:00  next week: 2026-11-09" in summary
    assert "arte  last occurrence: none" in summary
    assert "next week: 2025-04-28  <- to run" in summary
    assert "Week to process: 2025-04-28 for 1 channel(s): arte" in summary
