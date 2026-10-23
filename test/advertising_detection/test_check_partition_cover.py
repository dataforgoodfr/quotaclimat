from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from quotaclimat.data_ingestion.advertising.s01_detection.e01_check_partition_cover import (
    _format_days,
    _parts_between,
    compute_cover,
)
from quotaclimat.data_ingestion.advertising.s01_detection.tools.program import Show
from quotaclimat.data_ingestion.advertising.tools.segments import Segment

paris = ZoneInfo("Europe/Paris")
WEEK = "2025-10-13"  # A Monday
WEEK_START = datetime(2025, 10, 13, tzinfo=paris)


def make_show(name, day, start_hm, end_hm, grid_start=None, grid_end=None):
    return Show(
        channel_name="tf1",
        start=datetime.strptime(f"{day} {start_hm}", "%d %H:%M"),
        end=datetime.strptime(f"{day} {end_hm}", "%d %H:%M"),
        program_name=name,
        grid_start=grid_start,
        grid_end=grid_end,
    )


def bucket(skip=lambda paris_dt: False):
    # Every 2-minutes file of the week (and around it), except the skipped ones
    segments = []
    current = (WEEK_START - timedelta(days=1)).astimezone(timezone.utc)
    while current < WEEK_START + timedelta(days=8):
        if not skip(current.astimezone(paris)):
            segments.append(
                Segment(current, current + timedelta(minutes=2), channel="tf1")
            )
        current += timedelta(minutes=2)
    return segments


def cover(shows, segments):
    with patch(
        "quotaclimat.data_ingestion.advertising.s01_detection.tools.program.get_program",
        return_value=iter(shows),
    ):
        return compute_cover(segments=segments, start_date=WEEK, channel="tf1")


JT_EVERY_DAY = [make_show("JT 13h", day, "13:00", "13:40") for day in range(1, 8)]


class TestPartsBetween:
    def test_bounds_are_rounded_up_to_even_minutes(self):
        parts = _parts_between(
            datetime(2025, 10, 13, 12, 45, tzinfo=paris),
            datetime(2025, 10, 13, 12, 51, tzinfo=paris),
        )
        assert [p[0].astimezone(paris).strftime("%H:%M") for p in parts] == [
            "12:46",
            "12:48",
            "12:50",
        ]
        assert parts[-1][1].astimezone(paris).strftime("%H:%M") == "12:52"

    def test_empty_interval(self):
        moment = datetime(2025, 10, 13, 12, 45, tzinfo=paris)
        assert _parts_between(moment, moment) == []


class TestFormatDays:
    @pytest.mark.parametrize(
        "days,expected",
        [
            (set(range(7)), "every day"),
            ({0, 1, 2, 3, 4}, "mon-fri"),
            ({0, 2, 3}, "mon, wed-thu"),
            ({6}, "sun"),
        ],
    )
    def test_format(self, days, expected):
        assert _format_days(days) == expected


class TestComputeCover:
    def test_complete_bucket(self):
        report = cover(JT_EVERY_DAY, bucket())
        assert report.missing_parts == []
        assert all(show.status == "complete" for show in report.shows)
        assert "All expected files received" in report.summary()

    def test_same_slot_missing_every_day_is_written_once(self):
        report = cover(
            JT_EVERY_DAY, bucket(skip=lambda dt: dt.hour == 13 and dt.minute < 10)
        )
        summary = report.summary()
        assert report.count_shows("incomplete") == 7
        assert (
            "INCOMPLETE  JT 13h 13:00-13:40, every day (every airing day): "
            "10 min of the show missing" in summary
        )
        assert "every day              13:00-13:10" in summary

    def test_absent_show_and_missing_margins(self):
        shows = [
            make_show("JT 13h", 1, "13:00", "13:40"),
            make_show("JT 20h", 2, "20:00", "20:40"),
        ]
        segments = bucket(
            skip=lambda dt: (dt.weekday() == 0 and dt.hour == 13)
            or (dt.weekday() == 1 and dt.hour == 19 and dt.minute >= 50)
        )
        report = cover(shows, segments)
        statuses = {show.show.program_name: show.status for show in report.shows}
        assert statuses == {"JT 13h": "absent", "JT 20h": "margins"}
        summary = report.summary()
        assert "ABSENT      JT 13h 13:00-13:40, mon: show entirely missing" in summary
        assert (
            "MARGINS     JT 20h 20:00-20:40, tue: show complete, only margins missing (10 min)"
            in summary
        )

    def test_files_outside_the_program_are_unexpected(self):
        report = cover(JT_EVERY_DAY, bucket())
        expected_minutes = 7 * 70  # 12:46 -> 13:56 every day
        assert len(report.expected_parts) * 2 == expected_minutes
        # Only files of the analysed week count as unexpected
        assert len(report.unexpected_parts) * 2 == 7 * 24 * 60 - expected_minutes

    def test_outdated_program_grid_is_ignored(self):
        shows = [
            make_show("Old JT", 1, "13:00", "13:40", grid_end=date(2024, 6, 30)),
            make_show("New JT", 1, "13:00", "13:30", grid_start=date(2024, 7, 1)),
        ]
        report = cover(shows, bucket())
        assert [show.show.program_name for show in report.shows] == ["New JT"]

    def test_start_date_must_be_a_monday(self):
        with pytest.raises(ValueError):
            compute_cover(segments=[], start_date="2025-10-14", channel="tf1")
