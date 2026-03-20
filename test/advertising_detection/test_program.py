from datetime import datetime, timedelta
from unittest.mock import patch

from quotaclimat.data_ingestion.advertising_detection.tools.program import (
    Show,
    extend_program_by,
    get_channel_program,
    merge_shows_in_program,
)

# Helpers
SORW = Show.start_of_reference_week()  # datetime(1900, 1, 1, 0, 0)
EORW = Show.end_of_reference_week()  # SORW + 7 days


def make_show(channel, day, start_hm, end_hm):
    """Create a Show with weekday `day` (1-based, like program JSON) and HH:MM times."""
    start = datetime.strptime(f"{day} {start_hm}", "%d %H:%M")
    end = datetime.strptime(f"{day} {end_hm}", "%d %H:%M")
    return Show(channel_name=channel, start=start, end=end)


# ---------------------------------------------------------------------------
# Show.start_of_reference_week / end_of_reference_week
# ---------------------------------------------------------------------------


class TestWeekBoundaries:
    def test_start_of_reference_week_is_epoch(self):
        assert Show.start_of_reference_week() == datetime(1900, 1, 1, 0, 0)

    def test_end_of_reference_week_is_seven_days_later(self):
        assert (
            Show.end_of_reference_week()
            == Show.start_of_reference_week() + timedelta(days=7)
        )


# ---------------------------------------------------------------------------
# Show.for_week
# ---------------------------------------------------------------------------


class TestForWeek:
    def test_for_week_offsets_start_and_end(self):
        show = make_show("tf1", 2, "20:00", "21:30")
        week_start = datetime(2024, 1, 1)  # arbitrary Monday
        abs_start, abs_end = show.for_week(week_start)

        assert abs_start == datetime(2024, 1, 2, 20, 0)
        assert abs_end == datetime(2024, 1, 2, 21, 30)

    def test_for_week_same_offset_different_weeks(self):
        show = make_show("tf1", 2, "08:00", "09:00")
        week_a = datetime(2024, 1, 1)
        week_b = datetime(2024, 1, 8)
        start_a, end_a = show.for_week(week_a)
        start_b, end_b = show.for_week(week_b)
        assert start_b - start_a == timedelta(weeks=1)
        assert end_b - end_a == timedelta(weeks=1)


# ---------------------------------------------------------------------------
# merge_shows_in_program
# ---------------------------------------------------------------------------


class TestMergeShowsInProgram:
    def test_empty_list(self):
        assert merge_shows_in_program([]) == []

    def test_single_show_unchanged(self):
        show = make_show("tf1", 1, "20:00", "21:00")
        result = merge_shows_in_program([show])
        assert result == [show]

    def test_non_overlapping_shows_not_merged(self):
        a = make_show("tf1", 1, "20:00", "21:00")
        b = make_show("tf1", 1, "22:00", "23:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 2

    def test_adjacent_shows_merged(self):
        a = make_show("tf1", 1, "20:00", "21:00")
        b = make_show("tf1", 1, "21:00", "22:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 1
        assert result[0].start == a.start
        assert result[0].end == b.end

    def test_overlapping_shows_merged(self):
        a = make_show("tf1", 1, "20:00", "21:30")
        b = make_show("tf1", 1, "21:00", "22:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 1
        assert result[0].start == a.start
        assert result[0].end == b.end

    def test_different_channels_not_merged(self):
        a = make_show("tf1", 1, "20:00", "21:00")
        b = make_show("france2", 1, "21:00", "22:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 2

    def test_different_weekdays_not_merged(self):
        a = make_show("tf1", 1, "20:00", "21:00")
        b = make_show("tf1", 2, "21:00", "22:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 2

    def test_shows_sorted_before_merge(self):
        # Feed shows out of order; they should still be merged
        a = make_show("tf1", 1, "21:00", "22:00")
        b = make_show("tf1", 1, "20:00", "21:00")
        result = merge_shows_in_program([a, b])
        assert len(result) == 1
        assert result[0].start == b.start
        assert result[0].end == a.end

    def test_three_consecutive_shows_merged_into_one(self):
        a = make_show("tf1", 1, "08:00", "09:00")
        b = make_show("tf1", 1, "09:00", "10:00")
        c = make_show("tf1", 1, "10:00", "11:00")
        result = merge_shows_in_program([a, b, c])
        assert len(result) == 1
        assert result[0].start == a.start
        assert result[0].end == c.end

    def test_shows_contained_within_another_merged(self):
        a = make_show("tf1", 1, "20:00", "22:00")
        b = make_show("tf1", 1, "20:30", "21:30")
        result = merge_shows_in_program([a, b])
        assert len(result) == 1
        assert result[0].start == a.start
        assert result[0].end == a.end


# ---------------------------------------------------------------------------
# extend_program_by
# ---------------------------------------------------------------------------


class TestExtendProgramBy:
    def test_empty_program(self):
        assert extend_program_by([], timedelta(minutes=30)) == []

    def test_normal_extension_no_boundary(self):
        show = make_show("tf1", 2, "12:00", "13:00")
        result = extend_program_by([show], timedelta(minutes=30))
        assert len(result) == 1
        assert result[0].start == show.start - timedelta(minutes=30)
        assert result[0].end == show.end + timedelta(minutes=30)

    def test_extension_clamps_at_start_of_reference_week(self):
        # Show starts at the very beginning of the week; extending backward wraps to end-of-week
        show = make_show("tf1", 1, "00:10", "01:00")
        result = extend_program_by([show], timedelta(minutes=30))
        # One piece truncated to SORW, plus a wrap-around piece near EORW
        starts = [s.start for s in result]
        assert SORW in starts  # truncated piece starts at week boundary
        assert any(
            s.start > EORW - timedelta(hours=1) for s in result
        )  # wrap-around piece

    def test_extension_clamps_at_end_of_reference_week(self):
        # Show ends near the end of the week; extending forward wraps to start-of-week
        show = make_show("tf1", 7, "23:00", "23:50")
        result = extend_program_by([show], timedelta(minutes=30))
        ends = [s.end for s in result]
        assert EORW in ends  # truncated piece ends at week boundary
        assert any(
            s.end < timedelta(hours=1) + SORW for s in result
        )  # wrap-around piece

    def test_zero_delta_unchanged(self):
        show = make_show("tf1", 3, "10:00", "11:00")
        result = extend_program_by([show], timedelta(0))
        assert len(result) == 1
        assert result[0].start == show.start
        assert result[0].end == show.end

    def test_overlapping_extensions_are_merged(self):
        # Two shows that, after extension, overlap — should be merged into one
        a = make_show("tf1", 1, "20:00", "21:00")
        b = make_show("tf1", 1, "21:10", "22:00")
        result = extend_program_by([a, b], timedelta(minutes=10))
        assert len(result) == 1

    def test_long_overlapping_merges_everything(self):
        shows = [
            make_show("tf1", 1, "12:00", "12:10"),
            make_show("tf1", 2, "12:00", "12:10"),
            make_show("tf1", 3, "12:00", "12:10"),
            make_show("tf1", 4, "12:00", "12:10"),
            make_show("tf1", 5, "12:00", "12:10"),
            make_show("tf1", 6, "12:00", "12:10"),
            make_show("tf1", 7, "12:00", "12:10"),
        ]
        result = extend_program_by(shows, timedelta(hours=12))
        assert len(result) == 1
        assert result[0].start == SORW
        assert result[0].end == EORW


# ---------------------------------------------------------------------------
# get_channel_program
# ---------------------------------------------------------------------------


class TestGetChannelProgram:
    def _make_shows(self):
        return iter(
            [
                make_show("tf1", 1, "20:00", "21:00"),
                make_show("france2", 1, "20:00", "21:00"),
                make_show("tf1", 2, "08:00", "09:00"),
            ]
        )

    def test_filters_by_channel(self):
        with patch(
            "quotaclimat.data_ingestion.advertising_detection.tools.program.get_program",
            return_value=self._make_shows(),
        ):
            result = get_channel_program("tf1")
        assert all(s.channel_name == "tf1" for s in result)
        assert len(result) == 2

    def test_unknown_channel_returns_empty(self):
        with patch(
            "quotaclimat.data_ingestion.advertising_detection.tools.program.get_program",
            return_value=self._make_shows(),
        ):
            result = get_channel_program("bfmtv")
        assert result == []
