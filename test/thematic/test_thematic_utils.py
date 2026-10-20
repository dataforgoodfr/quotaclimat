import re
from datetime import date

import pytest

import quotaclimat.data_processing.thematic.thematic_utils as utils


START, END = date(2025, 3, 1), date(2025, 6, 1)


# ───────────────────────── make_destination_table_name ─────────────────────────
@pytest.mark.parametrize("media_type, expected", [
    ("audiovisuel", "inondations_kw_fr_20250301_20250601"),
    ("presse", "inondations_art_fr_20250301_20250601"),
])
def test_make_destination_table_name_format(media_type, expected):
    """Verify the {thematic}_{kw|art}_{country}_{YYYYMMDD}_{YYYYMMDD} format."""
    assert utils.make_destination_table_name("inondations", media_type, "fr", START, END) == expected


def test_make_destination_table_name_differs_within_same_months():
    """Verify that two periods within the same months give two different tables
    (a reset on one must not clear the other)."""
    a = utils.make_destination_table_name("inondations", "presse", "fr", START, END)
    b = utils.make_destination_table_name("inondations", "presse", "fr", date(2025, 3, 15), date(2025, 6, 20))
    assert a != b


def test_make_destination_table_name_raises_on_unknown_media_type():
    """Verify that a media_type outside MEDIA_TYPE_SHORT raises instead of going into the name."""
    with pytest.raises(ValueError):
        utils.make_destination_table_name("inondations", "tv", "fr", START, END)


def test_make_destination_table_name_raises_over_63_bytes():
    """Verify that a name of exactly 63 bytes is accepted and 64 bytes raises, since
    Postgres would silently truncate it."""
    suffix = "_art_befl_20250301_20250601"
    ok = "a" * (63 - len(suffix))
    assert len(utils.make_destination_table_name(ok, "presse", "befl", START, END)) == 63
    with pytest.raises(ValueError):
        utils.make_destination_table_name(ok + "a", "presse", "befl", START, END)


@pytest.mark.parametrize("thematic", ["Inondations", "inondations-2025", "écologie", "a; DROP TABLE x"])
def test_make_destination_table_name_raises_on_invalid_characters(thematic):
    """Verify that anything outside [a-z0-9_] raises (uppercase would be folded by Postgres,
    accents take 2 bytes, other characters would need quoting in the SQL)."""
    with pytest.raises(ValueError):
        utils.make_destination_table_name(thematic, "presse", "fr", START, END)


@pytest.mark.parametrize("media_type", list(utils.MEDIA_TYPE_SHORT))
@pytest.mark.parametrize("country", ["fr", "be", "befl"])
def test_make_destination_table_name_matches_clear_pattern(media_type, country):
    """Verify that every generated name is accepted by clear_destination_table's pattern,
    so that a MODE=reset never refuses a table created by the scripts."""
    name = utils.make_destination_table_name("inondations", media_type, country, START, END)
    assert re.fullmatch(utils.DESTINATION_TABLE_PATTERN, name)
