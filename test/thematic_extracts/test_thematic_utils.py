import re
from datetime import date
from pathlib import Path

import duckdb
import pytest

import quotaclimat.data_processing.thematic_extracts.thematic_utils as utils


# ───────────────────────── COUNTRIES ─────────────────────────
def test_france_channels_without_d8():
    """Verify that France scans its 18 channels, without d8 (C8)."""
    channels = utils.COUNTRIES["france"]["channels"]
    assert "d8" not in channels
    assert len(channels) == 18
    assert {"tf1", "franceinfotv", "france-info", "sud-radio"} <= set(channels)


# ───────────────────────── make_destination_table_name ─────────────────────────
@pytest.mark.parametrize("media_type, expected", [
    ("audiovisuel", "inondations_keywords_fr"),
    ("presse", "inondations_articles_fr"),
])
def test_make_destination_table_name_format(media_type, expected):
    """Verify the {thematic}_{keywords|articles}_{country} format."""
    assert utils.make_destination_table_name("inondations", media_type, "fr") == expected


def test_make_destination_table_name_raises_on_unknown_media_type():
    """Verify that a media_type outside MEDIA_TYPE_SHORT raises instead of going into the name."""
    with pytest.raises(ValueError):
        utils.make_destination_table_name("inondations", "tv", "fr")


def test_make_destination_table_name_raises_over_63_bytes():
    """Verify that a name of exactly 63 bytes is accepted and 64 bytes raises, since
    Postgres would silently truncate it."""
    suffix = "_articles_befl"
    ok = "a" * (63 - len(suffix))
    assert len(utils.make_destination_table_name(ok, "presse", "befl")) == 63
    with pytest.raises(ValueError):
        utils.make_destination_table_name(ok + "a", "presse", "befl")


@pytest.mark.parametrize("thematic", ["Inondations", "inondations-2025", "écologie", "a; DROP TABLE x"])
def test_make_destination_table_name_raises_on_invalid_characters(thematic):
    """Verify that anything outside [a-z0-9_] raises (uppercase would be folded by Postgres,
    accents take 2 bytes, other characters would need quoting in the SQL)."""
    with pytest.raises(ValueError):
        utils.make_destination_table_name(thematic, "presse", "fr")


@pytest.mark.parametrize("media_type", list(utils.MEDIA_TYPE_SHORT))
@pytest.mark.parametrize("country", ["fr", "be", "befl"])
def test_make_destination_table_name_matches_clear_pattern(media_type, country):
    """Verify that every generated name is accepted by clear_destination_table's pattern,
    so that a MODE=reset never refuses a table created by the scripts."""
    name = utils.make_destination_table_name("inondations", media_type, country)
    assert re.fullmatch(utils.DESTINATION_TABLE_PATTERN, name)


# ───────────────────────── get_periods ─────────────────────────
def test_get_periods_returns_dates(tmp_path):
    """Verify that the periods of the country are returned with start and end as dates."""
    p = tmp_path / "t.yml"
    p.write_text(
        "incendies:\n"
        "  periodes:\n"
        "    france:\n"
        "      - id: ete_2026\n"
        "        start: 2026-07-01\n"
        "        end: 2026-07-10\n"
    )
    assert utils.get_periods(p, "incendies", "france") == [
        {"id": "ete_2026", "start": date(2026, 7, 1), "end": date(2026, 7, 10)},
    ]


def test_get_periods_on_project_dict():
    """Verify that the periods of the project thematic_jobs_config.yml are valid."""
    yml = Path(utils.__file__).parent / "thematic_jobs_config.yml"
    assert utils.get_periods(yml, "incendies", "france")
    assert utils.get_periods(yml, "incendies", "belgiumfr")


# ───────────────────────── normalization ─────────────────────────
@pytest.mark.parametrize("text", [
    "L'Incendie de Forêt",
    "Kunming-Montréal",
    "Œuvre l´eau Ça",
    "l’été",
    "x`y′z＇ʼ‛ʹ‘",
])
def test_normalize_text_and_sql_give_the_same_result(text):
    """Verify that the python normalization (keywords) and the duckdb one (texts) give the
    same string: if they differ, a press keyword no longer matches the text, silently."""
    sql = duckdb.connect().execute(f"SELECT {utils.normalize_sql('?')}", [text]).fetchone()[0]
    assert sql == utils.normalize_text(text)
