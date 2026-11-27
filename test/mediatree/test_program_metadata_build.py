from quotaclimat.data_processing.mediatree.i8n.france.channel_program import (
    channels_programs_france,
)
from quotaclimat.data_processing.mediatree.program_metadata import (
    GRID_END_WITHOUT_LIMIT,
    build_program_metadata,
    generate_program_id,
)


def programs_of(channel, extended_perimeter=False):
    return [
        p
        for p in build_program_metadata(extended_perimeter=extended_perimeter)
        if p["channel_name"] == channel
    ]


def test_one_entry_per_weekday_with_duration_and_id():
    jt = [
        p
        for p in programs_of("tf1")
        if p["program_name"] == "JT 20h + météo"
        and p["program_grid_start"] == "2023-04-01"
    ]
    assert sorted(p["weekday"] for p in jt) == [1, 2, 3, 4, 5, 6, 7]
    assert all(p["duration"] == 45 for p in jt)
    assert all(p["channel_title"] == "TF1" for p in jt)
    assert all(
        p["id"]
        == generate_program_id("tf1", p["weekday"], p["program_name"], "2023-04-01")
        for p in jt
    )


def test_ids_of_existing_programs_are_kept():
    # Ids referenced by keywords already in the database must not change
    ids = {p["id"] for p in build_program_metadata(extended_perimeter=False)}
    assert "6fc646caf4ed30c62298b482634a2a537a69467e3abfd9098d397e826a67454f" in ids
    assert "046d5d696c00001f38d7aef02b4fc26cca632486104f2579c834d7265a34edfa" in ids


def test_open_grid_ends_in_2100_without_changing_the_sources():
    build_program_metadata(extended_perimeter=False)
    assert any(p["program_grid_end"] == "" for p in channels_programs_france)
    assert all(
        p["program_grid_end"] for p in build_program_metadata(extended_perimeter=False)
    )
    assert GRID_END_WITHOUT_LIMIT == "2100-01-01"


def test_extended_perimeter_programs():
    assert programs_of("tmc") == []
    assert {p["program_name"] for p in programs_of("tmc", extended_perimeter=True)} == {
        "Quotidien"
    }
