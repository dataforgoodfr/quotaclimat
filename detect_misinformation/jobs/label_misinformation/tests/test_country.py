import os
import sys

import pytest

sys.path.append(os.path.abspath("/app"))

from app.country import (
    ALL_COUNTRIES,
    BELGIUM_COUNTRY,
    BELGIUM_FLANDERS_COUNTRY,
    BRAZIL_COUNTRY,
    EXTENDED_FRANCE_COUNTRY,
    FRANCE_COUNTRY,
    GERMANY_COUNTRY,
    POLAND_COUNTRY,
    SPAIN_COUNTRY,
    LEGACY_COUNTRIES,
    PROD_COUNTRIES,
    Country,
    CountryCollection,
    get_countries,
    get_country_or_collection_from_code,
    get_country_or_collection_from_name,
)


def test_empty_country():
    with pytest.raises(TypeError):
        Country()


def test_empty_country_collection():
    assert CountryCollection() == ALL_COUNTRIES


def test_country_collection_presets():
    assert ALL_COUNTRIES == CountryCollection(
        name="all", code="all", countries=[BELGIUM_COUNTRY, BELGIUM_FLANDERS_COUNTRY, BRAZIL_COUNTRY, FRANCE_COUNTRY, GERMANY_COUNTRY, POLAND_COUNTRY, SPAIN_COUNTRY]
    )
    assert LEGACY_COUNTRIES == CountryCollection(
        name="legacy",
        code="legacy",
        language="french",
        countries=[BELGIUM_COUNTRY, FRANCE_COUNTRY],
    )
    assert PROD_COUNTRIES == CountryCollection(
        name="prod",
        code="prod",
        language="all",
        countries=[FRANCE_COUNTRY],
    )


def test_get_country_or_collection_from_name():
    for entity in (
        FRANCE_COUNTRY,
        BELGIUM_COUNTRY,
        BRAZIL_COUNTRY,
        GERMANY_COUNTRY,
        POLAND_COUNTRY,
        SPAIN_COUNTRY,
        PROD_COUNTRIES,
        LEGACY_COUNTRIES,
        ALL_COUNTRIES,
    ):
        assert get_country_or_collection_from_name(entity.name) == entity


def test_no_whisper_channels():
    country = FRANCE_COUNTRY
    assert "fr3-idf" in country.channels_whisper


def test_extended_france_country_excluded_from_all_countries():
    # EXTENDED_FRANCE_COUNTRY must only ever be run explicitly (COUNTRY=ext-fra),
    # never swept in by COUNTRY=all/prod/legacy.
    assert EXTENDED_FRANCE_COUNTRY not in ALL_COUNTRIES.countries
    assert EXTENDED_FRANCE_COUNTRY not in LEGACY_COUNTRIES.countries
    assert EXTENDED_FRANCE_COUNTRY not in PROD_COUNTRIES.countries


def test_extended_france_country_saves_as_france():
    # name (not code) drives what gets saved/queried downstream (S3 path,
    # Keywords.country filter, target DB country column), so it must stay "france".
    assert EXTENDED_FRANCE_COUNTRY.name == "france"
    assert EXTENDED_FRANCE_COUNTRY.code == "ext-fra"


def test_get_country_or_collection_from_code():
    for entity in (
        FRANCE_COUNTRY,
        EXTENDED_FRANCE_COUNTRY,
        BELGIUM_COUNTRY,
        BELGIUM_FLANDERS_COUNTRY,
        BRAZIL_COUNTRY,
        GERMANY_COUNTRY,
        POLAND_COUNTRY,
        SPAIN_COUNTRY,
        PROD_COUNTRIES,
        LEGACY_COUNTRIES,
        ALL_COUNTRIES,
    ):
        assert get_country_or_collection_from_code(entity.code) == entity


def test_get_countries_disambiguates_name_clash_by_code():
    # COUNTRY=france must resolve to the base perimeter only, never the extended one,
    # even though both entries share name="france".
    assert get_countries("france").countries == [FRANCE_COUNTRY]
    assert get_countries("ext-fra").countries == [EXTENDED_FRANCE_COUNTRY]


def test_get_countries_all_prod_legacy_exclude_extended_france():
    for identifier in ("all", "prod", "legacy"):
        assert EXTENDED_FRANCE_COUNTRY not in get_countries(identifier).countries
