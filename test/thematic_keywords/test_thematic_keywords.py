import pytest
import pandas as pd
from unittest.mock import patch
from pathlib import Path
import sys


PARENT_DIR = Path(__file__).resolve().parent.parent
# Ajoute le dossier parent au chemin de recherche Python
if str(PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(PARENT_DIR))
from thematic_keywords_generate import extract_adjacent_chunks_day
from thematic_keywords_generate import get_keyword_ome_hrfp_theme


FAKE_THEME_KEYWORDS = {
    "adaptation_climatique_solution": [
        {"keyword": "bassin de récupération", "language": "french", "high_risk_of_false_positive": False},
    ],
    "ressources_solutions": [
        {"keyword": "bassin de récupération", "language": "french", "high_risk_of_false_positive": False},
    ],
}

@patch("thematic_keywords_generate.THEME_KEYWORDS", FAKE_THEME_KEYWORDS)
def test_get_keyword_ome_hrfp_theme_drops_keywords_absent_from_dictionary():
    """Un mot absent du dictionnaire OME n'est pas conservé, un mot présent l'est avec ses thèmes et son hrfp."""
    result = get_keyword_ome_hrfp_theme(["bassin de récupération", "blabla"], language="french")

    assert "blabla" not in result
    assert set(result.keys()) == {"bassin de récupération"}
    assert result["bassin de récupération"]["hrfp"] is False
    assert set(result["bassin de récupération"]["theme"]) == {
        "adaptation_climatique_solution",
        "ressources_solutions",
    }


@patch("thematic_keywords_generate.THEME_KEYWORDS", FAKE_THEME_KEYWORDS)
def test_get_keyword_ome_hrfp_theme_raises_if_no_keyword_found():
    """Si aucun mot n'est dans le dictionnaire, la fonction lève une ValueError."""
    with pytest.raises(ValueError):
        get_keyword_ome_hrfp_theme(["blabla"], language="french")



import json
from datetime import date

import duckdb
import pytest

from thematic_keywords_generate import read_barometre_keywords, SOURCE_ALIAS


@pytest.fixture
def con():
    """Connexion DuckDB avec une fausse base source : barometre_source.public.keywords."""
    _con = duckdb.connect()
    _con.execute(f"ATTACH ':memory:' AS {SOURCE_ALIAS}")
    _con.execute(f"CREATE SCHEMA {SOURCE_ALIAS}.public")
    _con.execute(f"""
        CREATE TABLE {SOURCE_ALIAS}.public.keywords (
            id VARCHAR,
            channel_name VARCHAR,
            start TIMESTAMP,
            country VARCHAR,
            keywords_with_timestamp VARCHAR
        )
    """)
    yield _con
    _con.close()


def _insert(con, id_, kwts):
    con.execute(
        f"INSERT INTO {SOURCE_ALIAS}.public.keywords VALUES (?, ?, ?, ?, ?)",
        [id_, "tf1", "2026-03-30 12:00:00", "france", json.dumps(kwts, ensure_ascii=False)],
    )


def test_read_barometre_keywords_keeps_only_searched_keywords_with_direct_themes(con):
    """On garde un chunk si au moins une entrée a un keyword recherché ET un thème ne finissant pas par _indirectes."""
    searched = {"bassin de récupération": {"theme": ["adaptation_climatique_solution"], "hrfp": False}}

    # gardé : keyword recherché, thème direct
    _insert(con, "1", [{"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution"}])
    # exclu : keyword recherché mais thème _indirectes
    _insert(con, "2", [{"keyword": "bassin de récupération", "theme": "ressources_solutions_indirectes"}])
    # exclu : keyword non recherché
    _insert(con, "3", [{"keyword": "blabla", "theme": "adaptation_climatique_solution"}])
    # gardé : une entrée indirecte et une entrée directe pour le keyword recherché
    _insert(con, "4", [
        {"keyword": "bassin de récupération", "theme": "ressources_solutions_indirectes"},
        {"keyword": "bassin de récupération", "theme": "ressources_solutions"},
    ])
    # exclu : thème direct mais sur un keyword non recherché, thème indirect sur le keyword recherché
    _insert(con, "5", [
        {"keyword": "blabla", "theme": "ressources_solutions"},
        {"keyword": "bassin de récupération", "theme": "adaptation_climatique_solution_indirectes"},
    ])

    result = read_barometre_keywords(
        con, searched, start_date=date(2026, 3, 30), end_date=date(2026, 3, 31), country="france"
    )

    assert set(result["id"]) == {"1", "4"}
    assert result["id"].is_unique




@patch("thematic_keywords_generate.channel_titles_france", {"tf1": "TF1", "france2": "France 2"})
def test_extract_adjacent_chunks_day_no_duplicate_this_id():
    """Vérifie l'absence absolue de doublons dans la colonne `this_id`."""
    core_chunks = pd.DataFrame({
        "channel_name": ["tf1"],
        "start": pd.to_datetime(["2026-03-30 12:00:00+00:00"])
    })

    s3_day_df = pd.DataFrame({
        "channel_name": ["tf1", "tf1","tf1","tf1", "france2"],
        "start": pd.to_datetime([
            "2026-03-30 12:00:00+00:00",
            "2026-03-30 12:02:00+00:00",
            "2026-03-30 11:58:00+00:00",
            "2026-03-30 12:03:00+00:00",
            "2026-03-30 12:02:00+00:00"
        ]),
        "plaintext": ["Sous-titre 1", "Sous-titre 2", "Sous-titre 3", "Sous-titre 4", "Sous-titre 5"]
    })

    result = extract_adjacent_chunks_day(core_chunks, s3_day_df, gap="2 minutes 30 seconds")
    # Assertions
    assert not result["this_id"].duplicated().any(), "Des doublons ont été trouvés dans this_id"
    

# Expected: 3 chunks kepts
# number 1, 2 and 3. Number 4 is too far from number 1 and number 5 is on another channel.
@patch("thematic_keywords_generate.channel_titles_france", {"tf1": "TF1", "france2": "France 2"})
def test_extract_adjacent_chunks_day_number_of_chunks_kept():
    """Vérifie l'absence absolue de doublons dans la colonne `this_id`."""
    core_chunks = pd.DataFrame({
        "channel_name": ["tf1"],
        "start": pd.to_datetime(["2026-03-30 12:00:00+00:00"])
    })

    s3_day_df = pd.DataFrame({
        "channel_name": ["tf1", "tf1","tf1","tf1", "france2"],
        "start": pd.to_datetime([
            "2026-03-30 12:00:00+00:00",
            "2026-03-30 12:01:00+00:00",
            "2026-03-30 11:58:00+00:00",
            "2026-03-30 12:03:00+00:00",
            "2026-03-30 12:02:00+00:00"
        ]),
        "plaintext": ["Sous-titre 1", "Sous-titre 2", "Sous-titre 3", "Sous-titre 4", "Sous-titre 5"]
    })

    result = extract_adjacent_chunks_day(core_chunks, s3_day_df, gap="2 minutes 30 seconds")
    assert result.shape[0]==3
    
    