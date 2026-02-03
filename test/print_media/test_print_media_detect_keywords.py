"""Tests for keyword detection in Factiva articles (print media).

Tests cover:
- Overlap filtering (longest match priority)
- Canonical forms (plural handling)
- Theme detection and causal links
- HRFP (High Risk False Positive) separation
- Keyword lists and counts
- all_keywords field structure
"""

import re

import pytest

from quotaclimat.data_ingestion.factiva.utils_data_processing.detect_keywords import (
    search_keywords_in_text,
    search_keywords_with_canonical_forms,
)
from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    extract_keyword_data_from_article,
)

# ============================================================================
# Overlap filtering
# ============================================================================


def test_overlapping_keywords_longest_match_only():
    """Test that only the longest keyword is kept when keywords overlap.
    
    Example: "La réduction des émissions de gaz à effet de serre"
    Should only match "réduction des émissions de gaz à effet de serre", 
    NOT "effet de serre" or "gaz à effet de serre" separately.
    """
    text = "la réduction des émissions de gaz à effet de serre"
    
    keywords = [
        "effet de serre",
        "gaz à effet de serre",
        "réduction des émissions de gaz à effet de serre",
    ]
    
    # Test search_keywords_in_text
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Should only match the longest keyword
    assert len(result) == 1
    assert "réduction des émissions de gaz à effet de serre" in result
    assert "effet de serre" not in result
    assert "gaz à effet de serre" not in result


def test_overlapping_keywords_longest_match_only_canonical():
    """Test canonical form matching with overlapping keywords."""
    text = "la réduction des émissions de gaz à effet de serre"
    
    keywords = [
        "effet de serre",
        "gaz à effet de serre",
        "réduction des émissions de gaz à effet de serre",
    ]
    
    # Test search_keywords_with_canonical_forms
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Should only match the longest keyword
    assert len(result) == 1
    assert "réduction des émissions de gaz à effet de serre" in result
    assert "effet de serre" not in result
    assert "gaz à effet de serre" not in result


def test_non_overlapping_keywords_all_matched():
    """Test that non-overlapping keywords are all detected."""
    text = "le réchauffement climatique et la biodiversité"
    
    keywords = [
        "réchauffement climatique",
        "biodiversité",
    ]
    
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Both should be matched since they don't overlap
    assert len(result) == 2
    assert "réchauffement climatique" in result
    assert "biodiversité" in result


def test_three_level_nesting():
    """Test three levels of nested keywords."""
    text = "la pollution de l'air par les particules fines"
    
    keywords = [
        "pollution",
        "pollution de l'air",
        "pollution de l'air par les particules fines",
    ]
    
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Should only match the longest (most specific) keyword
    assert len(result) == 1
    assert "pollution de l'air par les particules fines" in result
    assert "pollution" not in result
    assert "pollution de l'air" not in result


def test_multiple_separate_overlapping_groups():
    """Test multiple groups of overlapping keywords in same text."""
    text = "la fonte des glaciers et l'élévation du niveau de la mer"
    
    keywords = [
        "fonte",
        "fonte des glaciers",
        "niveau de la mer",
        "élévation du niveau de la mer",
    ]
    
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Should match longest from each group
    assert len(result) == 2
    assert "fonte des glaciers" in result
    assert "élévation du niveau de la mer" in result
    assert "fonte" not in result
    assert "niveau de la mer" not in result


def test_duplicate_keywords_without_keep_duplicates():
    """Test that duplicates are removed when keep_duplicates=False."""
    text = "le climat le climat le climat"
    
    keywords = ["climat"]
    
    result = search_keywords_with_canonical_forms(text, keywords, keep_duplicates=False)
    
    # Should only return unique keywords
    assert len(result) == 1
    assert result == ["climat"]


def test_overlapping_with_plural_forms():
    """Test overlapping with plural forms."""
    text = "les émissions de gaz à effet de serre"
    
    keywords = [
        "émission de gaz à effet de serre",  # singular in dictionary
        "gaz à effet de serre",
    ]
    
    result = search_keywords_with_canonical_forms(text, keywords)
    
    # Should match longest keyword (in canonical form)
    assert len(result) == 1
    assert "émission de gaz à effet de serre" in result
    assert "gaz à effet de serre" not in result


# ============================================================================
# Canonical forms
# ============================================================================


def test_canonical_form_simple_plural():
    """Test that simple plural returns canonical singular form."""
    text = "les canicules sont fréquentes"
    keywords = ["canicule"]
    
    result = search_keywords_with_canonical_forms(text, keywords, keep_duplicates=False)
    
    # Should return "canicule" (singular), not "canicules"
    assert len(result) == 1
    assert "canicule" in result
    assert "canicules" not in result


def test_canonical_form_compound_keyword_plural():
    """Test compound keyword with plural returns canonical singular."""
    text = "les énergies renouvelables sont essentielles"
    keywords = ["énergie renouvelable"]
    
    result = search_keywords_with_canonical_forms(text, keywords, keep_duplicates=False)
    
    # Should return "énergie renouvelable" (singular canonical form)
    assert len(result) == 1
    assert "énergie renouvelable" in result
    assert "énergies renouvelables" not in result


def test_canonical_form_multiple_plurals():
    """Test multiple plural keywords return canonical forms."""
    text = "les canicules et les sécheresses augmentent"
    keywords = ["canicule", "sécheresse"]
    
    result = search_keywords_with_canonical_forms(text, keywords, keep_duplicates=False)
    
    # Should return both canonical (singular) forms
    assert len(result) == 2
    assert "canicule" in result
    assert "sécheresse" in result
    # Should NOT have plural forms
    assert "canicules" not in result
    assert "sécheresses" not in result


# ============================================================================
# test extract_keyword_data_from_article (integration with themes)
# ============================================================================


def test_single_keyword_with_theme():
    """Test detection of a single keyword with its theme."""
    text = "les canicules sont de plus en plus fréquentes"
    result = extract_keyword_data_from_article(text)
    
    # "canicule" is in changement_climatique_consequences, ressources and biodiversite_causes
    total_keywords = result["number_of_crises_no_hrfp"]
    assert total_keywords == 1
    
    # Check the keyword appears in at least one list
    all_keywords_text = str(result["all_keywords"])
    assert "canicule" in all_keywords_text.lower()


def test_causal_link():
    """Test exact detection of climate statement keyword."""
    text = "le réchauffement climatique est une réalité"
    result = extract_keyword_data_from_article(text)
    
    # "réchauffement climatique" is in changement_climatique_constat (line 47242)
    assert result["number_of_changement_climatique_constat_no_hrfp"] == 1
    assert "réchauffement climatique" in result["changement_climatique_constat_keywords"]
    # No other climate sub-themes
    assert result["number_of_changement_climatique_causes_no_hrfp"] == 0
    assert result["number_of_changement_climatique_consequences_no_hrfp"] == 0
    assert result["number_of_changement_climatique_solutions_no_hrfp"] == 0
    assert result["number_of_climat_no_hrfp"] == 1


def test_aggregated_crises():
    """Test exact aggregated climate count across sub-themes."""
    text = "réchauffement climatique gaz à effet de serre acidification des océans"
    result = extract_keyword_data_from_article(text)
    
    # acidification des océans is in the thre crises
    assert result["number_of_crises_no_hrfp"] == 3
    assert result["number_of_climat_no_hrfp"] == 3
    assert result["number_of_biodiversite_no_hrfp"] == 1
    assert result["number_of_ressources_no_hrfp"] == 1


def test_keyword_list_empty_when_no_keywords():
    """Test that keyword lists are empty when no keywords detected."""
    text = "le chat mange une pizza au fromage"
    result = extract_keyword_data_from_article(text)
    
    # All lists should be empty
    assert len(result["changement_climatique_constat_keywords"]) == 0
    assert len(result["changement_climatique_causes_keywords"]) == 0
    assert len(result["biodiversite_concepts_generaux_keywords"]) == 0
    # Counts should be zero
    assert result["number_of_changement_climatique_constat_no_hrfp"] == 0
    assert result["number_of_crises_no_hrfp"] == 0


def test_hrfp_vs_non_hrfp_separation():
    """Test that HRFP and non-HRFP keywords are counted separately."""
    # "réchauffement climatique" is non-HRFP
    # "agricole" is HRFP
    text = "réchauffement climatique impacte le mileu agricole"
    result = extract_keyword_data_from_article(text)
    
    # Non-HRFP: should have "réchauffement climatique"
    assert result["number_of_changement_climatique_constat_no_hrfp"] == 1
    assert result["number_of_changement_climatique_constat_hrfp"] == 1


def test_all_keywords_structure():
    """Test all_keywords field has correct structure."""
    text = "réchauffement climatique impacte le mileu agricole"
    result = extract_keyword_data_from_article(text)
    
    assert "all_keywords" in result
    assert len(result["all_keywords"]) == 4 # 1 theme for réchauffement climatique 'changement_climatique_constat) and 3 themes for agricole (ressources, biodiversite_concepts_generaux, changement_climatique_constat)
    
    # Find the "réchauffement climatique" entry
    rechauffement_entries = [kw for kw in result["all_keywords"] 
                            if kw["keyword"] == "réchauffement climatique"]
    assert len(rechauffement_entries) == 1 # Only 1 theme is associated with réchauffement climatique
    
    kw = rechauffement_entries[0]
    assert kw["keyword"] == "réchauffement climatique"
    assert kw["theme"] == "changement_climatique_constat"
    assert "category" in kw
    assert kw["count_keyword"] == 1
    assert kw["is_hrfp"] == False

    kw = result["all_keywords"][1]
    assert kw["keyword"] == "agricole"
    assert kw["theme"] == "changement_climatique_constat"
    assert "category" in kw
    assert kw["count_keyword"] == 1
    assert kw["is_hrfp"] == True


def test_all_keywords_count_keyword():
    """Test that count_keyword in all_keywords reflects actual occurrences."""
    text = "canicule canicule canicule canicule"
    result = extract_keyword_data_from_article(text)
    
    # Find "canicule" in all_keywords
    canicule_entries = [kw for kw in result["all_keywords"] if kw["keyword"] == "canicule"]
    assert len(canicule_entries) == 3
    
    # Each entry should have count_keyword = 4 (4 occurrences in text)
    for entry in canicule_entries:
        assert entry["count_keyword"] == 4


def test_overlapping_longest_only_in_extraction():
    """Test that overlapping keywords keep only longest in extraction."""
    # "gaz à effet de serre" contains "gaz" (if exists) and "effet de serre" (if exists)
    text = "gaz à effet de serre"
    result = extract_keyword_data_from_article(text)
    
    # We detect that "effet de serre" is not detected, but "gaz à effet de serre" is detected
    keywords = result["crises_keywords"]
    assert "gaz à effet de serre" in keywords
    
    # Count should reflect only longest match
    assert result["number_of_crises_no_hrfp"] == 1


def test_empty_text():
    """Test with empty text."""
    result = extract_keyword_data_from_article("")
    
    assert result["number_of_climat_no_hrfp"] == 0
    assert result["number_of_biodiversite_no_hrfp"] == 0
    assert result["number_of_ressources_no_hrfp"] == 0
    assert len(result["all_keywords"]) == 0
    assert len(result["crises_keywords"]) == 0


def test_case_insensitive_detection():
    """Test keyword detection is case-insensitive."""
    text = "RÉCHAUFFEMENT CLIMATIQUE et Gaz À Effet De Serre"
    result = extract_keyword_data_from_article(text)
    
    # Should detect despite different case
    assert result["number_of_changement_climatique_constat_no_hrfp"] == 1
    assert result["number_of_changement_climatique_causes_no_hrfp"] == 1
    assert result["number_of_crises_no_hrfp"] == 2


# ============================================================================
# INTEGRATION TEST - Full article with all crisis types
# ============================================================================


def test_full_article_integration():
    """
    Integration test with complete article containing multiple keywords.
    
    Based on actual keyword.py data (verified from screenshots):
    - "réchauffement climatique" → changement_climatique_constat (non-HRFP)
    - "gaz à effet de serre" → changement_climatique_causes (non-HRFP)
    - "acidification des océans" → 3 themes: changement_climatique_consequences, biodiversite_causes, ressources
    - "canicule" → 3 themes: changement_climatique_consequences, biodiversite_causes, ressources
    - "inondation" → changement_climatique_consequences (HRFP = TRUE)
    
    This test validates EXACT counts and keyword list contents.
    """
    article_text = """
    Le réchauffement climatique causé par les gaz à effet de serre 
    provoque l'acidification des océans et des canicules.
    Les inondations sont aussi plus fréquentes.
    """
    
    result = extract_keyword_data_from_article(article_text)
    
    # ========== EXACT KEYWORD COUNTS (non-HRFP) ==========
    # We have 4 unique non-HRFP keywords:
    # 1. "réchauffement climatique"
    # 2. "gaz à effet de serre" 
    # 3. "acidification des océans"
    # 4. "canicule"
    
    # Climate constat: "réchauffement climatique" only
    assert result["number_of_changement_climatique_constat_no_hrfp"] == 1
    assert result["changement_climatique_constat_keywords"] == ["réchauffement climatique"]
    
    # Climate causes: "gaz à effet de serre" only
    assert result["number_of_changement_climatique_causes_no_hrfp"] == 1
    assert result["changement_climatique_causes_keywords"] == ["gaz à effet de serre"]
    
    # Climate consequences: "acidification des océans" + "canicule"
    assert result["number_of_changement_climatique_consequences_no_hrfp"] == 2
    assert set(result["changement_climatique_consequences_keywords"]) == {
        "acidification des océans", 
        "canicule"
    }
    
    # Biodiversity causes: "acidification des océans" + "canicule"
    assert result["number_of_biodiversite_causes_no_hrfp"] == 2
    assert set(result["biodiversite_causes_keywords"]) == {
        "acidification des océans",
        "canicule"
    }
    
    # Resources: "acidification des océans" + "canicule"
    assert result["number_of_ressources_no_hrfp"] == 2
    assert set(result["ressources_constat_keywords"]) == {
        "acidification des océans",
        "canicule"
    }
    
    # ========== AGGREGATED CLIMATE KEYWORDS ==========
    # Climate = constat + causes + consequences (unique)
    # "réchauffement climatique" + "gaz à effet de serre" + "acidification des océans" + "canicule" = 4
    assert result["number_of_climat_no_hrfp"] == 4
    
    # ========== AGGREGATED BIODIVERSITY KEYWORDS ==========
    # Biodiversity = "acidification des océans" + "canicule" = 2
    assert result["number_of_biodiversite_no_hrfp"] == 2
    
    # ========== AGGREGATED ALL CRISES (non-HRFP) ==========
    # Total unique: 4 keywords (réchauffement, gaz, acidification, canicule)
    assert result["number_of_crises_no_hrfp"] == 4
    assert len(result["crises_keywords"]) == 4
    assert set(result["crises_keywords"]) == {
        "réchauffement climatique",
        "gaz à effet de serre",
        "acidification des océans",
        "canicule"
    }
    
    # ========== HRFP KEYWORDS ==========
    # "inondation" is HRFP in changement_climatique_consequences
    assert result["number_of_changement_climatique_consequences_hrfp"] == 1
    assert result["changement_climatique_consequences_keywords_hrfp"] == ["inondation"]
    
    # Climate HRFP aggregated
    assert result["number_of_climat_hrfp"] == 1
    
    # All crises HRFP
    assert result["number_of_crises_hrfp"] == 1
    assert result["crises_keywords_hrfp"] == ["inondation"]
    
    # ========== all_keywords FIELD ==========
    # Should have:
    # - "réchauffement climatique" → 1 theme (changement_climatique_constat)
    # - "gaz à effet de serre" → 1 theme (changement_climatique_causes)
    # - "acidification des océans" → 3 themes (consequences, biodiv_causes, ressources)
    # - "canicule" → 3 themes (consequences, biodiv_causes, ressources)
    # - "inondation" → 1 theme (consequences HRFP)
    # Total = 1 + 1 + 3 + 3 + 1 = 9 entries
    assert len(result["all_keywords"]) == 9, f"Expected 9 entries, got {len(result['all_keywords'])}"
    
    # Check structure
    for kw_entry in result["all_keywords"]:
        assert "keyword" in kw_entry
        assert "theme" in kw_entry
        assert "category" in kw_entry
        assert "count_keyword" in kw_entry
        assert "is_hrfp" in kw_entry
        assert isinstance(kw_entry["count_keyword"], int)
        assert isinstance(kw_entry["is_hrfp"], bool)
    
    # Check HRFP separation
    hrfp_entries = [kw for kw in result["all_keywords"] if kw["is_hrfp"]]
    non_hrfp_entries = [kw for kw in result["all_keywords"] if not kw["is_hrfp"]]
    
    assert len(hrfp_entries) == 1, "Should have exactly 1 HRFP entry (inondation)"
    assert len(non_hrfp_entries) == 8, "Should have exactly 8 non-HRFP entries"
    
    # Verify HRFP entry
    assert hrfp_entries[0]["keyword"] == "inondation"
    assert hrfp_entries[0]["theme"] == "changement_climatique_consequences"
    assert hrfp_entries[0]["is_hrfp"] == True
    
    # Verify multi-theme keywords
    acidification_entries = [kw for kw in result["all_keywords"] 
                            if kw["keyword"] == "acidification des océans"]
    assert len(acidification_entries) == 3, "acidification should appear in 3 themes"
    acidif_themes = {entry["theme"] for entry in acidification_entries}
    assert acidif_themes == {
        "changement_climatique_consequences",
        "biodiversite_causes",
        "ressources"
    }
    
    canicule_entries = [kw for kw in result["all_keywords"] 
                       if kw["keyword"] == "canicule"]
    assert len(canicule_entries) == 3, "canicule should appear in 3 themes"
    canicule_themes = {entry["theme"] for entry in canicule_entries}
    assert canicule_themes == {
        "changement_climatique_consequences",
        "biodiversite_causes",
        "ressources"
    }
    
    # ========== CANONICAL FORMS ==========
    # "canicules" in text should return "canicule" (singular canonical form)
    all_keyword_strings = [kw["keyword"] for kw in result["all_keywords"]]
    assert "canicule" in all_keyword_strings, "Should use singular 'canicule'"
    assert "canicules" not in all_keyword_strings, "Should NOT have plural 'canicules'"
