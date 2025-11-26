"""Extract and count keywords from Factiva articles (non-HRFP only)."""

import logging
from typing import Dict, List

from quotaclimat.data_ingestion.factiva.utils_data_processing.detect_keywords import (
    search_keywords_in_text,
)
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS


def get_non_hrfp_keywords_by_theme() -> Dict[str, List[str]]:
    """
    Extract non-HRFP (high risk of false positive) keywords from THEME_KEYWORDS.
    
    Returns:
        Dictionary mapping theme names to lists of keywords (French only, non-HRFP)
    """
    keywords_by_theme = {}
    
    for theme, keywords_list in THEME_KEYWORDS.items():
        non_hrfp_keywords = []
        for item in keywords_list:
            # Only keep French keywords that are NOT high risk of false positive
            if (
                not item.get("high_risk_of_false_positive", True)
                and item.get("language") == "french"
            ):
                non_hrfp_keywords.append(item["keyword"])
        
        if non_hrfp_keywords:
            keywords_by_theme[theme] = non_hrfp_keywords
    
    return keywords_by_theme


def find_keywords_in_text(text: str, keywords: List[str]) -> List[str]:
    """
    Find ALL occurrences of keywords in text (including duplicates).
    Uses search_keywords_in_text from detect_keywords module.
    
    Args:
        text: The text to search in
        keywords: List of keywords to search for
        
    Returns:
        List of ALL keywords found (with duplicates - one entry per occurrence)
    """
    if not text or not keywords:
        return []
    
    # Use the optimized function from detect_keywords with keep_duplicates=True
    return search_keywords_in_text(text, keywords, keep_duplicates=True)


def count_unique_keywords(keyword_list: List[str]) -> int:
    """
    Count unique keywords in a list.
    
    Args:
        keyword_list: List of keywords (may contain duplicates)
        
    Returns:
        Number of unique keywords
    """
    return len(set(keyword_list))


def extract_keyword_data_from_article(article_text: str) -> Dict:
    """
    Extract keyword counts AND lists for all themes from a Factiva article.
    
    Args:
        article_text: Combined text from article (title + body + snippet + art)
        
    Returns:
        Dictionary with:
        - keyword counts (unique keywords only) for each theme
        - keyword lists (all occurrences including duplicates) for each theme
        - aggregated counts by crisis type (climat, ressources, biodiversité)
    """
    if not article_text:
        article_text = ""
    
    # Get non-HRFP keywords organized by theme
    keywords_by_theme = get_non_hrfp_keywords_by_theme()
    
    # Initialize result dictionary
    result = {
        # Counts (unique keywords only)
        "number_of_changement_climatique_constat_no_hrfp": 0,
        "number_of_changement_climatique_causes_no_hrfp": 0,
        "number_of_changement_climatique_consequences_no_hrfp": 0,
        "number_of_attenuation_climatique_solutions_no_hrfp": 0,
        "number_of_adaptation_climatique_solutions_no_hrfp": 0,
        "number_of_ressources_constat_no_hrfp": 0,
        "number_of_ressources_solutions_no_hrfp": 0,
        "number_of_biodiversite_concepts_generaux_no_hrfp": 0,
        "number_of_biodiversite_causes_no_hrfp": 0,
        "number_of_biodiversite_consequences_no_hrfp": 0,
        "number_of_biodiversite_solutions_no_hrfp": 0,
        # Aggregated counts by crisis type
        "number_of_climat_no_hrfp": 0,
        "number_of_ressources_no_hrfp": 0,
        "number_of_biodiversite_no_hrfp": 0,
        # Keyword lists (all occurrences including duplicates)
        "changement_climatique_constat_keywords": [],
        "changement_climatique_causes_keywords": [],
        "changement_climatique_consequences_keywords": [],
        "attenuation_climatique_solutions_keywords": [],
        "adaptation_climatique_solutions_keywords": [],
        "ressources_constat_keywords": [],
        "ressources_solutions_keywords": [],
        "biodiversite_concepts_generaux_keywords": [],
        "biodiversite_causes_keywords": [],
        "biodiversite_consequences_keywords": [],
        "biodiversite_solutions_keywords": [],
    }
    
    # Map theme names to keys
    theme_to_count_key = {
        "changement_climatique_constat": "number_of_changement_climatique_constat_no_hrfp",
        "changement_climatique_causes": "number_of_changement_climatique_causes_no_hrfp",
        "changement_climatique_consequences": "number_of_changement_climatique_consequences_no_hrfp",
        "attenuation_climatique_solutions": "number_of_attenuation_climatique_solutions_no_hrfp",
        "adaptation_climatique_solutions": "number_of_adaptation_climatique_solutions_no_hrfp",
        "ressources": "number_of_ressources_constat_no_hrfp",
        "ressources_solutions": "number_of_ressources_solutions_no_hrfp",
        "biodiversite_concepts_generaux": "number_of_biodiversite_concepts_generaux_no_hrfp",
        "biodiversite_causes": "number_of_biodiversite_causes_no_hrfp",
        "biodiversite_consequences": "number_of_biodiversite_consequences_no_hrfp",
        "biodiversite_solutions": "number_of_biodiversite_solutions_no_hrfp",
    }
    
    theme_to_list_key = {
        "changement_climatique_constat": "changement_climatique_constat_keywords",
        "changement_climatique_causes": "changement_climatique_causes_keywords",
        "changement_climatique_consequences": "changement_climatique_consequences_keywords",
        "attenuation_climatique_solutions": "attenuation_climatique_solutions_keywords",
        "adaptation_climatique_solutions": "adaptation_climatique_solutions_keywords",
        "ressources": "ressources_constat_keywords",
        "ressources_solutions": "ressources_solutions_keywords",
        "biodiversite_concepts_generaux": "biodiversite_concepts_generaux_keywords",
        "biodiversite_causes": "biodiversite_causes_keywords",
        "biodiversite_consequences": "biodiversite_consequences_keywords",
        "biodiversite_solutions": "biodiversite_solutions_keywords",
    }
    
    # Find keywords for each theme
    for theme, keywords in keywords_by_theme.items():
        count_key = theme_to_count_key.get(theme)
        list_key = theme_to_list_key.get(theme)
        
        if count_key and list_key:
            # Find all keywords (including duplicates)
            found_keywords = find_keywords_in_text(article_text, keywords)
            
            # Store the list (with all occurrences)
            result[list_key] = found_keywords
            
            # Count unique keywords only
            unique_count = count_unique_keywords(found_keywords)
            result[count_key] = unique_count
            
            if unique_count > 0:
                logging.debug(f"Found {unique_count} unique keywords for theme {theme} (total occurrences: {len(found_keywords)})")
    
    # Calculate aggregated counts by crisis type
    # Climat = sum of all climat-related causal links (unique keywords across all links)
    climat_keywords = set()
    climat_keywords.update(result["changement_climatique_constat_keywords"])
    climat_keywords.update(result["changement_climatique_causes_keywords"])
    climat_keywords.update(result["changement_climatique_consequences_keywords"])
    climat_keywords.update(result["attenuation_climatique_solutions_keywords"])
    climat_keywords.update(result["adaptation_climatique_solutions_keywords"])
    result["number_of_climat_no_hrfp"] = len(climat_keywords)
    
    # Ressources = sum of ressources-related causal links (constat + solutions)
    ressources_keywords = set()
    ressources_keywords.update(result["ressources_constat_keywords"])
    ressources_keywords.update(result["ressources_solutions_keywords"])
    result["number_of_ressources_no_hrfp"] = len(ressources_keywords)
    
    # Biodiversité = sum of biodiversité-related causal links
    biodiversite_keywords = set()
    biodiversite_keywords.update(result["biodiversite_concepts_generaux_keywords"])
    biodiversite_keywords.update(result["biodiversite_causes_keywords"])
    biodiversite_keywords.update(result["biodiversite_consequences_keywords"])
    biodiversite_keywords.update(result["biodiversite_solutions_keywords"])
    result["number_of_biodiversite_no_hrfp"] = len(biodiversite_keywords)
    
    return result


def build_article_text(article_data: dict) -> str:
    """
    Build combined text from article attributes.
    
    Args:
        article_data: Dictionary with article data (from Factiva JSON)
        
    Returns:
        Combined text from title, body, snippet, and art fields
    """
    attributes = article_data.get("attributes", {})
    
    text_parts = []
    
    # Add title
    title = attributes.get("title")
    if title:
        text_parts.append(title)
    
    # Add body (main content)
    body = attributes.get("body")
    if body:
        text_parts.append(body)
    
    # Add snippet
    snippet = attributes.get("snippet")
    if snippet:
        text_parts.append(snippet)
    
    # Add art (captions and descriptions)
    art = attributes.get("art")
    if art:
        text_parts.append(art)
    
    # Combine all parts with spaces
    combined_text = " ".join(text_parts)
    
    return combined_text.lower()  # Convert to lowercase for matching

