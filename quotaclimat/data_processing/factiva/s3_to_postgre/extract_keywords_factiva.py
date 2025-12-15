"""Extract and count keywords from Factiva articles (HRFP and non-HRFP)."""

import logging
from collections import Counter
from typing import Dict, List

from quotaclimat.data_ingestion.factiva.utils_data_processing.detect_keywords import (
    search_keywords_in_text,
)
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS


def get_keywords_by_theme_and_hrfp() -> Dict[str, Dict[str, List[Dict]]]:
    """
    Extract keywords from THEME_KEYWORDS, separated by HRFP status.
    
    For each base theme (without suffix), combines keywords from:
    - The base theme (e.g., "changement_climatique_constat")
    - The _indirectes variant (e.g., "changement_climatique_constat_indirectes")
    - The _directes variant (e.g., "changement_climatique_constat_directes")
    
    Then separates them by their individual high_risk_of_false_positive flag.
    
    Returns:
        Dictionary with structure (using base theme names without suffixes):
        {
            "changement_climatique_constat": {
                "non_hrfp": [{"keyword": "...", "category": "..."}, ...],
                "hrfp": [{"keyword": "...", "category": "..."}, ...]
            }
        }
    """
    # First, group all keywords by base theme name
    base_themes = {}
    
    for theme, keywords_list in THEME_KEYWORDS.items():
        # Determine base theme name (remove _indirectes or _directes suffix)
        base_theme = theme
        if theme.endswith("_indirectes"):
            base_theme = theme[:-len("_indirectes")]
        elif theme.endswith("_directes"):
            base_theme = theme[:-len("_directes")]
        
        # Initialize base theme if not exists
        if base_theme not in base_themes:
            base_themes[base_theme] = []
        
        # Add all keywords from this theme to the base theme
        base_themes[base_theme].extend(keywords_list)
    
    # Now separate by HRFP status for each base theme
    keywords_by_theme = {}
    
    for base_theme, all_keywords in base_themes.items():
        non_hrfp_keywords = []
        hrfp_keywords = []
        seen_keywords = set()  # To avoid duplicates
        
        for item in all_keywords:
            # Only keep French keywords
            if item.get("language") == "french":
                keyword_text = item["keyword"]
                
                # Skip if we've already processed this keyword for this theme
                if keyword_text in seen_keywords:
                    continue
                seen_keywords.add(keyword_text)
                
                keyword_info = {
                    "keyword": keyword_text,
                    "category": item.get("category", "")
                }
                
                # Check individual keyword's HRFP flag
                if item.get("high_risk_of_false_positive", False):
                    hrfp_keywords.append(keyword_info)
                else:
                    non_hrfp_keywords.append(keyword_info)
        
        if non_hrfp_keywords or hrfp_keywords:
            keywords_by_theme[base_theme] = {
                "non_hrfp": non_hrfp_keywords,
                "hrfp": hrfp_keywords
            }
    
    return keywords_by_theme


def find_keywords_in_text(text: str, keywords_info: List[Dict]) -> List[Dict]:
    """
    Find ALL occurrences of keywords in text (including duplicates) with their metadata.
    
    Args:
        text: The text to search in
        keywords_info: List of dicts with 'keyword' and 'category' keys
        
    Returns:
        List of dicts with found keywords and their categories (with duplicates)
    """
    if not text or not keywords_info:
        return []
    
    # Extract just the keyword strings for search
    keyword_strings = [k["keyword"] for k in keywords_info]
    
    # Find all occurrences
    found_keywords = search_keywords_in_text(text, keyword_strings, keep_duplicates=True)
    
    # Create a mapping from keyword to category
    keyword_to_category = {k["keyword"]: k["category"] for k in keywords_info}
    
    # Build result with categories
    result = []
    for keyword in found_keywords:
        result.append({
            "keyword": keyword,
            "category": keyword_to_category.get(keyword, "")
        })
    
    return result


def count_unique_keywords(keyword_list: List[Dict]) -> int:
    """
    Count unique keywords in a list.
    
    Args:
        keyword_list: List of dicts with 'keyword' key (may contain duplicates)
        
    Returns:
        Number of unique keywords
    """
    return len(set(k["keyword"] for k in keyword_list))


def extract_keyword_strings(keyword_list: List[Dict]) -> List[str]:
    """
    Extract just the keyword strings from a list of keyword dicts.
    
    Args:
        keyword_list: List of dicts with 'keyword' key
        
    Returns:
        List of keyword strings
    """
    return [k["keyword"] for k in keyword_list]


def extract_keyword_data_from_article(article_text: str) -> Dict:
    """
    Extract keyword counts AND lists for all themes from a Factiva article.
    Handles both HRFP and non-HRFP keywords, and creates all_keywords field.
    
    Args:
        article_text: Combined text from article (title + body + snippet + art)
        
    Returns:
        Dictionary with:
        - keyword counts (unique keywords only) for each theme (HRFP and non-HRFP)
        - keyword lists (all occurrences including duplicates) for each theme (HRFP and non-HRFP)
        - aggregated counts by crisis type (climat, ressources, biodiversité) (HRFP and non-HRFP)
        - all_keywords: JSON with full metadata (keyword, theme, category, count_keyword, is_hrfp)
    """
    if not article_text:
        article_text = ""
    
    # Get keywords organized by theme and HRFP status
    keywords_by_theme = get_keywords_by_theme_and_hrfp()
    
    # Initialize result dictionary with all fields
    result = {
        # Non-HRFP counts (unique keywords only)
        "number_of_changement_climatique_constat_no_hrfp": 0,
        "number_of_changement_climatique_causes_no_hrfp": 0,
        "number_of_changement_climatique_consequences_no_hrfp": 0,
        "number_of_attenuation_climatique_solutions_no_hrfp": 0,
        "number_of_adaptation_climatique_solutions_no_hrfp": 0,
        "number_of_changement_climatique_solutions_no_hrfp": 0,
        "number_of_ressources_constat_no_hrfp": 0,
        "number_of_ressources_solutions_no_hrfp": 0,
        "number_of_biodiversite_concepts_generaux_no_hrfp": 0,
        "number_of_biodiversite_causes_no_hrfp": 0,
        "number_of_biodiversite_consequences_no_hrfp": 0,
        "number_of_biodiversite_solutions_no_hrfp": 0,
        # HRFP counts (unique keywords only)
        "number_of_changement_climatique_constat_hrfp": 0,
        "number_of_changement_climatique_causes_hrfp": 0,
        "number_of_changement_climatique_consequences_hrfp": 0,
        "number_of_attenuation_climatique_solutions_hrfp": 0,
        "number_of_adaptation_climatique_solutions_hrfp": 0,
        "number_of_changement_climatique_solutions_hrfp": 0,
        "number_of_ressources_constat_hrfp": 0,
        "number_of_ressources_solutions_hrfp": 0,
        "number_of_biodiversite_concepts_generaux_hrfp": 0,
        "number_of_biodiversite_causes_hrfp": 0,
        "number_of_biodiversite_consequences_hrfp": 0,
        "number_of_biodiversite_solutions_hrfp": 0,
        # Aggregated counts by crisis type - non-HRFP
        "number_of_climat_no_hrfp": 0,
        "number_of_ressources_no_hrfp": 0,
        "number_of_biodiversite_no_hrfp": 0,
        # Aggregated counts by crisis type - HRFP
        "number_of_climat_hrfp": 0,
        "number_of_ressources_hrfp": 0,
        "number_of_biodiversite_hrfp": 0,
        # Non-HRFP keyword lists (all occurrences including duplicates)
        "changement_climatique_constat_keywords": [],
        "changement_climatique_causes_keywords": [],
        "changement_climatique_consequences_keywords": [],
        "attenuation_climatique_solutions_keywords": [],
        "adaptation_climatique_solutions_keywords": [],
        "changement_climatique_solutions_keywords": [],
        "ressources_constat_keywords": [],
        "ressources_solutions_keywords": [],
        "biodiversite_concepts_generaux_keywords": [],
        "biodiversite_causes_keywords": [],
        "biodiversite_consequences_keywords": [],
        "biodiversite_solutions_keywords": [],
        # HRFP keyword lists (all occurrences including duplicates)
        "changement_climatique_constat_keywords_hrfp": [],
        "changement_climatique_causes_keywords_hrfp": [],
        "changement_climatique_consequences_keywords_hrfp": [],
        "attenuation_climatique_solutions_keywords_hrfp": [],
        "adaptation_climatique_solutions_keywords_hrfp": [],
        "changement_climatique_solutions_keywords_hrfp": [],
        "ressources_constat_keywords_hrfp": [],
        "ressources_solutions_keywords_hrfp": [],
        "biodiversite_concepts_generaux_keywords_hrfp": [],
        "biodiversite_causes_keywords_hrfp": [],
        "biodiversite_consequences_keywords_hrfp": [],
        "biodiversite_solutions_keywords_hrfp": [],
        # All keywords with full metadata
        "all_keywords": [],
    }
    
    # Map theme names to keys (using base theme names without suffixes)
    # The get_keywords_by_theme_and_hrfp() function already combines themes with their _indirectes variants
    theme_to_keys = {
        "changement_climatique_constat": {
            "count_no_hrfp": "number_of_changement_climatique_constat_no_hrfp",
            "count_hrfp": "number_of_changement_climatique_constat_hrfp",
            "list_no_hrfp": "changement_climatique_constat_keywords",
            "list_hrfp": "changement_climatique_constat_keywords_hrfp",
        },
        "changement_climatique_causes": {
            "count_no_hrfp": "number_of_changement_climatique_causes_no_hrfp",
            "count_hrfp": "number_of_changement_climatique_causes_hrfp",
            "list_no_hrfp": "changement_climatique_causes_keywords",
            "list_hrfp": "changement_climatique_causes_keywords_hrfp",
        },
        "changement_climatique_consequences": {
            "count_no_hrfp": "number_of_changement_climatique_consequences_no_hrfp",
            "count_hrfp": "number_of_changement_climatique_consequences_hrfp",
            "list_no_hrfp": "changement_climatique_consequences_keywords",
            "list_hrfp": "changement_climatique_consequences_keywords_hrfp",
        },
        "attenuation_climatique_solutions": {
            "count_no_hrfp": "number_of_attenuation_climatique_solutions_no_hrfp",
            "count_hrfp": "number_of_attenuation_climatique_solutions_hrfp",
            "list_no_hrfp": "attenuation_climatique_solutions_keywords",
            "list_hrfp": "attenuation_climatique_solutions_keywords_hrfp",
        },
        "adaptation_climatique_solutions": {
            "count_no_hrfp": "number_of_adaptation_climatique_solutions_no_hrfp",
            "count_hrfp": "number_of_adaptation_climatique_solutions_hrfp",
            "list_no_hrfp": "adaptation_climatique_solutions_keywords",
            "list_hrfp": "adaptation_climatique_solutions_keywords_hrfp",
        },
        "ressources": {
            "count_no_hrfp": "number_of_ressources_constat_no_hrfp",
            "count_hrfp": "number_of_ressources_constat_hrfp",
            "list_no_hrfp": "ressources_constat_keywords",
            "list_hrfp": "ressources_constat_keywords_hrfp",
        },
        "ressources_solutions": {
            "count_no_hrfp": "number_of_ressources_solutions_no_hrfp",
            "count_hrfp": "number_of_ressources_solutions_hrfp",
            "list_no_hrfp": "ressources_solutions_keywords",
            "list_hrfp": "ressources_solutions_keywords_hrfp",
        },
        "biodiversite_concepts_generaux": {
            "count_no_hrfp": "number_of_biodiversite_concepts_generaux_no_hrfp",
            "count_hrfp": "number_of_biodiversite_concepts_generaux_hrfp",
            "list_no_hrfp": "biodiversite_concepts_generaux_keywords",
            "list_hrfp": "biodiversite_concepts_generaux_keywords_hrfp",
        },
        "biodiversite_causes": {
            "count_no_hrfp": "number_of_biodiversite_causes_no_hrfp",
            "count_hrfp": "number_of_biodiversite_causes_hrfp",
            "list_no_hrfp": "biodiversite_causes_keywords",
            "list_hrfp": "biodiversite_causes_keywords_hrfp",
        },
        "biodiversite_consequences": {
            "count_no_hrfp": "number_of_biodiversite_consequences_no_hrfp",
            "count_hrfp": "number_of_biodiversite_consequences_hrfp",
            "list_no_hrfp": "biodiversite_consequences_keywords",
            "list_hrfp": "biodiversite_consequences_keywords_hrfp",
        },
        "biodiversite_solutions": {
            "count_no_hrfp": "number_of_biodiversite_solutions_no_hrfp",
            "count_hrfp": "number_of_biodiversite_solutions_hrfp",
            "list_no_hrfp": "biodiversite_solutions_keywords",
            "list_hrfp": "biodiversite_solutions_keywords_hrfp",
        },
    }
    
    # Storage for all_keywords construction
    all_keywords_data = []
    
    # Find keywords for each theme (both HRFP and non-HRFP)
    for theme, keywords_dict in keywords_by_theme.items():
        keys = theme_to_keys.get(theme)
        if not keys:
            logging.debug(f"Skipping theme {theme} - no mapping found")
            continue
        
        # Process non-HRFP keywords
        if keywords_dict.get("non_hrfp"):
            found_keywords = find_keywords_in_text(article_text, keywords_dict["non_hrfp"])
            result[keys["list_no_hrfp"]] = extract_keyword_strings(found_keywords)
            result[keys["count_no_hrfp"]] = count_unique_keywords(found_keywords)
            
            # Add to all_keywords_data
            for kw_dict in found_keywords:
                all_keywords_data.append({
                    "keyword": kw_dict["keyword"],
                    "theme": theme,
                    "category": kw_dict["category"],
                    "is_hrfp": False
                })
            
            if result[keys["count_no_hrfp"]] > 0:
                logging.debug(f"Found {result[keys['count_no_hrfp']]} unique non-HRFP keywords for theme {theme}")
        
        # Process HRFP keywords
        if keywords_dict.get("hrfp"):
            found_keywords = find_keywords_in_text(article_text, keywords_dict["hrfp"])
            result[keys["list_hrfp"]] = extract_keyword_strings(found_keywords)
            result[keys["count_hrfp"]] = count_unique_keywords(found_keywords)
            
            # Add to all_keywords_data
            for kw_dict in found_keywords:
                all_keywords_data.append({
                    "keyword": kw_dict["keyword"],
                    "theme": theme,
                    "category": kw_dict["category"],
                    "is_hrfp": True
                })
            
            if result[keys["count_hrfp"]] > 0:
                logging.debug(f"Found {result[keys['count_hrfp']]} unique HRFP keywords for theme {theme}")
    
    # Calculate combined climate solutions (attenuation + adaptation)
    # Non-HRFP
    combined_solutions_no_hrfp = set()
    combined_solutions_no_hrfp.update(result["attenuation_climatique_solutions_keywords"])
    combined_solutions_no_hrfp.update(result["adaptation_climatique_solutions_keywords"])
    result["changement_climatique_solutions_keywords"] = list(combined_solutions_no_hrfp)
    result["number_of_changement_climatique_solutions_no_hrfp"] = len(combined_solutions_no_hrfp)
    
    # HRFP
    combined_solutions_hrfp = set()
    combined_solutions_hrfp.update(result["attenuation_climatique_solutions_keywords_hrfp"])
    combined_solutions_hrfp.update(result["adaptation_climatique_solutions_keywords_hrfp"])
    result["changement_climatique_solutions_keywords_hrfp"] = list(combined_solutions_hrfp)
    result["number_of_changement_climatique_solutions_hrfp"] = len(combined_solutions_hrfp)
    
    # Calculate aggregated counts by crisis type
    # Climat non-HRFP = constat + causes + consequences + solutions
    climat_keywords_no_hrfp = set()
    climat_keywords_no_hrfp.update(result["changement_climatique_constat_keywords"])
    climat_keywords_no_hrfp.update(result["changement_climatique_causes_keywords"])
    climat_keywords_no_hrfp.update(result["changement_climatique_consequences_keywords"])
    climat_keywords_no_hrfp.update(result["changement_climatique_solutions_keywords"])
    result["number_of_climat_no_hrfp"] = len(climat_keywords_no_hrfp)
    
    # Climat HRFP
    climat_keywords_hrfp = set()
    climat_keywords_hrfp.update(result["changement_climatique_constat_keywords_hrfp"])
    climat_keywords_hrfp.update(result["changement_climatique_causes_keywords_hrfp"])
    climat_keywords_hrfp.update(result["changement_climatique_consequences_keywords_hrfp"])
    climat_keywords_hrfp.update(result["changement_climatique_solutions_keywords_hrfp"])
    result["number_of_climat_hrfp"] = len(climat_keywords_hrfp)
    
    # Ressources non-HRFP
    ressources_keywords_no_hrfp = set()
    ressources_keywords_no_hrfp.update(result["ressources_constat_keywords"])
    ressources_keywords_no_hrfp.update(result["ressources_solutions_keywords"])
    result["number_of_ressources_no_hrfp"] = len(ressources_keywords_no_hrfp)
    
    # Ressources HRFP
    ressources_keywords_hrfp = set()
    ressources_keywords_hrfp.update(result["ressources_constat_keywords_hrfp"])
    ressources_keywords_hrfp.update(result["ressources_solutions_keywords_hrfp"])
    result["number_of_ressources_hrfp"] = len(ressources_keywords_hrfp)
    
    # Biodiversité non-HRFP
    biodiversite_keywords_no_hrfp = set()
    biodiversite_keywords_no_hrfp.update(result["biodiversite_concepts_generaux_keywords"])
    biodiversite_keywords_no_hrfp.update(result["biodiversite_causes_keywords"])
    biodiversite_keywords_no_hrfp.update(result["biodiversite_consequences_keywords"])
    biodiversite_keywords_no_hrfp.update(result["biodiversite_solutions_keywords"])
    result["number_of_biodiversite_no_hrfp"] = len(biodiversite_keywords_no_hrfp)
    
    # Biodiversité HRFP
    biodiversite_keywords_hrfp = set()
    biodiversite_keywords_hrfp.update(result["biodiversite_concepts_generaux_keywords_hrfp"])
    biodiversite_keywords_hrfp.update(result["biodiversite_causes_keywords_hrfp"])
    biodiversite_keywords_hrfp.update(result["biodiversite_consequences_keywords_hrfp"])
    biodiversite_keywords_hrfp.update(result["biodiversite_solutions_keywords_hrfp"])
    result["number_of_biodiversite_hrfp"] = len(biodiversite_keywords_hrfp)
    
    # Build all_keywords with count_keyword for each unique keyword
    # Count occurrences of each (keyword, theme, category, is_hrfp) combination
    keyword_counter = Counter()
    for item in all_keywords_data:
        key = (item["keyword"], item["theme"], item["category"], item["is_hrfp"])
        keyword_counter[key] += 1
    
    # Build final all_keywords list
    all_keywords_list = []
    for (keyword, theme, category, is_hrfp), count in keyword_counter.items():
        all_keywords_list.append({
            "keyword": keyword,
            "theme": theme,
            "category": category,
            "count_keyword": count,
            "is_hrfp": is_hrfp
        })
    
    result["all_keywords"] = all_keywords_list
    
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

