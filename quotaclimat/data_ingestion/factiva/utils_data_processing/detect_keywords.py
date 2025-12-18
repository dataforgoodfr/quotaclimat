import re
from itertools import product
from typing import Dict, List, Tuple


def format_word_regex(word: str) -> str:
    """
    Transforms a word to handle plurals and special cases in the regex.

    Args:
        word: The word to transform

    Returns:
        The word transformed with regex rules for plurals
    """
    word = word.replace("'", "' ?")  # handles d'eau -> d' eau
    if not word.endswith("s") and not word.endswith("x") and not word.endswith("à"):
        return word + "s?"
    elif word.endswith("s"):
        return word + "?"
    elif word.endswith("x"):
        return word + "?"
    else:
        return word


def create_combined_regex_pattern(
    keywords_filtered: List[str], bigquery_compatible: bool = False
) -> str:
    """
    Creates a large combined regex pattern from a list of keywords.
    Handles line start and is case-insensitive.

    Args:
        keywords_filtered: List of keywords to transform into a regex
        bigquery_compatible: If True, generates a BigQuery-compatible regex (without \\b)

    Returns:
        Combined regex pattern to detect all keywords
    """
    if not keywords_filtered:
        return ""

    # Transform each keyword (which may contain multiple words)
    transformed_keywords = []

    for keyword in keywords_filtered:
        # Split multi-word keywords and apply format_word_regex to each word
        words = keyword.split(" ")
        transformed_words = [format_word_regex(word) for word in words]
        transformed_keyword = " ".join(transformed_words)

        # Escape special regex characters except those we want to keep
        # We keep the ? and spaces we added
        escaped_keyword = (
            re.escape(transformed_keyword).replace(r"\?", "?").replace(r"\ ", " ")
        )
        # BigQuery: \- is illegal outside of a character class; hyphen must stay literal
        if bigquery_compatible:
            # BigQuery: '-' must not be backslash-escaped; apostrophes cause SQL issues → use \x27
            escaped_keyword = escaped_keyword.replace(r"\-", "-")
            escaped_keyword = escaped_keyword.replace("'", r"\x27")
        transformed_keywords.append(escaped_keyword)

    # Create the combined pattern with alternatives (|)
    if bigquery_compatible:
        # Reduced class: ASCII + 'é', 'è', and apostrophe only
        limited_letters = "A-Za-z0-9"
        individual_patterns = [
            f"(?:^|[^{limited_letters}])({keyword})(?:$|[^{limited_letters}-])"
            for keyword in transformed_keywords
        ]
    else:
        # Standard version with \b
        individual_patterns = [
            f"(?:^|\\b){keyword}(?![\\w-])" for keyword in transformed_keywords
        ]

    # For BigQuery, avoid (?: ... ); use classic groups
    combined_pattern = (
        "(?i)(?:" + "|".join(individual_patterns) + ")"
        if bigquery_compatible
        else "(?i)(?:" + "|".join(individual_patterns) + ")"
    )

    return combined_pattern


def search_keywords_in_text(
    text: str, keywords_filtered: List[str], keep_duplicates: bool = False
) -> List[str]:
    """
    Searches all keywords present in a text using a combined regex.

    Args:
        text: The text to search in
        keywords_filtered: List of keywords to look for
        keep_duplicates: If True, returns all occurrences (with duplicates).
                        If False, returns unique keywords only (default).

    Returns:
        List of keywords found in the text
    """
    if not keywords_filtered or not text:
        return []

    # Create the combined regex pattern
    pattern = create_combined_regex_pattern(keywords_filtered)

    if not pattern:
        return []

    # Find all occurrences (case-insensitivity already in the pattern)
    matches = re.findall(pattern, text)

    # Return unique or all occurrences based on parameter
    if keep_duplicates:
        return matches
    else:
        return list(set(matches))


def is_keyword_in_text(keyword: str, text: str) -> bool:
    """
    Checks if a specific keyword is present in a text.
    Replicates the logic of is_word_in_sentence from the original file.

    Args:
        keyword: The keyword to search for
        text: The text to search in

    Returns:
        True if the keyword is found, False otherwise
    """
    if not keyword or not text:
        return False

    # Transform the keyword (can contain multiple words)
    words = keyword.split(" ")
    transformed_words = [format_word_regex(word) for word in words]
    transformed_keyword = " ".join(transformed_words)

    # Create the regex pattern with case-insensitivity and line start
    pattern = f"(?i)(?:^|\\b){transformed_keyword}(?![\\w-])"

    # Search (case-insensitivity already in the pattern)
    return bool(re.search(pattern, text))


def generate_variant_forms(keyword: str) -> List[str]:
    """
    Generates all possible variant forms (singular/plural) for a keyword.
    
    This generates the actual forms that format_word_regex would match,
    so we can create a reverse mapping: matched_form → canonical_form.
    
    Args:
        keyword: Canonical keyword (e.g., "canicule")
        
    Returns:
        List of possible forms (e.g., ["canicule", "canicules"])
    """
    variants = []
    words = keyword.split(" ")
    
    # Generate all combinations of singular/plural for multi-word keywords
    word_variants = []
    for word in words:
        # Handle apostrophe variants (d'eau → d'eau or d' eau)
        if "'" in word:
            # Both with and without space after apostrophe
            word_variants.append([word, word.replace("'", "' ")])
        elif not word.endswith('s') and not word.endswith('x') and not word.endswith('à'):
            # Add s for plural
            word_variants.append([word, word + "s"])
        elif word.endswith('s') or word.endswith('x'):
            # Already ends with s/x - singular only (format_word_regex makes it optional)
            word_variants.append([word])
        else:
            # Ends with à or other - no plural
            word_variants.append([word])
    
    # Generate all combinations
    for combination in product(*word_variants):
        variant = " ".join(combination)
        variants.append(variant.lower())
    
    return variants


def create_variant_to_canonical_mapping(keywords_filtered: List[str]) -> Dict[str, str]:
    """
    Creates a mapping from all possible variant forms to canonical forms.
    
    This allows fast O(1) lookup after regex matching to retrieve the canonical form.
    
    Args:
        keywords_filtered: List of canonical keywords from the dictionary
        
    Returns:
        Dict mapping variant forms to canonical forms
        Example: {"canicule": "canicule", "canicules": "canicule", ...}
    """
    variant_to_canonical = {}
    
    for canonical_keyword in keywords_filtered:
        variants = generate_variant_forms(canonical_keyword)
        for variant in variants:
            # Lowercase for case-insensitive matching
            variant_lower = variant.lower()
            if variant_lower not in variant_to_canonical:
                variant_to_canonical[variant_lower] = canonical_keyword
            # If collision (same variant for different canonical), keep first one
            # This should be rare with real-world keywords
    
    return variant_to_canonical


def search_keywords_with_canonical_forms(
    text: str, keywords_filtered: List[str], keep_duplicates: bool = False
) -> List[str]:
    """
    Searches keywords in text and returns CANONICAL forms (from dictionary).
    
    Fast implementation using pre-computed variant mapping instead of named groups.
    Performance: O(m) for regex + O(n) for lookups where n = number of matches (usually << 5900)
    
    Args:
        text: The text to search in
        keywords_filtered: List of canonical keywords from the dictionary
        keep_duplicates: If True, returns all occurrences (with duplicates).
                        If False, returns unique keywords only (default).
    
    Returns:
        List of CANONICAL keywords found (matching the dictionary forms)
    
    Example:
        >>> search_keywords_with_canonical_forms("Les canicules augmentent", ["canicule"])
        ["canicule"]  # Returns canonical form, not "canicules"
    """
    if not keywords_filtered or not text:
        return []
    
    # Step 1: Create variant → canonical mapping (pre-compute once per call)
    variant_to_canonical = create_variant_to_canonical_mapping(keywords_filtered)
    
    # Step 2: Use fast classic regex (no named groups!)
    pattern = create_combined_regex_pattern(keywords_filtered)
    
    if not pattern:
        return []
    
    # Step 3: Find all matches (fast regex)
    matches = re.findall(pattern, text)
    
    # Step 4: Map each match to its canonical form (fast O(1) dict lookup)
    canonical_matches = []
    for match in matches:
        match_lower = match.lower()
        canonical_form = variant_to_canonical.get(match_lower)
        if canonical_form:
            canonical_matches.append(canonical_form)
        else:
            # Fallback: keep original if no mapping found
            canonical_matches.append(match)
    
    # Return unique or all occurrences based on parameter
    if keep_duplicates:
        return canonical_matches
    else:
        return list(set(canonical_matches))