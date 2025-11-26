import re
from typing import List


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