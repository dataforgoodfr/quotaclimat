"""Utility functions for keyword detection on pandas DataFrames."""

import ast
import json
import warnings
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    extract_keyword_data_from_article,
)
from quotaclimat.data_processing.mediatree.keyword.macro_category import (
    MACRO_CATEGORIES,
)


def create_llm_columns_from_secteurs(
    df: pd.DataFrame,
    llm_col: str = "llm_secteurs",
) -> pd.DataFrame:
    """
    Create binary LLM sector columns from the ``llm_secteurs`` column.

    The ``llm_secteurs`` column is expected to contain a comma‑separated list of
    sector labels coming from the LLM and validated against ``SECTEURS_VALIDES``
    in ``llm_classif_secteur.py``. We map these textual sectors to 8 binary
    indicator columns:

    - ``llm_agri_alim``
    - ``llm_batiment``
    - ``llm_economie_circulaire`` (handles both "Économie circulaire"
      and "Economie circulaire")
    - ``llm_mobilite``
    - ``llm_eau``
    - ``llm_ecosysteme``
    - ``llm_energie``
    - ``llm_industrie``

    Args:
        df: DataFrame containing the LLM sector column.
        llm_col: Name of the column with the comma‑separated sector labels
            (default: "llm_secteurs").

    Returns:
        A copy of the DataFrame with the 8 binary sector columns added.
    """
    if llm_col not in df.columns:
        raise ValueError(
            f"Missing LLM column: {llm_col}. "
            "You must run the sector classification step (llm_secteurs) first."
        )

    df = df.copy()

    # Mapping from exact LLM sector labels to target column names
    sector_to_col: Dict[str, str] = {
        "Agriculture & alimentation": "llm_agri_alim",
        "Bâtiment & aménagement": "llm_batiment",
        "Économie circulaire": "llm_economie_circulaire",
        "Economie circulaire": "llm_economie_circulaire",  # alternate spelling
        "Mobilité": "llm_mobilite",
        "Eau": "llm_eau",
        "Ecosystème": "llm_ecosysteme",
        "Energie": "llm_energie",
        "Industrie": "llm_industrie",
    }

    target_cols = sorted(set(sector_to_col.values()))

    def _build_sector_flags(value) -> Dict[str, int]:
        # Initialize all flags to 0
        flags = {col: 0 for col in target_cols}

        if pd.isna(value) or str(value).strip() == "":
            return flags

        # llm_secteurs is a comma‑separated list: "Agriculture & alimentation, Eau, ..."
        raw_items = [item.strip() for item in str(value).split(",")]

        for item in raw_items:
            col_name = sector_to_col.get(item)
            if col_name is not None:
                flags[col_name] = 1

        return flags

    flags_df = df[llm_col].apply(_build_sector_flags).apply(pd.Series)

    # Ensure columns exist even if all zeros (e.g. sector never appears)
    for col in target_cols:
        if col not in flags_df.columns:
            flags_df[col] = 0

    df = pd.concat([df, flags_df[target_cols]], axis=1)

    # Basic statistics: count and percentage of articles per sector
    n_rows = len(df)
    print("✓ LLM sector columns created from 'llm_secteurs'")
    print(f"  Total articles: {n_rows}")
    print("  Sector distribution (count, % of total):")
    for col in target_cols:
        # Use numeric sum; avoid casting a full Series directly to int
        count = df[col].fillna(0).astype("int64").sum()
        pct = (count / n_rows * 100) if n_rows > 0 else 0.0
        print(f"    - {col}: {int(count)} articles ({pct:.1f}%)")

    return df


def create_keyword_sector_columns(
    df: pd.DataFrame,
    keywords_col: str = "all_keywords",
) -> pd.DataFrame:
    """
    Crée 8 colonnes de comptage de mots-clés par secteur à partir de ``all_keywords``.

    Pour chaque article :
      - on parse ``all_keywords`` (liste de dicts),
      - on extrait les keywords UNIQUES (un keyword ne compte qu'une fois même s'il a plusieurs thèmes),
      - on mappe ces keywords à leurs secteurs via MACRO_CATEGORIES,
      - on compte le nombre de keywords par secteur.

    Colonnes créées :
      - nb_agri_alim          ← agriculture
      - nb_batiment           ← batiments
      - nb_economie_circulaire← economie_ressources
      - nb_mobilite           ← transport
      - nb_eau                ← eau
      - nb_ecosysteme         ← ecosysteme
      - nb_energie            ← energie
      - nb_industrie          ← industrie
    """
    if keywords_col not in df.columns:
        raise ValueError(
            f"Missing keywords column: {keywords_col}. "
            "You must run keyword detection first."
        )

    from ast import literal_eval

    df = df.copy()

    # Mapping des champs de MACRO_CATEGORIES vers les colonnes de sortie
    sector_field_to_col = {
        "agriculture": "nb_agri_alim",
        "transport": "nb_mobilite",
        "batiments": "nb_batiment",
        "energie": "nb_energie",
        "industrie": "nb_industrie",
        "eau": "nb_eau",
        "ecosysteme": "nb_ecosysteme",
        "economie_ressources": "nb_economie_circulaire",
    }
    target_cols = list(sector_field_to_col.values())

    # Dictionnaire : keyword -> {agriculture: bool, ..., economie_ressources: bool}
    keyword_to_sectors: Dict[str, Dict[str, bool]] = {}
    for entry in MACRO_CATEGORIES:
        kw = entry["keyword"]
        keyword_to_sectors[kw] = {
            field: entry.get(field, False) for field in sector_field_to_col.keys()
        }

    rows_counts: list[Dict[str, int]] = []
    missing_keywords: set[str] = set()

    # Parcours ligne par ligne (simple et robuste)
    for value in df[keywords_col]:
        # Init compteurs à 0
        counts = {col: 0 for col in target_cols}

        if pd.isna(value) or str(value).strip() == "":
            rows_counts.append(counts)
            continue

        # Parse all_keywords : peut être une string "[{...}, {...}]" ou déjà une liste
        try:
            if isinstance(value, str):
                keywords_list = literal_eval(value)
            else:
                keywords_list = value
        except (ValueError, SyntaxError, TypeError):
            keywords_list = []

        if not isinstance(keywords_list, list):
            keywords_list = []

        # Keywords uniques (on ignore les doublons de thèmes)
        unique_keywords: set[str] = set()
        for item in keywords_list:
            if isinstance(item, dict) and "keyword" in item:
                unique_keywords.add(item["keyword"])

        # Comptage par secteur
        for kw in unique_keywords:
            sectors = keyword_to_sectors.get(kw)
            if sectors is None:
                missing_keywords.add(kw)
                continue

            for field, col_name in sector_field_to_col.items():
                if sectors.get(field, False):
                    counts[col_name] += 1

        rows_counts.append(counts)

    # DataFrame des compteurs + concat
    counts_df = pd.DataFrame(rows_counts, index=df.index)
    df = pd.concat([df, counts_df[target_cols]], axis=1)

    # Stats simples
    print("✓ Keyword sector count columns created from 'all_keywords'")
    print(f"  Total articles: {len(df)}")
    print("  Keyword sector distribution (total keywords per sector):")
    for col in target_cols:
        # Conversion robuste : utiliser numpy pour la somme puis convertir en int Python
        total_sum = df[col].values.sum()
        total_int = int(total_sum) if not pd.isna(total_sum) else 0
        print(f"    - {col}: {total_int} keywords")

    if missing_keywords:
        warnings.warn(
            f"Found {len(missing_keywords)} keywords not in MACRO_CATEGORIES. "
            f"First 10: {list(missing_keywords)[:10]}"
        )

    return df

def apply_keyword_detection_to_dataframe(df: pd.DataFrame, text_col: str = "text") -> pd.DataFrame:
    """
    Apply keyword detection to a pandas DataFrame.
    
    Mimics the logic from extract_keywords_factiva.py but for pandas DataFrames.
    Creates all keyword-related columns (counts, lists, aggregations) and populates them
    using regex detection, preserving canonical forms from the dictionary.
    
    Args:
        df: DataFrame with articles to process
        text_col: Name of the column containing the text to analyze (default: "text")
        
    Returns:
        DataFrame with all keyword columns added
        
    Example:
        >>> df = pd.DataFrame({'text': ['article about canicules...'], 'an': ['ABC123']})
        >>> df_with_keywords = apply_keyword_detection_to_dataframe(df)
        >>> df_with_keywords['number_of_climat_no_hrfp']  # Access keyword counts
    """
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Check that text column exists
    if text_col not in df.columns:
        raise ValueError(f"Column '{text_col}' not found in DataFrame. Available columns: {df.columns.tolist()}")
    
    # Initialize all keyword columns with empty values
    keyword_columns = _get_all_keyword_columns()
    
    for col_name, default_value in keyword_columns.items():
        # For list columns, we need to create a new list for each row
        # Use None first, then fill in the loop to avoid index issues
        if isinstance(default_value, list):
            df[col_name] = None
        else:
            df[col_name] = default_value
    
    # Apply keyword extraction to each row
    print(f"Applying keyword detection to {len(df)} articles...")
    
    for row_num, (idx, row) in enumerate(df.iterrows(), start=1):
        text = row[text_col]
        
        # Convert to lowercase for matching (same as in extract_keywords_factiva.py)
        if pd.notna(text) and isinstance(text, str):
            text_lower = text.lower()
        else:
            text_lower = ""
        
        # Extract keyword data using the same function as the PostgreSQL job
        keyword_data = extract_keyword_data_from_article(text_lower)
        
        # Update all keyword columns for this row
        for col_name in keyword_columns.keys():
            df.at[idx, col_name] = keyword_data.get(col_name, keyword_columns[col_name])
        
        # Progress indicator every 100 rows
        if row_num % 100 == 0:
            print(f"  Processed {row_num}/{len(df)} articles...")
    
    print("Keyword detection complete!")
    
    return df


def _get_all_keyword_columns() -> Dict[str, any]:
    """
    Get all keyword column names with their default values.
    
    Returns:
        Dictionary mapping column names to default values
    """
    # All columns from factiva_models.py lines 122-196
    columns = {
        # Keyword counts - non HRFP (high risk of false positive) - UNIQUE keywords only
        "number_of_changement_climatique_constat_no_hrfp": 0,
        "number_of_changement_climatique_causes_no_hrfp": 0,
        "number_of_changement_climatique_consequences_no_hrfp": 0,
        "number_of_attenuation_climatique_solutions_no_hrfp": 0,
        "number_of_adaptation_climatique_solutions_no_hrfp": 0,
        "number_of_changement_climatique_solutions_no_hrfp": 0,  # Combined solutions
        "number_of_ressources_constat_no_hrfp": 0,
        "number_of_ressources_solutions_no_hrfp": 0,
        "number_of_biodiversite_concepts_generaux_no_hrfp": 0,
        "number_of_biodiversite_causes_no_hrfp": 0,
        "number_of_biodiversite_consequences_no_hrfp": 0,
        "number_of_biodiversite_solutions_no_hrfp": 0,
        
        # Keyword counts - HRFP (high risk of false positive) - UNIQUE keywords only
        "number_of_changement_climatique_constat_hrfp": 0,
        "number_of_changement_climatique_causes_hrfp": 0,
        "number_of_changement_climatique_consequences_hrfp": 0,
        "number_of_attenuation_climatique_solutions_hrfp": 0,
        "number_of_adaptation_climatique_solutions_hrfp": 0,
        "number_of_changement_climatique_solutions_hrfp": 0,  # Combined solutions
        "number_of_ressources_constat_hrfp": 0,
        "number_of_ressources_solutions_hrfp": 0,
        "number_of_biodiversite_concepts_generaux_hrfp": 0,
        "number_of_biodiversite_causes_hrfp": 0,
        "number_of_biodiversite_consequences_hrfp": 0,
        "number_of_biodiversite_solutions_hrfp": 0,
        
        # Aggregated counts by crisis type - non HRFP (sum of causal links)
        "number_of_climat_no_hrfp": 0,
        "number_of_ressources_no_hrfp": 0,
        "number_of_biodiversite_no_hrfp": 0,
        
        # Aggregated counts by crisis type - HRFP (sum of causal links)
        "number_of_climat_hrfp": 0,
        "number_of_ressources_hrfp": 0,
        "number_of_biodiversite_hrfp": 0,
        
        # Aggregated counts for ALL crises combined - non HRFP (unique keywords across all crises)
        "number_of_crises_no_hrfp": 0,
        "crises_keywords": [],  # All unique keywords from all crises (non-HRFP)
        
        # Aggregated counts for ALL crises combined - HRFP (unique keywords across all crises)
        "number_of_crises_hrfp": 0,
        "crises_keywords_hrfp": [],  # All unique keywords from all crises (HRFP)
        
        # Keyword lists by causal link - non HRFP - JSON arrays with ALL occurrences (including duplicates)
        "changement_climatique_constat_keywords": [],
        "changement_climatique_causes_keywords": [],
        "changement_climatique_consequences_keywords": [],
        "attenuation_climatique_solutions_keywords": [],
        "adaptation_climatique_solutions_keywords": [],
        "changement_climatique_solutions_keywords": [],  # Combined solutions
        "ressources_constat_keywords": [],
        "ressources_solutions_keywords": [],
        "biodiversite_concepts_generaux_keywords": [],
        "biodiversite_causes_keywords": [],
        "biodiversite_consequences_keywords": [],
        "biodiversite_solutions_keywords": [],
        
        # Keyword lists by causal link - HRFP - JSON arrays with ALL occurrences (including duplicates)
        "changement_climatique_constat_keywords_hrfp": [],
        "changement_climatique_causes_keywords_hrfp": [],
        "changement_climatique_consequences_keywords_hrfp": [],
        "attenuation_climatique_solutions_keywords_hrfp": [],
        "adaptation_climatique_solutions_keywords_hrfp": [],
        "changement_climatique_solutions_keywords_hrfp": [],  # Combined solutions
        "ressources_constat_keywords_hrfp": [],
        "ressources_solutions_keywords_hrfp": [],
        "biodiversite_concepts_generaux_keywords_hrfp": [],
        "biodiversite_causes_keywords_hrfp": [],
        "biodiversite_consequences_keywords_hrfp": [],
        "biodiversite_solutions_keywords_hrfp": [],
        
        # All keywords with full metadata (keyword, theme, category, count_keyword, is_hrfp)
        "all_keywords": [],
    }
    
    return columns


def parse_comma_separated(value) -> List[str]:
    """
    Parse a comma-separated string value into a list.
    
    Args:
        value: String with comma-separated values, or None/NaN
        
    Returns:
        List of stripped string values
        
    Example:
        >>> parse_comma_separated("climat, biodiversite")
        ['climat', 'biodiversite']
        >>> parse_comma_separated(None)
        []
    """
    if pd.isna(value) or str(value).strip() == "":
        return []
    # Handle separations by ", " or ","
    return [item.strip() for item in str(value).split(",")]


def create_llm_columns_from_crises(df: pd.DataFrame, prefix: str = "LLM") -> pd.DataFrame:
    """
    Create LLM binary columns from 'crises' and 'maillons_par_crise' columns.
    
    This function mimics the logic from 2.1_llm_new_prompt.py to create:
    - Binary columns for each crisis (LLM_climat, LLM_biodiversite, LLM_ressources)
    - Binary columns for each causal link (LLM_climat_constat, etc.)
    - Overall crisis indicators (LLM_CRISE, LLM_PAS DE CRISE)
    
    Args:
        df: DataFrame with 'crises' and 'maillons_par_crise' columns
        prefix: Prefix for the created columns (default: "LLM")
        
    Returns:
        DataFrame with added LLM columns
        
    Example:
        >>> df = pd.DataFrame({
        ...     'crises': ['climat, biodiversite', 'ressources', ''],
        ...     'maillons_par_crise': ['climat_constat, biodiversite_solution', 'ressources_constat', '']
        ... })
        >>> df = create_llm_columns_from_crises(df)
        >>> df['LLM_climat'].tolist()
        [1, 0, 0]
    """
    df = df.copy()
    
    # Check required columns exist
    if 'crises' not in df.columns or 'maillons_par_crise' not in df.columns:
        raise ValueError("DataFrame must have 'crises' and 'maillons_par_crise' columns")
    
    # Copy main columns
    df[f"{prefix}_crises"] = df["crises"]
    df[f"{prefix}_maillons_par_crise"] = df["maillons_par_crise"]
    
    # Binary columns for crises
    crises_list = ["biodiversite", "climat", "ressources"]
    for crise in crises_list:
        col_name = f"{prefix}_{crise}"
        df[col_name] = df["crises"].apply(
            lambda x: 1 if crise in parse_comma_separated(x) else 0
        )
    
    # Binary columns for causal links
    maillons_list = [
        "biodiversite_constat",
        "biodiversite_cause",
        "biodiversite_consequence",
        "biodiversite_solution",
        "climat_constat",
        "climat_cause",
        "climat_consequence",
        "climat_solution",
        "ressources_constat",
        "ressources_cause",
        "ressources_consequence",
        "ressources_solution",
    ]
    
    for maillon in maillons_list:
        col_name = f"{prefix}_{maillon}"
        df[col_name] = df["maillons_par_crise"].apply(
            lambda x: 1 if maillon in parse_comma_separated(x) else 0
        )
    
    # Overall crisis indicators
    df[f"{prefix}_CRISE"] = (
        df[[f"{prefix}_biodiversite", f"{prefix}_climat", f"{prefix}_ressources"]].sum(axis=1) >= 1
    ).astype(int)
    df[f"{prefix}_PAS DE CRISE"] = 1 - df[f"{prefix}_CRISE"]
    
    # Crisis without ressources: biodiversité OR climat (ignoring ressources completely)
    df[f"{prefix}_CRISE_WITHOUT_RESSOURCES"] = (
        (df[f"{prefix}_biodiversite"] == 1) | (df[f"{prefix}_climat"] == 1)
    ).astype(int)
    
    # Special column for ressources: concept = constat OR cause OR consequence
    df[f"{prefix}_ressources_concept"] = (
        (df[f"{prefix}_ressources_constat"] == 1) |
        (df[f"{prefix}_ressources_cause"] == 1) |
        (df[f"{prefix}_ressources_consequence"] == 1)
    ).astype(int)
    
    print(f"✓ LLM columns created with prefix '{prefix}'")
    print(f"  - Articles with crises: {df[f'{prefix}_CRISE'].sum()}")
    print(f"  - Articles without crisis: {df[f'{prefix}_PAS DE CRISE'].sum()}")
    print(f"  - Articles with crisis (without ressources): {df[f'{prefix}_CRISE_WITHOUT_RESSOURCES'].sum()}")
    
    return df


def calculate_keyword_score(
    df: pd.DataFrame,
    crisis: str,
    multiplier_hrfp: float = 1.0
) -> pd.Series:
    """
    Calculate keyword score for a given crisis with HRFP multiplier.
    
    Formula: score = number_of_{crisis}_no_hrfp + multiplier_hrfp * number_of_{crisis}_hrfp
    
    Args:
        df: DataFrame with keyword count columns
        crisis: Crisis name ('climat', 'biodiversite', or 'ressources')
        multiplier_hrfp: Multiplier for HRFP keywords (default: 1.0)
        
    Returns:
        Series with calculated scores
        
    Example:
        >>> scores = calculate_keyword_score(df, 'climat', multiplier_hrfp=0.5)
    """
    col_no_hrfp = f"number_of_{crisis}_no_hrfp"
    col_hrfp = f"number_of_{crisis}_hrfp"
    
    if col_no_hrfp not in df.columns or col_hrfp not in df.columns:
        raise ValueError(f"Missing columns for crisis '{crisis}': {col_no_hrfp} or {col_hrfp}")
    
    score = df[col_no_hrfp] + multiplier_hrfp * df[col_hrfp]
    return score


def add_at_least_one_non_hrfp_word_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a boolean column indicating if the article has at least one non-HRFP keyword.
    
    Args:
        df: DataFrame with keyword count columns
        
    Returns:
        DataFrame with added 'at_least_one_non_hrfp_word' column
        
    Example:
        >>> df = add_at_least_one_non_hrfp_word_column(df)
        >>> df['at_least_one_non_hrfp_word'].sum()  # Count articles with non-HRFP keywords
    """
    df = df.copy()
    
    # Check if the required column exists
    if 'number_of_crises_no_hrfp' not in df.columns:
        raise ValueError("Column 'number_of_crises_no_hrfp' not found in DataFrame")
    
    # Create boolean column: True if at least one non-HRFP keyword exists
    df['at_least_one_non_hrfp_word'] = df['number_of_crises_no_hrfp'] > 0
    
    print(f"✓ Column 'at_least_one_non_hrfp_word' added")
    print(f"  - Articles with at least one non-HRFP keyword: {df['at_least_one_non_hrfp_word'].sum()}")
    print(f"  - Articles without non-HRFP keywords: {(~df['at_least_one_non_hrfp_word']).sum()}")
    
    return df


def evaluate_threshold(
    y_true: pd.Series,
    scores: pd.Series,
    threshold: float,
    at_least_one_non_hrfp_word: pd.Series = None
) -> Dict[str, float]:
    """
    Evaluate a threshold by calculating precision, recall, and F1-score.
    
    Args:
        y_true: Ground truth labels (0 or 1)
        scores: Keyword scores
        threshold: Threshold to apply (score >= threshold → prediction = 1)
        at_least_one_non_hrfp_word: Boolean Series indicating if article has at least one non-HRFP keyword
                                    If provided, predictions require both score >= threshold AND this to be True
        
    Returns:
        Dictionary with precision, recall, f1_score, and counts
    """
    # Predictions based on threshold
    y_pred = (scores >= threshold).astype(int)
    
    # Apply additional condition: must have at least one non-HRFP keyword
    if at_least_one_non_hrfp_word is not None:
        y_pred = (y_pred & at_least_one_non_hrfp_word).astype(int)
    
    # Calculate metrics
    precision = precision_score(y_true, y_pred, zero_division=0)
    recall = recall_score(y_true, y_pred, zero_division=0)
    f1 = f1_score(y_true, y_pred, zero_division=0)
    
    # Additional statistics
    true_positives = ((y_true == 1) & (y_pred == 1)).sum()
    false_positives = ((y_true == 0) & (y_pred == 1)).sum()
    true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
    false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
    
    # Calculate precision/recall ratio
    precision_recall_ratio = precision / recall if recall > 0 else float('inf')
    
    return {
        'precision': precision,
        'recall': recall,
        'f1_score': f1,
        'precision_recall_ratio': precision_recall_ratio,
        'true_positives': true_positives,
        'false_positives': false_positives,
        'true_negatives': true_negatives,
        'false_negatives': false_negatives,
        'total_predicted_positive': y_pred.sum(),
        'total_true_positive': y_true.sum(),
    }


def test_thresholds_for_crisis(
    df: pd.DataFrame,
    crisis: str,
    multiplier_hrfp: float = 1.0,
    thresholds: List[float] = None,
    llm_prefix: str = "LLM"
) -> pd.DataFrame:
    """
    Test multiple thresholds for a given crisis and calculate metrics.
    
    Args:
        df: DataFrame with keyword counts and LLM labels
        crisis: Crisis name ('climat', 'biodiversite', or 'ressources')
        multiplier_hrfp: Multiplier for HRFP keywords
        thresholds: List of thresholds to test (default: 1 to 10)
        llm_prefix: Prefix for LLM columns (default: "LLM")
        
    Returns:
        DataFrame with results for each threshold
    """
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    # Get ground truth from LLM
    llm_col = f"{llm_prefix}_{crisis}"
    if llm_col not in df.columns:
        raise ValueError(f"Missing LLM column: {llm_col}. Run create_llm_columns_from_crises() first.")
    
    y_true = df[llm_col]
    
    # Calculate keyword scores
    scores = calculate_keyword_score(df, crisis, multiplier_hrfp)
    
    # Get at_least_one_non_hrfp_word column if it exists
    at_least_one_non_hrfp = df.get('at_least_one_non_hrfp_word', None)
    
    # Test each threshold
    results = []
    for threshold in thresholds:
        metrics = evaluate_threshold(y_true, scores, threshold, at_least_one_non_hrfp)
        metrics['threshold'] = threshold
        metrics['crisis'] = crisis
        metrics['multiplier_hrfp'] = multiplier_hrfp
        results.append(metrics)
    
    results_df = pd.DataFrame(results)
    return results_df


def test_thresholds_all_crises(
    df: pd.DataFrame,
    multipliers_hrfp: Dict[str, List[float]] = None,
    thresholds: List[float] = None,
    llm_prefix: str = "LLM"
) -> pd.DataFrame:
    """
    Test thresholds for all three crises with different HRFP multipliers.
    
    Args:
        df: DataFrame with keyword counts and LLM labels
        multipliers_hrfp: Dictionary mapping crisis names to lists of multipliers to test
                         Example: {'climat': [0, 0.5, 1], 'biodiversite': [0, 0.5, 1], 'ressources': [0, 0.5, 1]}
        thresholds: List of thresholds to test (default: 1 to 10)
        llm_prefix: Prefix for LLM columns (default: "LLM")
        
    Returns:
        DataFrame with all results
    """
    if multipliers_hrfp is None:
        multipliers_hrfp = {
            'climat': [0, 0.5, 1.0],
            'biodiversite': [0, 0.5, 1.0],
            'ressources': [0, 0.5, 1.0],
        }
    
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    all_results = []
    
    for crisis in ['climat', 'biodiversite', 'ressources']:
        # print(f"\nTesting thresholds for {crisis}...")
        multipliers = multipliers_hrfp.get(crisis, [1.0])
        
        for multiplier in multipliers:
            # print(f"  - Multiplier HRFP: {multiplier}")
            results = test_thresholds_for_crisis(
                df, crisis, multiplier, thresholds, llm_prefix
            )
            all_results.append(results)
    
    # Combine all results
    combined_results = pd.concat(all_results, ignore_index=True)
    return combined_results


def plot_threshold_metrics_by_multiplier(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None
) -> None:
    """
    Create visualization plots for threshold optimization results.
    
    For each multiplier, creates a figure with 3 subplots (one per crisis),
    showing Precision, Recall, and F1-Score curves with markers for:
    - Best F1-score (red star)
    - Best Precision/Recall ratio closest to 1 (green star)
    
    Args:
        results_df: DataFrame with results from test_thresholds_all_crises()
        thresholds_to_test: List of thresholds tested (for x-axis ticks)
    """
    
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df['threshold'].unique())
    
    crises = ['climat', 'biodiversite', 'ressources']
    crisis_labels = {
        'climat': 'Climat',
        'biodiversite': 'Biodiversité',
        'ressources': 'Ressources'
    }
    
    # Get all unique multipliers
    all_multipliers = sorted(results_df['multiplier_hrfp'].unique())
    
    for multiplier in all_multipliers:
        # Create figure with 3 subplots (one per crisis)
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        
        fig.suptitle(f'Multiplier HRFP = {multiplier} - Métriques par crise', 
                     fontsize=16, fontweight='bold', y=1.02)
        
        for idx, crisis in enumerate(crises):
            ax = axes[idx]
            
            # Filter data for this crisis and multiplier
            crisis_data = results_df[
                (results_df['crisis'] == crisis) & 
                (results_df['multiplier_hrfp'] == multiplier)
            ]
            
            # Plot the 3 metrics (Precision, Recall, F1-Score)
            ax.plot(crisis_data['threshold'], crisis_data['precision'], 
                   'o-', label='Precision', linewidth=2, markersize=6, color='#3498db')
            ax.plot(crisis_data['threshold'], crisis_data['recall'], 
                   's-', label='Recall', linewidth=2, markersize=6, color='#e67e22')
            ax.plot(crisis_data['threshold'], crisis_data['f1_score'], 
                   '^-', label='F1-Score', linewidth=2, markersize=6, color='#9b59b6')
            
            # Mark the best F1-score with a red star
            best_f1_idx = crisis_data['f1_score'].idxmax()
            if pd.notna(best_f1_idx):
                best_row = crisis_data.loc[best_f1_idx]
                ax.scatter(best_row['threshold'], best_row['f1_score'], 
                         s=200, marker='*', color='#e74c3c',
                         edgecolors='black', linewidths=1.5, zorder=5,
                         label=f"Best F1 (@{best_row['threshold']:.0f})")
                # Add vertical line at best threshold
                ax.axvline(x=best_row['threshold'], color='red', linestyle='--', 
                          alpha=0.3, linewidth=1)
            
            # Mark the best Precision/Recall ratio (closest to 1) with a green star
            # Calculate ratio and find where it's closest to 1
            crisis_data_copy = crisis_data.copy()
            # Avoid division by zero
            crisis_data_copy['precision_recall_ratio'] = crisis_data_copy.apply(
                lambda row: abs((row['precision'] / row['recall']) - 1) if row['recall'] > 0 else float('inf'),
                axis=1
            )
            best_ratio_idx = crisis_data_copy['precision_recall_ratio'].idxmin()
            
            if pd.notna(best_ratio_idx) and crisis_data_copy.loc[best_ratio_idx, 'precision_recall_ratio'] != float('inf'):
                best_ratio_row = crisis_data.loc[best_ratio_idx]
                # Get the actual ratio value
                actual_ratio = best_ratio_row['precision'] / best_ratio_row['recall'] if best_ratio_row['recall'] > 0 else 0
                
                # Plot green star at the average of precision and recall for this point
                avg_metric = (best_ratio_row['precision'] + best_ratio_row['recall']) / 2
                ax.scatter(best_ratio_row['threshold'], avg_metric, 
                         s=200, marker='*', color='#27ae60',
                         edgecolors='black', linewidths=1.5, zorder=5,
                         label=f"Best P/R ratio (@{best_ratio_row['threshold']:.0f}, ratio={actual_ratio:.2f})")
                # Add vertical line at best ratio threshold
                ax.axvline(x=best_ratio_row['threshold'], color='green', linestyle=':', 
                          alpha=0.3, linewidth=1)
            
            ax.set_xlabel('Seuil', fontsize=12)
            ax.set_ylabel('Score', fontsize=12)
            ax.set_title(crisis_labels[crisis], fontsize=13, fontweight='bold')
            ax.legend(loc='best', fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.set_ylim(-0.05, 1.05)
            ax.set_xticks(thresholds_to_test)
        
        plt.tight_layout()
        plt.show()


def plot_nb_predicted_articles_by_multiplier(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None
) -> None:
    """
    Create visualization plots showing the number of predicted articles vs threshold.
    
    For each multiplier, creates a figure with 3 subplots (one per crisis),
    showing the evolution of predicted articles (decreasing curve) and a horizontal
    dashed line indicating the number of articles predicted by LLM.
    
    Args:
        results_df: DataFrame with results from test_thresholds_all_crises()
                   Must contain columns: threshold, crisis, multiplier_hrfp,
                   total_predicted_positive, total_true_positive
        thresholds_to_test: List of thresholds tested (for x-axis ticks)
    """
    
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df['threshold'].unique())
    
    crises = ['climat', 'biodiversite', 'ressources']
    crisis_labels = {
        'climat': 'Climat',
        'biodiversite': 'Biodiversité',
        'ressources': 'Ressources'
    }
    
    # Get all unique multipliers
    all_multipliers = sorted(results_df['multiplier_hrfp'].unique())
    
    for multiplier in all_multipliers:
        # Create figure with 3 subplots (one per crisis)
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        
        fig.suptitle(f'Multiplier HRFP = {multiplier} - Nombre d\'articles prédits par crise', 
                     fontsize=16, fontweight='bold', y=1.02)
        
        for idx, crisis in enumerate(crises):
            ax = axes[idx]
            
            # Filter data for this crisis and multiplier
            crisis_data = results_df[
                (results_df['crisis'] == crisis) & 
                (results_df['multiplier_hrfp'] == multiplier)
            ].copy()
            
            # Sort by threshold for proper line plotting
            crisis_data = crisis_data.sort_values('threshold')
            
            # Plot the number of predicted articles (decreasing curve)
            ax.plot(crisis_data['threshold'], crisis_data['total_predicted_positive'], 
                   'o-', label='Articles prédits (modèle)', linewidth=2.5, 
                   markersize=7, color='#3498db')
            
            # Get the LLM reference value (should be constant for all thresholds)
            llm_value = crisis_data['total_true_positive'].iloc[0]
            
            # Plot horizontal dashed line for LLM predictions
            ax.axhline(y=llm_value, color='#e74c3c', linestyle='--', 
                      linewidth=2, label=f'Articles prédits (LLM) = {llm_value}',
                      alpha=0.7)
            
            # Find and mark the threshold where model prediction is closest to LLM
            crisis_data['diff_from_llm'] = abs(crisis_data['total_predicted_positive'] - llm_value)
            closest_idx = crisis_data['diff_from_llm'].idxmin()
            
            if pd.notna(closest_idx):
                closest_row = crisis_data.loc[closest_idx]
                ax.scatter(closest_row['threshold'], closest_row['total_predicted_positive'], 
                         s=250, marker='*', color='#27ae60',
                         edgecolors='black', linewidths=1.5, zorder=5,
                         label=f"Plus proche LLM (@{closest_row['threshold']:.0f})")
                # Add vertical line at closest threshold
                ax.axvline(x=closest_row['threshold'], color='green', linestyle=':', 
                          alpha=0.3, linewidth=1.5)
            
            ax.set_xlabel('Seuil', fontsize=12)
            ax.set_ylabel('Nombre d\'articles', fontsize=12)
            ax.set_title(crisis_labels[crisis], fontsize=13, fontweight='bold')
            ax.legend(loc='best', fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.set_xticks(thresholds_to_test)
            
            # Set y-axis limits with some margin
            y_min = min(crisis_data['total_predicted_positive'].min(), llm_value) * 0.9
            y_max = max(crisis_data['total_predicted_positive'].max(), llm_value) * 1.1
            ax.set_ylim(y_min, y_max)
        
        plt.tight_layout()
        plt.show()


def test_thresholds_for_sector(
    df: pd.DataFrame,
    sector: str,
    thresholds: List[float] = None,
) -> pd.DataFrame:
    """
    Test multiple thresholds for a given sector using keyword counts.
    
    This mirrors ``test_thresholds_for_crisis`` but:
    - uses sector LLM columns (e.g. ``llm_agri_alim``),
    - uses keyword count columns (e.g. ``nb_agri_alim``) directly as scores,
    - does NOT use any HRFP multiplier.
    
    Args:
        df: DataFrame with sector keyword counts and LLM sector labels
        sector: Sector suffix (e.g. ``"agri_alim"``, ``"batiment"``)
        thresholds: List of thresholds to test (default: 1 to 10)
    
    Returns:
        DataFrame with results for each threshold
    """
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    llm_col = f"llm_{sector}"
    score_col = f"nb_{sector}"
    
    if llm_col not in df.columns:
        raise ValueError(
            f"Missing LLM sector column: {llm_col}. "
            "Run create_llm_columns_from_secteurs() first."
        )
    if score_col not in df.columns:
        raise ValueError(
            f"Missing keyword count column for sector '{sector}': {score_col}. "
            "You must run create_keyword_sector_columns() first."
        )
    
    y_true = df[llm_col]
    scores = df[score_col]
    
    results: List[Dict[str, float]] = []
    for threshold in thresholds:
        metrics = evaluate_threshold(y_true, scores, threshold)
        metrics["threshold"] = threshold
        metrics["sector"] = sector
        results.append(metrics)
    
    return pd.DataFrame(results)


def test_thresholds_all_sectors(
    df: pd.DataFrame,
    thresholds: List[float] = None,
) -> pd.DataFrame:
    """
    Test thresholds for all 8 sectors using keyword counts.
    
    This is the sector‑level equivalent of ``test_thresholds_all_crises`` but:
    - uses the binary sector columns from ``create_llm_columns_from_secteurs()``,
    - uses the keyword count columns from ``create_keyword_sector_columns()``,
    - does NOT use any HRFP multiplier (``nb_*`` columns are used directly as scores).
    
    Args:
        df: DataFrame with sector keyword counts and LLM sector labels
        thresholds: List of thresholds to test (default: 1 to 10)
    
    Returns:
        DataFrame with all results (one row per sector/threshold)
    """
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    # Suffixes correspond to:
    #  - llm_<suffix>  (LLM prediction per sector)
    #  - nb_<suffix>   (keyword count per sector)
    sectors = [
        "agri_alim",
        "batiment",
        "economie_circulaire",
        "mobilite",
        "eau",
        "ecosysteme",
        "energie",
        "industrie",
    ]
    
    all_results: List[pd.DataFrame] = []
    for sector in sectors:
        sector_results = test_thresholds_for_sector(df, sector, thresholds)
        all_results.append(sector_results)
    
    return pd.concat(all_results, ignore_index=True)


def plot_threshold_metrics_by_sector(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None,
) -> None:
    """
    Plot Precision, Recall and F1‑Score per sector as a function of the threshold.
    
    This is the sector‑level equivalent of ``plot_threshold_metrics_by_multiplier``,
    but without any HRFP multiplier dimension.
    
    Args:
        results_df: DataFrame with results from ``test_thresholds_all_sectors()``
        thresholds_to_test: List of thresholds tested (for x‑axis ticks)
    """
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df["threshold"].unique())
    
    sectors = [
        "agri_alim",
        "batiment",
        "economie_circulaire",
        "mobilite",
        "eau",
        "ecosysteme",
        "energie",
        "industrie",
    ]
    sector_labels = {
        "agri_alim": "Agriculture & alimentation",
        "batiment": "Bâtiment & aménagement",
        "economie_circulaire": "Économie circulaire",
        "mobilite": "Mobilité",
        "eau": "Eau",
        "ecosysteme": "Écosystème",
        "energie": "Énergie",
        "industrie": "Industrie",
    }
    
    n_sectors = len(sectors)
    n_rows, n_cols = 2, 4
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 8))
    axes = axes.flatten()
    
    fig.suptitle(
        "Métriques par secteur en fonction du seuil",
        fontsize=16,
        fontweight="bold",
        y=1.02,
    )
    
    for idx, sector in enumerate(sectors):
        ax = axes[idx]
        sector_data = results_df[results_df["sector"] == sector].copy()
        if sector_data.empty:
            ax.set_visible(False)
            continue
        
        sector_data = sector_data.sort_values("threshold")
        
        # Plot metrics
        ax.plot(
            sector_data["threshold"],
            sector_data["precision"],
            "o-",
            label="Precision",
            linewidth=2,
            markersize=6,
            color="#3498db",
        )
        ax.plot(
            sector_data["threshold"],
            sector_data["recall"],
            "s-",
            label="Recall",
            linewidth=2,
            markersize=6,
            color="#e67e22",
        )
        ax.plot(
            sector_data["threshold"],
            sector_data["f1_score"],
            "^-",
            label="F1-Score",
            linewidth=2,
            markersize=6,
            color="#9b59b6",
        )
        
        # Mark best F1‑score
        best_f1_idx = sector_data["f1_score"].idxmax()
        if pd.notna(best_f1_idx):
            best_row = sector_data.loc[best_f1_idx]
            ax.scatter(
                best_row["threshold"],
                best_row["f1_score"],
                s=200,
                marker="*",
                color="#e74c3c",
                edgecolors="black",
                linewidths=1.5,
                zorder=5,
                label=f"Best F1 (@{best_row['threshold']:.0f})",
            )
            ax.axvline(
                x=best_row["threshold"],
                color="red",
                linestyle="--",
                alpha=0.3,
                linewidth=1,
            )
        
        # Mark best Precision/Recall ratio (closest to 1)
        sector_data_copy = sector_data.copy()
        sector_data_copy["precision_recall_ratio"] = sector_data_copy.apply(
            lambda row: abs((row["precision"] / row["recall"]) - 1)
            if row["recall"] > 0
            else float("inf"),
            axis=1,
        )
        best_ratio_idx = sector_data_copy["precision_recall_ratio"].idxmin()
        if (
            pd.notna(best_ratio_idx)
            and sector_data_copy.loc[best_ratio_idx, "precision_recall_ratio"]
            != float("inf")
        ):
            best_ratio_row = sector_data.loc[best_ratio_idx]
            actual_ratio = (
                best_ratio_row["precision"] / best_ratio_row["recall"]
                if best_ratio_row["recall"] > 0
                else 0
            )
            avg_metric = (
                best_ratio_row["precision"] + best_ratio_row["recall"]
            ) / 2
            ax.scatter(
                best_ratio_row["threshold"],
                avg_metric,
                s=200,
                marker="*",
                color="#27ae60",
                edgecolors="black",
                linewidths=1.5,
                zorder=5,
                label=(
                    f"Best P/R ratio (@{best_ratio_row['threshold']:.0f}, "
                    f"ratio={actual_ratio:.2f})"
                ),
            )
            ax.axvline(
                x=best_ratio_row["threshold"],
                color="green",
                linestyle=":",
                alpha=0.3,
                linewidth=1,
            )
        
        ax.set_xlabel("Threshold", fontsize=12)
        ax.set_ylabel("Score", fontsize=12)
        ax.set_title(sector_labels.get(sector, sector), fontsize=13, fontweight="bold")
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(-0.05, 1.05)
        ax.set_xticks(thresholds_to_test)
    
    # Hide any unused subplots (should not happen but for safety)
    for j in range(n_sectors, len(axes)):
        axes[j].set_visible(False)
    
    plt.tight_layout()
    plt.show()


def plot_nb_predicted_articles_by_sector(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None,
) -> None:
    """
    Plot the number of predicted articles vs threshold for each sector.
    
    This is the sector‑level equivalent of ``plot_nb_predicted_articles_by_multiplier``,
    but without any HRFP multiplier dimension.
    
    Args:
        results_df: DataFrame with results from ``test_thresholds_all_sectors()``
                    Must contain columns: threshold, sector,
                    total_predicted_positive, total_true_positive
        thresholds_to_test: List of thresholds tested (for x‑axis ticks)
    """
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df["threshold"].unique())
    
    sectors = [
        "agri_alim",
        "batiment",
        "economie_circulaire",
        "mobilite",
        "eau",
        "ecosysteme",
        "energie",
        "industrie",
    ]
    sector_labels = {
        "agri_alim": "Agriculture & alimentation",
        "batiment": "Bâtiment & aménagement",
        "economie_circulaire": "Économie circulaire",
        "mobilite": "Mobilité",
        "eau": "Eau",
        "ecosysteme": "Écosystème",
        "energie": "Énergie",
        "industrie": "Industrie",
    }
    
    n_sectors = len(sectors)
    n_rows, n_cols = 2, 4
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 8))
    axes = axes.flatten()
    
    fig.suptitle(
        "Nombre d'articles prédits par secteur",
        fontsize=16,
        fontweight="bold",
        y=1.02,
    )
    
    for idx, sector in enumerate(sectors):
        ax = axes[idx]
        sector_data = results_df[results_df["sector"] == sector].copy()
        if sector_data.empty:
            ax.set_visible(False)
            continue
        
        sector_data = sector_data.sort_values("threshold")
        
        # Predicted articles curve
        ax.plot(
            sector_data["threshold"],
            sector_data["total_predicted_positive"],
            "o-",
            label="Articles prédits (modèle)",
            linewidth=2.5,
            markersize=7,
            color="#3498db",
        )
        
        # LLM reference value (constant for all thresholds)
        llm_value = sector_data["total_true_positive"].iloc[0]
        ax.axhline(
            y=llm_value,
            color="#e74c3c",
            linestyle="--",
            linewidth=2,
            label=f"Articles prédits (LLM) = {llm_value}",
            alpha=0.7,
        )
        
        # Threshold where model prediction is closest to LLM
        sector_data["diff_from_llm"] = abs(
            sector_data["total_predicted_positive"] - llm_value
        )
        closest_idx = sector_data["diff_from_llm"].idxmin()
        if pd.notna(closest_idx):
            closest_row = sector_data.loc[closest_idx]
            ax.scatter(
                closest_row["threshold"],
                closest_row["total_predicted_positive"],
                s=250,
                marker="*",
                color="#27ae60",
                edgecolors="black",
                linewidths=1.5,
                zorder=5,
                label=f"Plus proche LLM (@{closest_row['threshold']:.0f})",
            )
            ax.axvline(
                x=closest_row["threshold"],
                color="green",
                linestyle=":",
                alpha=0.3,
                linewidth=1.5,
            )
        
        ax.set_xlabel("Threshold", fontsize=12)
        ax.set_ylabel("Nombre d'articles", fontsize=12)
        ax.set_title(sector_labels.get(sector, sector), fontsize=13, fontweight="bold")
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_xticks(thresholds_to_test)
        
        # y‑axis limits with some margin
        y_min = min(sector_data["total_predicted_positive"].min(), llm_value) * 0.9
        y_max = max(sector_data["total_predicted_positive"].max(), llm_value) * 1.1
        ax.set_ylim(y_min, y_max)
    
    # Hide any unused subplots (for safety)
    for j in range(n_sectors, len(axes)):
        axes[j].set_visible(False)
    
    plt.tight_layout()
    plt.show()


def get_causal_link_mapping() -> Dict[str, Dict[str, str]]:
    """
    Get mapping from LLM causal link names to keyword column prefixes.
    
    Returns:
        Dictionary mapping crisis types to their causal links and column prefixes
        
    Example:
        >>> mapping = get_causal_link_mapping()
        >>> mapping['climat']['constat']
        'changement_climatique_constat'
    """
    return {
        'climat': {
            'constat': 'changement_climatique_constat',
            'cause': 'changement_climatique_causes',
            'consequence': 'changement_climatique_consequences',
            'solution': 'changement_climatique_solutions',
        },
        'biodiversite': {
            'constat': 'biodiversite_concepts_generaux',
            'cause': 'biodiversite_causes',
            'consequence': 'biodiversite_consequences',
            'solution': 'biodiversite_solutions',
        },
        'ressources': {
            'concept': 'ressources_constat',  # Special: concept uses constat column
            'solution': 'ressources_solutions',
        }
    }


def calculate_causal_link_score(
    df: pd.DataFrame,
    crisis: str,
    causal_link: str,
    multiplier_hrfp: float = 1.0
) -> pd.Series:
    """
    Calculate keyword score for a given causal link with HRFP multiplier.
    
    Args:
        df: DataFrame with keyword count columns
        crisis: Crisis name ('climat', 'biodiversite', or 'ressources')
        causal_link: Causal link name ('constat', 'cause', 'consequence', 'solution', or 'concept')
        multiplier_hrfp: Multiplier for HRFP keywords (default: 1.0)
        
    Returns:
        Series with calculated scores
    """
    mapping = get_causal_link_mapping()
    
    if crisis not in mapping:
        raise ValueError(f"Unknown crisis: {crisis}")
    
    if causal_link not in mapping[crisis]:
        raise ValueError(f"Unknown causal link '{causal_link}' for crisis '{crisis}'")
    
    col_prefix = mapping[crisis][causal_link]
    col_no_hrfp = f"number_of_{col_prefix}_no_hrfp"
    col_hrfp = f"number_of_{col_prefix}_hrfp"
    
    if col_no_hrfp not in df.columns or col_hrfp not in df.columns:
        raise ValueError(f"Missing columns for causal link: {col_no_hrfp} or {col_hrfp}")
    
    score = df[col_no_hrfp] + multiplier_hrfp * df[col_hrfp]
    return score


def get_optimal_crisis_threshold(
    results_df: pd.DataFrame,
    crisis: str,
    multiplier_hrfp: float
) -> float:
    """
    Get the optimal threshold for a crisis (where P/R ratio is closest to 1).
    
    Args:
        results_df: Results from test_thresholds_all_crises()
        crisis: Crisis name
        multiplier_hrfp: HRFP multiplier used
        
    Returns:
        Optimal threshold value
    """
    crisis_data = results_df[
        (results_df['crisis'] == crisis) &
        (results_df['multiplier_hrfp'] == multiplier_hrfp)
    ].copy()
    
    # Find threshold where |P/R - 1| is minimal
    crisis_data['pr_diff'] = abs(crisis_data['precision_recall_ratio'] - 1)
    best_idx = crisis_data['pr_diff'].idxmin()
    
    if pd.notna(best_idx):
        return crisis_data.loc[best_idx, 'threshold']
    else:
        # Fallback to best F1 if P/R calculation failed
        best_f1_idx = crisis_data['f1_score'].idxmax()
        return crisis_data.loc[best_f1_idx, 'threshold']


def test_thresholds_for_causal_link(
    df: pd.DataFrame,
    crisis: str,
    causal_link: str,
    crisis_threshold: float,
    multiplier_hrfp: float = 1.0,
    thresholds: List[float] = None,
    llm_prefix: str = "LLM"
) -> pd.DataFrame:
    """
    Test multiple thresholds for a causal link, with crisis threshold as prerequisite.
    
    Args:
        df: DataFrame with keyword counts and LLM labels
        crisis: Crisis name ('climat', 'biodiversite', or 'ressources')
        causal_link: Causal link ('constat', 'cause', 'consequence', 'solution', or 'concept')
        crisis_threshold: Fixed threshold for the crisis (prerequisite)
        multiplier_hrfp: Multiplier for HRFP keywords
        thresholds: List of thresholds to test (default: 1 to 10)
        llm_prefix: Prefix for LLM columns (default: "LLM")
        
    Returns:
        DataFrame with results for each threshold
    """
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    # Get ground truth from LLM
    llm_col = f"{llm_prefix}_{crisis}_{causal_link}"
    if llm_col not in df.columns:
        raise ValueError(f"Missing LLM column: {llm_col}")
    
    y_true = df[llm_col]
    
    # Calculate crisis and causal link scores
    crisis_score = calculate_keyword_score(df, crisis, multiplier_hrfp)
    causal_link_score = calculate_causal_link_score(df, crisis, causal_link, multiplier_hrfp)
    
    # Get at_least_one_non_hrfp_word column if it exists
    at_least_one_non_hrfp = df.get('at_least_one_non_hrfp_word', pd.Series([True] * len(df), index=df.index))
    
    # Test each threshold
    results = []
    for threshold in thresholds:
        # Prediction: crisis must be >= crisis_threshold AND causal_link >= threshold
        # AND at_least_one_non_hrfp_word must be True
        y_pred = (
            (crisis_score >= crisis_threshold) & 
            (causal_link_score >= threshold) & 
            at_least_one_non_hrfp
        ).astype(int)
        
        # Calculate metrics directly (don't use evaluate_threshold as it would re-apply threshold)
        
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        
        # Calculate precision/recall ratio
        precision_recall_ratio = precision / recall if recall > 0 else float('inf')
        
        # Additional statistics
        true_positives = ((y_true == 1) & (y_pred == 1)).sum()
        false_positives = ((y_true == 0) & (y_pred == 1)).sum()
        true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
        false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
        
        metrics = {
            'threshold': threshold,
            'crisis': crisis,
            'causal_link': causal_link,
            'crisis_threshold': crisis_threshold,
            'multiplier_hrfp': multiplier_hrfp,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'precision_recall_ratio': precision_recall_ratio,
            'true_positives': true_positives,
            'false_positives': false_positives,
            'true_negatives': true_negatives,
            'false_negatives': false_negatives,
            'total_predicted_positive': y_pred.sum(),
            'total_true_positive': y_true.sum(),
        }
        results.append(metrics)
    
    results_df = pd.DataFrame(results)
    return results_df


def test_thresholds_all_causal_links(
    df: pd.DataFrame,
    crisis_results_df: pd.DataFrame,
    multipliers_hrfp: Dict[str, List[float]] = None,
    thresholds: List[float] = None,
    llm_prefix: str = "LLM"
) -> pd.DataFrame:
    """
    Test thresholds for all causal links across all crises.
    
    For each crisis and multiplier, uses the optimal crisis threshold (best P/R ratio),
    then tests different thresholds for each causal link.
    
    Args:
        df: DataFrame with keyword counts and LLM labels
        crisis_results_df: Results from test_thresholds_all_crises() (to get optimal crisis thresholds)
        multipliers_hrfp: Dictionary mapping crisis names to lists of multipliers
        thresholds: List of thresholds to test (default: 1 to 10)
        llm_prefix: Prefix for LLM columns (default: "LLM")
        
    Returns:
        DataFrame with all results for causal links
    """
    if multipliers_hrfp is None:
        multipliers_hrfp = {
            'climat': [0, 0.5, 1.0],
            'biodiversite': [0, 0.5, 1.0],
            'ressources': [0, 0.5, 1.0],
        }
    
    if thresholds is None:
        thresholds = list(range(1, 11))
    
    causal_links_by_crisis = {
        'climat': ['constat', 'cause', 'consequence', 'solution'],
        'biodiversite': ['constat', 'cause', 'consequence', 'solution'],
        'ressources': ['concept', 'solution'],
    }
    
    all_results = []
    
    for crisis in ['climat', 'biodiversite', 'ressources']:
        # print(f"\nTesting causal links for {crisis}...")
        multipliers = multipliers_hrfp.get(crisis, [1.0])
        
        for multiplier in multipliers:
            # print(f"  - Multiplier HRFP: {multiplier}")
            
            # Get optimal crisis threshold for this crisis and multiplier
            crisis_threshold = get_optimal_crisis_threshold(
                crisis_results_df, crisis, multiplier
            )
            # print(f"    Using crisis threshold: {crisis_threshold} (best P/R ratio)")
            
            # Test each causal link
            for causal_link in causal_links_by_crisis[crisis]:
                # print(f"      Testing {causal_link}...")
                results = test_thresholds_for_causal_link(
                    df, crisis, causal_link, crisis_threshold,
                    multiplier, thresholds, llm_prefix
                )
                all_results.append(results)
    
    # Combine all results
    combined_results = pd.concat(all_results, ignore_index=True)
    return combined_results


def plot_causal_link_nb_predicted_articles_by_multiplier(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None
) -> None:
    """
    Create visualization plots showing the number of predicted articles vs threshold for causal links.
    
    For each multiplier, creates a figure with 10 subplots:
    - 4 for climat (constat, cause, consequence, solution)
    - 4 for biodiversité (constat, cause, consequence, solution)
    - 2 for ressources (concept, solution)
    
    Shows the evolution of predicted articles (decreasing curve) and a horizontal
    dashed line indicating the number of articles predicted by LLM.
    
    Args:
        results_df: DataFrame with results from test_thresholds_all_causal_links()
                   Must contain columns: threshold, crisis, causal_link, multiplier_hrfp,
                   total_predicted_positive, total_true_positive
        thresholds_to_test: List of thresholds tested (for x-axis ticks)
    """
    
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df['threshold'].unique())
    
    causal_link_labels = {
        'constat': 'Constat',
        'cause': 'Cause',
        'consequence': 'Conséquence',
        'solution': 'Solution',
        'concept': 'Concept',
    }
    
    # Get all unique multipliers
    all_multipliers = sorted(results_df['multiplier_hrfp'].unique())
    
    for multiplier in all_multipliers:
        # Create figure with 10 subplots (4 climat + 4 biodiv + 2 ressources)
        fig, axes = plt.subplots(2, 5, figsize=(25, 10))
        axes = axes.flatten()
        
        fig.suptitle(f'Maillons Causaux - Multiplier HRFP = {multiplier} - Nombre d\'articles prédits', 
                     fontsize=18, fontweight='bold', y=0.995)
        
        subplot_idx = 0
        
        # Define the order for causal links (not alphabetical!)
        causal_links_order = {
            'climat': ['constat', 'cause', 'consequence', 'solution'],
            'biodiversite': ['constat', 'cause', 'consequence', 'solution'],
            'ressources': ['concept', 'solution'],
        }
        
        # Plot for each crisis
        for crisis in ['climat', 'biodiversite', 'ressources']:
            crisis_data = results_df[
                (results_df['crisis'] == crisis) &
                (results_df['multiplier_hrfp'] == multiplier)
            ]
            
            # Use predefined order instead of sorted
            causal_links = [link for link in causal_links_order[crisis] 
                          if link in crisis_data['causal_link'].unique()]
            
            for causal_link in causal_links:
                if subplot_idx >= 10:
                    break
                    
                ax = axes[subplot_idx]
                subplot_idx += 1
                
                # Filter data for this causal link
                link_data = crisis_data[crisis_data['causal_link'] == causal_link].copy()
                
                if len(link_data) == 0:
                    ax.axis('off')
                    continue
                
                # Sort by threshold for proper line plotting
                link_data = link_data.sort_values('threshold')
                
                # Get crisis threshold used
                crisis_threshold = link_data['crisis_threshold'].iloc[0]
                
                # Plot the number of predicted articles (decreasing curve)
                ax.plot(link_data['threshold'], link_data['total_predicted_positive'], 
                       'o-', label='Articles prédits (modèle)', linewidth=2.5, 
                       markersize=7, color='#3498db')
                
                # Get the LLM reference value (should be constant for all thresholds)
                llm_value = link_data['total_true_positive'].iloc[0]
                
                # Plot horizontal dashed line for LLM predictions
                ax.axhline(y=llm_value, color='#e74c3c', linestyle='--', 
                          linewidth=2, label=f'Articles prédits (LLM) = {llm_value}',
                          alpha=0.7)
                
                # Find and mark the threshold where model prediction is closest to LLM
                link_data['diff_from_llm'] = abs(link_data['total_predicted_positive'] - llm_value)
                closest_idx = link_data['diff_from_llm'].idxmin()
                
                if pd.notna(closest_idx):
                    closest_row = link_data.loc[closest_idx]
                    ax.scatter(closest_row['threshold'], closest_row['total_predicted_positive'], 
                             s=250, marker='*', color='#27ae60',
                             edgecolors='black', linewidths=1.5, zorder=5,
                             label=f"Plus proche LLM (@{closest_row['threshold']:.0f})")
                    # Add vertical line at closest threshold
                    ax.axvline(x=closest_row['threshold'], color='green', linestyle=':', 
                              alpha=0.3, linewidth=1.5)
                
                ax.set_xlabel('Threshold (maillon)', fontsize=10)
                ax.set_ylabel('Nombre d\'articles', fontsize=10)
                ax.set_title(f"{crisis.capitalize()} - {causal_link_labels[causal_link]}\n(Seuil crise: {crisis_threshold})", 
                            fontsize=11, fontweight='bold')
                ax.legend(loc='best', fontsize=8)
                ax.grid(True, alpha=0.3)
                ax.set_xticks(thresholds_to_test)
                
                # Set y-axis limits with some margin
                y_min = min(link_data['total_predicted_positive'].min(), llm_value) * 0.9
                y_max = max(link_data['total_predicted_positive'].max(), llm_value) * 1.1
                ax.set_ylim(y_min, y_max)
        
        # Hide unused subplots
        for idx in range(subplot_idx, 10):
            axes[idx].axis('off')
        
        plt.tight_layout()
        plt.show()


def plot_causal_link_metrics_by_multiplier(
    results_df: pd.DataFrame,
    thresholds_to_test: List[float] = None
) -> None:
    """
    Create visualization plots for causal link threshold optimization.
    
    For each multiplier, creates a figure with 10 subplots:
    - 4 for climat (constat, cause, consequence, solution)
    - 4 for biodiversité (constat, cause, consequence, solution)
    - 2 for ressources (concept, solution)
    
    Shows Precision, Recall, and F1-Score curves with markers for:
    - Best F1-score (red star)
    - Best Precision/Recall ratio closest to 1 (green star)
    
    Args:
        results_df: DataFrame with results from test_thresholds_all_causal_links()
        thresholds_to_test: List of thresholds tested (for x-axis ticks)
    """
    
    if thresholds_to_test is None:
        thresholds_to_test = sorted(results_df['threshold'].unique())
    
    causal_link_labels = {
        'constat': 'Constat',
        'cause': 'Cause',
        'consequence': 'Conséquence',
        'solution': 'Solution',
        'concept': 'Concept',
    }
    
    # Get all unique multipliers
    all_multipliers = sorted(results_df['multiplier_hrfp'].unique())
    
    for multiplier in all_multipliers:
        # Create figure with 10 subplots (4 climat + 4 biodiv + 2 ressources)
        fig, axes = plt.subplots(2, 5, figsize=(25, 10))
        axes = axes.flatten()
        
        fig.suptitle(f'Maillons Causaux - Multiplier HRFP = {multiplier}', 
                     fontsize=18, fontweight='bold', y=0.995)
        
        subplot_idx = 0
        
        # Define the order for causal links (not alphabetical!)
        causal_links_order = {
            'climat': ['constat', 'cause', 'consequence', 'solution'],
            'biodiversite': ['constat', 'cause', 'consequence', 'solution'],
            'ressources': ['concept', 'solution'],
        }
        
        # Plot for each crisis
        for crisis in ['climat', 'biodiversite', 'ressources']:
            crisis_data = results_df[
                (results_df['crisis'] == crisis) &
                (results_df['multiplier_hrfp'] == multiplier)
            ]
            
            # Use predefined order instead of sorted
            causal_links = [link for link in causal_links_order[crisis] 
                          if link in crisis_data['causal_link'].unique()]
            
            for causal_link in causal_links:
                if subplot_idx >= 10:
                    break
                    
                ax = axes[subplot_idx]
                subplot_idx += 1
                
                # Filter data for this causal link
                link_data = crisis_data[crisis_data['causal_link'] == causal_link]
                
                if len(link_data) == 0:
                    ax.axis('off')
                    continue
                
                # Get crisis threshold used
                crisis_threshold = link_data['crisis_threshold'].iloc[0]
                
                # Plot the 3 metrics
                ax.plot(link_data['threshold'], link_data['precision'], 
                       'o-', label='Precision', linewidth=2, markersize=6, color='#3498db')
                ax.plot(link_data['threshold'], link_data['recall'], 
                       's-', label='Recall', linewidth=2, markersize=6, color='#e67e22')
                ax.plot(link_data['threshold'], link_data['f1_score'], 
                       '^-', label='F1-Score', linewidth=2, markersize=6, color='#9b59b6')
                
                # Mark best F1-score with red star
                best_f1_idx = link_data['f1_score'].idxmax()
                if pd.notna(best_f1_idx):
                    best_row = link_data.loc[best_f1_idx]
                    ax.scatter(best_row['threshold'], best_row['f1_score'], 
                             s=150, marker='*', color='#e74c3c',
                             edgecolors='black', linewidths=1.5, zorder=5)
                    ax.axvline(x=best_row['threshold'], color='red', linestyle='--', 
                              alpha=0.3, linewidth=1)
                
                # Mark best P/R ratio with green star
                link_data_copy = link_data.copy()
                link_data_copy['pr_diff'] = abs(link_data_copy['precision_recall_ratio'] - 1)
                best_ratio_idx = link_data_copy['pr_diff'].idxmin()
                
                if pd.notna(best_ratio_idx):
                    best_ratio_row = link_data.loc[best_ratio_idx]
                    avg_metric = (best_ratio_row['precision'] + best_ratio_row['recall']) / 2
                    ax.scatter(best_ratio_row['threshold'], avg_metric, 
                             s=150, marker='*', color='#27ae60',
                             edgecolors='black', linewidths=1.5, zorder=5)
                    ax.axvline(x=best_ratio_row['threshold'], color='green', linestyle=':', 
                              alpha=0.3, linewidth=1)
                
                ax.set_xlabel('Threshold (maillon)', fontsize=10)
                ax.set_ylabel('Score', fontsize=10)
                ax.set_title(f"{crisis.capitalize()} - {causal_link_labels[causal_link]}\n(Seuil crise: {crisis_threshold})", 
                            fontsize=11, fontweight='bold')
                ax.legend(loc='best', fontsize=8)
                ax.grid(True, alpha=0.3)
                ax.set_ylim(-0.05, 1.05)
                ax.set_xticks(thresholds_to_test)
        
        # Hide unused subplots
        for idx in range(subplot_idx, 10):
            axes[idx].axis('off')
        
        plt.tight_layout()
        plt.show()


def predict_crises_and_causal_links(
    df: pd.DataFrame,
    threshold_multiplier: float,
    threshold_biod_clim_ress: str,
    threshold_clim_const_cause_conse_solut: str,
    threshold_ress_const_solut: str,
    threshold_biod_const_cause_conse_solut: str = None,
    verbose: bool = True
) -> pd.DataFrame:
    """
    Make predictions for crises and causal links using threshold comparisons.
    
    This function applies the same logic as the production system:
    1. Calculates scores using HRFP multiplier
    2. Compares scores to thresholds
    3. Checks at_least_one_non_hrfp_word condition
    4. For causal links, also checks crisis threshold
    
    Args:
        df: DataFrame with keyword count columns (must have been processed with 
            apply_keyword_detection_to_dataframe)
        threshold_multiplier: Multiplier for HRFP keywords (e.g., 0.25)
        threshold_biod_clim_ress: Comma-separated string with thresholds for 
            biodiversité, climat, ressources (e.g., "4,3,3")
        threshold_clim_const_cause_conse_solut: Comma-separated string with thresholds 
            for climat causal links: constat, cause, consequence, solution (e.g., "2,1,1,1")
        threshold_ress_const_solut: Comma-separated string with thresholds for 
            ressources causal links: constat, solution (e.g., "1,1")
        threshold_biod_const_cause_conse_solut: Optional comma-separated string with 
            thresholds for biodiversité causal links: constat, cause, consequence, solution
            If None, will use same values as threshold_clim_const_cause_conse_solut
        verbose: Whether to print prediction statistics (default: True)
    
    Returns:
        DataFrame with added prediction columns:
        - PRED_biodiversite, PRED_climat, PRED_ressources (binary predictions for crises)
        - PRED_biodiversite_constat, PRED_biodiversite_cause, etc. (binary predictions for causal links)
        - PRED_CRISE (1 if at least one crisis predicted, 0 otherwise)
        
    Example:
        >>> df = apply_keyword_detection_to_dataframe(df)
        >>> df = add_at_least_one_non_hrfp_word_column(df)
        >>> df = predict_crises_and_causal_links(
        ...     df,
        ...     threshold_multiplier=0.25,
        ...     threshold_biod_clim_ress="4,3,3",
        ...     threshold_clim_const_cause_conse_solut="2,1,1,1",
        ...     threshold_ress_const_solut="1,1"
        ... )
        >>> df['PRED_climat'].sum()  # Count predicted climate crises
    """
    df = df.copy()
    
    # Parse threshold strings
    def parse_thresholds(threshold_str: str, expected_count: int) -> List[float]:
        """Parse comma-separated threshold string into list of floats."""
        parts = [float(x.strip()) for x in threshold_str.split(",")]
        if len(parts) != expected_count:
            raise ValueError(
                f"Expected {expected_count} thresholds, got {len(parts)} in '{threshold_str}'"
            )
        return parts
    
    # Parse crisis thresholds
    biod_thresh, clim_thresh, ress_thresh = parse_thresholds(threshold_biod_clim_ress, 3)
    
    # Parse causal link thresholds
    clim_causal = parse_thresholds(threshold_clim_const_cause_conse_solut, 4)
    clim_constat_thresh, clim_cause_thresh, clim_consequence_thresh, clim_solution_thresh = clim_causal
    
    ress_causal = parse_thresholds(threshold_ress_const_solut, 2)
    ress_constat_thresh, ress_solution_thresh = ress_causal
    
    # Parse biodiversité causal link thresholds (use same as climat if not provided)
    if threshold_biod_const_cause_conse_solut is None:
        biod_causal = clim_causal
    else:
        biod_causal = parse_thresholds(threshold_biod_const_cause_conse_solut, 4)
    biod_constat_thresh, biod_cause_thresh, biod_consequence_thresh, biod_solution_thresh = biod_causal
    
    # Ensure at_least_one_non_hrfp_word column exists
    if 'at_least_one_non_hrfp_word' not in df.columns:
        df = add_at_least_one_non_hrfp_word_column(df)
    
    # Calculate scores for crises
    score_biodiversite = calculate_keyword_score(df, 'biodiversite', threshold_multiplier)
    score_climat = calculate_keyword_score(df, 'climat', threshold_multiplier)
    score_ressources = calculate_keyword_score(df, 'ressources', threshold_multiplier)
    
    # Predict crises: score >= threshold AND at_least_one_non_hrfp_word
    df['PRED_biodiversite'] = (
        (score_biodiversite >= biod_thresh) & 
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_climat'] = (
        (score_climat >= clim_thresh) & 
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_ressources'] = (
        (score_ressources >= ress_thresh) & 
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    # Overall crisis prediction
    df['PRED_CRISE'] = (
        (df['PRED_biodiversite'] == 1) |
        (df['PRED_climat'] == 1) |
        (df['PRED_ressources'] == 1)
    ).astype(int)
    
    # Crisis without ressources: biodiversité OR climat (ignoring ressources completely)
    df['PRED_CRISE_WITHOUT_RESSOURCES'] = (
        (df['PRED_biodiversite'] == 1) | (df['PRED_climat'] == 1)
    ).astype(int)
    
    # Calculate scores for causal links
    # Climat causal links
    score_climat_constat = calculate_causal_link_score(df, 'climat', 'constat', threshold_multiplier)
    score_climat_cause = calculate_causal_link_score(df, 'climat', 'cause', threshold_multiplier)
    score_climat_consequence = calculate_causal_link_score(df, 'climat', 'consequence', threshold_multiplier)
    score_climat_solution = calculate_causal_link_score(df, 'climat', 'solution', threshold_multiplier)
    
    # Biodiversité causal links
    score_biodiversite_constat = calculate_causal_link_score(df, 'biodiversite', 'constat', threshold_multiplier)
    score_biodiversite_cause = calculate_causal_link_score(df, 'biodiversite', 'cause', threshold_multiplier)
    score_biodiversite_consequence = calculate_causal_link_score(df, 'biodiversite', 'consequence', threshold_multiplier)
    score_biodiversite_solution = calculate_causal_link_score(df, 'biodiversite', 'solution', threshold_multiplier)
    
    # Ressources causal links
    score_ressources_constat = calculate_causal_link_score(df, 'ressources', 'concept', threshold_multiplier)
    score_ressources_solution = calculate_causal_link_score(df, 'ressources', 'solution', threshold_multiplier)
    
    # Predict causal links: score_maillon >= threshold_maillon AND score_crise >= threshold_crise 
    # AND at_least_one_non_hrfp_word
    
    # Climat causal links
    df['PRED_climat_constat'] = (
        (score_climat_constat >= clim_constat_thresh) &
        (score_climat >= clim_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_climat_cause'] = (
        (score_climat_cause >= clim_cause_thresh) &
        (score_climat >= clim_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_climat_consequence'] = (
        (score_climat_consequence >= clim_consequence_thresh) &
        (score_climat >= clim_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_climat_solution'] = (
        (score_climat_solution >= clim_solution_thresh) &
        (score_climat >= clim_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    # Biodiversité causal links
    df['PRED_biodiversite_constat'] = (
        (score_biodiversite_constat >= biod_constat_thresh) &
        (score_biodiversite >= biod_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_biodiversite_cause'] = (
        (score_biodiversite_cause >= biod_cause_thresh) &
        (score_biodiversite >= biod_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_biodiversite_consequence'] = (
        (score_biodiversite_consequence >= biod_consequence_thresh) &
        (score_biodiversite >= biod_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_biodiversite_solution'] = (
        (score_biodiversite_solution >= biod_solution_thresh) &
        (score_biodiversite >= biod_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    # Ressources causal links
    df['PRED_ressources_constat'] = (
        (score_ressources_constat >= ress_constat_thresh) &
        (score_ressources >= ress_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    df['PRED_ressources_solution'] = (
        (score_ressources_solution >= ress_solution_thresh) &
        (score_ressources >= ress_thresh) &
        df['at_least_one_non_hrfp_word']
    ).astype(int)
    
    # Special column for ressources: concept = constat OR cause OR consequence
    # Note: In the LLM columns, ressources_concept is created from constat OR cause OR consequence
    # But for keyword-based prediction, we only have constat (which maps to concept)
    df['PRED_ressources_concept'] = df['PRED_ressources_constat']
    
    if verbose:
        print("✓ Predictions added with prefix 'PRED'")
        print(f"  - Articles predicted with crisis: {df['PRED_CRISE'].sum()}")
        print(f"  - Climat: {df['PRED_climat'].sum()}")
        print(f"  - Biodiversité: {df['PRED_biodiversite'].sum()}")
        print(f"  - Ressources: {df['PRED_ressources'].sum()}")
        print(f"  - Crisis (without ressources): {df['PRED_CRISE_WITHOUT_RESSOURCES'].sum()}")
    
    return df


def compare_llm_vs_keyword_predictions(
    df: pd.DataFrame,
    llm_prefix: str = "LLM",
    pred_prefix: str = "PRED",
    verbose: bool = True
) -> pd.DataFrame:
    """
    Compare LLM predictions vs keyword-based predictions and calculate metrics.
    
    Calculates precision, recall, and F1-score for each crisis and causal link,
    and compares the number of predictions from both methods.
    
    Args:
        df: DataFrame with both LLM columns (LLM_*) and keyword prediction columns (PRED_*)
        llm_prefix: Prefix for LLM columns (default: "LLM")
        pred_prefix: Prefix for keyword prediction columns (default: "PRED")
        verbose: Whether to print summary statistics (default: True)
    
    Returns:
        DataFrame with metrics for each crisis and causal link:
        - type: 'crise' or 'maillon'
        - name: Name of the crisis or causal link (e.g., 'climat', 'climat_constat')
        - nb_pred_llm: Number of positive predictions from LLM
        - nb_pred_keywords: Number of positive predictions from keywords
        - pct_pred_diff: Percentage difference in predictions |nb_pred_llm - nb_pred_keywords| / nb_pred_llm
        - nb_true_positives: True positives (both LLM and keywords predict positive)
        - nb_false_positives: False positives (keywords predict positive but LLM doesn't)
        - nb_false_negatives: False negatives (LLM predicts positive but keywords don't)
        - nb_true_negatives: True negatives (both predict negative)
        - precision: Precision score
        - recall: Recall score
        - f1_score: F1 score
        
    Example:
        >>> df = create_llm_columns_from_crises(df)
        >>> df = predict_crises_and_causal_links(df, ...)
        >>> metrics_df = compare_llm_vs_keyword_predictions(df)
        >>> print(metrics_df)
    """
    results = []
    
    # Define crises and causal links to compare
    crises = ['biodiversite', 'climat', 'ressources']
    
    causal_links = {
        'climat': ['constat', 'cause', 'consequence', 'solution'],
        'biodiversite': ['constat', 'cause', 'consequence', 'solution'],
        'ressources': ['concept', 'solution'],  # Note: concept maps to constat in keywords
    }
    
    # Compare crises
    for crisis in crises:
        llm_col = f"{llm_prefix}_{crisis}"
        pred_col = f"{pred_prefix}_{crisis}"
        
        if llm_col not in df.columns or pred_col not in df.columns:
            print(f"Warning: Missing columns {llm_col} or {pred_col}, skipping...")
            continue
        
        y_true = df[llm_col].astype(int)
        y_pred = df[pred_col].astype(int)
        
        # Calculate metrics
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        
        # Calculate confusion matrix components
        true_positives = ((y_true == 1) & (y_pred == 1)).sum()
        false_positives = ((y_true == 0) & (y_pred == 1)).sum()
        false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
        true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
        
        nb_pred_llm = y_true.sum()
        nb_pred_keywords = y_pred.sum()
        pct_pred_diff = abs(nb_pred_llm - nb_pred_keywords) / nb_pred_llm if nb_pred_llm > 0 else 0
        
        results.append({
            'type': 'crise',
            'name': crisis,
            'nb_pred_llm': nb_pred_llm,
            'nb_pred_keywords': nb_pred_keywords,
            'pct_pred_diff': pct_pred_diff,
            'nb_true_positives': int(true_positives),
            'nb_false_positives': int(false_positives),
            'nb_false_negatives': int(false_negatives),
            'nb_true_negatives': int(true_negatives),
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
        })
    
    # Compare causal links
    for crisis in crises:
        for causal_link in causal_links.get(crisis, []):
            # Handle special case: ressources concept maps to constat in keywords
            if crisis == 'ressources' and causal_link == 'concept':
                llm_col = f"{llm_prefix}_{crisis}_{causal_link}"
                pred_col = f"{pred_prefix}_{crisis}_constat"  # concept -> constat
            else:
                llm_col = f"{llm_prefix}_{crisis}_{causal_link}"
                pred_col = f"{pred_prefix}_{crisis}_{causal_link}"
            
            if llm_col not in df.columns or pred_col not in df.columns:
                print(f"Warning: Missing columns {llm_col} or {pred_col}, skipping...")
                continue
            
            y_true = df[llm_col].astype(int)
            y_pred = df[pred_col].astype(int)
            
            # Calculate metrics
            precision = precision_score(y_true, y_pred, zero_division=0)
            recall = recall_score(y_true, y_pred, zero_division=0)
            f1 = f1_score(y_true, y_pred, zero_division=0)
            
            # Calculate confusion matrix components
            true_positives = ((y_true == 1) & (y_pred == 1)).sum()
            false_positives = ((y_true == 0) & (y_pred == 1)).sum()
            false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
            true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
            
            nb_pred_llm = y_true.sum()
            nb_pred_keywords = y_pred.sum()
            pct_pred_diff = abs(nb_pred_llm - nb_pred_keywords) / nb_pred_llm if nb_pred_llm > 0 else 0
            
            results.append({
                'type': 'maillon',
                'name': f"{crisis}_{causal_link}",
                'nb_pred_llm': nb_pred_llm,
                'nb_pred_keywords': nb_pred_keywords,
                'pct_pred_diff': pct_pred_diff,
                'nb_true_positives': int(true_positives),
                'nb_false_positives': int(false_positives),
                'nb_false_negatives': int(false_negatives),
                'nb_true_negatives': int(true_negatives),
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
            })
    
    # Also compare overall crisis indicator
    llm_crise_col = f"{llm_prefix}_CRISE"
    pred_crise_col = f"{pred_prefix}_CRISE"
    
    if llm_crise_col in df.columns and pred_crise_col in df.columns:
        y_true = df[llm_crise_col].astype(int)
        y_pred = df[pred_crise_col].astype(int)
        
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        
        true_positives = ((y_true == 1) & (y_pred == 1)).sum()
        false_positives = ((y_true == 0) & (y_pred == 1)).sum()
        false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
        true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
        
        nb_pred_llm = y_true.sum()
        nb_pred_keywords = y_pred.sum()
        pct_pred_diff = abs(nb_pred_llm - nb_pred_keywords) / nb_pred_llm if nb_pred_llm > 0 else 0
        
        results.append({
            'type': 'crise',
            'name': 'CRISE',
            'nb_pred_llm': nb_pred_llm,
            'nb_pred_keywords': nb_pred_keywords,
            'pct_pred_diff': pct_pred_diff,
            'nb_true_positives': int(true_positives),
            'nb_false_positives': int(false_positives),
            'nb_false_negatives': int(false_negatives),
            'nb_true_negatives': int(true_negatives),
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
        })
    
    # Also compare crisis without ressources
    llm_crise_no_ress_col = f"{llm_prefix}_CRISE_WITHOUT_RESSOURCES"
    pred_crise_no_ress_col = f"{pred_prefix}_CRISE_WITHOUT_RESSOURCES"
    
    if llm_crise_no_ress_col in df.columns and pred_crise_no_ress_col in df.columns:
        y_true = df[llm_crise_no_ress_col].astype(int)
        y_pred = df[pred_crise_no_ress_col].astype(int)
        
        precision = precision_score(y_true, y_pred, zero_division=0)
        recall = recall_score(y_true, y_pred, zero_division=0)
        f1 = f1_score(y_true, y_pred, zero_division=0)
        
        true_positives = ((y_true == 1) & (y_pred == 1)).sum()
        false_positives = ((y_true == 0) & (y_pred == 1)).sum()
        false_negatives = ((y_true == 1) & (y_pred == 0)).sum()
        true_negatives = ((y_true == 0) & (y_pred == 0)).sum()
        
        nb_pred_llm = y_true.sum()
        nb_pred_keywords = y_pred.sum()
        pct_pred_diff = abs(nb_pred_llm - nb_pred_keywords) / nb_pred_llm if nb_pred_llm > 0 else 0
        
        results.append({
            'type': 'crise',
            'name': 'CRISE_WITHOUT_RESSOURCES',
            'nb_pred_llm': nb_pred_llm,
            'nb_pred_keywords': nb_pred_keywords,
            'pct_pred_diff': pct_pred_diff,
            'nb_true_positives': int(true_positives),
            'nb_false_positives': int(false_positives),
            'nb_false_negatives': int(false_negatives),
            'nb_true_negatives': int(true_negatives),
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
        })
    
    # Create DataFrame
    metrics_df = pd.DataFrame(results)
    
    # Reorder columns for better readability
    column_order = [
        'type', 'name', 'nb_pred_llm', 'nb_pred_keywords', 'pct_pred_diff',
        'nb_true_positives', 'nb_false_positives', 'nb_false_negatives', 'nb_true_negatives',
        'precision', 'recall', 'f1_score'
    ]
    metrics_df = metrics_df[column_order]
    
    if verbose:
        print("✓ Metrics calculated for LLM vs keyword predictions")
        print(f"  - Total comparisons: {len(metrics_df)}")
        print(f"  - Crises: {len(metrics_df[metrics_df['type'] == 'crise'])}")
        print(f"  - Causal links: {len(metrics_df[metrics_df['type'] == 'maillon'])}")
    
    return metrics_df


def split_dataframe_by_word_count_bins(
    df: pd.DataFrame,
    word_count_limits: List[int],
    word_count_col: str = 'word_count',
    test_size: float = 0.2,
    shuffle: bool = True,
    random_state: int = None,
    verbose: bool = True
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame into bins based on word count limits and perform train-test split on each bin.
    
    This function creates word count bins, splits the data into these bins, and performs
    train-test split on each bin. It returns both the individual bin dataframes and
    concatenated train/test dataframes.
    
    Args:
        df: DataFrame to split (must contain a word_count column)
        word_count_limits: List of word count limits to define bins
                          Example: [350, 600] creates 3 bins: (-inf, 350), [350, 600), [600, +inf)
        word_count_col: Name of the column containing word counts (default: 'word_count')
        test_size: Proportion of test set (default: 0.2)
        shuffle: Whether to shuffle before splitting (default: True)
        random_state: Random state for reproducibility (default: None)
        verbose: Whether to print progress information (default: True)
        random_state: Random state for reproducibility (default: 42)
    
    Returns:
        Tuple containing:
        - dfs_by_word_count_bin: Dictionary mapping bin labels to full dataframes for each bin
        - dfs_train: Dictionary mapping bin labels to training dataframes for each bin
        - dfs_test: Dictionary mapping bin labels to test dataframes for each bin
        - df_train_concat: Concatenated training dataframe (all bins combined)
        - df_test_concat: Concatenated test dataframe (all bins combined)
    
    Example:
        >>> word_count_limits = [350, 600]
        >>> dfs_by_bin, dfs_train, dfs_test, df_train_all, df_test_all = split_dataframe_by_word_count_bins(
        ...     df, word_count_limits, test_size=0.2, random_state=42
        ... )
        >>> print(f"Bins: {list(dfs_by_bin.keys())}")
        >>> print(f"Total train samples: {len(df_train_all)}")
    """
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Check that word_count column exists
    if word_count_col not in df.columns:
        raise ValueError(f"Column '{word_count_col}' not found in DataFrame. Available columns: {df.columns.tolist()}")
    
    # Create bins based on word_count_limits
    # bins will be: [-inf, limit1, limit2, ..., +inf]
    bins = [-float('inf')] + word_count_limits + [float('inf')]
    
    # Create labels for the bins
    # Example: for [350, 600], labels will be: ["0-{350}", "350-{600}", "{600}+"]
    labels = [f"0-{word_count_limits[0]}"]
    labels += [f"{word_count_limits[i]}-{word_count_limits[i+1]}" for i in range(len(word_count_limits)-1)]
    labels.append(f"{word_count_limits[-1]}+")
    
    # Create the word_count_bin column using pd.cut
    df['word_count_bin'] = pd.cut(
        df[word_count_col],
        bins=bins,
        labels=labels,
        right=True
    )
    
    if verbose:
        print(f"✓ Created {len(labels)} word count bins:")
        for label in labels:
            count = (df['word_count_bin'] == label).sum()
            print(f"  - {label}: {count} articles")
    
    # Initialize dictionaries to store results
    dfs_by_word_count_bin = {}
    dfs_train = {}
    dfs_test = {}
    
    for label in labels:
        # Get dataframe for this bin
        df_bin = df[df['word_count_bin'] == label].copy()
        
        # Skip empty bins
        if df_bin.shape[0] == 0:
            if verbose:
                print(f"  - {label}: SKIPPED (empty bin)")
            continue
        
        # Store the full bin dataframe
        dfs_by_word_count_bin[label] = df_bin
        
        # Perform train-test split
        df_train_bin, df_test_bin = train_test_split(
            df_bin,
            test_size=test_size,
            shuffle=shuffle,
            random_state=random_state
        )
        
        # Store train and test splits
        dfs_train[label] = df_train_bin
        dfs_test[label] = df_test_bin
        
        if verbose:
            print(f"  - {label}: {len(df_train_bin)} train, {len(df_test_bin)} test")
    
    # Concatenate all train and test dataframes
    if dfs_train:
        df_train_concat = pd.concat(list(dfs_train.values()), ignore_index=False)
        df_test_concat = pd.concat(list(dfs_test.values()), ignore_index=False)
        
        if verbose:
            print(f"\n✓ Concatenated results:")
            print(f"  - Total train: {len(df_train_concat)} articles")
            print(f"  - Total test: {len(df_test_concat)} articles")
    else:
        if verbose:
            print("\n⚠ Warning: No bins created (all bins were empty)")
        df_train_concat = pd.DataFrame()
        df_test_concat = pd.DataFrame()
    
    return dfs_by_word_count_bin, dfs_train, dfs_test, df_train_concat, df_test_concat


def filter_env_articles(
    df: pd.DataFrame,
    nb_climat: List[int] = None,
    nb_biodiv: List[int] = None,
    nb_ressources: List[int] = None,
    word_count: List[int] = None,
    word_count_col: str = "word_count",
) -> pd.DataFrame:
    """
    Filter a DataFrame to keep only environmental articles based on keyword counts
    and word-count-dependent thresholds.
    
    An article is considered "environmental" if it is classified as at least one of the
    three crises (climat, biodiversite, ressources) according to:
    
        number_of_<crise>_no_hrfp >= threshold_for_this_article
    
    where the threshold depends on the article word count.
    
    Threshold logic:
        - word_count defines the limits of the word count bins.
        - For a list of limits [w1, w2, ..., wk], we create k+1 bins:
              (-inf, w1], (w1, w2], ..., (wk-1, wk], (wk, +inf)
        - For each crisis, nb_<crise> must have length k+1, one threshold per bin.
    
    Default parameters:
        - nb_climat     = [2, 2]
        - nb_biodiv     = [2, 3]
        - nb_ressources = [2, 3]
        - word_count    = [600]
    
    With these defaults:
        - Articles with word_count <= 600 use the first threshold.
        - Articles with word_count > 600 use the second threshold.
    
    Args:
        df: Input DataFrame containing keyword count columns and a word count column.
        nb_climat: List of thresholds for climat by word count bin.
        nb_biodiv: List of thresholds for biodiversite by word count bin.
        nb_ressources: List of thresholds for ressources by word count bin.
        word_count: List of word count limits defining the bins.
        word_count_col: Name of the column containing word counts (default: "word_count").
    
    Returns:
        DataFrame filtered to only environmental articles.
    """
    df = df.copy()
    
    # Default parameters
    if word_count is None:
        word_count = [600]
    if nb_climat is None:
        nb_climat = [2, 2]
    if nb_biodiv is None:
        nb_biodiv = [2, 3]
    if nb_ressources is None:
        nb_ressources = [2, 3]
    
    # Basic checks
    if word_count_col not in df.columns:
        raise ValueError(
            f"Column '{word_count_col}' not found in DataFrame. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    required_cols = [
        "number_of_climat_no_hrfp",
        "number_of_ressources_no_hrfp",
        "number_of_biodiversite_no_hrfp",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"Missing required columns in DataFrame: {missing_cols}. "
            "Make sure keyword detection has been applied."
        )
    
    # Number of word count bins = len(word_count) + 1
    n_bins = len(word_count) + 1
    for name, lst in [
        ("nb_climat", nb_climat),
        ("nb_biodiv", nb_biodiv),
        ("nb_ressources", nb_ressources),
    ]:
        if len(lst) != n_bins:
            raise ValueError(
                f"Parameter '{name}' must have length {n_bins} "
                f"(one threshold per word count bin), got {len(lst)}."
            )
    
    # Convert thresholds and limits to numpy arrays
    word_count_limits = list(word_count)
    wc_values = df[word_count_col].fillna(0).to_numpy()
    
    # Determine bin index for each article based on word_count
    # bins: (-inf, w1], (w1, w2], ..., (wk, +inf)
    # np.searchsorted with side='right' gives exactly this behaviour
    bin_indices = np.searchsorted(word_count_limits, wc_values, side="right")
    
    # Build per-row thresholds for each crisis
    nb_climat_arr = np.asarray(nb_climat)
    nb_biodiv_arr = np.asarray(nb_biodiv)
    nb_ress_arr = np.asarray(nb_ressources)
    
    clim_thresholds = nb_climat_arr[bin_indices]
    biodiv_thresholds = nb_biodiv_arr[bin_indices]
    ress_thresholds = nb_ress_arr[bin_indices]
    
    # Apply thresholds
    climat_counts = df["number_of_climat_no_hrfp"].to_numpy()
    biodiv_counts = df["number_of_biodiversite_no_hrfp"].to_numpy()
    ress_counts = df["number_of_ressources_no_hrfp"].to_numpy()
    
    mask_climat = climat_counts >= clim_thresholds
    mask_biodiv = biodiv_counts >= biodiv_thresholds
    mask_ress = ress_counts >= ress_thresholds
    
    # An article is environmental if it matches at least one crisis
    env_mask = mask_climat | mask_biodiv | mask_ress
    
    return df[env_mask].copy()


def compare_thresholds_per_bin(
    dfs_by_bin: Dict[str, pd.DataFrame],
    threshold_multiplier: float,
    threshold_biod_clim_ress_list: List[str],
    threshold_clim_const_cause_conse_solut_list: List[str],
    threshold_ress_const_solut_list: List[str],
    threshold_biod_const_cause_conse_solut_list: List[str],
    threshold_biod_clim_ress_baseline: str,
    threshold_clim_const_cause_conse_solut_baseline: str,
    threshold_ress_const_solut_baseline: str,
    threshold_biod_const_cause_conse_solut_baseline: str,
    verbose: bool = True
) -> Tuple[pd.DataFrame, List[pd.DataFrame], List[pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    """
    Compare optimized thresholds per bin vs baseline thresholds.
    
    For each bin, applies predictions with both optimized and baseline thresholds,
    calculates metrics, and compares F1-scores.
    
    Args:
        dfs_by_bin: Dictionary mapping bin labels to DataFrames
        threshold_multiplier: Multiplier for HRFP keywords (same for all bins)
        threshold_biod_clim_ress_list: List of thresholds for each bin (biodiv, climat, ressources)
        threshold_clim_const_cause_conse_solut_list: List of thresholds for climat causal links per bin
        threshold_ress_const_solut_list: List of thresholds for ressources causal links per bin
        threshold_biod_const_cause_conse_solut_list: List of thresholds for biodiv causal links per bin
        threshold_biod_clim_ress_baseline: Baseline thresholds (biodiv, climat, ressources)
        threshold_clim_const_cause_conse_solut_baseline: Baseline thresholds for climat causal links
        threshold_ress_const_solut_baseline: Baseline thresholds for ressources causal links
        threshold_biod_const_cause_conse_solut_baseline: Baseline thresholds for biodiv causal links
        verbose: Whether to print progress information (default: True)
    
    Returns:
        Tuple containing:
        - comparison_df: DataFrame with comparison (optimized vs baseline) for each bin + ALL row, including:
            * F1-scores for biodiv, climat, ressources, crise_no_ress
            * Precision for biodiv, climat, ressources, crise_no_ress
            * Recall for biodiv, climat, ressources, crise_no_ress
            * pct_pred_diff (percentage difference in number of predictions)
            * All threshold parameters used
        - all_df_preds: List of DataFrames with predictions using optimized thresholds
        - all_df_preds_baseline: List of DataFrames with predictions using baseline thresholds
        - df_pred_concat: Concatenated DataFrame with all optimized predictions
        - df_pred_baseline_concat: Concatenated DataFrame with all baseline predictions
    
    Example:
        >>> comparison_df, df_preds, df_preds_baseline = compare_thresholds_per_bin(
        ...     dfs_test,
        ...     multiplier_hrfp=0.25,
        ...     threshold_biod_clim_ress_list=["4,3,3", "5,4,4"],
        ...     ...
        ... )
    """
    all_df_preds = []
    all_df_preds_baseline = []
    comparison_results = []
    
    bin_labels = list(dfs_by_bin.keys())
    
    # Validate that all lists have the same length as bins
    if len(threshold_biod_clim_ress_list) != len(bin_labels):
        raise ValueError(f"threshold_biod_clim_ress_list length ({len(threshold_biod_clim_ress_list)}) "
                        f"must match number of bins ({len(bin_labels)})")
    if len(threshold_clim_const_cause_conse_solut_list) != len(bin_labels):
        raise ValueError(f"threshold_clim_const_cause_conse_solut_list length ({len(threshold_clim_const_cause_conse_solut_list)}) "
                        f"must match number of bins ({len(bin_labels)})")
    if len(threshold_ress_const_solut_list) != len(bin_labels):
        raise ValueError(f"threshold_ress_const_solut_list length ({len(threshold_ress_const_solut_list)}) "
                        f"must match number of bins ({len(bin_labels)})")
    if len(threshold_biod_const_cause_conse_solut_list) != len(bin_labels):
        raise ValueError(f"threshold_biod_const_cause_conse_solut_list length ({len(threshold_biod_const_cause_conse_solut_list)}) "
                        f"must match number of bins ({len(bin_labels)})")
    
    for i, (label, df_bin) in enumerate(dfs_by_bin.items()):
        df_pred = predict_crises_and_causal_links(
            df_bin,
            threshold_multiplier=threshold_multiplier,
            threshold_biod_clim_ress=threshold_biod_clim_ress_list[i],
            threshold_clim_const_cause_conse_solut=threshold_clim_const_cause_conse_solut_list[i],
            threshold_biod_const_cause_conse_solut=threshold_biod_const_cause_conse_solut_list[i],
            threshold_ress_const_solut=threshold_ress_const_solut_list[i],
            verbose=verbose
        )
        all_df_preds.append(df_pred)
        
        metrics_df = compare_llm_vs_keyword_predictions(df_pred, verbose=verbose)
        
        # Extract F1-scores, precision, recall and pct_pred_diff for optimized thresholds
        f1_biodiv = metrics_df.loc[metrics_df["name"] == "biodiversite", "f1_score"].values[0]
        f1_climat = metrics_df.loc[metrics_df["name"] == "climat", "f1_score"].values[0]
        f1_ressources = metrics_df.loc[metrics_df["name"] == "ressources", "f1_score"].values[0]
        f1_crise_no_ress = metrics_df.loc[metrics_df["name"] == "CRISE_WITHOUT_RESSOURCES", "f1_score"].values[0]
        
        precision_biodiv = metrics_df.loc[metrics_df["name"] == "biodiversite", "precision"].values[0]
        precision_climat = metrics_df.loc[metrics_df["name"] == "climat", "precision"].values[0]
        precision_ressources = metrics_df.loc[metrics_df["name"] == "ressources", "precision"].values[0]
        precision_crise_no_ress = metrics_df.loc[metrics_df["name"] == "CRISE_WITHOUT_RESSOURCES", "precision"].values[0]
        
        recall_biodiv = metrics_df.loc[metrics_df["name"] == "biodiversite", "recall"].values[0]
        recall_climat = metrics_df.loc[metrics_df["name"] == "climat", "recall"].values[0]
        recall_ressources = metrics_df.loc[metrics_df["name"] == "ressources", "recall"].values[0]
        recall_crise_no_ress = metrics_df.loc[metrics_df["name"] == "CRISE_WITHOUT_RESSOURCES", "recall"].values[0]
        
        pct_pred_diff_biodiv = metrics_df.loc[metrics_df["name"] == "biodiversite", "pct_pred_diff"].values[0]
        pct_pred_diff_climat = metrics_df.loc[metrics_df["name"] == "climat", "pct_pred_diff"].values[0]
        pct_pred_diff_ressources = metrics_df.loc[metrics_df["name"] == "ressources", "pct_pred_diff"].values[0]
        pct_pred_diff_crise_no_ress = metrics_df.loc[metrics_df["name"] == "CRISE_WITHOUT_RESSOURCES", "pct_pred_diff"].values[0]
        
        df_pred_baseline = predict_crises_and_causal_links(
            df_bin,
            threshold_multiplier=threshold_multiplier,
            threshold_biod_clim_ress=threshold_biod_clim_ress_baseline,
            threshold_clim_const_cause_conse_solut=threshold_clim_const_cause_conse_solut_baseline,
            threshold_biod_const_cause_conse_solut=threshold_biod_const_cause_conse_solut_baseline,
            threshold_ress_const_solut=threshold_ress_const_solut_baseline,
            verbose=verbose
        )
        all_df_preds_baseline.append(df_pred_baseline)
        
        metrics_df_baseline = compare_llm_vs_keyword_predictions(df_pred_baseline, verbose=verbose)
        
        # Extract F1-scores, precision, recall and pct_pred_diff for baseline
        f1_biodiv_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "biodiversite", "f1_score"].values[0]
        f1_climat_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "climat", "f1_score"].values[0]
        f1_ressources_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "ressources", "f1_score"].values[0]
        f1_crise_no_ress_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "CRISE_WITHOUT_RESSOURCES", "f1_score"].values[0]
        
        precision_biodiv_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "biodiversite", "precision"].values[0]
        precision_climat_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "climat", "precision"].values[0]
        precision_ressources_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "ressources", "precision"].values[0]
        precision_crise_no_ress_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "CRISE_WITHOUT_RESSOURCES", "precision"].values[0]
        
        recall_biodiv_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "biodiversite", "recall"].values[0]
        recall_climat_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "climat", "recall"].values[0]
        recall_ressources_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "ressources", "recall"].values[0]
        recall_crise_no_ress_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "CRISE_WITHOUT_RESSOURCES", "recall"].values[0]
        
        pct_pred_diff_biodiv_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "biodiversite", "pct_pred_diff"].values[0]
        pct_pred_diff_climat_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "climat", "pct_pred_diff"].values[0]
        pct_pred_diff_ressources_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "ressources", "pct_pred_diff"].values[0]
        pct_pred_diff_crise_no_ress_baseline = metrics_df_baseline.loc[metrics_df_baseline["name"] == "CRISE_WITHOUT_RESSOURCES", "pct_pred_diff"].values[0]
        
        # Store comparison results
        comparison_results.append({
            'bin_label': label,
            'n_articles': len(df_bin),
            
            # Optimized thresholds parameters
            'multiplier_hrfp': threshold_multiplier,
            'threshold_biod_clim_ress_optimized': threshold_biod_clim_ress_list[i],
            'threshold_clim_causal_optimized': threshold_clim_const_cause_conse_solut_list[i],
            'threshold_biod_causal_optimized': threshold_biod_const_cause_conse_solut_list[i],
            'threshold_ress_causal_optimized': threshold_ress_const_solut_list[i],
            
            # Baseline thresholds parameters
            'threshold_biod_clim_ress_baseline': threshold_biod_clim_ress_baseline,
            'threshold_clim_causal_baseline': threshold_clim_const_cause_conse_solut_baseline,
            'threshold_biod_causal_baseline': threshold_biod_const_cause_conse_solut_baseline,
            'threshold_ress_causal_baseline': threshold_ress_const_solut_baseline,
            
            # Optimized F1-scores
            'f1_biodiv_optimized': f1_biodiv,
            'f1_climat_optimized': f1_climat,
            'f1_ressources_optimized': f1_ressources,
            'f1_crise_no_ress_optimized': f1_crise_no_ress,
            
            # Baseline F1-scores
            'f1_biodiv_baseline': f1_biodiv_baseline,
            'f1_climat_baseline': f1_climat_baseline,
            'f1_ressources_baseline': f1_ressources_baseline,
            'f1_crise_no_ress_baseline': f1_crise_no_ress_baseline,
            
            # Optimized precision
            'precision_biodiv_optimized': precision_biodiv,
            'precision_climat_optimized': precision_climat,
            'precision_ressources_optimized': precision_ressources,
            'precision_crise_no_ress_optimized': precision_crise_no_ress,
            
            # Baseline precision
            'precision_biodiv_baseline': precision_biodiv_baseline,
            'precision_climat_baseline': precision_climat_baseline,
            'precision_ressources_baseline': precision_ressources_baseline,
            'precision_crise_no_ress_baseline': precision_crise_no_ress_baseline,
            
            # Optimized recall
            'recall_biodiv_optimized': recall_biodiv,
            'recall_climat_optimized': recall_climat,
            'recall_ressources_optimized': recall_ressources,
            'recall_crise_no_ress_optimized': recall_crise_no_ress,
            
            # Baseline recall
            'recall_biodiv_baseline': recall_biodiv_baseline,
            'recall_climat_baseline': recall_climat_baseline,
            'recall_ressources_baseline': recall_ressources_baseline,
            'recall_crise_no_ress_baseline': recall_crise_no_ress_baseline,
            
            # Optimized pct_pred_diff
            'pct_pred_diff_biodiv_optimized': pct_pred_diff_biodiv,
            'pct_pred_diff_climat_optimized': pct_pred_diff_climat,
            'pct_pred_diff_ressources_optimized': pct_pred_diff_ressources,
            'pct_pred_diff_crise_no_ress_optimized': pct_pred_diff_crise_no_ress,
            
            # Baseline pct_pred_diff
            'pct_pred_diff_biodiv_baseline': pct_pred_diff_biodiv_baseline,
            'pct_pred_diff_climat_baseline': pct_pred_diff_climat_baseline,
            'pct_pred_diff_ressources_baseline': pct_pred_diff_ressources_baseline,
            'pct_pred_diff_crise_no_ress_baseline': pct_pred_diff_crise_no_ress_baseline,
        })
    
    # Create comparison DataFrame
    comparison_df = pd.DataFrame(comparison_results)
    
    # Initialize concatenated dataframes
    df_pred_concat = pd.DataFrame()
    df_pred_baseline_concat = pd.DataFrame()
    
    # Add concatenated results (ALL bins combined)
    if all_df_preds and all_df_preds_baseline:
        if verbose:
            print("\n" + "=" * 80)
            print("📊 Computing metrics on ALL BINS CONCATENATED")
            print("=" * 80)
        
        # Concatenate all predictions
        df_pred_concat = pd.concat(all_df_preds, ignore_index=False)
        df_pred_baseline_concat = pd.concat(all_df_preds_baseline, ignore_index=False)
        
        # Compute metrics on concatenated data
        metrics_df_concat = compare_llm_vs_keyword_predictions(df_pred_concat, verbose=False)
        metrics_df_baseline_concat = compare_llm_vs_keyword_predictions(df_pred_baseline_concat, verbose=False)
        
        # Extract metrics
        f1_biodiv_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "biodiversite", "f1_score"].values[0]
        f1_climat_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "climat", "f1_score"].values[0]
        f1_ressources_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "ressources", "f1_score"].values[0]
        f1_crise_no_ress_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "f1_score"].values[0]
        
        precision_biodiv_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "biodiversite", "precision"].values[0]
        precision_climat_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "climat", "precision"].values[0]
        precision_ressources_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "ressources", "precision"].values[0]
        precision_crise_no_ress_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "precision"].values[0]
        
        recall_biodiv_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "biodiversite", "recall"].values[0]
        recall_climat_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "climat", "recall"].values[0]
        recall_ressources_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "ressources", "recall"].values[0]
        recall_crise_no_ress_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "recall"].values[0]
        
        pct_pred_diff_biodiv_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "biodiversite", "pct_pred_diff"].values[0]
        pct_pred_diff_climat_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "climat", "pct_pred_diff"].values[0]
        pct_pred_diff_ressources_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "ressources", "pct_pred_diff"].values[0]
        pct_pred_diff_crise_no_ress_concat = metrics_df_concat.loc[metrics_df_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "pct_pred_diff"].values[0]
        
        f1_biodiv_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "biodiversite", "f1_score"].values[0]
        f1_climat_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "climat", "f1_score"].values[0]
        f1_ressources_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "ressources", "f1_score"].values[0]
        f1_crise_no_ress_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "f1_score"].values[0]
        
        precision_biodiv_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "biodiversite", "precision"].values[0]
        precision_climat_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "climat", "precision"].values[0]
        precision_ressources_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "ressources", "precision"].values[0]
        precision_crise_no_ress_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "precision"].values[0]
        
        recall_biodiv_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "biodiversite", "recall"].values[0]
        recall_climat_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "climat", "recall"].values[0]
        recall_ressources_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "ressources", "recall"].values[0]
        recall_crise_no_ress_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "recall"].values[0]
        
        pct_pred_diff_biodiv_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "biodiversite", "pct_pred_diff"].values[0]
        pct_pred_diff_climat_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "climat", "pct_pred_diff"].values[0]
        pct_pred_diff_ressources_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "ressources", "pct_pred_diff"].values[0]
        pct_pred_diff_crise_no_ress_baseline_concat = metrics_df_baseline_concat.loc[metrics_df_baseline_concat["name"] == "CRISE_WITHOUT_RESSOURCES", "pct_pred_diff"].values[0]
        
        # Create concatenated row
        concat_row = {
            'bin_label': 'ALL',
            'n_articles': len(df_pred_concat),
            
            # Optimized thresholds parameters (use "mixed" since they vary by bin)
            'multiplier_hrfp': threshold_multiplier,
            'threshold_biod_clim_ress_optimized': 'mixed',
            'threshold_clim_causal_optimized': 'mixed',
            'threshold_biod_causal_optimized': 'mixed',
            'threshold_ress_causal_optimized': 'mixed',
            
            # Baseline thresholds parameters
            'threshold_biod_clim_ress_baseline': threshold_biod_clim_ress_baseline,
            'threshold_clim_causal_baseline': threshold_clim_const_cause_conse_solut_baseline,
            'threshold_biod_causal_baseline': threshold_biod_const_cause_conse_solut_baseline,
            'threshold_ress_causal_baseline': threshold_ress_const_solut_baseline,
            
            # Optimized F1-scores
            'f1_biodiv_optimized': f1_biodiv_concat,
            'f1_climat_optimized': f1_climat_concat,
            'f1_ressources_optimized': f1_ressources_concat,
            'f1_crise_no_ress_optimized': f1_crise_no_ress_concat,
            
            # Baseline F1-scores
            'f1_biodiv_baseline': f1_biodiv_baseline_concat,
            'f1_climat_baseline': f1_climat_baseline_concat,
            'f1_ressources_baseline': f1_ressources_baseline_concat,
            'f1_crise_no_ress_baseline': f1_crise_no_ress_baseline_concat,
            
            # Optimized precision
            'precision_biodiv_optimized': precision_biodiv_concat,
            'precision_climat_optimized': precision_climat_concat,
            'precision_ressources_optimized': precision_ressources_concat,
            'precision_crise_no_ress_optimized': precision_crise_no_ress_concat,
            
            # Baseline precision
            'precision_biodiv_baseline': precision_biodiv_baseline_concat,
            'precision_climat_baseline': precision_climat_baseline_concat,
            'precision_ressources_baseline': precision_ressources_baseline_concat,
            'precision_crise_no_ress_baseline': precision_crise_no_ress_baseline_concat,
            
            # Optimized recall
            'recall_biodiv_optimized': recall_biodiv_concat,
            'recall_climat_optimized': recall_climat_concat,
            'recall_ressources_optimized': recall_ressources_concat,
            'recall_crise_no_ress_optimized': recall_crise_no_ress_concat,
            
            # Baseline recall
            'recall_biodiv_baseline': recall_biodiv_baseline_concat,
            'recall_climat_baseline': recall_climat_baseline_concat,
            'recall_ressources_baseline': recall_ressources_baseline_concat,
            'recall_crise_no_ress_baseline': recall_crise_no_ress_baseline_concat,
            
            # Optimized pct_pred_diff
            'pct_pred_diff_biodiv_optimized': pct_pred_diff_biodiv_concat,
            'pct_pred_diff_climat_optimized': pct_pred_diff_climat_concat,
            'pct_pred_diff_ressources_optimized': pct_pred_diff_ressources_concat,
            'pct_pred_diff_crise_no_ress_optimized': pct_pred_diff_crise_no_ress_concat,
            
            # Baseline pct_pred_diff
            'pct_pred_diff_biodiv_baseline': pct_pred_diff_biodiv_baseline_concat,
            'pct_pred_diff_climat_baseline': pct_pred_diff_climat_baseline_concat,
            'pct_pred_diff_ressources_baseline': pct_pred_diff_ressources_baseline_concat,
            'pct_pred_diff_crise_no_ress_baseline': pct_pred_diff_crise_no_ress_baseline_concat,
        }
        
        # Append concatenated row to comparison_df
        comparison_df = pd.concat([comparison_df, pd.DataFrame([concat_row])], ignore_index=True)
        
        if verbose:
            print("\n✅ Concatenated metrics (ALL bins):")
            print(f"  - Total articles: {len(df_pred_concat)}")
            print(f"\n  F1-Scores (Optimized):")
            print(f"    biodiversite: {f1_biodiv_concat:.4f}")
            print(f"    climat: {f1_climat_concat:.4f}")
            print(f"    ressources: {f1_ressources_concat:.4f}")
            print(f"    CRISE_WITHOUT_RESSOURCES: {f1_crise_no_ress_concat:.4f}")
            print(f"\n  F1-Scores (Baseline):")
            print(f"    biodiversite: {f1_biodiv_baseline_concat:.4f}")
            print(f"    climat: {f1_climat_baseline_concat:.4f}")
            print(f"    ressources: {f1_ressources_baseline_concat:.4f}")
            print(f"    CRISE_WITHOUT_RESSOURCES: {f1_crise_no_ress_baseline_concat:.4f}")
            print(f"\n  Improvements:")
            print(f"    biodiversite: {f1_biodiv_concat - f1_biodiv_baseline_concat:+.4f}")
            print(f"    climat: {f1_climat_concat - f1_climat_baseline_concat:+.4f}")
            print(f"    ressources: {f1_ressources_concat - f1_ressources_baseline_concat:+.4f}")
            print(f"    CRISE_WITHOUT_RESSOURCES: {f1_crise_no_ress_concat - f1_crise_no_ress_baseline_concat:+.4f}")
    
    return comparison_df, all_df_preds, all_df_preds_baseline, df_pred_concat, df_pred_baseline_concat


def cross_validate_thresholds_per_bin(
    df: pd.DataFrame,
    word_count_limits: List[int],
    threshold_multiplier: float,
    threshold_biod_clim_ress_list: List[str],
    threshold_clim_const_cause_conse_solut_list: List[str],
    threshold_ress_const_solut_list: List[str],
    threshold_biod_const_cause_conse_solut_list: List[str],
    threshold_biod_clim_ress_baseline: str,
    threshold_clim_const_cause_conse_solut_baseline: str,
    threshold_ress_const_solut_baseline: str,
    threshold_biod_const_cause_conse_solut_baseline: str,
    n_splits: int = 5,
    test_size: float = 0.2,
    shuffle: bool = True,
    word_count_col: str = 'word_count'
) -> pd.DataFrame:
    """
    Perform cross-validation by doing multiple train-test splits and averaging metrics.
    
    This function performs multiple train-test splits with different random states,
    calculates metrics for each split using compare_thresholds_per_bin, and then
    aggregates the results by calculating mean and standard deviation of F1-scores,
    precision, recall, and pct_pred_diff for each bin.
    
    Args:
        df: DataFrame to split and evaluate
        word_count_limits: List of word count limits to define bins
        threshold_multiplier: Multiplier for HRFP keywords
        threshold_biod_clim_ress_list: List of optimized thresholds for each bin
        threshold_clim_const_cause_conse_solut_list: List of optimized thresholds for climat causal links
        threshold_ress_const_solut_list: List of optimized thresholds for ressources causal links
        threshold_biod_const_cause_conse_solut_list: List of optimized thresholds for biodiv causal links
        threshold_biod_clim_ress_baseline: Baseline thresholds
        threshold_clim_const_cause_conse_solut_baseline: Baseline thresholds for climat causal links
        threshold_ress_const_solut_baseline: Baseline thresholds for ressources causal links
        threshold_biod_const_cause_conse_solut_baseline: Baseline thresholds for biodiv causal links
        n_splits: Number of train-test splits to perform (default: 5)
        test_size: Proportion of test set (default: 0.2)
        shuffle: Whether to shuffle before splitting (default: True)
        word_count_col: Name of the column containing word counts (default: 'word_count')
    
    Returns:
        DataFrame with aggregated metrics for each bin, containing:
        - bin_label: Label of the word count bin
        - metric: Name of the metric (e.g., 'f1_biodiv', 'precision_climat', 'recall_ressources', 'pct_pred_diff_climat')
        - optimized_mean: Mean value for optimized thresholds
        - optimized_std: Standard deviation for optimized thresholds
        - baseline_mean: Mean value for baseline thresholds
        - baseline_std: Standard deviation for baseline thresholds
        - improvement_mean: Mean improvement (optimized - baseline)
        - improvement_std: Standard deviation of improvement
    
    Example:
        >>> summary_df = cross_validate_thresholds_per_bin(
        ...     df,
        ...     word_count_limits=[350, 600],
        ...     threshold_multiplier=0.25,
        ...     threshold_biod_clim_ress_list=["2,2,2", "2,2,2", "3,2,2"],
        ...     ...,
        ...     n_splits=5
        ... )
    """
    print(f"Starting cross-validation with {n_splits} splits...")
    print(f"Word count bins: {word_count_limits}")
    print(f"Test size: {test_size}, Shuffle: {shuffle}")
    print("=" * 80)
    
    all_comparison_dfs = []
    
    for split_idx in range(n_splits):
        print(f"\n📊 Split {split_idx + 1}/{n_splits} (random_state={split_idx})")
        print("-" * 80)
        
        # Perform train-test split with different random state each time
        dfs_by_bin, dfs_train, dfs_test, df_train_concat, df_test_concat = split_dataframe_by_word_count_bins(
            df,
            word_count_limits=word_count_limits,
            word_count_col=word_count_col,
            test_size=test_size,
            shuffle=shuffle,
            random_state=split_idx,  # Use split_idx as random_state for reproducibility
            verbose=False  # Don't print details for each split
        )
        
        # Use test set for evaluation
        if split_idx == 0:
            print(f"\nEvaluating on test set ({len(df_test_concat)} total articles per split)...")
        
        comparison_df, _, _, _, _ = compare_thresholds_per_bin(
            dfs_test,
            threshold_multiplier=threshold_multiplier,
            threshold_biod_clim_ress_list=threshold_biod_clim_ress_list,
            threshold_clim_const_cause_conse_solut_list=threshold_clim_const_cause_conse_solut_list,
            threshold_ress_const_solut_list=threshold_ress_const_solut_list,
            threshold_biod_const_cause_conse_solut_list=threshold_biod_const_cause_conse_solut_list,
            threshold_biod_clim_ress_baseline=threshold_biod_clim_ress_baseline,
            threshold_clim_const_cause_conse_solut_baseline=threshold_clim_const_cause_conse_solut_baseline,
            threshold_ress_const_solut_baseline=threshold_ress_const_solut_baseline,
            threshold_biod_const_cause_conse_solut_baseline=threshold_biod_const_cause_conse_solut_baseline,
            verbose=False  # Don't print details for each split
        )
        
        # Add split index
        comparison_df['split_idx'] = split_idx
        all_comparison_dfs.append(comparison_df)
    
    # Concatenate all results
    all_results = pd.concat(all_comparison_dfs, ignore_index=True)
    
    print("\n\n" + "=" * 80)
    print("📊 AGGREGATING RESULTS ACROSS ALL SPLITS")
    print("=" * 80)
    
    # Define metrics to aggregate
    metrics_to_aggregate = [
        # F1-scores
        ('f1_biodiv', 'f1_biodiv_optimized', 'f1_biodiv_baseline'),
        ('f1_climat', 'f1_climat_optimized', 'f1_climat_baseline'),
        ('f1_ressources', 'f1_ressources_optimized', 'f1_ressources_baseline'),
        ('f1_crise_no_ress', 'f1_crise_no_ress_optimized', 'f1_crise_no_ress_baseline'),
        # Precision
        ('precision_biodiv', 'precision_biodiv_optimized', 'precision_biodiv_baseline'),
        ('precision_climat', 'precision_climat_optimized', 'precision_climat_baseline'),
        ('precision_ressources', 'precision_ressources_optimized', 'precision_ressources_baseline'),
        ('precision_crise_no_ress', 'precision_crise_no_ress_optimized', 'precision_crise_no_ress_baseline'),
        # Recall
        ('recall_biodiv', 'recall_biodiv_optimized', 'recall_biodiv_baseline'),
        ('recall_climat', 'recall_climat_optimized', 'recall_climat_baseline'),
        ('recall_ressources', 'recall_ressources_optimized', 'recall_ressources_baseline'),
        ('recall_crise_no_ress', 'recall_crise_no_ress_optimized', 'recall_crise_no_ress_baseline'),
        # Pct pred diff
        ('pct_pred_diff_biodiv', 'pct_pred_diff_biodiv_optimized', 'pct_pred_diff_biodiv_baseline'),
        ('pct_pred_diff_climat', 'pct_pred_diff_climat_optimized', 'pct_pred_diff_climat_baseline'),
        ('pct_pred_diff_ressources', 'pct_pred_diff_ressources_optimized', 'pct_pred_diff_ressources_baseline'),
        ('pct_pred_diff_crise_no_ress', 'pct_pred_diff_crise_no_ress_optimized', 'pct_pred_diff_crise_no_ress_baseline'),
    ]
    
    summary_results = []
    
    # IMPORTANT: Filter out 'ALL' rows from individual splits to avoid double counting
    # We only want to aggregate across individual bins, not the already-aggregated ALL rows
    all_results_bins_only = all_results[all_results['bin_label'] != 'ALL'].copy()
    
    for bin_label in all_results_bins_only['bin_label'].unique():
        bin_data = all_results_bins_only[all_results_bins_only['bin_label'] == bin_label]
        
        for metric_name, optimized_col, baseline_col in metrics_to_aggregate:
            optimized_values = bin_data[optimized_col]
            baseline_values = bin_data[baseline_col]
            improvement_values = optimized_values - baseline_values
            
            summary_results.append({
                'bin_label': bin_label,
                'metric': metric_name,
                'optimized_mean': optimized_values.mean(),
                'optimized_std': optimized_values.std(),
                'baseline_mean': baseline_values.mean(),
                'baseline_std': baseline_values.std(),
                'improvement_mean': improvement_values.mean(),
                'improvement_std': improvement_values.std(),
                'optimized_min': optimized_values.min(),
                'optimized_max': optimized_values.max(),
                'baseline_min': baseline_values.min(),
                'baseline_max': baseline_values.max(),
            })
    
    summary_df = pd.DataFrame(summary_results)
    
    # Add "ALL" summary (aggregated across all bins)
    print("\n" + "=" * 80)
    print("📊 Computing aggregated metrics across ALL BINS")
    print("=" * 80)
    
    all_summary_results = []
    
    for metric_name, optimized_col, baseline_col in metrics_to_aggregate:
        # Aggregate across all bins (excluding already-aggregated ALL rows)
        all_optimized_values = []
        all_baseline_values = []
        
        for bin_label in all_results_bins_only['bin_label'].unique():
            bin_data = all_results_bins_only[all_results_bins_only['bin_label'] == bin_label]
            all_optimized_values.extend(bin_data[optimized_col].tolist())
            all_baseline_values.extend(bin_data[baseline_col].tolist())
        
        all_optimized_values = pd.Series(all_optimized_values)
        all_baseline_values = pd.Series(all_baseline_values)
        all_improvement_values = all_optimized_values - all_baseline_values
        
        all_summary_results.append({
            'bin_label': 'ALL',
            'metric': metric_name,
            'optimized_mean': all_optimized_values.mean(),
            'optimized_std': all_optimized_values.std(),
            'baseline_mean': all_baseline_values.mean(),
            'baseline_std': all_baseline_values.std(),
            'improvement_mean': all_improvement_values.mean(),
            'improvement_std': all_improvement_values.std(),
            'optimized_min': all_optimized_values.min(),
            'optimized_max': all_optimized_values.max(),
            'baseline_min': all_baseline_values.min(),
            'baseline_max': all_baseline_values.max(),
        })
    
    # Append ALL rows to summary_df
    summary_df = pd.concat([summary_df, pd.DataFrame(all_summary_results)], ignore_index=True)
    
    # Display summary
    print("\n📈 Summary Statistics:")
    print("-" * 80)
    
    # Get unique bin labels, but put 'ALL' at the end
    bin_labels = [label for label in summary_df['bin_label'].unique() if label != 'ALL']
    if 'ALL' in summary_df['bin_label'].unique():
        bin_labels.append('ALL')
    
    for bin_label in bin_labels:
        if bin_label == 'ALL':
            print("\n" + "=" * 80)
            print(f"🔹 ALL BINS COMBINED")
            print("=" * 80)
        else:
            print(f"\n🔹 Bin: {bin_label}")
        
        bin_summary = summary_df[summary_df['bin_label'] == bin_label]
        
        # Display F1-scores
        print("\n  F1-Scores:")
        f1_metrics = bin_summary[bin_summary['metric'].str.startswith('f1_')]
        for _, row in f1_metrics.iterrows():
            metric_short = row['metric'].replace('f1_', '')
            print(f"    {metric_short:20s}: Optimized={row['optimized_mean']:.4f}±{row['optimized_std']:.4f}, "
                  f"Baseline={row['baseline_mean']:.4f}±{row['baseline_std']:.4f}, "
                  f"Δ={row['improvement_mean']:+.4f}±{row['improvement_std']:.4f}")
        
        # Display pct_pred_diff
        print("\n  Pct Pred Diff:")
        pct_metrics = bin_summary[bin_summary['metric'].str.startswith('pct_pred_diff_')]
        for _, row in pct_metrics.iterrows():
            metric_short = row['metric'].replace('pct_pred_diff_', '')
            print(f"    {metric_short:20s}: Optimized={row['optimized_mean']:.4f}±{row['optimized_std']:.4f}, "
                  f"Baseline={row['baseline_mean']:.4f}±{row['baseline_std']:.4f}, "
                  f"Δ={row['improvement_mean']:+.4f}±{row['improvement_std']:.4f}")
    
    print("\n" + "=" * 80)
    print(f"✅ Cross-validation completed with {n_splits} splits")
    print("=" * 80)
    
    return summary_df


def compute_metrics_on_concatenated_predictions(
    all_df_preds: List[pd.DataFrame],
    all_df_preds_baseline: List[pd.DataFrame],
    verbose: bool = True
) -> pd.DataFrame:
    """
    Compute metrics on concatenated predictions from multiple bins.
    
    This is a utility function that concatenates all prediction DataFrames
    and computes the overall metrics.
    
    Args:
        all_df_preds: List of DataFrames with optimized predictions
        all_df_preds_baseline: List of DataFrames with baseline predictions
        verbose: Whether to print summary (default: True)
    
    Returns:
        DataFrame with comparison metrics (similar to a single row of comparison_df)
    
    Example:
        >>> # After calling compare_thresholds_per_bin
        >>> comparison_df, all_df_preds, all_df_preds_baseline, _, _ = compare_thresholds_per_bin(...)
        >>> # Compute metrics on concatenated data
        >>> df_pred = pd.concat(all_df_preds)
        >>> df_pred_baseline = pd.concat(all_df_preds_baseline)
        >>> metrics = compare_llm_vs_keyword_predictions(df_pred)
        >>> metrics_baseline = compare_llm_vs_keyword_predictions(df_pred_baseline)
    """
    if not all_df_preds or not all_df_preds_baseline:
        raise ValueError("Lists of predictions cannot be empty")
    
    # Concatenate all predictions
    df_pred_concat = pd.concat(all_df_preds, ignore_index=False)
    df_pred_baseline_concat = pd.concat(all_df_preds_baseline, ignore_index=False)
    
    if verbose:
        print("=" * 80)
        print("📊 Computing metrics on CONCATENATED predictions")
        print("=" * 80)
        print(f"Total articles: {len(df_pred_concat)}")
    
    # Compute metrics
    metrics_optimized = compare_llm_vs_keyword_predictions(df_pred_concat, verbose=verbose)
    metrics_baseline = compare_llm_vs_keyword_predictions(df_pred_baseline_concat, verbose=verbose)
    
    # Extract key metrics
    result_data = []
    
    for crisis_name in ['biodiversite', 'climat', 'ressources', 'CRISE_WITHOUT_RESSOURCES']:
        opt_row = metrics_optimized[metrics_optimized['name'] == crisis_name]
        base_row = metrics_baseline[metrics_baseline['name'] == crisis_name]
        
        if len(opt_row) > 0 and len(base_row) > 0:
            result_data.append({
                'crisis': crisis_name,
                'f1_optimized': opt_row['f1_score'].values[0],
                'f1_baseline': base_row['f1_score'].values[0],
                'f1_improvement': opt_row['f1_score'].values[0] - base_row['f1_score'].values[0],
                'pct_pred_diff_optimized': opt_row['pct_pred_diff'].values[0],
                'pct_pred_diff_baseline': base_row['pct_pred_diff'].values[0],
                'precision_optimized': opt_row['precision'].values[0],
                'precision_baseline': base_row['precision'].values[0],
                'recall_optimized': opt_row['recall'].values[0],
                'recall_baseline': base_row['recall'].values[0],
            })
    
    result_df = pd.DataFrame(result_data)
    
    if verbose:
        print("\n📈 Summary:")
        print(result_df.to_string(index=False))
    
    return result_df
