"""Utility functions for keyword detection on pandas DataFrames."""

from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score

from quotaclimat.data_processing.factiva.s3_to_postgre.extract_keywords_factiva import (
    extract_keyword_data_from_article,
)


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
    
    # Special column for ressources: concept = constat OR cause OR consequence
    df[f"{prefix}_ressources_concept"] = (
        (df[f"{prefix}_ressources_constat"] == 1) |
        (df[f"{prefix}_ressources_cause"] == 1) |
        (df[f"{prefix}_ressources_consequence"] == 1)
    ).astype(int)
    
    print(f"✓ LLM columns created with prefix '{prefix}'")
    print(f"  - Articles with crises: {df[f'{prefix}_CRISE'].sum()}")
    print(f"  - Articles without crisis: {df[f'{prefix}_PAS DE CRISE'].sum()}")
    
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


def evaluate_threshold(
    y_true: pd.Series,
    scores: pd.Series,
    threshold: float
) -> Dict[str, float]:
    """
    Evaluate a threshold by calculating precision, recall, and F1-score.
    
    Args:
        y_true: Ground truth labels (0 or 1)
        scores: Keyword scores
        threshold: Threshold to apply (score >= threshold → prediction = 1)
        
    Returns:
        Dictionary with precision, recall, f1_score, and counts
    """
    # Predictions based on threshold
    y_pred = (scores >= threshold).astype(int)
    
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
    
    # Test each threshold
    results = []
    for threshold in thresholds:
        metrics = evaluate_threshold(y_true, scores, threshold)
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
        print(f"\nTesting thresholds for {crisis}...")
        multipliers = multipliers_hrfp.get(crisis, [1.0])
        
        for multiplier in multipliers:
            print(f"  - Multiplier HRFP: {multiplier}")
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
            
            ax.set_xlabel('Threshold', fontsize=12)
            ax.set_ylabel('Score', fontsize=12)
            ax.set_title(crisis_labels[crisis], fontsize=13, fontweight='bold')
            ax.legend(loc='best', fontsize=10)
            ax.grid(True, alpha=0.3)
            ax.set_ylim(-0.05, 1.05)
            ax.set_xticks(thresholds_to_test)
        
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
    
    # Test each threshold
    results = []
    for threshold in thresholds:
        # Prediction: crisis must be >= crisis_threshold AND causal_link >= threshold
        y_pred = ((crisis_score >= crisis_threshold) & (causal_link_score >= threshold)).astype(int)
        
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
        print(f"\nTesting causal links for {crisis}...")
        multipliers = multipliers_hrfp.get(crisis, [1.0])
        
        for multiplier in multipliers:
            print(f"  - Multiplier HRFP: {multiplier}")
            
            # Get optimal crisis threshold for this crisis and multiplier
            crisis_threshold = get_optimal_crisis_threshold(
                crisis_results_df, crisis, multiplier
            )
            print(f"    Using crisis threshold: {crisis_threshold} (best P/R ratio)")
            
            # Test each causal link
            for causal_link in causal_links_by_crisis[crisis]:
                print(f"      Testing {causal_link}...")
                results = test_thresholds_for_causal_link(
                    df, crisis, causal_link, crisis_threshold,
                    multiplier, thresholds, llm_prefix
                )
                all_results.append(results)
    
    # Combine all results
    combined_results = pd.concat(all_results, ignore_index=True)
    return combined_results


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
