{{ config(
    materialized='incremental',
    unique_key=['publication_day', 'source_code']
) }}

/*
Factiva Environmental Indicators Model

This model aggregates environmental crisis indicators for Factiva articles by:
1. Using pre-calculated prediction flags (predict_*) from factiva_articles table
2. Handling duplicate articles (excluding DUP, adjusting counts for source-days with duplicates)
3. Aggregating counts by day and source
4. Starting from source_classification to ensure ALL sources appear
5. Showing 0s for sources with no articles on a given day

Materialization Strategy:
- materialized='incremental' with unique_key for flexibility
- Run with --full-refresh to ensure all changes are captured (default behavior)
- Can be run without --full-refresh for faster incremental updates if needed

The --full-refresh flag ensures all changes are captured:
- Article updates (prediction flags, duplicate status)
- Stats updates (article counts)

Prediction Flags:
- Prediction flags are PRE-CALCULATED in factiva_articles by Python code
- Python code replicates the exact logic from the previous version of this SQL model
- Flags are calculated based on keyword scores, HRFP multipliers, article length segments, and thresholds
- This simplification dramatically improves query performance

Duplicate Handling:
- Articles with duplicate_status = 'DUP' are NEVER included in calculations
- If 0 DUP: All articles are considered normally
- If exactly 1 DUP: The DUP article is excluded, source-day is kept, count_total_articles is reduced by 1 (min 0)
- If 2+ DUP: The entire source-day is COMPLETELY EXCLUDED from all calculations

Date Filtering:
- Only shows days that are 4+ days old (e.g., if today is Dec 19, shows Dec 15 and earlier)
- This allows time for data completeness and duplicate detection
*/

WITH 
-- Get all distinct publication days from stats_factiva_articles (4+ days old only)
all_publication_days AS (
    SELECT DISTINCT
        DATE(publication_datetime) AS publication_day
    FROM {{ source('public', 'stats_factiva_articles') }}
    WHERE DATE(publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
),

-- Create a complete grid of all source-days combinations
all_source_days AS (
    SELECT
        pd.publication_day,
        sc.source_code,
        sc.source_name,
        sc.source_type,
        sc.source_owner
    FROM all_publication_days pd
    CROSS JOIN {{ source('public', 'source_classification') }} sc
),

-- In incremental mode, identify days/sources that need recalculation
{% if is_incremental() %}
days_to_recalculate AS (
    -- Get the last update timestamp from the target table
    WITH last_update AS (
        SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamp) AS max_updated_at
        FROM {{ this }}
    )
    
    -- Find days/sources with updated articles (4+ days old only)
    SELECT DISTINCT
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code
    FROM {{ source('public', 'factiva_articles') }} fa
    CROSS JOIN last_update
    WHERE fa.updated_at > last_update.max_updated_at
        AND DATE(fa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
    
    UNION
    
    -- Find days/sources with updated stats (4+ days old only)
    SELECT DISTINCT
        DATE(sfa.publication_datetime) AS publication_day,
        sfa.source_code
    FROM {{ source('public', 'stats_factiva_articles') }} sfa
    CROSS JOIN last_update
    WHERE sfa.updated_at > last_update.max_updated_at
        AND DATE(sfa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
),
{% endif %}

-- Count duplicates per source-day
source_day_duplicate_counts AS (
    SELECT 
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code,
        COUNT(*) FILTER (WHERE fa.duplicate_status = 'DUP') as dup_count
    FROM {{ source('public', 'factiva_articles') }} fa
    WHERE fa.is_deleted = FALSE
        AND fa.publication_datetime IS NOT NULL
        AND DATE(fa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
    GROUP BY DATE(fa.publication_datetime), fa.source_code
),

-- Identify source-days to completely exclude (2+ DUP)
source_days_to_exclude AS (
    SELECT publication_day, source_code
    FROM source_day_duplicate_counts
    WHERE dup_count >= 2
),

-- Identify source-days with exactly 1 DUP (need to adjust count_total_articles)
source_days_with_one_dup AS (
    SELECT publication_day, source_code
    FROM source_day_duplicate_counts
    WHERE dup_count = 1
),

-- Aggregate articles using pre-calculated prediction flags
daily_aggregates AS (
    SELECT
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code,
        
        -- Count articles by crisis type (using prediction flags)
        COUNT(*) FILTER (WHERE fa.predict_climat) AS count_climat,
        COUNT(*) FILTER (WHERE fa.predict_biodiversite) AS count_biodiversite,
        COUNT(*) FILTER (WHERE fa.predict_ressources) AS count_ressources,
        COUNT(*) FILTER (WHERE fa.predict_at_least_one_crise) AS count_at_least_one_crise,
        
        -- Count articles by climate causal links (using prediction flags)
        COUNT(*) FILTER (WHERE fa.predict_climat_constat) AS count_climat_constat,
        COUNT(*) FILTER (WHERE fa.predict_climat_cause) AS count_climat_cause,
        COUNT(*) FILTER (WHERE fa.predict_climat_consequence) AS count_climat_consequence,
        COUNT(*) FILTER (WHERE fa.predict_climat_solution) AS count_climat_solution,
        
        -- Count articles by biodiversity causal links (using prediction flags)
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_constat) AS count_biodiversite_constat,
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_cause) AS count_biodiversite_cause,
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_consequence) AS count_biodiversite_consequence,
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_solution) AS count_biodiversite_solution,
        
        -- Count articles by resource causal links (using prediction flags)
        COUNT(*) FILTER (WHERE fa.predict_ressources_constat) AS count_ressources_constat,
        COUNT(*) FILTER (WHERE fa.predict_ressources_solution) AS count_ressources_solution,
        
        -- Count articles with combined climate causal links
        COUNT(*) FILTER (WHERE fa.predict_climat_cause AND fa.predict_climat_consequence) AS count_climat_cause_consequence,
        COUNT(*) FILTER (WHERE fa.predict_climat_constat AND fa.predict_climat_consequence) AS count_climat_constat_consequence,
        
        -- Count articles with combined biodiversity causal links
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_cause AND fa.predict_biodiversite_consequence) AS count_biodiversite_cause_consequence,
        COUNT(*) FILTER (WHERE fa.predict_biodiversite_constat AND fa.predict_biodiversite_consequence) AS count_biodiversite_constat_consequence
        
    FROM {{ source('public', 'factiva_articles') }} fa
    WHERE fa.is_deleted = FALSE
        AND fa.publication_datetime IS NOT NULL
        -- Only consider days that are 4+ days old
        AND DATE(fa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
        -- Exclude source-days with 2+ DUP completely
        AND (DATE(fa.publication_datetime), fa.source_code) NOT IN (
            SELECT publication_day, source_code FROM source_days_to_exclude
        )
        -- Exclude individual articles marked as DUP (but keep their source-day if only 1 DUP)
        AND (fa.duplicate_status IS NULL OR fa.duplicate_status != 'DUP')
    
    {% if is_incremental() %}
        -- In incremental mode, recalculate ALL articles for days/sources that had updates
        AND (DATE(fa.publication_datetime), fa.source_code) IN (
            SELECT publication_day, source_code FROM days_to_recalculate
        )
    {% endif %}
    
    GROUP BY 
        DATE(fa.publication_datetime),
        fa.source_code
),

-- Aggregate stats by day (sum all hourly stats to daily)
daily_stats AS (
    SELECT
        DATE(sfa.publication_datetime) AS publication_day,
        sfa.source_code,
        SUM(sfa.count) AS count_total_articles
    FROM {{ source('public', 'stats_factiva_articles') }} sfa
    WHERE DATE(sfa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
    GROUP BY 
        DATE(sfa.publication_datetime),
        sfa.source_code
)

-- Final output: Start from all_source_days and LEFT JOIN everything
SELECT
    asd.publication_day,
    asd.source_code,
    asd.source_name,
    asd.source_type,
    asd.source_owner,
    
    -- Get total article count from stats (aggregated to day level)
    -- If source-day has exactly 1 DUP, subtract 1 from count (minimum 0)
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM source_days_with_one_dup)
        THEN GREATEST(COALESCE(ds.count_total_articles, 0) - 1, 0)
        ELSE COALESCE(ds.count_total_articles, 0)
    END AS count_total_articles,
    
    -- Crisis counts (aggregated, 0 if no articles)
    COALESCE(da.count_climat, 0) AS count_climat,
    COALESCE(da.count_biodiversite, 0) AS count_biodiversite,
    COALESCE(da.count_ressources, 0) AS count_ressources,
    COALESCE(da.count_at_least_one_crise, 0) AS count_at_least_one_crise,
    
    -- Climate causal link counts (0 if no articles)
    COALESCE(da.count_climat_constat, 0) AS count_climat_constat,
    COALESCE(da.count_climat_cause, 0) AS count_climat_cause,
    COALESCE(da.count_climat_consequence, 0) AS count_climat_consequence,
    COALESCE(da.count_climat_solution, 0) AS count_climat_solution,
    
    -- Biodiversity causal link counts (0 if no articles)
    COALESCE(da.count_biodiversite_constat, 0) AS count_biodiversite_constat,
    COALESCE(da.count_biodiversite_cause, 0) AS count_biodiversite_cause,
    COALESCE(da.count_biodiversite_consequence, 0) AS count_biodiversite_consequence,
    COALESCE(da.count_biodiversite_solution, 0) AS count_biodiversite_solution,
    
    -- Resource causal link counts (0 if no articles)
    COALESCE(da.count_ressources_constat, 0) AS count_ressources_constat,
    COALESCE(da.count_ressources_solution, 0) AS count_ressources_solution,
    
    -- Combined climate causal link counts (0 if no articles)
    COALESCE(da.count_climat_cause_consequence, 0) AS count_climat_cause_consequence,
    COALESCE(da.count_climat_constat_consequence, 0) AS count_climat_constat_consequence,
    
    -- Combined biodiversity causal link counts (0 if no articles)
    COALESCE(da.count_biodiversite_cause_consequence, 0) AS count_biodiversite_cause_consequence,
    COALESCE(da.count_biodiversite_constat_consequence, 0) AS count_biodiversite_constat_consequence,
    
    -- Metadata
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM all_source_days asd
LEFT JOIN daily_aggregates da
    ON asd.publication_day = da.publication_day
    AND asd.source_code = da.source_code
LEFT JOIN daily_stats ds
    ON asd.publication_day = ds.publication_day
    AND asd.source_code = ds.source_code

ORDER BY
    asd.publication_day DESC,
    asd.source_code ASC
