{{ config(
    materialized='incremental',
    unique_key=['publication_day', 'source_code']
) }}

/*
Factiva Environmental Indicators Model

This model aggregates environmental crisis indicators for Factiva articles by:
1. Using pre-calculated prediction flags (predict_*) from factiva_articles table
2. Handling duplicate articles and outlier days
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
- If 0-15 DUP: Source-day is kept, ALL articles are counted (including DUP articles)
- If > 15 DUP: Source-day is an OUTLIER - all counts are replaced by the median of that source (calculated from valid days with <= 15 DUP)

Word Count Filtering:
- Only articles with word_count >= MINIMAL_WORD_COUNT are included in crisis counts

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

-- Identify outlier source-days (> 15 DUP)
outlier_source_days AS (
    SELECT publication_day, source_code
    FROM source_day_duplicate_counts
    WHERE dup_count > 15
),

-- Aggregate articles using pre-calculated prediction flags (non-outlier days only)
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
        -- Exclude outlier source-days (> 15 DUP)
        AND (DATE(fa.publication_datetime), fa.source_code) NOT IN (
            SELECT publication_day, source_code FROM outlier_source_days
        )
        -- NOTE: DUP articles are NOT excluded - they are counted normally for non-outlier days
        -- Only include articles with sufficient word count for crisis counts
        AND COALESCE(fa.word_count, 0) >= {{ env_var('MINIMAL_WORD_COUNT', '0') | int }}
    
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

-- Aggregate stats by day (sum all hourly stats to daily) - non-outlier days only
daily_stats AS (
    SELECT
        DATE(sfa.publication_datetime) AS publication_day,
        sfa.source_code,
        SUM(sfa.count) AS count_total_articles
    FROM {{ source('public', 'stats_factiva_articles') }} sfa
    WHERE DATE(sfa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
        -- Exclude outlier source-days (> 15 DUP)
        AND (DATE(sfa.publication_datetime), sfa.source_code) NOT IN (
            SELECT publication_day, source_code FROM outlier_source_days
        )
    GROUP BY 
        DATE(sfa.publication_datetime),
        sfa.source_code
),

-- Calculate medians for each source from valid days (< 15 DUP)
-- These medians will be used to replace counts for outlier days (> 15 DUP)
source_medians AS (
    SELECT
        da.source_code,
        
        -- Median of count_total_articles from stats
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ds.count_total_articles) AS median_count_total_articles,
        
        -- Medians of crisis counts
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat) AS median_count_climat,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite) AS median_count_biodiversite,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_ressources) AS median_count_ressources,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_at_least_one_crise) AS median_count_at_least_one_crise,
        
        -- Medians of climate causal links
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_constat) AS median_count_climat_constat,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_cause) AS median_count_climat_cause,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_consequence) AS median_count_climat_consequence,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_solution) AS median_count_climat_solution,
        
        -- Medians of biodiversity causal links
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_constat) AS median_count_biodiversite_constat,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_cause) AS median_count_biodiversite_cause,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_consequence) AS median_count_biodiversite_consequence,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_solution) AS median_count_biodiversite_solution,
        
        -- Medians of resource causal links
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_ressources_constat) AS median_count_ressources_constat,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_ressources_solution) AS median_count_ressources_solution,
        
        -- Medians of combined climate causal links
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_cause_consequence) AS median_count_climat_cause_consequence,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_climat_constat_consequence) AS median_count_climat_constat_consequence,
        
        -- Medians of combined biodiversity causal links
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_cause_consequence) AS median_count_biodiversite_cause_consequence,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.count_biodiversite_constat_consequence) AS median_count_biodiversite_constat_consequence
        
    FROM daily_aggregates da
    INNER JOIN daily_stats ds 
        ON da.publication_day = ds.publication_day 
        AND da.source_code = ds.source_code
    GROUP BY da.source_code
)

-- Final output: Start from all_source_days and LEFT JOIN everything
SELECT
    asd.publication_day,
    asd.source_code,
    asd.source_name,
    asd.source_type,
    asd.source_owner,
    
    -- Flag to indicate if this row uses median values (outlier day with > 15 DUP)
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN TRUE
        ELSE FALSE
    END AS outlier,
    
    -- Get total article count from stats
    -- If outlier day (> 15 DUP): use median, otherwise use actual count
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_total_articles, 0)
        ELSE COALESCE(ds.count_total_articles, 0)
    END AS count_total_articles,
    
    -- Crisis counts - use median for outlier days, actual counts otherwise
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat, 0)
        ELSE COALESCE(da.count_climat, 0)
    END AS count_climat,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite, 0)
        ELSE COALESCE(da.count_biodiversite, 0)
    END AS count_biodiversite,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_ressources, 0)
        ELSE COALESCE(da.count_ressources, 0)
    END AS count_ressources,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_at_least_one_crise, 0)
        ELSE COALESCE(da.count_at_least_one_crise, 0)
    END AS count_at_least_one_crise,
    
    -- Climate causal link counts - use median for outlier days
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_constat, 0)
        ELSE COALESCE(da.count_climat_constat, 0)
    END AS count_climat_constat,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_cause, 0)
        ELSE COALESCE(da.count_climat_cause, 0)
    END AS count_climat_cause,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_consequence, 0)
        ELSE COALESCE(da.count_climat_consequence, 0)
    END AS count_climat_consequence,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_solution, 0)
        ELSE COALESCE(da.count_climat_solution, 0)
    END AS count_climat_solution,
    
    -- Biodiversity causal link counts - use median for outlier days
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_constat, 0)
        ELSE COALESCE(da.count_biodiversite_constat, 0)
    END AS count_biodiversite_constat,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_cause, 0)
        ELSE COALESCE(da.count_biodiversite_cause, 0)
    END AS count_biodiversite_cause,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_consequence, 0)
        ELSE COALESCE(da.count_biodiversite_consequence, 0)
    END AS count_biodiversite_consequence,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_solution, 0)
        ELSE COALESCE(da.count_biodiversite_solution, 0)
    END AS count_biodiversite_solution,
    
    -- Resource causal link counts - use median for outlier days
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_ressources_constat, 0)
        ELSE COALESCE(da.count_ressources_constat, 0)
    END AS count_ressources_constat,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_ressources_solution, 0)
        ELSE COALESCE(da.count_ressources_solution, 0)
    END AS count_ressources_solution,
    
    -- Combined climate causal link counts - use median for outlier days
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_cause_consequence, 0)
        ELSE COALESCE(da.count_climat_cause_consequence, 0)
    END AS count_climat_cause_consequence,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_climat_constat_consequence, 0)
        ELSE COALESCE(da.count_climat_constat_consequence, 0)
    END AS count_climat_constat_consequence,
    
    -- Combined biodiversity causal link counts - use median for outlier days
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_cause_consequence, 0)
        ELSE COALESCE(da.count_biodiversite_cause_consequence, 0)
    END AS count_biodiversite_cause_consequence,
    
    CASE 
        WHEN (asd.publication_day, asd.source_code) IN (SELECT publication_day, source_code FROM outlier_source_days)
        THEN COALESCE(sm.median_count_biodiversite_constat_consequence, 0)
        ELSE COALESCE(da.count_biodiversite_constat_consequence, 0)
    END AS count_biodiversite_constat_consequence,
    
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
LEFT JOIN source_medians sm
    ON asd.source_code = sm.source_code

ORDER BY
    asd.publication_day DESC,
    asd.source_code ASC
