{{ config(
    materialized='incremental',
    unique_key=['publication_day', 'source_code']
) }}

/*
Factiva Environmental Indicators Model

This model calculates environmental crisis indicators for Factiva articles by:
1. Joining factiva_articles, stats_factiva_articles, and source_classification
2. Computing crisis scores based on keyword counts and HRFP multipliers
3. Comparing scores to thresholds to determine crisis labeling
4. Aggregating counts by day and source
5. Excluding duplicate articles and source-days with any duplicates

Materialization Strategy:
- materialized='incremental' with unique_key for flexibility
- Run with --full-refresh to ensure all changes are captured (default behavior)
- Can be run without --full-refresh for faster incremental updates if needed

The --full-refresh flag ensures all changes are captured:
- Article updates (keywords, classifications, duplicate status)
- Stats updates (article counts)
- Threshold changes (MULTIPLIER_*, THRESHOLD_*)

Duplicate Handling:
- Articles with duplicate_status = 'DUP' are NEVER included in calculations
- If 0 DUP: All articles are considered normally
- If exactly 1 DUP: The DUP article is excluded, source-day is kept, count_total_articles is reduced by 1 (min 0)
- If 2+ DUP: The entire source-day is COMPLETELY EXCLUDED from all calculations

Environment Variables:
- MULTIPLIER_HRFP_CLIMAT (default: 0): Multiplier for climate HRFP keywords
- MULTIPLIER_HRFP_BIODIV (default: 0): Multiplier for biodiversity HRFP keywords
- MULTIPLIER_HRFP_RESSOURCE (default: 0): Multiplier for resource HRFP keywords
- THRESHOLD_BIOD_CLIM_RESS (default: "1,1,1"): Thresholds for biodiv, climat, ressource (format: "x,y,z")
*/

WITH 
-- Parse environment variables for thresholds
thresholds AS (
    SELECT
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 1) AS FLOAT) AS threshold_biodiv,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 2) AS FLOAT) AS threshold_climat,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 3) AS FLOAT) AS threshold_ressource
),

-- Parse environment variables for HRFP multipliers
multipliers AS (
    SELECT
        CAST('{{ env_var("MULTIPLIER_HRFP_CLIMAT", "0") }}' AS FLOAT) AS multiplier_climat,
        CAST('{{ env_var("MULTIPLIER_HRFP_BIODIV", "0") }}' AS FLOAT) AS multiplier_biodiv,
        CAST('{{ env_var("MULTIPLIER_HRFP_RESSOURCE", "0") }}' AS FLOAT) AS multiplier_ressource
),

-- In incremental mode, identify days/sources that need recalculation
{% if is_incremental() %}
days_to_recalculate AS (
    -- Get the last update timestamp from the target table
    WITH last_update AS (
        SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamp) AS max_updated_at
        FROM {{ this }}
    )
    
    -- Find days/sources with updated articles
    SELECT DISTINCT
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code
    FROM {{ source('public', 'factiva_articles') }} fa
    CROSS JOIN last_update
    WHERE fa.updated_at > last_update.max_updated_at
    
    UNION
    
    -- Find days/sources with updated stats
    SELECT DISTINCT
        DATE(sfa.publication_datetime) AS publication_day,
        sfa.source_code
    FROM {{ source('public', 'stats_factiva_articles') }} sfa
    CROSS JOIN last_update
    WHERE sfa.updated_at > last_update.max_updated_at
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

-- Calculate scores for each article
article_scores AS (
    SELECT
        fa.an,
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code,
        sc.source_name,
        sc.source_type,
        
        -- Calculate climate score
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_climat_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_climat_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_climat_hrfp, 0))
        END AS score_climat,
        
        -- Calculate biodiversity score
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_hrfp, 0))
        END AS score_biodiversite,
        
        -- Calculate resource score
        CASE 
            WHEN m.multiplier_ressource = 0 THEN 
                COALESCE(fa.number_of_ressources_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_ressources_no_hrfp, 0) + 
                (m.multiplier_ressource * COALESCE(fa.number_of_ressources_hrfp, 0))
        END AS score_ressources
        
    FROM {{ source('public', 'factiva_articles') }} fa
    INNER JOIN {{ source('public', 'source_classification') }} sc
        ON fa.source_code = sc.source_code
    CROSS JOIN multipliers m
    WHERE fa.is_deleted = FALSE
        AND fa.publication_datetime IS NOT NULL
        -- Exclude source-days with 2+ DUP completely
        AND (DATE(fa.publication_datetime), fa.source_code) NOT IN (
            SELECT publication_day, source_code FROM source_days_to_exclude
        )
        -- Exclude individual articles marked as DUP (but keep their source-day if only 1 DUP)
        AND (fa.duplicate_status IS NULL OR fa.duplicate_status != 'DUP')
    
    {% if is_incremental() %}
        -- In incremental mode, recalculate ALL articles for days/sources that had updates
        -- This ensures aggregates are correct even if only 1 article or 1 stat changed
        -- Monitors both factiva_articles.updated_at AND stats_factiva_articles.updated_at
        AND (DATE(fa.publication_datetime), fa.source_code) IN (
            SELECT publication_day, source_code FROM days_to_recalculate
        )
    {% endif %}
),

-- Label articles based on thresholds
article_labels AS (
    SELECT
        a.*,
        t.threshold_climat,
        t.threshold_biodiv,
        t.threshold_ressource,
        
        -- Determine if article meets threshold for each crisis
        CASE WHEN a.score_climat >= t.threshold_climat THEN 1 ELSE 0 END AS is_climat,
        CASE WHEN a.score_biodiversite >= t.threshold_biodiv THEN 1 ELSE 0 END AS is_biodiversite,
        CASE WHEN a.score_ressources >= t.threshold_ressource THEN 1 ELSE 0 END AS is_ressources,
        
        -- Determine if article has at least one crisis
        CASE 
            WHEN a.score_climat >= t.threshold_climat 
                OR a.score_biodiversite >= t.threshold_biodiv 
                OR a.score_ressources >= t.threshold_ressource 
            THEN 1 
            ELSE 0 
        END AS is_at_least_one_crise
        
    FROM article_scores a
    CROSS JOIN thresholds t
),

-- Aggregate by day and source
daily_aggregates AS (
    SELECT
        al.publication_day,
        al.source_code,
        al.source_name,
        al.source_type,
        
        -- Count articles by crisis type
        SUM(al.is_climat) AS count_climat,
        SUM(al.is_biodiversite) AS count_biodiversite,
        SUM(al.is_ressources) AS count_ressources,
        SUM(al.is_at_least_one_crise) AS count_at_least_one_crise,
        
        -- Metadata
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
        
    FROM article_labels al
    GROUP BY 
        al.publication_day,
        al.source_code,
        al.source_name,
        al.source_type
)

-- Final output: join with stats to get total article count
SELECT
    da.publication_day,
    da.source_code,
    da.source_name,
    da.source_type,
    
    -- Get total article count from stats (aggregated to day level)
    -- If source-day has exactly 1 DUP, subtract 1 from count (minimum 0)
    CASE 
        WHEN (da.publication_day, da.source_code) IN (SELECT publication_day, source_code FROM source_days_with_one_dup)
        THEN GREATEST(COALESCE(SUM(sfa.count), 0) - 1, 0)
        ELSE COALESCE(SUM(sfa.count), 0)
    END AS count_total_articles,
    
    -- Crisis counts
    da.count_climat,
    da.count_biodiversite,
    da.count_ressources,
    da.count_at_least_one_crise,
    
    -- Metadata
    da.created_at,
    da.updated_at
    
FROM daily_aggregates da
LEFT JOIN {{ source('public', 'stats_factiva_articles') }} sfa
    ON da.source_code = sfa.source_code
    AND DATE(sfa.publication_datetime) = da.publication_day

GROUP BY
    da.publication_day,
    da.source_code,
    da.source_name,
    da.source_type,
    da.count_climat,
    da.count_biodiversite,
    da.count_ressources,
    da.count_at_least_one_crise,
    da.created_at,
    da.updated_at

ORDER BY
    da.publication_day DESC,
    da.source_code ASC
