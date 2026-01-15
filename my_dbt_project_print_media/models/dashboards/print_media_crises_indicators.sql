{{ config(
    materialized='incremental',
    unique_key=['publication_day', 'source_code']
) }}

/*
Factiva Environmental Indicators Model

This model calculates environmental crisis indicators for Factiva articles by:
1. Starting from source_classification to ensure ALL sources appear
2. Computing crisis scores based on keyword counts and HRFP multipliers
3. Comparing scores to thresholds to determine crisis labeling
4. Aggregating counts by day and source
5. Excluding duplicate articles and source-days with any duplicates
6. Showing 0s for sources with no articles on a given day

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

Date Filtering:
- Only shows days that are 4+ days old (e.g., if today is Dec 19, shows Dec 15 and earlier)
- This allows time for data completeness and duplicate detection

Environment Variables:
- MULTIPLIER_HRFP_CLIMAT (default: 0): Multiplier for climate HRFP keywords
- MULTIPLIER_HRFP_BIODIV (default: 0): Multiplier for biodiversity HRFP keywords
- MULTIPLIER_HRFP_RESSOURCE (default: 0): Multiplier for resource HRFP keywords
- THRESHOLD_BIOD_CLIM_RESS (default: "1,1,1"): Thresholds for biodiv, climat, ressource (format: "x,y,z")
- THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT (default: "1,1,1,1"): Thresholds for biodiversity causal links (format: "constat,cause,consequence,solution")
- THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT (default: "2,1,1,1"): Thresholds for climate causal links (format: "constat,cause,consequence,solution")
- THRESHOLD_RESS_CONST_SOLUT (default: "1,1"): Thresholds for resource causal links (format: "constat,solution")

Causal Link Labeling Logic:
For an article to be counted in a causal link (e.g., count_climat_constat), TWO conditions must be met:
1. The causal link score must meet its threshold (e.g., score_climat_constat >= threshold_climat_constat)
2. The global crisis score must meet its threshold (e.g., score_climat >= threshold_climat)
*/

WITH 
-- Parse environment variables for thresholds
thresholds AS (
    SELECT
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 1) AS FLOAT) AS threshold_biodiv,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 2) AS FLOAT) AS threshold_climat,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CLIM_RESS", "1,1,1") }}', ',', 3) AS FLOAT) AS threshold_ressource
),

-- Parse environment variables for causal link thresholds
causal_thresholds AS (
    SELECT
        -- Biodiversity causal links: constat, cause, consequence, solution
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT", "1,1,1,1") }}', ',', 1) AS FLOAT) AS threshold_biodiv_constat,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT", "1,1,1,1") }}', ',', 2) AS FLOAT) AS threshold_biodiv_cause,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT", "1,1,1,1") }}', ',', 3) AS FLOAT) AS threshold_biodiv_consequence,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_BIOD_CONST_CAUSE_CONSE_SOLUT", "1,1,1,1") }}', ',', 4) AS FLOAT) AS threshold_biodiv_solution,
        -- Climate causal links: constat, cause, consequence, solution
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT", "2,1,1,1") }}', ',', 1) AS FLOAT) AS threshold_climat_constat,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT", "2,1,1,1") }}', ',', 2) AS FLOAT) AS threshold_climat_cause,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT", "2,1,1,1") }}', ',', 3) AS FLOAT) AS threshold_climat_consequence,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_CLIM_CONST_CAUSE_CONSE_SOLUT", "2,1,1,1") }}', ',', 4) AS FLOAT) AS threshold_climat_solution,
        -- Resource causal links: constat, solution
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_RESS_CONST_SOLUT", "1,1") }}', ',', 1) AS FLOAT) AS threshold_ressources_constat,
        CAST(SPLIT_PART('{{ env_var("THRESHOLD_RESS_CONST_SOLUT", "1,1") }}', ',', 2) AS FLOAT) AS threshold_ressources_solution
),

-- Parse environment variables for HRFP multipliers
multipliers AS (
    SELECT
        CAST('{{ env_var("MULTIPLIER_HRFP_CLIMAT", "0") }}' AS FLOAT) AS multiplier_climat,
        CAST('{{ env_var("MULTIPLIER_HRFP_BIODIV", "0") }}' AS FLOAT) AS multiplier_biodiv,
        CAST('{{ env_var("MULTIPLIER_HRFP_RESSOURCE", "0") }}' AS FLOAT) AS multiplier_ressource
),

-- Get all distinct publication days from stats_factiva_articles (4+ days old only)
-- This ensures we have ALL days that have data, even if no articles in factiva_articles
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

-- Calculate scores for each article
article_scores AS (
    SELECT
        fa.an,
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code,
        sc.source_name,
        sc.source_type,
        sc.source_owner,
        
        -- Calculate climate score (aggregated)
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_climat_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_climat_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_climat_hrfp, 0))
        END AS score_climat,
        
        -- Calculate biodiversity score (aggregated)
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_hrfp, 0))
        END AS score_biodiversite,
        
        -- Calculate resource score (aggregated)
        CASE 
            WHEN m.multiplier_ressource = 0 THEN 
                COALESCE(fa.number_of_ressources_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_ressources_no_hrfp, 0) + 
                (m.multiplier_ressource * COALESCE(fa.number_of_ressources_hrfp, 0))
        END AS score_ressources,
        
        -- Calculate climate causal link scores
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_changement_climatique_constat_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_changement_climatique_constat_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_changement_climatique_constat_hrfp, 0))
        END AS score_climat_constat,
        
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_changement_climatique_causes_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_changement_climatique_causes_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_changement_climatique_causes_hrfp, 0))
        END AS score_climat_cause,
        
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_changement_climatique_consequences_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_changement_climatique_consequences_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_changement_climatique_consequences_hrfp, 0))
        END AS score_climat_consequence,
        
        CASE 
            WHEN m.multiplier_climat = 0 THEN 
                COALESCE(fa.number_of_changement_climatique_solutions_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_changement_climatique_solutions_no_hrfp, 0) + 
                (m.multiplier_climat * COALESCE(fa.number_of_changement_climatique_solutions_hrfp, 0))
        END AS score_climat_solution,
        
        -- Calculate biodiversity causal link scores
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_concepts_generaux_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_concepts_generaux_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_concepts_generaux_hrfp, 0))
        END AS score_biodiv_constat,
        
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_causes_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_causes_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_causes_hrfp, 0))
        END AS score_biodiv_cause,
        
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_consequences_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_consequences_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_consequences_hrfp, 0))
        END AS score_biodiv_consequence,
        
        CASE 
            WHEN m.multiplier_biodiv = 0 THEN 
                COALESCE(fa.number_of_biodiversite_solutions_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_biodiversite_solutions_no_hrfp, 0) + 
                (m.multiplier_biodiv * COALESCE(fa.number_of_biodiversite_solutions_hrfp, 0))
        END AS score_biodiv_solution,
        
        -- Calculate resource causal link scores
        CASE 
            WHEN m.multiplier_ressource = 0 THEN 
                COALESCE(fa.number_of_ressources_constat_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_ressources_constat_no_hrfp, 0) + 
                (m.multiplier_ressource * COALESCE(fa.number_of_ressources_constat_hrfp, 0))
        END AS score_ressources_constat,
        
        CASE 
            WHEN m.multiplier_ressource = 0 THEN 
                COALESCE(fa.number_of_ressources_solutions_no_hrfp, 0)
            ELSE 
                COALESCE(fa.number_of_ressources_solutions_no_hrfp, 0) + 
                (m.multiplier_ressource * COALESCE(fa.number_of_ressources_solutions_hrfp, 0))
        END AS score_ressources_solution
        
    FROM {{ source('public', 'factiva_articles') }} fa
    INNER JOIN {{ source('public', 'source_classification') }} sc
        ON fa.source_code = sc.source_code
    CROSS JOIN multipliers m
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
        
        -- Determine if article meets threshold for each crisis (aggregated)
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
        END AS is_at_least_one_crise,
        
        -- Determine if article meets threshold for each climate causal link
        -- Double condition: causal link threshold AND global crisis threshold
        CASE 
            WHEN a.score_climat_constat >= ct.threshold_climat_constat 
                AND a.score_climat >= t.threshold_climat 
            THEN 1 ELSE 0 
        END AS is_climat_constat,
        CASE 
            WHEN a.score_climat_cause >= ct.threshold_climat_cause 
                AND a.score_climat >= t.threshold_climat 
            THEN 1 ELSE 0 
        END AS is_climat_cause,
        CASE 
            WHEN a.score_climat_consequence >= ct.threshold_climat_consequence 
                AND a.score_climat >= t.threshold_climat 
            THEN 1 ELSE 0 
        END AS is_climat_consequence,
        CASE 
            WHEN a.score_climat_solution >= ct.threshold_climat_solution 
                AND a.score_climat >= t.threshold_climat 
            THEN 1 ELSE 0 
        END AS is_climat_solution,
        
        -- Determine if article meets threshold for each biodiversity causal link
        -- Double condition: causal link threshold AND global crisis threshold
        CASE 
            WHEN a.score_biodiv_constat >= ct.threshold_biodiv_constat 
                AND a.score_biodiversite >= t.threshold_biodiv 
            THEN 1 ELSE 0 
        END AS is_biodiversite_constat,
        CASE 
            WHEN a.score_biodiv_cause >= ct.threshold_biodiv_cause 
                AND a.score_biodiversite >= t.threshold_biodiv 
            THEN 1 ELSE 0 
        END AS is_biodiversite_cause,
        CASE 
            WHEN a.score_biodiv_consequence >= ct.threshold_biodiv_consequence 
                AND a.score_biodiversite >= t.threshold_biodiv 
            THEN 1 ELSE 0 
        END AS is_biodiversite_consequence,
        CASE 
            WHEN a.score_biodiv_solution >= ct.threshold_biodiv_solution 
                AND a.score_biodiversite >= t.threshold_biodiv 
            THEN 1 ELSE 0 
        END AS is_biodiversite_solution,
        
        -- Determine if article meets threshold for each resource causal link
        -- Double condition: causal link threshold AND global crisis threshold
        CASE 
            WHEN a.score_ressources_constat >= ct.threshold_ressources_constat 
                AND a.score_ressources >= t.threshold_ressource 
            THEN 1 ELSE 0 
        END AS is_ressources_constat,
        CASE 
            WHEN a.score_ressources_solution >= ct.threshold_ressources_solution 
                AND a.score_ressources >= t.threshold_ressource 
            THEN 1 ELSE 0 
        END AS is_ressources_solution
        
    FROM article_scores a
    CROSS JOIN thresholds t
    CROSS JOIN causal_thresholds ct
),

-- Aggregate by day and source (from articles only)
daily_aggregates AS (
    SELECT
        al.publication_day,
        al.source_code,
        
        -- Count articles by crisis type
        SUM(al.is_climat) AS count_climat,
        SUM(al.is_biodiversite) AS count_biodiversite,
        SUM(al.is_ressources) AS count_ressources,
        SUM(al.is_at_least_one_crise) AS count_at_least_one_crise,
        
        -- Count articles by climate causal links
        SUM(al.is_climat_constat) AS count_climat_constat,
        SUM(al.is_climat_cause) AS count_climat_cause,
        SUM(al.is_climat_consequence) AS count_climat_consequence,
        SUM(al.is_climat_solution) AS count_climat_solution,
        
        -- Count articles by biodiversity causal links
        SUM(al.is_biodiversite_constat) AS count_biodiversite_constat,
        SUM(al.is_biodiversite_cause) AS count_biodiversite_cause,
        SUM(al.is_biodiversite_consequence) AS count_biodiversite_consequence,
        SUM(al.is_biodiversite_solution) AS count_biodiversite_solution,
        
        -- Count articles by resource causal links
        SUM(al.is_ressources_constat) AS count_ressources_constat,
        SUM(al.is_ressources_solution) AS count_ressources_solution
        
    FROM article_labels al
    GROUP BY 
        al.publication_day,
        al.source_code
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
