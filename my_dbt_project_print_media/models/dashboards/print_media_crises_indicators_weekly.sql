{{ config(
    materialized='incremental',
    unique_key=['publication_week', 'source_code']
) }}

/*
Factiva Environmental Indicators Weekly Aggregation

This model aggregates the daily print_media_crises_indicators by week:
1. Groups data by week (Monday to Sunday) and source
2. Sums all counts across the week
3. Only includes COMPLETE weeks (where the Sunday of that week is covered in the daily data)
4. Weeks start on Monday (publication_week = date of Monday)

Materialization Strategy:
- materialized='incremental' with unique_key for flexibility
- Run with --full-refresh to ensure all changes are captured
*/

WITH 
-- Get the maximum publication_day available in the daily table
max_day AS (
    SELECT MAX(publication_day) AS max_publication_day
    FROM {{ ref('print_media_crises_indicators') }}
),

-- Aggregate daily data to weekly level
weekly_aggregates AS (
    SELECT
        DATE_TRUNC('week', pci.publication_day)::DATE AS publication_week,
        -- Aggregate La Tribune (TRDS) into La Tribune.fr (TBNWEB)
        CASE 
            WHEN pci.source_code = 'TRDS' THEN 'TBNWEB'
            ELSE pci.source_code
        END AS source_code,
        CASE 
            WHEN pci.source_code = 'TRDS' THEN 'La Tribune.fr'
            ELSE pci.source_name
        END AS source_name,
        pci.source_type,
        pci.source_owner,
        pci.source_region,
        
        -- Sum all counts across the week
        SUM(pci.count_total_articles) AS count_total_articles,
        SUM(pci.count_climat) AS count_climat,
        SUM(pci.count_biodiversite) AS count_biodiversite,
        SUM(pci.count_ressources) AS count_ressources,
        SUM(pci.count_at_least_one_crise) AS count_at_least_one_crise,
        
        -- Climate causal links
        SUM(pci.count_climat_constat) AS count_climat_constat,
        SUM(pci.count_climat_cause) AS count_climat_cause,
        SUM(pci.count_climat_consequence) AS count_climat_consequence,
        SUM(pci.count_climat_solution) AS count_climat_solution,
        
        -- Biodiversity causal links
        SUM(pci.count_biodiversite_constat) AS count_biodiversite_constat,
        SUM(pci.count_biodiversite_cause) AS count_biodiversite_cause,
        SUM(pci.count_biodiversite_consequence) AS count_biodiversite_consequence,
        SUM(pci.count_biodiversite_solution) AS count_biodiversite_solution,
        
        -- Resource causal links
        SUM(pci.count_ressources_constat) AS count_ressources_constat,
        SUM(pci.count_ressources_solution) AS count_ressources_solution,
        
        -- Combined climate causal links
        SUM(pci.count_climat_cause_consequence) AS count_climat_cause_consequence,
        SUM(pci.count_climat_constat_consequence) AS count_climat_constat_consequence,
        
        -- Combined biodiversity causal links
        SUM(pci.count_biodiversite_cause_consequence) AS count_biodiversite_cause_consequence,
        SUM(pci.count_biodiversite_constat_consequence) AS count_biodiversite_constat_consequence
        
    FROM {{ ref('print_media_crises_indicators') }} pci
    CROSS JOIN max_day md
    
    -- Only include complete weeks (where Sunday of that week <= max_publication_day)
    WHERE (DATE_TRUNC('week', pci.publication_day)::DATE + INTERVAL '6 days')::DATE <= md.max_publication_day
    
    {% if is_incremental() %}
        -- In incremental mode, only recalculate weeks that might have been updated
        AND DATE_TRUNC('week', pci.publication_day)::DATE >= (
            SELECT COALESCE(MAX(publication_week) - INTERVAL '7 days', '1970-01-01'::DATE)
            FROM {{ this }}
        )
    {% endif %}
    
    GROUP BY 
        DATE_TRUNC('week', pci.publication_day)::DATE,
        CASE 
            WHEN pci.source_code = 'TRDS' THEN 'TBNWEB'
            ELSE pci.source_code
        END,
        CASE 
            WHEN pci.source_code = 'TRDS' THEN 'La Tribune.fr'
            ELSE pci.source_name
        END,
        pci.source_type,
        pci.source_owner,
        pci.source_region
)

SELECT
    publication_week,
    source_code,
    source_name,
    source_type,
    source_owner,
    source_region,
    count_total_articles,
    count_climat,
    count_biodiversite,
    count_ressources,
    count_at_least_one_crise,
    count_climat_constat,
    count_climat_cause,
    count_climat_consequence,
    count_climat_solution,
    count_biodiversite_constat,
    count_biodiversite_cause,
    count_biodiversite_consequence,
    count_biodiversite_solution,
    count_ressources_constat,
    count_ressources_solution,
    count_climat_cause_consequence,
    count_climat_constat_consequence,
    count_biodiversite_cause_consequence,
    count_biodiversite_constat_consequence,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM weekly_aggregates

ORDER BY
    publication_week DESC,
    source_code ASC

