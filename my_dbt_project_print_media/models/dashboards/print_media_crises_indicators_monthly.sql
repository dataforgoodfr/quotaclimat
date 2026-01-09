{{ config(
    materialized='incremental',
    unique_key=['publication_month', 'source_code']
) }}

/*
Factiva Environmental Indicators Monthly Aggregation

This model aggregates the daily print_media_crises_indicators by month:
1. Groups data by month and source
2. Sums all counts across the month
3. Only includes COMPLETE months (where the last day of that month is covered in the daily data)
4. publication_month = first day of the month

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

-- Aggregate daily data to monthly level
monthly_aggregates AS (
    SELECT
        DATE_TRUNC('month', pci.publication_day)::DATE AS publication_month,
        pci.source_code,
        pci.source_name,
        pci.source_type,
        
        -- Sum all counts across the month
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
        SUM(pci.count_ressources_solution) AS count_ressources_solution
        
    FROM {{ ref('print_media_crises_indicators') }} pci
    CROSS JOIN max_day md
    
    -- Only include complete months (where last day of month <= max_publication_day)
    WHERE (DATE_TRUNC('month', pci.publication_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= md.max_publication_day
    
    {% if is_incremental() %}
        -- In incremental mode, only recalculate months that might have been updated
        AND DATE_TRUNC('month', pci.publication_day)::DATE >= (
            SELECT COALESCE(MAX(publication_month) - INTERVAL '1 month', '1970-01-01'::DATE)
            FROM {{ this }}
        )
    {% endif %}
    
    GROUP BY 
        DATE_TRUNC('month', pci.publication_day)::DATE,
        pci.source_code,
        pci.source_name,
        pci.source_type
)

SELECT
    publication_month,
    source_code,
    source_name,
    source_type,
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
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM monthly_aggregates

ORDER BY
    publication_month DESC,
    source_code ASC

