{{ config(
    materialized='incremental',
    unique_key=['publication_month', 'source_code', 'keyword', 'sector']
) }}

/*
Factiva Keywords by Sector - Monthly Aggregation

This model aggregates the daily print_media_thematics_keywords by month:
1. Groups data by month, source, keyword, and sector
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
    FROM {{ ref('print_media_thematics_keywords') }}
),

-- Aggregate daily data to monthly level
monthly_aggregates AS (
    SELECT
        DATE_TRUNC('month', ptk.publication_day)::DATE AS publication_month,
        -- Aggregate La Tribune (TRDS) into La Tribune.fr (TBNWEB)
        CASE 
            WHEN ptk.source_code = 'TRDS' THEN 'TBNWEB'
            ELSE ptk.source_code
        END AS source_code,
        CASE 
            WHEN ptk.source_code = 'TRDS' THEN 'La Tribune.fr'
            ELSE ptk.source_name
        END AS source_name,
        ptk.media_all,
        ptk.source_type,
        ptk.keyword,
        ptk.sector,
        ptk.is_hrfp,
        
        -- Sum all counts across the month (includes both normal and outlier days)
        SUM(ptk.nb_articles) AS nb_articles,
        SUM(ptk.nb_keyword_occurences) AS nb_keyword_occurences,
        
        -- Mark month as outlier if ANY day in the month is an outlier
        BOOL_OR(ptk.outlier) AS outlier
        
    FROM {{ ref('print_media_thematics_keywords') }} ptk
    CROSS JOIN max_day md
    
    -- Only include complete months (where last day of month <= max_publication_day)
    WHERE (DATE_TRUNC('month', ptk.publication_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE <= md.max_publication_day
    
    {% if is_incremental() %}
        -- In incremental mode, only recalculate months that might have been updated
        AND DATE_TRUNC('month', ptk.publication_day)::DATE >= (
            SELECT COALESCE(MAX(publication_month) - INTERVAL '1 month', '1970-01-01'::DATE)
            FROM {{ this }}
        )
    {% endif %}
    
    GROUP BY 
        DATE_TRUNC('month', ptk.publication_day)::DATE,
        CASE 
            WHEN ptk.source_code = 'TRDS' THEN 'TBNWEB'
            ELSE ptk.source_code
        END,
        CASE 
            WHEN ptk.source_code = 'TRDS' THEN 'La Tribune.fr'
            ELSE ptk.source_name
        END,
        ptk.media_all,
        ptk.source_type,
        ptk.keyword,
        ptk.sector,
        ptk.is_hrfp
)

SELECT
    publication_month,
    source_code,
    source_name,
    media_all,
    source_type,
    keyword,
    sector,
    nb_articles,
    nb_keyword_occurences,
    is_hrfp,
    outlier,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM monthly_aggregates

ORDER BY
    publication_month DESC,
    source_code ASC,
    keyword ASC,
    sector ASC
