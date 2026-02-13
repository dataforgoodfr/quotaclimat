{{ config(
    materialized='incremental',
    unique_key=['publication_week', 'source_code', 'keyword', 'sector']
) }}

/*
Factiva Keywords by Sector - Weekly Aggregation

This model aggregates the daily print_media_thematics_keywords by week:
1. Groups data by week (Monday to Sunday), source, keyword, and sector
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
    FROM {{ ref('print_media_thematics_keywords') }}
),

-- Aggregate daily data to weekly level
weekly_aggregates AS (
    SELECT
        DATE_TRUNC('week', ptk.publication_day)::DATE AS publication_week,
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
        
        -- Sum all counts across the week (includes both normal and outlier days)
        SUM(ptk.nb_articles) AS nb_articles,
        SUM(ptk.nb_keyword_occurences) AS nb_keyword_occurences,
        
        -- Mark week as outlier if ANY day in the week is an outlier
        BOOL_OR(ptk.outlier) AS outlier
        
    FROM {{ ref('print_media_thematics_keywords') }} ptk
    CROSS JOIN max_day md
    
    -- Only include complete weeks (where Sunday of that week <= max_publication_day)
    WHERE (DATE_TRUNC('week', ptk.publication_day)::DATE + INTERVAL '6 days')::DATE <= md.max_publication_day
    
    {% if is_incremental() %}
        -- In incremental mode, only recalculate weeks that might have been updated
        AND DATE_TRUNC('week', ptk.publication_day)::DATE >= (
            SELECT COALESCE(MAX(publication_week) - INTERVAL '7 days', '1970-01-01'::DATE)
            FROM {{ this }}
        )
    {% endif %}
    
    GROUP BY 
        DATE_TRUNC('week', ptk.publication_day)::DATE,
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
    publication_week,
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
    
FROM weekly_aggregates

ORDER BY
    publication_week DESC,
    source_code ASC,
    keyword ASC,
    sector ASC
