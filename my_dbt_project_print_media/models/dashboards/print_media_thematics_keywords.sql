{{ config(
    materialized='incremental',
    unique_key=['publication_day', 'source_code', 'keyword', 'sector']
) }}

/*
Factiva Keywords by Sector - Daily Aggregation

This model aggregates keyword usage by sector for Factiva articles:
1. Extracts keywords from all_keywords JSON field in factiva_articles
2. Joins with keyword_macro_category to assign sectors to each keyword
3. Aggregates by day, source, keyword, and sector
4. Only includes articles with word_count >= MINIMAL_WORD_COUNT
5. Only shows days that are 4+ days old (same as print_media_crises_indicators)
6. Only includes articles where predict_at_least_one_crise = TRUE
7. Excludes duplicate articles (duplicate_status != 'DUP')

For each keyword x source x day x sector combination:
- nb_articles: count of distinct articles containing that keyword
- nb_keyword_occurences: sum of count_keyword from all_keywords JSON
- is_hrfp: taken from all_keywords (if keyword appears with both true/false, use false)

Sector Mapping:
- Is Empty → "empty"
- General → "General"
- Agriculture → "Agriculture & Alimentation"
- Transport → "Mobilite"
- Batiments → "Batiment & amenagement"
- Energie → "Energie"
- Industrie → "Industrie"
- Eau → "Eau"
- Ecosysteme → "Ecosysteme"
- Economie Ressources → "Economie circulaire"

Note: If a keyword belongs to multiple sectors, it will appear on separate rows
*/

WITH 
-- Extract keywords from all_keywords JSON field
article_keywords AS (
    SELECT
        fa.an,
        DATE(fa.publication_datetime) AS publication_day,
        fa.source_code,
        json_array_elements(fa.all_keywords) AS keyword_obj
    FROM {{ source('public', 'factiva_articles') }} fa
    WHERE fa.is_deleted = FALSE
        AND fa.publication_datetime IS NOT NULL
        -- Only consider days that are 4+ days old
        AND DATE(fa.publication_datetime) <= CURRENT_DATE - INTERVAL '4 days'
        -- Exclude duplicate articles
        AND fa.duplicate_status != 'DUP'
        -- Only include articles predicted to have at least one crisis
        AND fa.predict_at_least_one_crise = TRUE
        -- Only include articles with sufficient word count
        AND COALESCE(fa.word_count, 0) >= {{ env_var('MINIMAL_WORD_COUNT', '0') | int }}
        -- Only include articles with all_keywords
        AND fa.all_keywords IS NOT NULL
        AND json_array_length(fa.all_keywords) > 0
),

-- Parse keyword details from JSON
-- Note: Same keyword can appear multiple times with different themes
parsed_keywords AS (
    SELECT
        ak.an,
        ak.publication_day,
        ak.source_code,
        (ak.keyword_obj->>'keyword')::TEXT AS keyword,
        (ak.keyword_obj->>'count_keyword')::INTEGER AS count_keyword,
        (ak.keyword_obj->>'is_hrfp')::BOOLEAN AS is_hrfp
    FROM article_keywords ak
    WHERE ak.keyword_obj->>'keyword' IS NOT NULL
),

-- Deduplicate keywords per article (same keyword can have multiple themes with same count)
deduplicated_keywords AS (
    SELECT
        pk.an,
        pk.publication_day,
        pk.source_code,
        pk.keyword,
        BOOL_AND(pk.is_hrfp) AS is_hrfp,
        MAX(pk.count_keyword) AS count_keyword
    FROM parsed_keywords pk
    GROUP BY 
        pk.an,
        pk.publication_day,
        pk.source_code,
        pk.keyword
),

-- Aggregate keywords BEFORE unpivoting sectors (to avoid duplicating count_keyword)
keyword_aggregates AS (
    SELECT
        dk.publication_day,
        dk.source_code,
        dk.keyword,
        BOOL_AND(dk.is_hrfp) AS is_hrfp,
        COUNT(DISTINCT dk.an) AS nb_articles,
        SUM(dk.count_keyword) AS nb_keyword_occurences
    FROM deduplicated_keywords dk
    GROUP BY 
        dk.publication_day,
        dk.source_code,
        dk.keyword
),

-- Join with keyword_macro_category to get sectors
keywords_with_sectors AS (
    SELECT
        ka.publication_day,
        ka.source_code,
        ka.keyword,
        ka.is_hrfp,
        ka.nb_articles,
        ka.nb_keyword_occurences,
        CASE 
            WHEN sector_col = 'is_empty' THEN 'empty'
            WHEN sector_col = 'general' THEN 'General'
            WHEN sector_col = 'agriculture' THEN 'Agriculture & Alimentation'
            WHEN sector_col = 'transport' THEN 'Mobilite'
            WHEN sector_col = 'batiments' THEN 'Batiment & amenagement'
            WHEN sector_col = 'energie' THEN 'Energie'
            WHEN sector_col = 'industrie' THEN 'Industrie'
            WHEN sector_col = 'eau' THEN 'Eau'
            WHEN sector_col = 'ecosysteme' THEN 'Ecosysteme'
            WHEN sector_col = 'economie_ressources' THEN 'Economie circulaire'
        END AS sector
    FROM keyword_aggregates ka
    LEFT JOIN {{ source('public', 'keyword_macro_category') }} kmc
        ON LOWER(TRIM(ka.keyword)) = LOWER(TRIM(kmc.keyword))
    CROSS JOIN LATERAL (
        VALUES 
            ('is_empty', kmc.is_empty),
            ('general', kmc.general),
            ('agriculture', kmc.agriculture),
            ('transport', kmc.transport),
            ('batiments', kmc.batiments),
            ('energie', kmc.energie),
            ('industrie', kmc.industrie),
            ('eau', kmc.eau),
            ('ecosysteme', kmc.ecosysteme),
            ('economie_ressources', kmc.economie_ressources)
    ) AS sectors(sector_col, sector_flag)
    WHERE sector_flag = TRUE  -- Only include sectors where the flag is true
),

-- Select final aggregates (filter if incremental)
daily_aggregates AS (
    SELECT
        kws.publication_day,
        kws.source_code,
        kws.keyword,
        kws.sector,
        kws.is_hrfp,
        kws.nb_articles,
        kws.nb_keyword_occurences
    FROM keywords_with_sectors kws
    
    {% if is_incremental() %}
        -- In incremental mode, only recalculate days that might have been updated
        WHERE kws.publication_day >= (
            SELECT COALESCE(MAX(publication_day) - INTERVAL '7 days', '1970-01-01'::DATE)
            FROM {{ this }}
        )
    {% endif %}
)

-- Final output with source metadata
SELECT
    da.publication_day,
    da.source_code,
    sc.source_name,
    sc.source_type,
    sc.media_all,
    da.keyword,
    da.sector,
    da.nb_articles,
    da.nb_keyword_occurences,
    da.is_hrfp,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM daily_aggregates da
LEFT JOIN {{ source('public', 'source_classification') }} sc
    ON da.source_code = sc.source_code

-- Exclude France24.com and OuestFrance (ecology-only, no total article counts for proportion)
WHERE da.source_code NOT IN ('HTFRFR', 'OUESTFR')

ORDER BY
    da.publication_day DESC,
    da.source_code ASC,
    da.keyword ASC,
    da.sector ASC
