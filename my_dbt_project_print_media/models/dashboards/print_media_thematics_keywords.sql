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
6. Handles outlier days (> 15 DUP) by replacing counts with medians

For each keyword x source x day x sector combination:
- nb_articles: count of distinct articles containing that keyword
- nb_keyword_occurences: sum of count_keyword from all_keywords JSON
- is_hrfp: taken from all_keywords (if keyword appears with both true/false, use false)
- outlier: TRUE if this source-day has > 15 duplicates (counts are replaced with medians)

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

Outlier Handling:
- If 0-15 DUP: Source-day is kept, actual counts are used
- If > 15 DUP: Source-day is an OUTLIER - counts are replaced by the median for that keyword x sector x source
  (calculated from valid days with <= 15 DUP). If median = 0, use 1 instead.

Note: If a keyword belongs to multiple sectors, it will appear on separate rows
*/

WITH 
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
-- Extract keywords from all_keywords JSON field (non-outlier days only)
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
        -- Exclude outlier source-days (> 15 DUP)
        AND (DATE(fa.publication_datetime), fa.source_code) NOT IN (
            SELECT publication_day, source_code FROM outlier_source_days
        )
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

-- Select final aggregates (already aggregated, just filter if incremental)
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
),

-- Calculate medians for each keyword x sector x source from valid days (<= 15 DUP)
-- These medians will be used to replace counts for outlier days (> 15 DUP)
keyword_sector_medians AS (
    SELECT
        da.source_code,
        da.keyword,
        da.sector,
        
        -- Median of nb_articles
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.nb_articles) AS median_nb_articles,
        
        -- Median of nb_keyword_occurences
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY da.nb_keyword_occurences) AS median_nb_keyword_occurences
        
    FROM daily_aggregates da
    GROUP BY da.source_code, da.keyword, da.sector
),

-- Extract keywords from outlier days to know which keywords appear on outlier days
outlier_keywords AS (
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
        -- ONLY include outlier source-days (> 15 DUP)
        AND (DATE(fa.publication_datetime), fa.source_code) IN (
            SELECT publication_day, source_code FROM outlier_source_days
        )
        -- Only include articles with sufficient word count
        AND COALESCE(fa.word_count, 0) >= {{ env_var('MINIMAL_WORD_COUNT', '0') | int }}
        -- Only include articles with all_keywords
        AND fa.all_keywords IS NOT NULL
        AND json_array_length(fa.all_keywords) > 0
),

-- Parse outlier keywords
parsed_outlier_keywords AS (
    SELECT
        ok.an,
        ok.publication_day,
        ok.source_code,
        (ok.keyword_obj->>'keyword')::TEXT AS keyword,
        (ok.keyword_obj->>'count_keyword')::INTEGER AS count_keyword,
        (ok.keyword_obj->>'is_hrfp')::BOOLEAN AS is_hrfp
    FROM outlier_keywords ok
    WHERE ok.keyword_obj->>'keyword' IS NOT NULL
),

-- Deduplicate outlier keywords per article (same keyword can have multiple themes with same count)
deduplicated_outlier_keywords AS (
    SELECT
        pok.an,
        pok.publication_day,
        pok.source_code,
        pok.keyword,
        BOOL_AND(pok.is_hrfp) AS is_hrfp,
        MAX(pok.count_keyword) AS count_keyword
    FROM parsed_outlier_keywords pok
    GROUP BY 
        pok.an,
        pok.publication_day,
        pok.source_code,
        pok.keyword
),

-- Aggregate outlier keywords BEFORE unpivoting sectors
outlier_keyword_aggregates AS (
    SELECT
        dok.publication_day,
        dok.source_code,
        dok.keyword,
        BOOL_AND(dok.is_hrfp) AS is_hrfp,
        COUNT(DISTINCT dok.an) AS nb_articles,
        SUM(dok.count_keyword) AS nb_keyword_occurences
    FROM deduplicated_outlier_keywords dok
    GROUP BY 
        dok.publication_day,
        dok.source_code,
        dok.keyword
),

-- Join outlier keywords with sectors
outlier_keywords_with_sectors AS (
    SELECT
        oka.publication_day,
        oka.source_code,
        oka.keyword,
        oka.is_hrfp,
        oka.nb_articles,
        oka.nb_keyword_occurences,
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
    FROM outlier_keyword_aggregates oka
    LEFT JOIN {{ source('public', 'keyword_macro_category') }} kmc
        ON LOWER(TRIM(oka.keyword)) = LOWER(TRIM(kmc.keyword))
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
    WHERE sector_flag = TRUE
),

-- Create outlier aggregates with median values
outlier_aggregates AS (
    SELECT
        oks.publication_day,
        oks.source_code,
        oks.keyword,
        oks.sector,
        oks.is_hrfp,
        -- Use median, or 1 if median is 0
        CASE 
            WHEN COALESCE(ksm.median_nb_articles, 0) = 0 THEN 1
            ELSE ksm.median_nb_articles
        END AS nb_articles,
        CASE 
            WHEN COALESCE(ksm.median_nb_keyword_occurences, 0) = 0 THEN 1
            ELSE ksm.median_nb_keyword_occurences
        END AS nb_keyword_occurences
    FROM outlier_keywords_with_sectors oks
    LEFT JOIN keyword_sector_medians ksm
        ON oks.source_code = ksm.source_code
        AND oks.keyword = ksm.keyword
        AND oks.sector = ksm.sector
),

-- Combine normal and outlier aggregates
combined_aggregates AS (
    -- Normal days (non-outlier)
    SELECT
        da.publication_day,
        da.source_code,
        da.keyword,
        da.sector,
        da.is_hrfp,
        da.nb_articles,
        da.nb_keyword_occurences,
        FALSE AS outlier
    FROM daily_aggregates da
    
    UNION ALL
    
    -- Outlier days (with median values)
    SELECT
        oa.publication_day,
        oa.source_code,
        oa.keyword,
        oa.sector,
        oa.is_hrfp,
        oa.nb_articles,
        oa.nb_keyword_occurences,
        TRUE AS outlier
    FROM outlier_aggregates oa
)

-- Final output with source metadata
SELECT
    ca.publication_day,
    ca.source_code,
    sc.source_name,
    sc.source_type,
    sc.media_all,
    ca.keyword,
    ca.sector,
    ca.nb_articles,
    ca.nb_keyword_occurences,
    ca.is_hrfp,
    ca.outlier,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
    
FROM combined_aggregates ca
LEFT JOIN {{ source('public', 'source_classification') }} sc
    ON ca.source_code = sc.source_code

-- Exclude France24.com
WHERE ca.source_code <> 'HTFRFR'

ORDER BY
    ca.publication_day DESC,
    ca.source_code ASC,
    ca.keyword ASC,
    ca.sector ASC
