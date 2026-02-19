import duckdb
import pandas as pd
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
print(duckdb.__version__)

dictionary_df = duckdb.sql("SELECT * FROM 'my_dbt_project/seeds/dictionary_new.csv'").df()
print(dictionary_df.head())
instagram_df = pd.read_csv('analyse/press/data/input/instagram_data.csv')
batch = instagram_df.head(1000)
print("batch. ensuring lowercase. plaintext -> cleaned_text")
batch = duckdb.sql("""
SELECT
    *,
    file_name as id,
    lower(caption_text) AS cleaned_text
FROM batch
""")

batch.show()
batch = batch.df()

# Precompute keyword information sorted by length (longest first)
keyword_info = duckdb.sql("""
    SELECT
        keyword,
        string_split(keyword, ' ') AS parts,
        array_length(string_split(keyword, ' ')) AS len,
        string_to_array(keyword, ' ') AS token_array
    FROM dictionary_df
    ORDER BY len DESC
""").df()

# Get all document tokens
doc_tokens = duckdb.sql("""
    SELECT
        b.id AS document_id,
        b.cleaned_text,
        t.value AS token,
        t.ordinality AS pos
    FROM batch b
    CROSS JOIN UNNEST(string_split(b.cleaned_text, ' ')) WITH ORDINALITY AS t(value, ordinality)
    ORDER BY document_id, pos
""").df()

# Create a more efficient approach - instead of nested subqueries,
# we'll directly match keywords in a single pass
matches = duckdb.sql("""
    WITH
    -- Create a lookup of all possible keyword starts
    keyword_starts AS (
        SELECT
            keyword,
            parts[1] AS first_token,
            len,
            parts AS keyword_parts
        FROM keyword_info
    ),
    -- Find potential matches by first token
    potential_matches AS (
        SELECT
            dt.document_id,
            dt.pos AS start_pos,
            dt.pos + ks.len - 1 AS end_pos,
            ks.keyword,
            ks.len,
            dt.cleaned_text,
            ks.keyword_parts
        FROM doc_tokens dt
        JOIN keyword_starts ks ON dt.token = ks.first_token
        WHERE dt.pos + ks.len - 1 <= (
            SELECT array_length(string_split(cleaned_text, ' '))
            FROM batch b
            WHERE b.id = dt.document_id
        )
    ),
    -- Validate full matches efficiently
    validated_matches AS (
        SELECT
            pm.document_id,
            pm.start_pos,
            pm.end_pos,
            pm.keyword,
            pm.len,
            pm.cleaned_text,
            -- Direct validation without nested subquery
            CASE
                WHEN pm.len = 1 THEN 1
                ELSE (
                    SELECT
                        CASE
                            WHEN list(dt2.token ORDER BY dt2.pos) = pm.keyword_parts THEN 1
                            ELSE 0
                        END
                    FROM doc_tokens dt2
                    WHERE dt2.document_id = pm.document_id
                    AND dt2.pos BETWEEN pm.start_pos AND pm.end_pos
                )
            END AS is_valid_match
        FROM potential_matches pm
    )
    SELECT
        document_id,
        cleaned_text,
        start_pos,
        end_pos,
        len,
        keyword
    FROM validated_matches
    WHERE is_valid_match = 1
""")

print("matches")
matches.show()
matches = matches.df()

# Continue with the rest of the processing
ranked = duckdb.sql("""
SELECT
        *,
        MAX(end_pos) OVER (
            PARTITION BY document_id
            ORDER BY start_pos, len DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end
    FROM matches
""")

print("ranked")
ranked.show()
ranked = ranked.df()

final_matches = duckdb.sql("""
SELECT
        document_id,
        cleaned_text,
        start_pos,
        keyword
    FROM ranked
    WHERE prev_max_end IS NULL OR start_pos > prev_max_end
""")

print("final_matches")
final_matches.show()
final_matches = final_matches.df()

dictionary_hrfp = duckdb.sql("""
SELECT
    keyword,
    case
        when contains(theme, '_indirectes') then true
        else high_risk_of_false_positive
    end as high_risk_of_false_positive,
    language,
    category,
    replace(theme, '_indirectes', '') theme
FROM dictionary_df
""").df()

keywords_found = duckdb.sql("""
SELECT
    fm.document_id,
    fm.cleaned_text,
    list(fm.keyword ORDER BY fm.keyword) AS matched_keywords,
    list_sort(list(start_pos ORDER BY fm.keyword)) AS positions,
    list_distinct(list(d.theme ORDER BY d.theme)) themes,
    list_distinct(list(high_risk_of_false_positive)) hrfp,
    not min(high_risk_of_false_positive) validated
FROM final_matches fm
left join dictionary_hrfp d
    on d.keyword = fm.keyword
GROUP BY document_id, cleaned_text
ORDER BY document_id
""")

keywords_found.show()
keywords_found = keywords_found.df()

classifications = duckdb.sql("""
SELECT
    b.file_name,
    b.user_pk,
    b.user_username,
    b.user_full_name,
    b.user_id,
    b.like_count,
    b.comment_count,
    b.caption_text,
    coalesce(kf.matched_keywords, []) matched_keywords,
    coalesce(kf.positions, []) positions,
    coalesce(kf.themes, []) themes,
    coalesce(kf.hrfp, []) hrfp,
    coalesce(kf.validated, false) validated 
FROM batch b
left join keywords_found kf on b.file_name = kf.document_id
order by 
validated desc,
b.id
""")
                             
classifications.show()
classifications.to_csv('analyse/press/data/output/instagram_data_classified.csv')
classifications = classifications.df()

result = duckdb.sql("""
SELECT
    distinct validated,
    count(*)
FROM classifications
group by validated
""")
result.show()
