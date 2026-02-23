import duckdb
import pandas as pd
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from quotaclimat.data_ingestion.scrap_sitemap import get_consistent_hash
from tqdm import tqdm
print(duckdb.__version__)

dictionary_df = duckdb.sql("SELECT * FROM 'my_dbt_project/seeds/dictionary_new.csv'").df()
print(dictionary_df.head())
instagram_df = pd.read_csv('analyse/press/data/input/instagram_data.csv')

batch_size = 10000
n_batches = len(instagram_df) // batch_size + 1

result_dfs = []

for idx in tqdm(range(n_batches), total=n_batches):
    start_idx = idx * batch_size
    end_idx = (idx + 1) * batch_size if (idx + 1) * batch_size < len(instagram_df) else len(instagram_df) - 1


    batch = instagram_df.loc[start_idx:end_idx]
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
    batch = batch.drop_duplicates(subset=["file_name"])
    # Precompute keyword information sorted by length (longest first)
    keyword_info = duckdb.sql("""
        SELECT
            keyword,
            string_split(keyword, ' ') AS parts,
            array_length(string_split(keyword, ' ')) AS len,
            string_to_array(keyword, ' ') AS token_array
        FROM dictionary_df
        ORDER BY len DESC
    """)

    keyword_info.show()
    keyword_info = keyword_info.df()

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
        fm.keyword,
        fm.start_pos,
        d.theme,
        d.high_risk_of_false_positive,
        case when contains(d.theme, 'adaptation_climatique') then true else false end as is_adaptation_climatique,
        case when contains(d.theme, 'attenuation_climatique') then true else false end as is_attenuation_climatique,
        case when contains(d.theme, 'biodiversite') then true else false end as is_biodiversite,
        case when contains(d.theme, 'changement_climatique') then true else false end as is_changement_climatique,
        case when contains(d.theme, 'ressources') then true else false end as is_ressources
    FROM final_matches fm
    left join dictionary_hrfp d
        on d.keyword = fm.keyword
    ORDER BY document_id
    """)

    keywords_found.show()
    keywords_found = keywords_found.df()


    keywords_grouped = duckdb.sql("""
    SELECT
        kf.document_id,
        kf.cleaned_text,
        list(kf.keyword ORDER BY kf.keyword) AS matched_keywords,
        list_sort(list(start_pos ORDER BY kf.keyword)) AS positions,
        list_distinct(list(kf.theme ORDER BY kf.theme)) themes,
        list_distinct(list(kf.high_risk_of_false_positive)) hrfp,
        not min(kf.high_risk_of_false_positive) validated,
        max(kf.is_adaptation_climatique) is_adaptation_climatique,
        max(kf.is_attenuation_climatique) is_attenuation_climatique,
        max(kf.is_biodiversite) is_biodiversite,
        max(kf.is_changement_climatique) is_changement_climatique,
        max(kf.is_ressources) is_ressources
    FROM keywords_found kf
    GROUP BY kf.document_id, kf.cleaned_text
    ORDER BY kf.document_id
    """)

    keywords_grouped.show()
    keywords_grouped = keywords_grouped.df()

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
        coalesce(kg.matched_keywords, []) matched_keywords,
        coalesce(kg.positions, []) positions,
        coalesce(kg.themes, []) themes,
        coalesce(kg.hrfp, []) hrfp,
        coalesce(kg.validated, false) validated,
        coalesce(is_adaptation_climatique, false) is_adaptation_climatique,
        coalesce(is_attenuation_climatique, false) is_attenuation_climatique,
        coalesce(is_biodiversite, false) is_biodiversite,
        coalesce(is_changement_climatique, false) is_changement_climatique,
        coalesce(is_ressources, false) is_ressources
    FROM batch b
    left join keywords_grouped kg on b.file_name = kg.document_id
    order by 
    validated desc,
    b.id
    """)
                                
    classifications.show()
    classifications = classifications.df()

    result_dfs.append(classifications)

result_df = pd.concat(result_dfs)
result_df.insert(
        0,
        "id",
        (result_df.file_name + result_df.user_pk.astype(str)).apply(
            get_consistent_hash
        ),
    )
result_df.insert(
        0,
        "datetime",
        pd.to_datetime(result_df.file_name.str.split("Z_").apply(lambda x: x[0])),
    )
result_df.to_csv('analyse/press/data/output/instagram_data_classified.csv', index=False)

result = duckdb.sql("""
SELECT
    distinct validated,
    count(*)
FROM result_df
group by validated
""")
result.show()
result_df.info()