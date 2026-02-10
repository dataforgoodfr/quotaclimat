import duckdb
from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
print(duckdb.__version__)
# duckdb.execute("INSTALL fts")
# duckdb.execute("LOAD fts")
WINDOW_SIZE = 20
N_WINDOWS = 120 / WINDOW_SIZE

dictionary_df = duckdb.sql("SELECT * FROM 'my_dbt_project/seeds/dictionary_new.csv'").df()
keywords_df = duckdb.sql("SELECT * FROM 'my_dbt_project/seeds/keywords.csv'").df()
stop_word = duckdb.sql("SELECT * FROM 'my_dbt_project/seeds/stop_word.csv'").df()

keywords_df = duckdb.sql("""
SELECT
    *,
    regexp_replace(plaintext, '<unk>', '', 'g') AS cleaned_text
FROM keywords_df                      
""").df()

filtered_keywords = duckdb.sql("""
WITH stopword_regex AS (
    SELECT
        '(?i)(^|\\s|[[:punct:]])(' ||
        string_agg(
            regexp_replace(
                context,
                '([.\\^$*+?()\\[\\]{}|\\\\])',
                '\\\\\\1',
                'g'
            ),
            '|'
            ORDER BY length(context) DESC
        )
        || ')(\\s|[[:punct:]]|$)' AS pattern
    FROM stop_word
)
SELECT
    id,
    channel_name,
    channel_radio,
    program_metadata_id,
    country,
    start,
    plaintext,
    created_at,
    srt,
    trim(
        regexp_replace(
            regexp_replace(
                kw.cleaned_text,
                s.pattern,
                '\\1\\3',
                'g'
            ),
            '\\s+',
            ' ',
            'g'
        )
    ) AS filtered_text
FROM keywords_df kw
CROSS JOIN stopword_regex s;
""").df()#.to_table("filtered_keywords")



keyword_found = duckdb.sql("""
WITH keyword_regex AS (
    SELECT
        '(?i)\\b(' ||
        string_agg(
            regexp_replace(
                keyword,
                '([.\\^$*+?()\\[\\]{}|\\\\])',
                '\\\\\\1',
                'g'
            ),
            '|'
        )
        || ')\\b' AS pattern
    FROM dictionary_df
)
SELECT
    *,
    regexp_extract_all(kw.filtered_text, kr.pattern) AS keywords_found
    FROM filtered_keywords kw
CROSS JOIN keyword_regex kr
""").df()

keywords_with_themes = duckdb.sql("""
select 
    kf.id,
    kf.channel_name,
    kf.channel_radio,
    kf.program_metadata_id,
    kf.country,
    kf.start,
    kf.plaintext,
    kf.created_at,
    kf.srt,
    kf.keywords_found,
    string_agg(d.theme) themes
from 
    keyword_found kf
left join dictionary_df d
    on d.keyword in kf.keywords_found
group by
    kf.id,
    kf.channel_name,
    kf.channel_radio,
    kf.program_metadata_id,
    kf.country,
    kf.start,
    kf.plaintext,
    kf.created_at,
    kf.srt,
    kf.keywords_found
order by
    kf.id        
"""
).df()

srt_words = duckdb.sql("""
    SELECT
        kwt.id,
        lower(j.value ->> 'text') AS word,
        CAST(j.value ->> 'cts_in_ms' AS BIGINT) AS timestamp
    FROM keywords_with_themes kwt,
         json_each(kwt.srt) AS j
    order by kwt.id
""").df()

segment_start = duckdb.sql("""
    SELECT
        sw.id,
        min(sw.timestamp) start_timestamp
    FROM srt_words sw
    group by sw.id
    order by sw.id
""")

document_keywords = duckdb.sql(
"""
SELECT
    kwt.id,
    kwt.plaintext,
    lower(k.keyword) AS keyword
FROM keywords_with_themes kwt,
    unnest(list_distinct(kwt.keywords_found)) AS k(keyword)
order by kwt.id
"""
).df()

# matched_keywords = duckdb.sql(
# """
# SELECT
#     s.id,
#     s.word AS keyword,
#     s.timestamp
# FROM srt_words s
# INNER JOIN document_keywords kd
#     ON s.word = kd.keyword
#     AND s.id = kd.id
# order by
#     s.id,
#     keyword,
#     s.timestamp
# """
# ).df()
matched_keywords = duckdb.sql(
"""
SELECT
    kd.id,
    kd.keyword,
    contains(kd.plaintext, kd.keyword) contains_kwd,
    s.timestamp
FROM document_keywords kd
LEFT JOIN srt_words s
    ON s.word = kd.keyword
    AND s.id = kd.id
order by
    kd.id,
    kd.keyword,
    s.timestamp
"""
).df()

enriched_keywords = duckdb.sql(
f"""
SELECT
    mk.id,
    mk.keyword,
    mk.timestamp,
    case
        when (mk.timestamp - ss.start_timestamp) / 1000 >= 120 then {N_WINDOWS}
        else CEIL(((mk.timestamp - ss.start_timestamp) / 1000) / {WINDOW_SIZE})
    end::smallint window_number,
    d.theme,
    d.high_risk_of_false_positive hrfp,
    list_distinct(array_agg(d.category)) categories
FROM matched_keywords mk
LEFT JOIN dictionary_df d
    ON lower(mk.keyword) = lower(d.keyword)
left join segment_start ss
    on mk.id = ss.id
group by 
    mk.id,
    mk.keyword,
    mk.timestamp,
    window_number,
    d.theme,
    hrfp
order by
    mk.id,
    mk.keyword,
    mk.timestamp,
    d.theme,
    hrfp
"""
).df()


# keywords_with_timestamp = duckdb.sql("""
# SELECT
#     id,
#     to_json(
#         list(
#             json_object(
#                 'keyword', keyword,
#                 'timestamp', timestamp,
#                 'window_number', window_number,
#                 'theme', themes,
#                 'categorie', categories
#             )
#             ORDER BY timestamp
#         )
#     ) AS extracted_keywords
# FROM enriched_keywords
# GROUP BY id
# """).show()

keywords_with_validations = duckdb.sql("""
SELECT
    id,
    window_number,
    replace(theme, '_indirectes', '') theme,
    not min(
        case
           when contains(theme, '_indirectes') then true
           else hrfp
        end
    ) contains_validated_keyword,
    array_agg(keyword) AS window_keywords       
FROM enriched_keywords
group by 
    id,
    window_number,
    theme
""").df()

keyword_windows = duckdb.sql(
f"""
SELECT
    k.id,
    k.window_number,
    k_minus.window_number window_minus_1,
    k_plus.window_number window_plus_1,
    k.theme,
    k.contains_validated_keyword,
    k_minus.contains_validated_keyword validated_keyword_window_minus_1,
    k_plus.contains_validated_keyword validated_keyword_window_plus_1,
    coalesce(
        k.contains_validated_keyword
        or k_minus.contains_validated_keyword 
        or k_plus.contains_validated_keyword,
        false
    ) fully_validated,
    k.window_keywords
FROM keywords_with_validations k
left join keywords_with_validations k_minus
on k.id=k_minus.id and k.theme=k_minus.theme and k.window_number-1=k_minus.window_number
left join keywords_with_validations k_plus
on k.id=k_plus.id and k.theme=k_plus.theme and k.window_number+1=k_plus.window_number
order by
    --k.id,
    --k.contains_validated_keyword desc,
    window_minus_1,
    window_plus_1,
    k.window_number,
    k.theme,
    validated_keyword_window_minus_1,
    validated_keyword_window_plus_1
"""
).df()

print((keywords_df.number_of_keywords>0).sum())
validated_windows_by_theme = duckdb.sql(
"""
select
    id, 
    window_number,
    theme,
    list_distinct(window_keywords) unique_keywords,
    length(list_distinct(window_keywords)) n_keywords,
    max(fully_validated) fully_validated
from 
    keyword_windows    
group by id, window_number, theme, unique_keywords, n_keywords
order by n_keywords, id
""").df()
duckdb.sql(
"""
select
    id,
    sum(subquery.fully_validated_by_window) number_of_keywords
from (
    select 
        id,
        window_number,
        max(fully_validated) fully_validated_by_window
    from 
        validated_windows_by_theme    
    group by id, window_number
) subquery
group by id
""").show()
print(keywords_df.id.nunique())

# The reason there is only 48 ids and at the beginning we had 51 keywords is because in step document_keywords
# We use an unnest function that basically cross multiplies the elements of list_distinct(kwt.keywords_found)
# with each row.
# Unnest a list, generating 3 rows ((1, 10), (2, 10), (3, 10)):
# SELECT unnest([1, 2, 3]), 10;
# However, for rows where list_distinct(kwt.keywords_found) = [], there is no product as no keyword was found ! So we loose the ID.
# When we get our results remember to join them back in with the initial dataframe and coalesce to 0
# However when we perform the inner join for matched_keywords, we lose another one.. why is that ?


duckdb.sql("""
WITH
doc_tokens AS (
    SELECT
        d.id AS document_id,
        t.value AS token,
        t.ordinality AS pos
    FROM filtered_keywords d
    CROSS JOIN UNNEST(string_split(d.filtered_text, ' ')) WITH ORDINALITY AS t(value, ordinality)
),
dict_tokens AS (
    SELECT
        keyword,
        string_split(keyword, ' ') AS parts,
        array_length(string_split(keyword, ' ')) AS len
    FROM dictionary_df
),
matches AS (
    SELECT
        dt.document_id,
        dt.pos AS start_pos,
        dt.pos + dk.len - 1 AS end_pos,
        dk.keyword,
        dk.len
    FROM doc_tokens dt
    JOIN dict_tokens dk
      ON (
          SELECT
              list(dt2.token ORDER BY dt2.pos)
          FROM doc_tokens dt2
          WHERE
              dt2.document_id = dt.document_id
              AND dt2.pos BETWEEN dt.pos AND dt.pos + dk.len - 1
      ) = dk.parts
),
ranked AS (
    SELECT
        *,
        MAX(end_pos) OVER (
            PARTITION BY document_id
            ORDER BY start_pos, len DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end
    FROM matches
),
final_matches AS (
    SELECT
        document_id,
        start_pos,
        keyword
    FROM ranked
    WHERE prev_max_end IS NULL OR start_pos > prev_max_end
)
SELECT
    document_id,
    list(keyword ORDER BY keyword) AS matched_keywords,
    list_sort(list(start_pos ORDER BY keyword)) AS positions
FROM final_matches
GROUP BY document_id
ORDER BY document_id
limit 20
""").show()

duckdb.sql(
"""
select * from keywords_with_validations order by id limit 20
"""
).show()