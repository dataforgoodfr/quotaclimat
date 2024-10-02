-- L'étape 'staging' est utilisé pour réaliser des opérations de nettoyage basiques : https://docs.getdbt.com/best-practices/how-we-structure/2-staging

with keywords as (
    select * from {{ source('quotaclimat', 'keywords') }}
),

renamed as (
    select
        id as keyword_id,
        case
            when channel_name = '' then null
            else channel_name
        end as channel_name,
        case
            when channel_title = '' then null
            else channel_title
        end as channel_title,
        case
            when channel_program = '' then null
            else channel_program
        end as channel_program,
        case
            when channel_program_type = '' then null
            else channel_program_type
        end as channel_program_type,
        start,
        TRIM(REPLACE(plaintext, '<unk>', '')) as plain_text,
        theme,
        case
            when theme::text like '%changement_climatique_constat_indirectes%' then TRUE
            else FALSE
        end as is_climatic_change_subject,
        case
            when theme::text like '%biodiversite_concepts_generaux_indirectes%' then TRUE
            else FALSE
        end as is_biodiversity_general_indirect_concept,
        created_at,
        updated_at::timestamp with time zone as updated_at,
        keywords_with_timestamp->1->>'keywords' as first_keyword,
        to_timestamp((keywords_with_timestamp->0->>'timestamp')::bigint / 1000) AS first_keyword_date,
        json_array_length(keywords_with_timestamp) as keywords_count
    from keywords
)

select * from renamed