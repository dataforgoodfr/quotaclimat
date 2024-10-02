/* L'étape 'staging' est utilisé pour réaliser des opérations de transformations plus avancées (join, group by, ...) :
https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate */

with keywords as (
    select
        *
    from {{ ref('stg_keywords') }}
),

keywords_grouped_by_days_and_channels as (
    select
        channel_title,
        start::date as start,
        count(*)
    from keywords
    group by 1, 2
)

select * from keywords_grouped_by_days_and_channels