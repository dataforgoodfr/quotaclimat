{{
    config(
        materialized='table',
        schema='analytics',
    )
}}

{#- One row per misinformation case validated in Label Studio: the rows of task_global_completion with
    the first annotation ("Annotation Version" = 1) answering 'Correct' alone ('Correct,Incorrect', when
    annotators disagree, is left out), same definition as ad_occurrence_mesinfo, all countries.
    With the channel attributes of program_metadata and the emission of analytics.program on the air at
    the start of the segment (data_item_start, UTC; Programmes Google Sheet, NULL outside the emissions
    listed there). Built in the analytics schema after task_global_completion (see entrypoints/dbt.sh). -#}
WITH mesinfo AS (
    SELECT *
    FROM {{ source('analytics', 'task_global_completion') }}
    WHERE mesinfo_choice = 'Correct'
      AND "Annotation Version" = 1
),
-- channel attributes of the most recent grid, per country (the same channel_name may exist in several countries)
channels AS (
    SELECT DISTINCT ON (channel_name, country)
        channel_name,
        country,
        channel_title,
        public,
        infocontinue,
        radio
    FROM {{ source('public', 'program_metadata') }}
    ORDER BY channel_name, country, program_grid_end DESC NULLS FIRST, program_grid_start DESC NULLS LAST
),
mesinfo_emissions AS (
    {{ program_emission_at('mesinfo', 'task_aggregate_id', 'data_item_channel_name', 'data_item_start') }}
)
SELECT
    m.*,
    c.channel_title,
    c.public,
    c.infocontinue,
    c.radio,
    e.emission_id,
    e.emission,
    e.presentation AS emission_presentation,
    e.rediffusion AS emission_rediffusion,
    e.emission_start,
    e.emission_end
FROM mesinfo m
LEFT JOIN channels c ON c.channel_name = m.data_item_channel_name AND c.country = m.country
LEFT JOIN mesinfo_emissions e ON e.task_aggregate_id = m.task_aggregate_id
