{{
    config(
        materialized='table',
    )
}}

-- Ad tunnels, aggregated from the occurrences of ad_occurrence_tunnels.
SELECT
    tunnel_id,
    channel_name,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date
FROM {{ ref('ad_occurrence_tunnels') }}
GROUP BY tunnel_id, channel_name
