{{
    config(
        materialized='table',
    )
}}

-- An ad tunnel is a sequence of consecutive ads on a channel, where each ad starts
-- within {{ var('ad_tunnel_tolerance_sec', 5) }} seconds of the end of the previous one.
WITH occ AS (
    SELECT
        o.channel_name,
        o.occurrence_date AS start_date,
        o.occurrence_date + (a.duration_sec || ' seconds')::interval AS end_date
    FROM {{ source('advertising', 'ad_occurrence') }} o
    JOIN {{ source('advertising', 'ad') }} a ON a.id = o.ad_id
    WHERE a.fragment_type != 'OTHER'
),
flagged AS (
    SELECT
        occ.*,
        CASE
            WHEN LAG(end_date) OVER w IS NULL
                 OR ABS(EXTRACT(EPOCH FROM (start_date - LAG(end_date) OVER w))) > {{ var('ad_tunnel_tolerance_sec', 5) }}
            THEN 1
            ELSE 0
        END AS is_new_tunnel
    FROM occ
    WINDOW w AS (PARTITION BY channel_name ORDER BY start_date)
),
grouped AS (
    SELECT
        channel_name,
        start_date,
        end_date,
        SUM(is_new_tunnel) OVER (PARTITION BY channel_name ORDER BY start_date) AS tunnel_seq
    FROM flagged
)
SELECT
    channel_name || '@' || FLOOR(EXTRACT(EPOCH FROM MIN(start_date)))::bigint AS tunnel_id,
    channel_name,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date
FROM grouped
GROUP BY channel_name, tunnel_seq
