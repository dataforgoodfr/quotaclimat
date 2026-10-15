{{
    config(
        materialized='table',
    )
}}

{% set tolerance_sec = var('ad_tunnel_tolerance_sec', 5) %}

-- One row per non-OTHER, non-deleted occurrence with the ad tunnel it belongs to.
-- An ad tunnel is a sequence of consecutive ads on a channel, where each ad starts
-- at most {{ tolerance_sec }} seconds after the end of all the previous ads of the tunnel.
-- Overlapping ads (starting before the previous one ends) stay in the same tunnel.
WITH occ AS (
    SELECT
        o.id AS occurrence_id,
        o.channel_name,
        o.occurrence_date AS start_date,
        o.occurrence_date + (a.duration_sec || ' seconds')::interval AS end_date
    FROM {{ source('advertising', 'ad_occurrence') }} o
    JOIN {{ source('advertising', 'ad') }} a ON a.id = o.ad_id
    WHERE a.fragment_type != 'OTHER'
      AND o.deleted_at IS NULL
),
flagged AS (
    SELECT
        occ.*,
        CASE
            WHEN MAX(end_date) OVER w_previous IS NULL
                 OR start_date > MAX(end_date) OVER w_previous + interval '{{ tolerance_sec }} seconds'
            THEN 1
            ELSE 0
        END AS is_new_tunnel
    FROM occ
    WINDOW w_previous AS (
        PARTITION BY channel_name ORDER BY start_date, end_date, occurrence_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    )
),
grouped AS (
    SELECT
        occurrence_id,
        channel_name,
        start_date,
        end_date,
        SUM(is_new_tunnel) OVER (
            PARTITION BY channel_name ORDER BY start_date, end_date, occurrence_id
            ROWS UNBOUNDED PRECEDING
        ) AS tunnel_seq
    FROM flagged
)
SELECT
    occurrence_id,
    channel_name || '@' || FLOOR(EXTRACT(EPOCH FROM
        MIN(start_date) OVER (PARTITION BY channel_name, tunnel_seq)
    ))::bigint AS tunnel_id,
    channel_name,
    start_date,
    end_date
FROM grouped
