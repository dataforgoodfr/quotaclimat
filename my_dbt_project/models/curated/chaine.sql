{#- curated.chaine: one row per monitored channel (program_metadata), with the attributes of its most recent
    grid, as cas_de_mesinformation does: the attributes of a channel do not change between its grids in
    production. Documented in _curated.yml. -#}
WITH chaines AS (
    SELECT DISTINCT ON (channel_name)
        channel_name,
        channel_title,
        country,
        public,
        infocontinue,
        radio
    FROM {{ source('public', 'program_metadata') }}
    ORDER BY channel_name, program_grid_end DESC NULLS FIRST, program_grid_start DESC NULLS LAST
)
SELECT
    channel_name::text AS chaine_id,
    channel_title::text AS chaine_nom,
    {{ pays_fr('country') }} AS pays,
    COALESCE(public, FALSE) AS est_service_public,
    COALESCE(infocontinue, FALSE) AS est_info_continue,
    COALESCE(radio, FALSE) AS est_radio
FROM chaines
