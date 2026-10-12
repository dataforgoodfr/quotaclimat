-- Contrôle du rattachement des segments aux programmes, chaînes françaises : segments de public.keywords
-- dont le program_metadata_id stocké à l'ingestion diffère du programme recalculé à partir de l'heure.
-- Le recalcul suit la logique de metabase_taux_mesinformation_par_emission.sql et de la macro dbt program_at
-- (start, timestamp with time zone, converti en heure de Paris, jour de semaine, programmes après minuit,
-- validité de grille, créneaux élargis de 5 min comme à l'ingestion), mais directement sur
-- public.program_metadata, sans le découpage en émissions.
-- Variables Metabase, type Texte, heure de Paris :
--   {{debut}} inclus, ex. 2026-09-01 ou 2026-09-01 06:00
--   {{fin}}   exclu,  ex. 2026-09-08
-- Colonnes de diagnostic sur le programme stocké :
--   stocke_dans_creneau : le segment tombe dans son créneau, un jour de semaine où il est diffusé
--   stocke_ecart_minutes : minutes avant le début (négatif) ou après la fin (positif) du créneau le plus proche
--   stocke_dans_creneau_heure_utc : idem avec l'heure UTC lue comme une heure de Paris (id calculé sur l'UTC)
--   stocke_grille_valide : le jour du programme est dans program_grid_start / program_grid_end
WITH bornes AS (
    SELECT
        CAST({{debut}} AS timestamp) AS debut,
        CAST({{fin}} AS timestamp) AS fin
),
-- même lecture des horaires que analytics.program : 'H:MM', fin après minuit au-delà de 1440
grille AS (
    SELECT
        pm.id,
        pm.channel_name,
        pm.weekday,
        pm.channel_program,
        pm.start AS programme_debut,
        pm."end" AS programme_fin,
        pm.program_grid_start::date AS grid_start,
        COALESCE(pm.program_grid_end::date, '2100-01-01'::date) AS grid_end,
        m.start_minute,
        m.end_minute_of_day + CASE WHEN m.end_minute_of_day <= m.start_minute THEN 1440 ELSE 0 END AS end_minute
    FROM public.program_metadata pm
    CROSS JOIN LATERAL (
        SELECT
            SPLIT_PART(pm.start::text, ':', 1)::int * 60 + SPLIT_PART(pm.start::text, ':', 2)::int AS start_minute,
            SPLIT_PART(pm."end"::text, ':', 1)::int * 60 + SPLIT_PART(pm."end"::text, ':', 2)::int AS end_minute_of_day
    ) m
    WHERE pm.country = 'france'
),
-- segments de la période ; bornes de la période converties en instants pour le filtre
segments AS MATERIALIZED (
    SELECT
        k.id,
        k.start,
        k.channel_name,
        k.program_metadata_id AS id_stocke,
        k.start AT TIME ZONE 'Europe/Paris' AS paris_time,
        -- pour le diagnostic : l'heure UTC, comme si elle était une heure de Paris
        k.start AT TIME ZONE 'UTC' AS heure_utc
    FROM bornes b
    JOIN public.keywords k
      ON k.start >= b.debut AT TIME ZONE 'Europe/Paris'
     AND k.start < b.fin AT TIME ZONE 'Europe/Paris'
    WHERE k.country = 'france'
),
-- jour de grille et minute dans ce jour : le jour même, la veille pour un programme de la veille qui finit
-- après minuit, le lendemain pour la marge avant un programme qui commence à minuit
segments_jour AS MATERIALIZED (
    SELECT
        s.id,
        s.start,
        s.channel_name,
        o.day_offset,
        s.paris_time::date - o.day_offset AS jour,
        EXTRACT(ISODOW FROM s.paris_time::date - o.day_offset)::int AS weekday,
        EXTRACT(EPOCH FROM s.paris_time - (s.paris_time::date - o.day_offset)) / 60 AS minute
    FROM segments s
    CROSS JOIN (VALUES (-1), (0), (1)) AS o(day_offset)
    WHERE o.day_offset = 0
       OR (o.day_offset = 1
           AND EXTRACT(EPOCH FROM s.paris_time - s.paris_time::date) / 60 + 1440
               < (SELECT MAX(end_minute) FROM grille) + 5)
       OR (o.day_offset = -1 AND EXTRACT(EPOCH FROM s.paris_time - s.paris_time::date) / 60 >= 1440 - 5)
),
-- programme recalculé, créneaux élargis de 5 min comme à l'ingestion (api_to_s3.py, epoch_margin de la
-- France) ; priorité au programme qui contient vraiment le segment, puis au plus proche, puis mêmes
-- priorités que la macro program_at (sans émission)
calcul AS (
    SELECT DISTINCT ON (s.id, s.start)
        s.id,
        s.start,
        g.id AS id_calcule
    FROM segments_jour s
    JOIN grille g
      ON g.channel_name = s.channel_name
     AND g.weekday = s.weekday
     AND s.jour BETWEEN COALESCE(g.grid_start, '-infinity'::date) AND g.grid_end
     AND s.minute >= g.start_minute - 5
     AND s.minute < g.end_minute + 5
    ORDER BY s.id, s.start,
        s.minute >= g.start_minute AND s.minute < g.end_minute DESC,
        GREATEST(g.start_minute - s.minute, s.minute - g.end_minute),
        g.grid_start DESC NULLS LAST, s.day_offset, g.start_minute DESC
),
differences AS (
    SELECT
        s.*,
        c.id_calcule
    FROM segments s
    LEFT JOIN calcul c ON c.id = s.id AND c.start = s.start
    WHERE s.id_stocke IS DISTINCT FROM c.id_calcule
)
SELECT
    CASE
        WHEN d.id_stocke IS NULL THEN 'pas d''id stocké'
        WHEN gs.id IS NULL THEN 'id stocké absent de program_metadata (France)'
        WHEN d.id_calcule IS NULL THEN 'aucun programme recalculé'
        ELSE 'programme différent'
    END AS categorie,
    -- piste la plus probable, à confirmer ligne à ligne
    CASE
        WHEN d.id_stocke IS NOT NULL AND gs.id IS NULL THEN NULL
        -- marges que le recalcul ne reproduit pas : 5 à 6 min (réattribution,
        -- channel_program.get_matching_program_hour), ou segment de la marge d'un programme qui tombe dans le
        -- créneau du programme voisin (le recalcul prend le voisin, l'ingestion l'un ou l'autre)
        WHEN gs.id IS NOT NULL AND NOT st.dans_creneau AND ABS(st.ecart) <= 6 THEN 'marge d''ingestion (5 à 6 min)'
        WHEN gs.id IS NOT NULL AND NOT COALESCE(st.dans_creneau, FALSE) AND su.dans_creneau
            THEN 'décalage UTC / Paris'
        WHEN gs.id IS NOT NULL AND st.weekday_ok IS NULL THEN 'jour de semaine du programme stocké différent'
        WHEN gs.id IS NOT NULL AND NOT st.dans_creneau THEN 'hors créneau du programme stocké'
        WHEN gs.id IS NOT NULL AND NOT (st.jour BETWEEN COALESCE(gs.grid_start, '-infinity'::date) AND gs.grid_end)
            THEN 'grille du programme stocké non valide ce jour'
        -- l'ingestion compare program_grid_end (minuit) à l'horodatage : elle exclut le dernier jour de grille
        WHEN gc.grid_end = d.paris_time::date THEN 'dernier jour de la grille recalculée'
        WHEN gs.id IS NOT NULL AND gc.id IS NOT NULL THEN 'programmes qui se chevauchent'
    END AS piste,
    d.channel_name,
    d.paris_time AS heure_paris,
    d.heure_utc,
    EXTRACT(ISODOW FROM d.paris_time)::int AS jour_semaine_paris,
    -- programme stocké
    d.id_stocke,
    gs.channel_program AS stocke_programme,
    gs.weekday AS stocke_jour_semaine,
    gs.programme_debut AS stocke_debut,
    gs.programme_fin AS stocke_fin,
    gs.grid_start AS stocke_grid_start,
    gs.grid_end AS stocke_grid_end,
    st.dans_creneau AS stocke_dans_creneau,
    ROUND(st.ecart::numeric, 1) AS stocke_ecart_minutes,
    su.dans_creneau AS stocke_dans_creneau_heure_utc,
    st.jour BETWEEN COALESCE(gs.grid_start, '-infinity'::date) AND gs.grid_end AS stocke_grille_valide,
    -- programme recalculé
    d.id_calcule,
    gc.channel_program AS calcule_programme,
    gc.weekday AS calcule_jour_semaine,
    gc.programme_debut AS calcule_debut,
    gc.programme_fin AS calcule_fin,
    gc.grid_start AS calcule_grid_start,
    gc.grid_end AS calcule_grid_end
FROM differences d
LEFT JOIN grille gs ON gs.id = d.id_stocke
LEFT JOIN grille gc ON gc.id = d.id_calcule
-- créneau du programme stocké le plus proche du segment (veille, jour même ou lendemain), en heure de Paris
LEFT JOIN LATERAL (
    SELECT
        TRUE AS weekday_ok,
        d.paris_time::date - o.day_offset AS jour,
        x.minute >= gs.start_minute AND x.minute < gs.end_minute AS dans_creneau,
        CASE
            WHEN x.minute < gs.start_minute THEN x.minute - gs.start_minute
            WHEN x.minute >= gs.end_minute THEN x.minute - gs.end_minute
            ELSE 0
        END AS ecart
    FROM (VALUES (-1), (0), (1)) AS o(day_offset)
    CROSS JOIN LATERAL (
        SELECT EXTRACT(EPOCH FROM d.paris_time - (d.paris_time::date - o.day_offset)) / 60 AS minute
    ) x
    WHERE EXTRACT(ISODOW FROM d.paris_time::date - o.day_offset) = gs.weekday
    ORDER BY x.minute >= gs.start_minute AND x.minute < gs.end_minute DESC,
        GREATEST(gs.start_minute - x.minute, x.minute - gs.end_minute)
    LIMIT 1
) st ON TRUE
-- même chose avec l'heure UTC lue comme une heure de Paris
LEFT JOIN LATERAL (
    SELECT BOOL_OR(x.minute >= gs.start_minute AND x.minute < gs.end_minute) AS dans_creneau
    FROM (VALUES (-1), (0), (1)) AS o(day_offset)
    CROSS JOIN LATERAL (
        SELECT EXTRACT(EPOCH FROM d.heure_utc - (d.heure_utc::date - o.day_offset)) / 60 AS minute
    ) x
    WHERE EXTRACT(ISODOW FROM d.heure_utc::date - o.day_offset) = gs.weekday
) su ON TRUE
ORDER BY categorie, piste, d.channel_name, d.start
