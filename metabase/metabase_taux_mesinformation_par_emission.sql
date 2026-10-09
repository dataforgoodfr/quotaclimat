-- Taux de mésinformation par émission (chaînes d'info en continue, Google Sheet Programmes)
-- Variables Metabase, type Texte, heure de Paris :
--   {{debut}} inclus, ex. 2026-09-01 ou 2026-09-01 06:00
--   {{fin}}   exclu,  ex. 2026-10-01
-- nb_cas : cas validés (analytics.cas_de_mesinformation, 1re annotation 'Correct') attribués à l'émission
-- volume_heures : durée de diffusion théorique de l'émission sur la période, d'après la grille
--   (analytics.program), coupée aux bornes de la période
WITH bornes AS (
    SELECT
        CAST({{debut}} AS timestamp) AS debut,
        CAST({{fin}} AS timestamp) AS fin
),
-- une ligne par diffusion (émission x jour), en heure de Paris ; on part de la veille de {{debut}}
-- pour les émissions qui finissent après minuit
diffusions AS (
    SELECT
        p.channel_name,
        p.channel_title,
        p.emission,
        p.rediffusion,
        GREATEST(j.jour + p.start_minute * INTERVAL '1 minute', b.debut) AS diffusion_debut,
        LEAST(j.jour + p.end_minute * INTERVAL '1 minute', b.fin) AS diffusion_fin
    FROM bornes b
    CROSS JOIN LATERAL generate_series(
        date_trunc('day', b.debut) - INTERVAL '1 day', b.fin, INTERVAL '1 day'
    ) AS j(jour)
    JOIN analytics.program p
      ON p.weekday = EXTRACT(ISODOW FROM j.jour)
     AND j.jour::date BETWEEN COALESCE(p.grid_start, '-infinity'::date) AND p.grid_end
    WHERE p.start_minute IS NOT NULL
      AND p.end_minute IS NOT NULL
),
volume AS (
    SELECT
        channel_name,
        COALESCE(channel_title, channel_name) AS chaine,
        emission,
        rediffusion,
        COUNT(*) AS nb_diffusions,
        SUM(EXTRACT(EPOCH FROM diffusion_fin - diffusion_debut)) / 3600.0 AS volume_heures
    FROM diffusions
    WHERE diffusion_fin > diffusion_debut
    GROUP BY 1, 2, 3, 4
),
-- data_item_start est en UTC, converti en heure de Paris comme dans la macro program_emission_at
cas AS (
    SELECT
        c.data_item_channel_name AS channel_name,
        c.emission,
        c.emission_rediffusion AS rediffusion,
        COUNT(*) AS nb_cas
    FROM analytics.cas_de_mesinformation c
    CROSS JOIN bornes b
    WHERE c.emission IS NOT NULL
      AND (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' >= b.debut
      AND (c.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' < b.fin
    GROUP BY 1, 2, 3
)
SELECT
    v.chaine,
    v.emission,
    v.rediffusion,
    v.nb_diffusions,
    ROUND(v.volume_heures::numeric, 1) AS volume_heures,
    COALESCE(c.nb_cas, 0) AS nb_cas,
    ROUND((COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 3) AS cas_par_heure,
    ROUND((10 * COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 2) AS cas_pour_10h
FROM volume v
LEFT JOIN cas c
  ON c.channel_name = v.channel_name
 AND c.emission = v.emission
 AND c.rediffusion = v.rediffusion
ORDER BY cas_par_heure DESC, volume_heures DESC
