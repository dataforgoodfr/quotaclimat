-- Taux de mésinformation et part d'environnement par émission (chaînes d'info en continue, Google Sheet Programmes)
-- Variables Metabase, type Texte, heure de Paris :
--   {{debut}} inclus, ex. 2026-09-01 ou 2026-09-01 06:00
--   {{fin}}   exclu,  ex. 2026-10-01
-- volume_heures : durée de diffusion théorique de l'émission sur la période, d'après la grille
--   (analytics.program), coupée aux bornes de la période
-- env_heures : temps consacré à l'environnement, même calcul que core_query_environmental_shares
--   (public.keywords.number_of_keywords fenêtres de 20 s), segments rattachés à l'émission à l'antenne
-- nb_cas : cas validés (analytics.cas_de_mesinformation, 1re annotation 'Correct') attribués à l'émission
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
        p.country,
        p.emission,
        p.rediffusion,
        p.grid_start,
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
-- segments de 2 min de public.keywords (start en UTC) rattachés à l'émission à l'antenne à leur début ;
-- si deux émissions se chevauchent, la plus récente grille gagne, comme dans la macro program_emission_at
env_segments AS (
    SELECT DISTINCT ON (k.id, k.start)
        d.channel_name,
        d.emission,
        d.rediffusion,
        k.number_of_keywords
    FROM bornes b
    JOIN public.keywords k
      -- pré-filtre large en UTC, le filtre exact en heure de Paris est dans la jointure avec diffusions
      ON k.start >= b.debut - INTERVAL '1 day'
     AND k.start < b.fin + INTERVAL '1 day'
    JOIN diffusions d
      ON d.channel_name = k.channel_name
     AND d.country = k.country
     AND (k.start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' >= d.diffusion_debut
     AND (k.start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris' < d.diffusion_fin
    ORDER BY k.id, k.start, d.grid_start DESC NULLS LAST, d.diffusion_debut DESC
),
env AS (
    SELECT
        channel_name,
        emission,
        rediffusion,
        SUM(number_of_keywords) * 20 / 3600.0 AS env_heures
    FROM env_segments
    GROUP BY 1, 2, 3
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
    ROUND(COALESCE(e.env_heures, 0)::numeric, 2) AS env_heures,
    ROUND((100 * COALESCE(e.env_heures, 0) / v.volume_heures)::numeric, 2) AS part_env_pct,
    COALESCE(c.nb_cas, 0) AS nb_cas,
    ROUND((COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 3) AS cas_par_heure,
    ROUND((10 * COALESCE(c.nb_cas, 0) / v.volume_heures)::numeric, 2) AS cas_pour_10h,
    -- NULL quand l'émission n'a pas parlé d'environnement sur la période
    ROUND((COALESCE(c.nb_cas, 0) / NULLIF(e.env_heures, 0))::numeric, 2) AS cas_par_heure_env
FROM volume v
LEFT JOIN env e
  ON e.channel_name = v.channel_name
 AND e.emission = v.emission
 AND e.rediffusion = v.rediffusion
LEFT JOIN cas c
  ON c.channel_name = v.channel_name
 AND c.emission = v.emission
 AND c.rediffusion = v.rediffusion
ORDER BY cas_par_heure DESC, volume_heures DESC
