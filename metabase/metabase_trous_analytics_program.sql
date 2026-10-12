-- Programmes suivis (program_metadata) dont analytics.program ne couvre pas tout le créneau un jour donné,
-- chaînes françaises : dans un programme découpé en émissions, le temps qu'aucune émission du Google Sheet
-- ne couvre n'a pas de ligne dans analytics.program.
-- Variable Metabase, type Texte : {{jour}}, ex. 2026-09-07
WITH lignes AS (
    SELECT
        p.program_metadata_id,
        p.channel_title,
        p.channel_program,
        p.program_start,
        p.program_end,
        p.emission,
        p.start,
        p."end",
        p.start_minute,
        p.duration_minutes
    FROM analytics.program p
    WHERE p.country = 'france'
      AND p.weekday = EXTRACT(ISODOW FROM CAST({{jour}} AS date))
      AND CAST({{jour}} AS date) BETWEEN COALESCE(p.grid_start, '-infinity'::date) AND p.grid_end
),
programmes AS (
    SELECT
        program_metadata_id,
        channel_title,
        channel_program,
        program_start,
        program_end,
        -- même lecture des horaires que analytics.program, fin après minuit au-delà de 1440
        e.end_minute_of_day - s.program_start_minute
            + CASE WHEN e.end_minute_of_day <= s.program_start_minute THEN 1440 ELSE 0 END AS duree_programme,
        SUM(duration_minutes) AS minutes_couvertes,
        STRING_AGG(emission || ' ' || start || '-' || "end", ', ' ORDER BY lignes.start_minute) AS emissions
    FROM lignes
    CROSS JOIN LATERAL (
        SELECT SPLIT_PART(program_start, ':', 1)::int * 60 + SPLIT_PART(program_start, ':', 2)::int AS program_start_minute
    ) s
    CROSS JOIN LATERAL (
        SELECT SPLIT_PART(program_end, ':', 1)::int * 60 + SPLIT_PART(program_end, ':', 2)::int AS end_minute_of_day
    ) e
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    channel_title AS chaine,
    channel_program AS programme,
    program_start AS debut,
    program_end AS fin,
    duree_programme,
    minutes_couvertes,
    duree_programme - minutes_couvertes AS minutes_sans_ligne,
    emissions
FROM programmes
WHERE minutes_couvertes <> duree_programme
ORDER BY minutes_sans_ligne DESC, chaine
