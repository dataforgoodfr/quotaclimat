{{
    config(
        materialized='table',
        schema='analytics',
        indexes=[
            {'columns': ['task_aggregate_id', 'theme'], 'unique': True},
            {'columns': ['theme']},
        ],
    )
}}

{#- analytics.cas_theme: the themes of the validated misinformation cases of the French channels
    (analytics.cas_de_mesinformation), found by keywords in the claim written by the annotator in Label Studio
    only: not in the explanation, which often describes the context of the debate (a case on wind power whose
    explanation compares it to electric cars), nor in the transcript (a 2 min segment mixes subjects: on the
    production cases, the transcript almost doubles the cases of the automobile theme, mostly cases on renewable
    energy or nuclear power where a car keyword is said in passing). One row per case and theme with at least one
    keyword in the claim; with cas_de_mesinformation.emission_cle, a theme's cases can be counted per emission
    (Metabase).
    Keywords: the seed public.mesinfo_theme_mots_cles (seeds/themes, versioned in the repository; tuned on the
    production cases): a Postgres regular expression,
    case-insensitive unless casse; force: fort, the keyword is enough to tie the case to the theme; faible,
    ambiguous out of context (batterie, autonomie, 2035...).
    pertinence: forte (strong keyword in the claim), moyenne (weak keywords only). faible is no longer given
    (it was a weak keyword in the explanation only); mots_cles_explication is kept, always NULL, so that the
    queries reading it do not break.
    Every case is to be reviewed by a person before any external use: the keywords only shortlist them. -#}

WITH mots_cles AS (
    SELECT theme, mot_cle, motif, force, casse
    FROM {{ ref('mesinfo_theme_mots_cles') }}
),
-- keywords found in the claim of each case
trouves AS (
    SELECT
        c.task_aggregate_id,
        m.theme,
        m.mot_cle,
        m.force
    FROM {{ ref('cas_de_mesinformation') }} c
    JOIN mots_cles m
      ON CASE WHEN m.casse THEN c.claims ~ m.motif ELSE c.claims ~* m.motif END
    WHERE c.country = 'france'
),
par_cas AS (
    SELECT
        task_aggregate_id,
        theme,
        string_agg(DISTINCT mot_cle, ', ') AS mots_cles_affirmation,
        COUNT(*) FILTER (WHERE force = 'fort') AS nb_forts
    FROM trouves
    GROUP BY task_aggregate_id, theme
)
SELECT
    task_aggregate_id,
    theme,
    CASE WHEN nb_forts > 0 THEN 'forte' ELSE 'moyenne' END AS pertinence,
    mots_cles_affirmation,
    NULL::text AS mots_cles_explication
FROM par_cas
