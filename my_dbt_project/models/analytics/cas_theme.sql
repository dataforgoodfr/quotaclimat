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
    (analytics.cas_de_mesinformation), found by keywords in the claim and the explanation written by the annotator
    in Label Studio (not in the transcript: a 2 min segment mixes subjects). One row per case and theme with at
    least one keyword; with cas_de_mesinformation.emission_cle, a theme's cases can be counted per emission
    (Metabase).
    Keywords: the seed mesinfo_theme_mots_cles (seeds/themes, versioned in the repository; tuned on the production
    cases, see metabase/metabase_cas_theme_vehicule_electrique.sql): a Postgres regular expression,
    case-insensitive unless casse; force: fort, the keyword is enough to tie the case to the theme; faible,
    ambiguous out of context (batterie, autonomie, 2035...).
    pertinence: forte (strong keyword in the claim), moyenne (strong keyword in the explanation only, or weak
    keyword in the claim), faible (weak keyword in the explanation only). The explanation often describes the
    context of the debate rather than the claim: a strong keyword there is not enough for forte.
    Every case is to be reviewed by a person before any external use: the keywords only shortlist them. -#}

WITH mots_cles AS (
    SELECT theme, mot_cle, motif, force, casse
    FROM {{ ref('mesinfo_theme_mots_cles') }}
),
-- keywords found, per case and field
trouves AS (
    SELECT
        c.task_aggregate_id,
        m.theme,
        t.champ,
        m.mot_cle,
        m.force
    FROM {{ ref('cas_de_mesinformation') }} c
    CROSS JOIN LATERAL (
        VALUES
            ('affirmation', c.claims),
            ('explication', c.explanations)
    ) AS t(champ, texte)
    JOIN mots_cles m
      ON CASE WHEN m.casse THEN t.texte ~ m.motif ELSE t.texte ~* m.motif END
    WHERE c.country = 'france'
),
par_cas AS (
    SELECT
        task_aggregate_id,
        theme,
        string_agg(DISTINCT mot_cle, ', ') FILTER (WHERE champ = 'affirmation') AS mots_cles_affirmation,
        string_agg(DISTINCT mot_cle, ', ') FILTER (WHERE champ = 'explication') AS mots_cles_explication,
        COUNT(*) FILTER (WHERE champ = 'affirmation' AND force = 'fort') AS nb_forts_affirmation,
        COUNT(*) FILTER (WHERE champ = 'explication' AND force = 'fort') AS nb_forts_explication,
        COUNT(*) FILTER (WHERE champ = 'affirmation') AS nb_affirmation
    FROM trouves
    GROUP BY task_aggregate_id, theme
)
SELECT
    task_aggregate_id,
    theme,
    CASE
        WHEN nb_forts_affirmation > 0 THEN 'forte'
        WHEN nb_forts_explication > 0 OR nb_affirmation > 0 THEN 'moyenne'
        ELSE 'faible'
    END AS pertinence,
    mots_cles_affirmation,
    mots_cles_explication
FROM par_cas
