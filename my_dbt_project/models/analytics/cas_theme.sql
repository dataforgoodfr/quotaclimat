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
    Keywords (mots_cles below, versioned here; tuned on the production cases, see
    metabase/metabase_cas_theme_vehicule_electrique.sql): a Postgres regular expression, case-insensitive unless
    casse; \m and \M: start and end of word. force: fort, the keyword is enough to tie the case to the theme;
    faible, ambiguous out of context (batterie, autonomie, 2035...).
    pertinence: forte (strong keyword in the claim), moyenne (strong keyword in the explanation only, or weak
    keyword in the claim), faible (weak keyword in the explanation only). The explanation often describes the
    context of the debate rather than the claim: a strong keyword there is not enough for forte.
    Every case is to be reviewed by a person before any external use: the keywords only shortlist them. -#}

WITH mots_cles (theme, mot_cle, motif, force, casse) AS (
    VALUES
        ('automobile', 'voiture électrique', '\m(voiture|v[ée]hicule|auto|automobile|bagnole)s?\s+(100\s*%\s*)?[ée]lectriques?\M', 'fort', FALSE),
        ('automobile', 'mobilité électrique', 'mobilit[ée]\s+[ée]lectrique|[ée]lectromobilit[ée]|[ée]lectrification\s+(des\s+v[ée]hicules|du\s+parc|des\s+voitures|de\s+l.automobile)|\mroul\w*\s+(qu.)?en\s+[ée]lectrique|\mmotorisations?\M', 'fort', FALSE),
        ('automobile', 'borne de recharge', '\mbornes?\s+(de\s+)?recharge', 'fort', FALSE),
        ('automobile', 'voiture thermique', '\m(voiture|v[ée]hicule|moteur|auto)s?\s+thermiques?\M', 'fort', FALSE),
        ('automobile', 'fin du thermique', '\mfin\s+(du|des)\s+(moteurs?\s+)?thermiques?\M|\minterdiction\s+(du|des)\s+(moteurs?\s+|voitures?\s+)?thermiques?\M', 'fort', FALSE),
        ('automobile', 'ZFE', '\mZFE\M|\mzones?\s+([àa]\s+)?faibles?\s+[ée]missions?', 'fort', FALSE),
        ('automobile', 'leasing social', '\mleasing\s+social\M', 'fort', FALSE),
        ('automobile', 'constructeur', '\m(Tesla|Stellantis)\M|\mindustrie\s+automobile|\mconstructeurs?\s+automobiles?', 'fort', FALSE),
        ('automobile', 'voiture essence / diesel', '\mvoitures?\s+(essence|diesel)\M', 'fort', FALSE),
        ('automobile', 'hybride rechargeable', '\mhybrides?\s+rechargeables?\M', 'fort', FALSE),
        ('automobile', 'carburant alternatif', '\mbio-?carburants?\M|\mcarburants?\s+(de\s+synth[èe]se|alternatifs?)\M|(super-?)?[ée]thanol\M|\mE85\M|\mGPL\M', 'fort', FALSE),
        ('automobile', 'camion / bus électrique', '\m(camion|poids[\s-]lourd|bus)s?\s+(\S+\s+){0,3}[ée]lectriques?\M', 'fort', FALSE),
        ('automobile', 'batterie de voiture', '\mrecycl\w*\s+(des\s+)?batteries|\mbatteries?\s+([ée]lectriques?|de\s+voitures?|lithium)\M', 'fort', FALSE),
        ('automobile', 'VE', '\mVE\M', 'faible', TRUE),
        ('automobile', 'batterie', '\mbatteries?\M', 'faible', FALSE),
        ('automobile', 'recharge', '\mrecharger?\M|\mrecharges\M', 'faible', FALSE),
        ('automobile', 'autonomie', '\mautonomie\M', 'faible', FALSE),
        ('automobile', 'lithium / cobalt / terres rares', '\m(lithium|cobalt)\M|\mterres?\s+rares?\M', 'faible', FALSE),
        ('automobile', 'hybride', '\mhybrides?\M', 'faible', FALSE),
        ('automobile', 'diesel', '\mdiesels?\M', 'faible', FALSE),
        ('automobile', 'électrification', '[ée]lectrification', 'faible', FALSE),
        ('automobile', '2035', '\m2035\M', 'faible', FALSE),
        ('automobile', 'bonus écologique', '\mbonus\s+[ée]cologique\M', 'faible', FALSE)
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
