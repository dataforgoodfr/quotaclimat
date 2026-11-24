{{
    config(
        materialized='table',
        schema='analytics',
        indexes=[
            {'columns': ['affirmation_id', 'cible_code'], 'unique': True},
            {'columns': ['task_aggregate_id']},
        ],
    )
}}

{#- analytics.cas_affirmation_cible: one row per claim of a misinformation case and per target ("cible") it
    attacks or defends, from the Cas_de_mesinfo Google Sheet (seeds ref_cas_affirmations and ref_cas_cibles, see
    my_dbt_project/external_sources.yml). The tab 1_affirmations has one column per target ("attaque", "défend"
    or empty), unpivoted here; a target column added to the tab must be added to external_sources.yml and to the
    list below. est_sens_reference: the claim goes the most common direction of its target (tab 0_cibles), for
    information only: the categories attacked depend on the direction of the grid rows (cas_categorie_visee). -#}
{%- set cibles = ['enr', 'prx', 'nuc', 'fos', 've', 'vt', 'pau', 'aer', 'vel', 'trc', 'bat', 'clim', 'elv', 'agr',
                  'sci', 'mes', 'rol'] %}
WITH affirmations AS (
    {{ source_or_empty('advertising', 'cas_affirmations', ['affirmation_id', 'task_aggregate_id'] + cibles) }}
),
cibles AS (
    {{ source_or_empty('advertising', 'cas_cibles', ['cible_code', 'cible', 'famille', 'sens_reference']) }}
)
SELECT
    trim(a.affirmation_id)::text AS affirmation_id,
    trim(a.task_aggregate_id)::text AS task_aggregate_id,
    u.cible_code,
    c.cible,
    c.famille,
    lower(trim(u.sens)) AS sens,
    lower(trim(c.sens_reference)) AS sens_reference,
    lower(trim(u.sens)) = lower(trim(c.sens_reference)) AS est_sens_reference
FROM affirmations a
CROSS JOIN LATERAL (
    VALUES
    {%- for c in cibles %}
        ('{{ c | upper }}', a.{{ c }}::text){{ "," if not loop.last }}
    {%- endfor %}
) AS u(cible_code, sens)
LEFT JOIN cibles c ON upper(trim(c.cible_code)) = u.cible_code
WHERE NULLIF(trim(u.sens), '') IS NOT NULL
