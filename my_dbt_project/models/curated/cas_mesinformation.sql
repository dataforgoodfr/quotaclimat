{{
    config(
        indexes=[
            {'columns': ['debut_paris']},
            {'columns': ['chaine_id']},
        ],
    )
}}

{#- curated.cas_mesinformation: one row per validated misinformation case (analytics.cas_de_mesinformation, all
    countries), with French column names, the time in Paris time, the speakers and the correction translated,
    the channel attributes of curated.chaine, the emission of curated.emission_jour (French channels) and the
    themes of analytics.cas_theme (French channels, every pertinence). Left out: the detection internals (LLM
    model, reason and score), the Label Studio project and annotation fields, the annotators' comments.
    Documented in _curated.yml. -#}
{%- set locuteurs = [
    ('Journalist', 'journaliste'),
    ('Commentator', 'chroniqueur'),
    ('Guest', 'invite'),
    ('Politician', 'politique'),
    ('Audience', 'public'),
] -%}
{%- set libelles = {
    'Journalist': 'journaliste', 'Commentator': 'chroniqueur', 'Guest': 'invité', 'Politician': 'personnalité politique',
    'Audience': 'public', 'Unknown': 'inconnu',
} -%}
{%- set themes = ['automobile'] -%}
WITH themes AS (
    SELECT task_aggregate_id, string_agg(theme, ', ' ORDER BY theme) AS themes
    {%- for theme in themes %},
    bool_or(theme = '{{ theme }}') AS est_theme_{{ theme }}
    {%- endfor %}
    FROM {{ ref('cas_theme') }}
    GROUP BY task_aggregate_id
),
-- speakers chosen by the annotator, comma separated in Label Studio ('Guest,Journalist')
locuteurs AS (
    SELECT
        m.task_aggregate_id,
        string_agg(
            CASE l.locuteur
            {%- for code, libelle in libelles.items() %}
                WHEN '{{ code }}' THEN '{{ libelle }}'
            {%- endfor %}
                ELSE l.locuteur
            END, ', ' ORDER BY l.locuteur
        ) AS locuteurs
    FROM {{ ref('cas_de_mesinformation') }} m
    CROSS JOIN LATERAL unnest(string_to_array(m.locuteur_choice, ',')) AS l(locuteur)
    GROUP BY m.task_aggregate_id
)
SELECT
    m.task_aggregate_id::text AS cas_id,
    ((m.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::timestamp AS debut_paris,
    ((m.data_item_start AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris')::date AS jour,
    m.data_item_channel_name::text AS chaine_id,
    COALESCE(c.chaine_nom, m.channel_title, m.data_item_channel_title)::text AS chaine_nom,
    {{ pays_fr('m.country') }} AS pays,
    COALESCE(c.est_service_public, m.public, FALSE) AS est_service_public,
    COALESCE(c.est_info_continue, m.infocontinue, FALSE) AS est_info_continue,
    COALESCE(c.est_radio, m.radio, FALSE) AS est_radio,
    m.program::text AS programme,
    CASE WHEN m.emission_cle IS NOT NULL THEN (m.emission_cle || '|' || m.jour_programme) END::text AS emission_jour_id,
    NULLIF(trim(m.claims), '')::text AS affirmation,
    NULLIF(trim(m.explanations), '')::text AS explication,
    NULLIF(trim(m.debunk_references), '')::text AS sources_verification,
    COALESCE(lo.locuteurs, 'non renseigné')::text AS locuteurs,
{%- for code, nom in locuteurs %}
    COALESCE(m.locuteur_choice LIKE '%{{ code }}%', FALSE) AS est_dit_par_{{ nom }},
{%- endfor %}
    (CASE m.mesinfo_corrected
        WHEN 'corrected' THEN 'corrigée'
        WHEN 'not_corrected' THEN 'non corrigée'
        ELSE 'non renseignée'
    END)::text AS correction,
    COALESCE(NULLIF(trim(m.data_item_plaintext_whisper), ''), m.data_item_plaintext)::text AS transcription,
    NULLIF(m.data_item_url_mediatree, '')::text AS lien_extrait,
    th.themes::text AS themes
{%- for theme in themes %},
    COALESCE(th.est_theme_{{ theme }}, FALSE) AS est_theme_{{ theme }}
{%- endfor %}
FROM {{ ref('cas_de_mesinformation') }} m
LEFT JOIN {{ ref('chaine') }} c ON c.chaine_id = m.data_item_channel_name
LEFT JOIN locuteurs lo ON lo.task_aggregate_id = m.task_aggregate_id
LEFT JOIN themes th ON th.task_aggregate_id = m.task_aggregate_id
