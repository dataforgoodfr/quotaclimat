-- Contrôle du fuseau de data_item_start, chaînes françaises : pour chaque cas de mésinformation, le
-- programme noté dans la tâche Label Studio (data.item.channel_program, posé à la création du segment)
-- face au programme que la macro dbt program_at a trouvé (analytics.program, data_item_start lu en UTC).
-- Beaucoup de lignes « different » avec un écart d'1 ou 2 h entre les créneaux : data_item_start n'est
-- pas de l'UTC et la macro program_at (cas_de_mesinformation.program_id) est décalée.
-- start_brut montre la chaîne d'origine du JSON : un suffixe +02:00 ou Z est ignoré par le cast ::TIMESTAMP.
-- Variables Metabase, type Texte : {{debut}} inclus, {{fin}} exclu, ex. 2026-09-01 et 2026-09-08
SELECT
    c.data_item_channel_program IS DISTINCT FROM p.channel_program AS different,
    c.data_item_channel_name AS chaine,
    lta.data::jsonb #>> ARRAY['item', 'start'] AS start_brut,
    c.data_item_start,
    c.data_item_channel_program AS programme_label_studio,
    p.channel_program AS programme_dbt,
    p.program_start AS programme_dbt_debut,
    p.program_end AS programme_dbt_fin,
    c.task_aggregate_id
FROM analytics.cas_de_mesinformation c
LEFT JOIN analytics.program p ON p.id = c.program_id
LEFT JOIN public.labelstudio_task_aggregate lta ON lta.task_aggregate_id = c.task_aggregate_id
WHERE c.country = 'france'
  AND c.data_item_start >= CAST({{debut}} AS timestamp) - INTERVAL '3 hours'
  AND c.data_item_start < CAST({{fin}} AS timestamp) + INTERVAL '3 hours'
ORDER BY different DESC, chaine, c.data_item_start
