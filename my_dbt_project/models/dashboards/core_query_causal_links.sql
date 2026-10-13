{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
  )
}}

{% set process_month = var("process_month", date_trunc('month', current_date)) %}

SELECT
  public.keywords.id,
  public.keywords.channel_title,
  public.keywords.country,
  public.keywords.start,
  kw_consequence ->> 'keyword' AS keyword,
  CASE
    WHEN LOWER(kw_consequence ->> 'theme') LIKE '%climat%' THEN 'Crise climatique'
    WHEN LOWER(kw_consequence ->> 'theme') LIKE '%biodiversite%' THEN 'Crise de la biodiversité'
    WHEN LOWER(kw_consequence ->> 'theme') LIKE '%ressource%' THEN 'Crise des ressources'
    ELSE 'Autre'
  END AS crise,
  (
    SELECT COUNT(*)
    FROM public.keywords k2
    WHERE k2.channel_title = public.keywords.channel_title
      -- same channel of the same country (several countries share the "RTL" title),
      -- which also lets Postgres use ix_keywords_country_channel_name_start
      AND k2.country = public.keywords.country
      AND k2.channel_name = public.keywords.channel_name
      AND k2.number_of_changement_climatique_constat_no_hrfp > 0
      AND k2.start BETWEEN public.keywords.start - interval '4 minutes' AND public.keywords.start + interval '4 minutes'
       and date_trunc('month', public.keywords.start) = cast('{{ var("process_month") }}' as date)
  ) AS nb_constats_climat_neighbor,
  (
    SELECT COUNT(*)
    FROM public.keywords k3
    WHERE k3.channel_title = public.keywords.channel_title
      -- same channel of the same country (several countries share the "RTL" title),
      -- which also lets Postgres use ix_keywords_country_channel_name_start
      AND k3.country = public.keywords.country
      AND k3.channel_name = public.keywords.channel_name
      AND k3.number_of_biodiversite_concepts_generaux_no_hrfp > 0
      AND k3.start BETWEEN public.keywords.start - interval '4 minutes' AND public.keywords.start + interval '4 minutes'
       and date_trunc('month', public.keywords.start) = cast('{{ var("process_month") }}' as date)
  ) AS nb_constats_biodiversite_neighbor
FROM public.keywords
CROSS JOIN LATERAL json_array_elements(public.keywords.keywords_with_timestamp::json) kw_consequence
WHERE LOWER(kw_consequence ->> 'theme') LIKE '%consequence%'
  -- range on the raw column (not date_trunc(start)) so that only the partitions of process_month are read
  and public.keywords.start >= cast('{{ var("process_month") }}' as date)
  and public.keywords.start < cast('{{ var("process_month") }}' as date) + interval '1 month'