{# Same table as core_query_thematics_keywords_i8n (same columns, same order), computed from the days of core_query_thematics_keywords_i8n_daily
   (occurrences) and from the program grid (weekly duration, same computation as in core_query_thematics_keywords_i8n). Built next to it during
   the double run, compared with it in double_run_comparison. #}
{{ config(materialized='table') }}

WITH program_durations AS (
  SELECT
    pm.channel_title,
    pm.channel_program,
    pm.country,
    pm.weekday,
    CAST(pm.program_grid_start AS date) AS program_start,
    CAST(pm.program_grid_end AS date) AS program_end,
    pm.duration_minutes
  FROM public.program_metadata pm
),
program_weeks AS (
  SELECT
    pd.channel_title,
    pd.channel_program,
    pd.country,
    pd.duration_minutes,
    pd.weekday,
    generate_series(
      date_trunc('week', pd.program_start),
      date_trunc('week', pd.program_end),
      interval '1 week'
    )::date AS week_start
  FROM program_durations pd
),
program_airings AS (
  SELECT
    channel_title,
    channel_program,
    country,
    duration_minutes,
    -- calculate actual airing date per week + weekday offset
    (week_start + (weekday - 1) * INTERVAL '1 day')::date AS airing_date,
    week_start
  FROM program_weeks
),
weekly_program_durations AS (
  SELECT
    pa.channel_title,
    pa.country,
    pa.week_start AS week,
    SUM(pa.duration_minutes) AS weekly_duration_minutes
  FROM program_airings pa
  where pa.channel_title!='Das Erste'
  GROUP BY pa.channel_title, pa.country, pa.week_start
  union all
  select 
		case 
      when tm.channel_name='zdf' then 'ZDF'
      when tm.channel_name='daserste' then 'Das Erste'
      else tm.channel_name
    end channel_title,
		tm.country,
		DATE_TRUNC('week', tm.start::timestamp)::date as week,
		SUM(tm.duration_minutes) weekly_duration_minutes
	from time_monitored tm 
	where tm.country='belgium' 
    or tm.channel_name='zdf' 
    or tm.channel_name='daserste'
	group by
		channel_title,
		tm.country,
		week
),

weekly AS (
  SELECT
    channel_title, country,
    DATE_TRUNC('week', day::timestamp)::date AS week,
    category,
    high_risk_of_false_positive,
    is_solution,
    is_consequence,
    is_cause,
    is_general_concepts,
    is_statement,
    crise_type,
    theme,
    keyword,
    general,
    agriculture,
    transport,
    batiments,
    energie,
    industrie,
    eau,
    ecosysteme,
    economie_ressources,
    SUM(count) AS count
  FROM {{ ref('core_query_thematics_keywords_i8n_daily') }}
  GROUP BY channel_title, country, 3, category, high_risk_of_false_positive, is_solution, is_consequence, is_cause, is_general_concepts, is_statement, crise_type, theme, keyword, general, agriculture, transport, batiments, energie, industrie, eau, ecosysteme, economie_ressources
)
SELECT
  w.channel_title,
  w.country,
  w.week,
  w.category,
  w.high_risk_of_false_positive,
  w.is_solution,
  w.is_consequence,
  w.is_cause,
  w.is_general_concepts,
  w.is_statement,
  w.crise_type,
  w.theme,
  w.keyword,
  w.general,
  w.agriculture,
  w.transport,
  w.batiments,
  w.energie,
  w.industrie,
  w.eau,
  w.ecosysteme,
  w.economie_ressources,
  CAST(SUM(w.count) AS bigint) AS count,
  COALESCE(wpd.weekly_duration_minutes, 0) AS sum_duration_minutes
FROM weekly w
LEFT JOIN weekly_program_durations wpd
  ON wpd.channel_title = w.channel_title AND wpd.week = w.week AND wpd.country = w.country
GROUP BY
  w.channel_title, w.country, w.week,
  w.category, w.high_risk_of_false_positive, w.is_solution, w.is_consequence, w.is_cause, w.is_general_concepts, w.is_statement, w.crise_type, w.theme, w.keyword, w.general, w.agriculture, w.transport, w.batiments, w.energie, w.industrie, w.eau, w.ecosysteme, w.economie_ressources,
  wpd.weekly_duration_minutes
