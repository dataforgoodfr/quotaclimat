{#
  Recompute window of the incremental models of models/daily (one row per day, standard dbt incremental:
  is_incremental() filter + unique_key on the day, strategy delete+insert).

  daily_since(): the first day recomputed, the earliest of today (Paris) minus the var recompute_lookback_days
  (default 14) and the var recompute_since (YYYY-MM-DD, set by entrypoints/dbt.sh to the day before the period
  imported by the Kestra flow that runs dbt).

  daily_window(column): in an incremental run, keeps the rows of the source dated from that day; in a full rebuild
  (--full-refresh, or the first run), TRUE. The column is compared as it is with a constant: Postgres then reads only
  the partitions of keywords of the window. The constant is read in the time zone of the session, like the days of
  the models (CAST(start AS date)), so each recomputed day is complete. time_monitored.start is passed with a cast
  (CAST(start AS timestamp)): a timestamp in production, text in the test seed.
#}

{% macro daily_since() %}
  {%- set lookback_days = var('recompute_lookback_days', 14) | int -%}
  {%- set since = modules.datetime.datetime.now(modules.pytz.timezone('Europe/Paris')).date()
                  - modules.datetime.timedelta(days=lookback_days) -%}
  {%- set requested = var('recompute_since', '') -%}
  {%- if requested -%}
    {%- set requested_day = modules.datetime.datetime.strptime(requested | string, '%Y-%m-%d').date() -%}
    {%- if requested_day < since -%}
      {%- set since = requested_day -%}
    {%- endif -%}
  {%- endif -%}
  {{- since.strftime('%Y-%m-%d') -}}
{% endmacro %}

{% macro daily_window(column) %}
  {%- if is_incremental() -%}
    {{ column }} >= '{{ daily_since() }}'
  {%- else -%}
    TRUE
  {%- endif -%}
{% endmacro %}
