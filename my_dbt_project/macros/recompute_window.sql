{#
  Recompute window of the incremental models of models/dashboards and models/homepage.

  These models aggregate the whole public.keywords table by week (or by month). A full rebuild reads every
  partition of the table and takes most of the dbt run, so a normal run only recomputes the recent periods:
  - the first period (week or month) recomputed starts at the earliest of: today (Paris) minus the var
    recompute_lookback_days (default 35), and the var recompute_since (YYYY-MM-DD, set by entrypoints/dbt.sh
    to the day before the period imported by the Kestra flow that runs dbt, DBT_IMPORT_START_DATE and
    DBT_IMPORT_DAYS_BACK);
  - that day is moved back to the start of its period (Monday for a week, the 1st for a month), so that every
    recomputed period is complete;
  - a pre-hook deletes the rows of the window from the table (recompute_window_delete), then the recomputed rows
    are inserted, in the same transaction (delete+insert): a period or a row that no longer exists in the sources
    disappears too, older periods keep their last build.
  With --full-refresh (Kestra input full_refresh, or the weekly full rebuild of the dbt flow), is_incremental()
  is false: no filter, the whole history is rebuilt.

  Usage in a model:
    {{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key=['week'],
              pre_hook="{{ recompute_window_delete('week', 'week') }}") }}
    {{ recompute_window_begin() }}
    ... the query, with AND {{ recompute_window_condition('k.start', 'week') }} on each scan of the dated tables
    {{ recompute_window_end('"start"', 'week') }}
  The condition reads one more day than the window (safety margin); recompute_window_end then keeps only the
  complete periods of the window, so a partly read period never replaces a complete one.
#}

{% macro recompute_since(grain='week') %}
  {%- set lookback_days = var('recompute_lookback_days', 35) | int -%}
  {%- set today = modules.datetime.datetime.now(modules.pytz.timezone('Europe/Paris')).date() -%}
  {%- set since = today - modules.datetime.timedelta(days=lookback_days) -%}
  {%- set requested = var('recompute_since', '') -%}
  {%- if requested -%}
    {%- set requested_day = modules.datetime.datetime.strptime(requested | string, '%Y-%m-%d').date() -%}
    {%- if requested_day < since -%}
      {%- set since = requested_day -%}
    {%- endif -%}
  {%- endif -%}
  {%- if grain == 'month' -%}
    {%- set since = since.replace(day=1) -%}
  {%- elif grain == 'week' -%}
    {%- set since = since - modules.datetime.timedelta(days=since.weekday()) -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error("recompute_since: unknown grain '" ~ grain ~ "', expected 'week' or 'month'") }}
  {%- endif -%}
  {{- since.strftime('%Y-%m-%d') -}}
{% endmacro %}

{# true in a full rebuild; in an incremental run, keeps the rows dated from the window start minus one day #}
{% macro recompute_window_condition(column, grain='week') %}
  {%- if is_incremental() -%}
    {%- set since = modules.datetime.datetime.strptime(recompute_since(grain) | trim, '%Y-%m-%d').date() -%}
    {#- no cast on the column: Postgres reads only the partitions of keywords (timestamptz in production) of the
        window when the column is compared as it is with a constant, which takes the type of the column -#}
    {{ column }} >= '{{ (since - modules.datetime.timedelta(days=1)).strftime('%Y-%m-%d') }}'
  {%- else -%}
    TRUE
  {%- endif -%}
{% endmacro %}

{% macro recompute_window_begin() %}
  {%- if is_incremental() %}SELECT * FROM ({% endif -%}
{% endmacro %}

{# keeps the periods of the window only (column: the period of the model's output, e.g. "start" or week) #}
{% macro recompute_window_end(column, grain='week') %}
  {%- if is_incremental() %}
) AS recomputed_periods
WHERE {{ column }} >= '{{ recompute_since(grain) | trim }}'
  {%- endif -%}
{% endmacro %}

{# pre-hook, in the transaction of the insert: deletes the rows of the window (column: the period of the model's output,
   unquoted, e.g. 'start' or 'week'; for a model without period, the date of its rows). Nothing in a full rebuild. #}
{% macro recompute_window_delete(column, grain='week') %}
  {%- if is_incremental() -%}
    DELETE FROM {{ this }} WHERE "{{ column }}" >= '{{ recompute_since(grain) | trim }}'
  {%- endif -%}
{% endmacro %}
