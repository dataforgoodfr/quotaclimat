{#
  Rolling window of the incremental weekly models (models/dashboards, models/homepage).

  These models aggregate public.keywords by week (or by month for the homepage). Read from the whole table at every
  run they cost ~20 minutes each (mostly disk reads), so an incremental run only recomputes the last
  `dashboards_lookback_weeks` weeks (var, 8 by default) and replaces the rows of those weeks (delete+insert on the
  unique_key of the model). Older weeks keep their last build.

  The window always starts on a Monday (the weeks are complete, so the sums and the shares are right), or, with
  align='month', on the first Monday of a month (a month is the average of its whole weeks, see the homepage model).

  A full refresh (`dbt run --full-refresh`, or `dbt.sh` with the `full-refresh` argument) or a first build reads
  everything: in_window() is then TRUE. Run a full refresh when:
    - the keywords of past weeks were reprocessed beyond the window (UPDATE=true replay of old days),
    - the program grid (program_metadata) was changed for past weeks,
    - the columns of a model changed.
  `dbt.sh` also does it on the first day of each month, as a safety net.

  Usage: WHERE {{ in_window('"public"."keywords"."start"') }}
#}

{% macro window_start(align='week') %}
  {%- set weeks = var('dashboards_lookback_weeks', 8) | int -%}
  {%- if align == 'month' -%}
    date_trunc('week', date_trunc('month', date_trunc('week', now() - make_interval(weeks => {{ weeks }}))) + interval '6 days')
  {%- else -%}
    date_trunc('week', now() - make_interval(weeks => {{ weeks }}))
  {%- endif -%}
{% endmacro %}

{% macro in_window(column, align='week') %}
  {%- if is_incremental() -%}
    {{ column }} >= {{ window_start(align) }}
  {%- else -%}
    TRUE
  {%- endif -%}
{% endmacro %}
