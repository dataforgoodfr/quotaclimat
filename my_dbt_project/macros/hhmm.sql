{#- HH:MM of a number of minutes since the start of the day, after midnight for 1440 and beyond
    (e.g. 1500 -> 01:00), 24:00 for exactly 1440. -#}
{% macro hhmm(minutes) -%}
  CASE WHEN {{ minutes }} = 1440 THEN '24:00'
    ELSE LPAD((({{ minutes }} / 60) % 24)::text, 2, '0') || ':' || LPAD(({{ minutes }} % 60)::text, 2, '0')
  END
{%- endmacro %}
