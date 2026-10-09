{#- Generic test: fails on the non-NULL values of the column that do not match the POSIX regular
    expression `pattern` (e.g. a time "b" instead of "10:00" in a reference Google Sheet). -#}
{% test matches_pattern(model, column_name, pattern) %}
SELECT {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND {{ column_name }}::text !~ '{{ pattern | replace("'", "''") }}'
{% endtest %}
