{#- Generic test: at most one row per combination of the name keys (see name_key.sql) of `columns`, NULL and
    empty values counting as one value. Two spellings of the same name ("Škoda" and "SKODA") are duplicates,
    since the models join on the name keys. -#}
{% test unique_name_keys(model, columns) %}
SELECT
  {% for c in columns %}COALESCE({{ name_key(c ~ '::text') }}, '') AS key_{{ loop.index }}, {% endfor %}
  COUNT(*) AS nb_rows
FROM {{ model }}
GROUP BY {% for c in columns %}{{ loop.index }}{{ ", " if not loop.last }}{% endfor %}
HAVING COUNT(*) > 1
{% endtest %}
