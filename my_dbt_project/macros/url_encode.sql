{#- Percent-encoding of a text for a URL query parameter, the SQL version of urllib.parse.quote(text, safe=''):
    every character but A-Z, a-z, 0-9 and _ . - ~ becomes the %XX of its UTF-8 bytes ("Coca-Cola®" ->
    "Coca-Cola%C2%AE", a space -> "%20"). NULL stays NULL, '' gives ''. One subquery per value: apply it to
    distinct values on large tables. Checked against Python by test_url_encode_matches_python (pytest_tests). -#}
{% macro url_encode(column) -%}
  CASE WHEN {{ column }} IS NOT NULL THEN COALESCE((
    SELECT string_agg(
      CASE WHEN chars.c ~ '^[A-Za-z0-9_.~-]$' THEN chars.c
        ELSE regexp_replace(upper(encode(convert_to(chars.c, 'UTF8'), 'hex')), '(..)', '%\1', 'g')
      END, '' ORDER BY chars.i)
    FROM regexp_split_to_table({{ column }}, '') WITH ORDINALITY AS chars(c, i)
  ), '') END
{%- endmacro %}
