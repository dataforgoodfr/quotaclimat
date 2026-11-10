{#- Identity columns of the tab Marques of the Inventaire_des_marques Google Sheet (see ad_brands). Cast to text
    first: a seed column holding only digits is loaded as an integer when dbt ignores its column_types (partial
    parsing of a seed CSV added since the last parse), the leading zeros are put back below. -#}

{#- 9-digit SIREN: spaces and dots removed, leading zeros put back when the sheet read it as a number
    ("5520135" -> "005520135"); NULL when empty or not a SIREN. -#}
{% macro clean_siren(column) -%}
  CASE
    WHEN REGEXP_REPLACE({{ column }}::text, '[\s.]', '', 'g') ~ '^\d{7,9}$'
      THEN LPAD(REGEXP_REPLACE({{ column }}::text, '[\s.]', '', 'g'), 9, '0')
  END
{%- endmacro %}

{#- Department code: "1" -> "01" (number in the sheet), "2a" -> "2A", "971" kept; NULL when empty. -#}
{% macro clean_department(column) -%}
  CASE
    WHEN TRIM({{ column }}::text) ~ '^\d$' THEN '0' || TRIM({{ column }}::text)
    ELSE NULLIF(UPPER(TRIM({{ column }}::text)), '')
  END
{%- endmacro %}

{#- ISO 3166-1 alpha-2 country code, upper case; NULL when empty. -#}
{% macro clean_country(column) -%}
  NULLIF(UPPER(TRIM({{ column }}::text)), '')
{%- endmacro %}
