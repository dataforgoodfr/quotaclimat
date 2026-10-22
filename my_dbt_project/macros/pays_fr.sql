{#- French name of a country of program_metadata (country column: france, belgium...), for the curated schema.
    A new country is left NULL, which fails the accepted_values test of curated.chaine.pays: add it here. -#}
{% macro pays_fr(country) -%}
    (CASE {{ country }}
        WHEN 'france' THEN 'France'
        WHEN 'belgium' THEN 'Belgique'
        WHEN 'brazil' THEN 'Brésil'
        WHEN 'germany' THEN 'Allemagne'
        WHEN 'poland' THEN 'Pologne'
        WHEN 'spain' THEN 'Espagne'
    END)::text
{%- endmacro %}
