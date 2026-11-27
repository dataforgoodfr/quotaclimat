{#- Revoke of the grants of an existing relation (incremental models such as emission_jour): dbt reads the
    current grantees from the database without quotes (rrs-read-dev), so the default revoke fails on the
    hyphen of the role names. The grantees of +grants (dbt_project.yml) are written with their quotes. -#}
{%- macro postgres__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation.render() }} from
    {%- for grantee in grantees %} {{ grantee if grantee.startswith('"') else '"' ~ grantee ~ '"' }}{{ ',' if not loop.last }}{% endfor %}
{%- endmacro -%}
