{#- Overrides dbt-postgres' postgres__get_show_grant_sql: returns the grantees between double quotes, the way the
    +grants of dbt_project.yml write them ('"rrs-read-dev"': a role name with hyphens must be quoted).

    dbt compares these existing grants with the configured ones only when it updates an existing relation, that is in
    an incremental run (a full refresh or a table rebuild creates a new relation and grants it from scratch). Without
    the quotes, the names never match: dbt revokes "rrs-read-dev" written without quotes and the run fails with
    `syntax error at or near "-"`. Happens since the models of models/daily run incrementally
    (macros/daily_window.sql). -#}
{%- macro postgres__get_show_grant_sql(relation) -%}
  select '"' || grantee || '"' as grantee, privilege_type
  from {{ relation.information_schema('role_table_grants') }}
      where grantor = current_role
        and grantee != current_role
        and table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
{%- endmacro -%}
