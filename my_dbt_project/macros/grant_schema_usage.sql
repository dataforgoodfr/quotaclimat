{#- on-run-end hook: grants usage on the schema of each model and seed built by the run to the roles of its `select`
    grants. dbt's +grants only grant select on the relations: without usage on their schema, a role cannot read
    them ("permission denied for schema"). The readonly privilege of the Scaleway database users only covers the
    schemas that existed when it was set, so a schema created later by dbt (curated) was not readable by Metabase.
    Usage alone gives access to no table: it only makes the select grants of dbt effective.
    Once at the end of the run, not in a post-hook: concurrent grants on the same schema fail in Postgres ("tuple
    concurrently updated"). A refused grant (schema owned by another role, e.g. public) only raises a warning, so
    that it never fails the run. Idempotent. -#}
{% macro grant_schema_usage(results) -%}
    {%- set grantees_by_schema = {} -%}
    {%- for result in results
        if result.node.resource_type in ['model', 'seed'] and result.status == 'success' -%}
        {%- for grantee in (result.node.config.grants or {}).get('select', []) -%}
            {%- do grantees_by_schema.setdefault(result.node.schema, []).append(grantee) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- for schema, grantees in grantees_by_schema.items() %}
        do $$ begin
            grant usage on schema {{ adapter.quote(schema) }} to {{ grantees | unique | join(', ') }};
        exception when insufficient_privilege then
            raise warning 'grant usage on schema % refused: %', {{ "'" ~ schema ~ "'" }}, sqlerrm;
        end $$;
    {%- else %}
        select 1;
    {%- endfor %}
{%- endmacro %}
