{#- on-run-end hook: grants usage on each schema of `schemas` to the roles of the `select` grants of the models
    built in it by this run. dbt's +grants only grant select on the relations: without usage on their schema, a
    role cannot read them ("permission denied for schema"). Needed for a schema created by dbt after the database
    users (curated): the readonly privilege of Scaleway only covers the schemas that existed when it was set.
    Once at the end of the run, not in a post-hook: concurrent grants on the same schema fail in Postgres ("tuple
    concurrently updated"). Idempotent. -#}
{% macro grant_schema_usage(results, schemas) -%}
    {%- set grantees_by_schema = {} -%}
    {%- for result in results
        if result.node.resource_type == 'model' and result.status == 'success' and result.node.schema in schemas -%}
        {%- for grantee in (result.node.config.grants or {}).get('select', []) -%}
            {%- do grantees_by_schema.setdefault(result.node.schema, []).append(grantee) -%}
        {%- endfor -%}
    {%- endfor -%}
    {%- for schema, grantees in grantees_by_schema.items() %}
        grant usage on schema {{ schema }} to {{ grantees | unique | join(', ') }};
    {%- else %}
        select 1;
    {%- endfor %}
{%- endmacro %}
