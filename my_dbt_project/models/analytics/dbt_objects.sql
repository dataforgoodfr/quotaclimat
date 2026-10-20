{{
    config(
        materialized='table',
        schema='analytics',
    )
}}

{#- One row per table or view built by this dbt project (models and seeds, enabled ones only), read from the
    dbt graph: a table of the database missing here is not (or no longer) built by dbt, e.g. the leftover of a
    model removed or renamed (Metabase dashboard "Tables orphelines dbt"). The graph does not depend on
    --select, so every run rebuilds the full list. Seeds of seeds/ref are listed only when their CSV was
    downloaded before the run (see external_sources.yml). -#}
{%- set relations = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() -%}
        {%- if node.resource_type in ['model', 'seed', 'snapshot'] and node.config.materialized != 'ephemeral' -%}
            {%- do relations.append(node) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

{% for node in relations | sort(attribute='unique_id') -%}
{#- the analytics models without a schema config (task_global_completion...) are built by the
    `--target analytics` run of entrypoints/dbt.sh, whereas the graph gives the schema of the current target -#}
{%- set schema_name = 'analytics' if node.resource_type == 'model' and node.config.schema is none
    and node.original_file_path.startswith('models/analytics/') else node.schema -%}
SELECT
    {{ dbt.string_literal(schema_name) }}::text AS schema_name,
    {{ dbt.string_literal(node.alias or node.name) }}::text AS table_name,
    {{ dbt.string_literal(node.resource_type) }}::text AS resource_type,
    {{ dbt.string_literal(node.config.materialized) }}::text AS materialized,
    {{ dbt.string_literal(node.original_file_path) }}::text AS file_path
{% if not loop.last %}UNION ALL
{% endif -%}
{%- else -%}
{#- parse time (execute is false): an empty relation with the same columns -#}
SELECT NULL::text AS schema_name, NULL::text AS table_name, NULL::text AS resource_type,
    NULL::text AS materialized, NULL::text AS file_path
WHERE FALSE
{%- endfor %}
