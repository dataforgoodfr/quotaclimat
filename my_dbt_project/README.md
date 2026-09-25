# my_dbt_project

dbt project that transforms the raw tables populated by the ingestion pipelines (`keywords`, `program_metadata`, `dictionary`, `keyword_macro_category`, `time_monitored`, `labelstudio_task_aggregate`, `labelstudio_task_completion_aggregate`) into the queries used by Metabase dashboards, plus an `analytics` layer of intermediate tables used to speed those dashboards up. See the root [README's "Materialized view - dbt" section](../README.md#materialized-view---dbt) for how to run dbt locally via the `testconsole` Docker container.

## Layout
```
models/
  homepage/    -- views for the homepage
  dashboards/  -- core queries feeding Metabase dashboards (incremental tables)
  analytics/   -- intermediate/aggregated tables used by dashboards models
  schema.yml   -- column-level documentation and tests for the models above
seeds/         -- CSV snapshots of the source tables, loaded with `dbt seed` for local/test data
pytest_tests/  -- Python tests that run dbt commands and assert on the resulting tables
```
`macros/`, `snapshots/`, `analyses/` and `tests/` (generic/singular dbt tests) exist but are currently empty.

## Profiles / targets
Connection profiles are defined in `dbt/profiles.yml` (`DBT_PROFILES_DIR=/app/my_dbt_project/dbt` inside the Docker images), with two targets:
* `docker` (default): connects to schema `public` - used for the raw seeded tables and most local development.
* `analytics`: connects to schema `analytics` - used to build/query the `analytics` models (see `--target analytics` below).

Both read `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_USER`/`POSTGRES_PASSWORD`/`POSTGRES_DB` from the environment.

`target.name` (`dbt run --target <name>`, e.g. a `prod` target configured outside this repo for production runs) and the `DBT_ENV` env variable are both used in `dbt_project.yml` to gate behaviour between local/test and production - see below.

## Models

### `homepage/`
`homepage_environment_by_media_by_month`: materialized as a `view` (set in `dbt_project.yml`), aggregated monthly environmental coverage percentage by channel.

### `dashboards/`
The core queries feeding Metabase dashboards. All are `materialized='incremental'` (most with `on_schema_change='append_new_columns'`), each keyed to avoid duplicate rows on re-run (`unique_key`, or `incremental_strategy='append'` for `core_query_causal_links`):
* `core_query_environmental_shares` / `core_query_environmental_shares_i8n` / `core_query_environmental_shares_be`: % of airtime dedicated to environmental topics (climate/biodiversity/resources), by week and channel, for France / other countries / Belgium respectively. The `_be` (and `_i8n`) variants compute the denominator (`sum_duration_minutes`) differently depending on whether a channel has an editorial grid in `program_metadata` (`has_grid`): in-perimeter captured minutes if it does, full `time_monitored` otherwise - see `models/schema.yml` for the full column documentation, including the `sum_time_monitored`/`sum_monitored_in_perimeter_min` control columns.
* `core_query_thematics_keywords` / `core_query_thematics_keywords_i8n` / `core_query_thematics_keywords_be`: keyword-level occurrence counts by week, channel, theme and category, sharing the same denominator logic as the environmental shares queries above.
* `core_query_causal_links`: causal-link query, appended incrementally (see the root README's "Causal query - too slow" note on why this is not a full-refresh).
* `thematic_query_ocean`: ocean-specific thematic keyword query.

### `analytics/`
Intermediate/pre-aggregated tables (used by dashboards, or directly as a faster source for Metabase - see the root README's "Analytics" section), built with `dbt run --target analytics`:
* `task_global_completion` (`materialized='table'`): joins `labelstudio_task_aggregate`/`labelstudio_task_completion_aggregate` with `keywords` to produce one row per fact-checked segment, with model classification, speaker type one-hot columns, and misinformation percentage by week/program.
* `environmental_shares_with_desinfo_counts` (`materialized='incremental'`, keyed on `start`/`channel_name`/`country`): weekly environmental airtime share alongside misinformation counts (from `task_global_completion`) per channel.

### `advertising/`
Tables built from the `advertising` schema (ad detection and classification pipelines, tables created by alembic), in the default target schema (`public`), as part of the regular `dbt run`. Sources are declared in `models/advertising/sources.yml`. Occurrences with a `deleted_at` are ignored.
* `ad_occurrences_classified` (`materialized='table'`): one row per occurrence of a classified ad, with channel metadata from `program_metadata` and French sector / product category labels. The reference table is a Metabase CSV upload whose name carries a timestamp; after a new upload, pass `--vars '{"ad_classification_table": "<new_name>"}'`. When that table does not exist (CI, extended perimeter database), the model still builds, with empty labels.
* `ad_tunnels` (`materialized='table'`): ad tunnels per channel, i.e. consecutive non-`OTHER` fragments where each one starts at most `ad_tunnel_tolerance_sec` seconds (default 5) after the end of the previous ones; overlapping fragments stay in the same tunnel. `tunnel_id` is `channel_name@<epoch of start_date>`.

### Access grants (`+grants` in `dbt_project.yml`)
`analytics`/`dashboards`/`advertising` models grant `select` conditionally (`advertising` uses the same list as `analytics`):
* **Regular perimeter** (`EXTENDED_PERIMETER` unset/`false`): nothing granted when `DBT_ENV` is unset/`dev`; `rrs-read-dev`, `rrs-read-prod` and `climateguard-reader-user` (only `climateguard-reader-user` for `dashboards`) granted when `DBT_ENV=prod`.
* **Extended perimeter** (`EXTENDED_PERIMETER=true`, see the root README's "Extended perimeter (Droit à l'info)" section): the `extended-perimeter` database only has `rrs-read-{dev,prod}` users (no `climateguard-reader-user`), so only the `rrs-read` user matching `DBT_ENV` is granted `select`.

## Seeds
`seeds/*.csv` are snapshots of the source tables used to have deterministic local/test data. They are loaded with `dbt seed --select <name>` (see the root README for the exact commands used in the `testconsole`/analytics workflows).

Each seed except `labelstudio_task_aggregate`/`labelstudio_task_completion_aggregate` is `enabled: "{{ target.name != 'prod' }}"` and has a pre-hook that deletes any existing rows in the matching table before loading, guarded the same way - so seeding is a no-op against a `prod` target. `keywords` additionally has a post-hook adding its primary key and adjusting `program_metadata_id`'s type, since `dbt seed` infers types/constraints from the CSV and doesn't recreate them.

## Tests
`pytest_tests/` contains Python tests (`test_dbt_model_homepage.py`, `test_dbt_model_analytics.py`) that shell out to dbt commands (seed/run) against a real Postgres connection and assert on the resulting rows - see the root README's "Test" section for how to run the full suite (`pytest -vv -k dbt`, or via `docker compose up test`). They're distinct from dbt's own generic/singular tests (`tests/`, currently unused) and the column-level `tests:` (e.g. `not_null`) declared in `models/schema.yml`.
