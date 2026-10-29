# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**QuotaClimat / L'Observatoire des médias sur l'écologie (OME)** is a data pipeline that measures what proportion of media news (TV, radio, press) covers environmental/climate topics across multiple countries (France, Belgium, Germany, Poland, Spain, Brazil). It ingests from Mediatree API (TV transcripts), Factiva (press), Le Monde FTP (web), and Label Studio (annotations), then processes keywords and stores aggregated data for Metabase dashboards.

## Commands

### Setup
```bash
poetry install          # Install dependencies
```

### Testing
```bash
docker compose up test                                          # Full test suite (requires Docker)
docker compose up testconsole -d && docker compose exec testconsole bash  # Interactive dev shell
pytest -vv -k "test_name"                                       # Run specific test locally
```

### Linting / Formatting
```bash
poetry run black .
poetry run isort .
poetry run ruff check .
```

### Database Migrations (Alembic)
```bash
poetry run alembic revision --autogenerate -m "description"    # Generate migration after model change
poetry run alembic upgrade head                                 # Apply migrations
```

### DBT (run inside testconsole container)
```bash
poetry run dbt debug
poetry run dbt seed
poetry run dbt run --models <model_name>
```

### Keyword Dictionary Update
```bash
# After downloading updated Excel from shared drive:
poetry run python3 transform_excel_to_json.py
```

### Program/Channel Metadata Update
```bash
poetry run python3 transform_program.py
```

## Architecture

The system is a **batch data pipeline** with these stages:

### 1. Data Ingestion (`quotaclimat/data_ingestion/`)
Multiple source-specific ingestion modules:
- **Mediatree**: TV/radio transcripts and metadata via API
- **Factiva**: Press articles
- **Le Monde FTP**: Web press
- **Label Studio**: Climate fact-checking annotations
- **Sitemap scraping**: News website articles

### 2. Keyword Detection (`quotaclimat/data_processing/mediatree/`)
Core NLP pipeline using **Aho-Corasick automaton** (`pyahocorasick`) + **spaCy** for lemmatization. Keywords are compiled from Excel dictionaries into JSON, organized by:
- Theme (climate, biodiversity, energy, transport, circular_economy, etc.)
- Language (fr, de, es, nl, pt, pl, it, da, el)
- Country (`i8n/` subdirectory per country)

After detection, keywords are classified into macro categories and stored with counts per TV program.

### 3. Database Layer (`postgres/`)
PostgreSQL via SQLAlchemy ORM. Key tables:
- `keywords`: Detected keyword occurrences with counts and theme classification
- `program_metadata`: TV/radio program schedule grid (~1.2MB JSON source)
- `channel_metadata`: Channel configuration and broadcast hours
- `sitemap_table`: Scraped web articles
- `stop_word`: Advertising/repetitive phrases to filter out

Schema evolution is managed with **Alembic** migrations (`alembic/versions/`). The entrypoint runs `alembic upgrade head` automatically.

### 4. DBT Transforms (`my_dbt_project/`)
SQL transformations over the PostgreSQL data to produce materialized analytics views. Used for Metabase dashboards. Seeds are in `my_dbt_project/seeds/`.

### 5. S3 / Cloud Storage
Intermediate and raw data is partitioned and stored in **Scaleway Object Storage** (S3-compatible) via `s3fs`/`boto3`, organized by `country/year/month/day/channel`.

### 6. Advertising Detection & Classification (`quotaclimat/data_ingestion/advertising/`)
Ads detected around TV/radio programs go through a separate 5-stage LLM pipeline (`s03_classification/`: transcribe → classify ad type/brand/sector → dictionary match → sub-category classify → canonicalise brand), then consolidation (`s04_consolidation/`: `e01_merge_split_ads` merges ads split into several detected pieces, `e02_manual_corrections` applies the corrections listed by humans in the `Corrections_publicites` Google Sheet: ad brand/sector/category/content type, occurrences moved to a new ad, junk occurrences) and enrichment (`s05_enrichment/`: `e01_brand_groups` proposes brand → company → ultimate parent company via INPI/GLEIF/Wikidata, written to a Google Sheet for human review; `e02_screenshots` stores mosaics of frames of every ad and occurrence in the advertising bucket, listed in `advertising.ad_screenshot`, to check ads and their metadata by eye in Metabase). dbt then builds `advertising.publicites`/`pub_annonceur_emission_jour` from the classified occurrences (see `my_dbt_project/README.md`). See the per-stage READMEs for env vars and status-flow details.

### 7. RRS — Risk Response System (`rrs/`)
A separate module/database that detects, clusters and tracks **disinformation narratives** (not just environmental keyword coverage) per topic ("subject": climate, insecurity, environmental_health). Reuses climate keyword segments already detected by the main pipeline (imported via DuckDB `ATTACH`), re-scans S3 transcripts for other subjects, classifies misinformation risk with an LLM (Mistral), and clusters recurring narrative claims with a 3-step LLM pipeline (Mistral/Claude Haiku). Own PostgreSQL database and Alembic project (`rrs/alembic.ini`, run from repo root). See `rrs/README.md`.

### Extended perimeter ("Droit à l'info")
On top of the regular France TV perimeter, a wider set of French channels (`tf1`, `france2`, `fr3-idf`, `rtl`, `france5`, `tmc`, `lcp`) is tracked under country code `ext-fra` in its own isolated `extended-perimeter` database, S3 buckets and Label Studio project — toggled by the `EXTENDED_PERIMETER` env var and threading through `transform_program.py`, dbt grants, and the RRS `source=extended` flows. It shares all the regular French-language detection/subtitle logic. See the root README's "Extended perimeter" section, `infrastructure/Readme.md`'s RRS section, and `rrs/README.md`'s "Extended perimeter" section for the full picture across app/infra/RRS.

### Orchestration — Kestra
Production runs are scheduled by Kestra flows in `infrastructure/kestra/flows/`. The `main-pipeline` flow runs twice daily (01:10 and 12:30 Europe/Paris) and chains, in order: `extended-perimeter` → `rrs-climate` (extended, no clustering) → `rrs-insecurity` (extended, no clustering) → `barometre` → `rrs-climate` (barometre, with clustering) → `rrs-insecurity` (barometre, with clustering). Each step fails the whole chain if it fails (`transmitFailed: true`). `metabase-sync` (separate flow) pushes versioned `metabase/*.sql` queries to Metabase. Dev mirrors prod in the `*-dev` namespaces/flows, with no schedule (run manually).

### Main Entry Points
- `quotaclimat/data_processing/mediatree/api_import.py` — main daily import job
- `quotaclimat/data_ingestion/ingest_db/ingest_sitemap_in_db.py` — web scraping
- `quotaclimat/data_processing/mediatree/stop_word/main.py` — stop word detection
- `docker-entrypoint.sh` — full pipeline: migrate → transform program metadata → import → ingest labelstudio → dbt

## Docker Services

Key services in `docker-compose.yml`:
- `test` — runs full pytest suite
- `testconsole` — interactive shell for development/debugging (sleeps 12000s)
- `postgres_db` — local PostgreSQL (db: `barometre`, user: `user`, password: `password`)
- `metabase` — BI dashboard at localhost:3000
- `nginxtest` — local nginx used by sitemap scraping tests

Secrets (API credentials) are mounted from `secrets/` directory as Docker secrets.

## Key Environment Variables

| Variable | Purpose |
|---|---|
| `ENV` | `docker` or `dev` — controls parsing scope |
| `POSTGRES_*` | Database connection |
| `MEDIATREE_USER/PASSWORD` | Mediatree API credentials (from secrets) |
| `BUCKET`, `BUCKET_SECRET`, `BUCKET_NAME` | Scaleway S3 config |
| `START_DATE`, `NUMBER_OF_PREVIOUS_DAYS` | Batch job date range |
| `CHANNEL` | Limit processing to a specific channel |
| `UPDATE` | Reprocess existing data |
| `MODIN_ENGINE` | Set to `ray` for distributed pandas |
| `EXTENDED_PERIMETER` | `true`/`false` — toggles the extended France perimeter ("Droit à l'info") across `transform_program.py`, Label Studio ingestion and dbt grants |
| `RRS_PG_*` | RRS PostgreSQL connection (separate database from `barometre`) |
| `ANTHROPIC_API_KEY`, `MISTRAL_API_KEY` | LLM providers used by RRS clustering/misinformation classification and the advertising classification VLM/ASR pipeline |

## CI/CD (GitHub Actions)

- **Every push**: runs pytest against a PostgreSQL service container
- **Main branch push**: bumps version (`poetry version patch`), builds Docker images, pushes to Scaleway Container Registry, updates Scaleway serverless job definitions
- Scaleway jobs run as serverless batch jobs (not always-on services)

## Multi-Country / i18n

Country-specific keyword dictionaries live under `quotaclimat/data_processing/mediatree/i8n/<country>/`. Each country has its own keyword JSON and language-specific spaCy NLP pipeline. The `i8n/srt-to-mediatree-format.py` script handles subtitle format conversion.

## Documentation map (where to look)

Each README below is the authoritative source for its area — read it instead of guessing from code when working in that area:

| File | Covers |
|---|---|
| `README.md` (root) | Full dev setup (pyenv/poetry/docker), Mediatree import/replay/update workflows, stop words, Alembic migrations, channel/program metadata, extended perimeter ("Droit à l'info"), SRT-to-Mediatree conversion, dbt materialized views, Label Studio ingestion, analytics layer |
| `infrastructure/Readme.md` | Infra stack (OpenTofu/Terragrunt/Ansible/mise), project layout, Vaultwarden secrets, `make` targets, orchestrator (Kestra/GlitchTip) deployment, RRS extended-perimeter infra, full per-target resource inventory (advertising/barometre/rrs/orchestrator) |
| `metabase/README.md` | Versioned Metabase SQL questions: folder layout, `questions.yml` manifests, variable substitution rules, Kestra sync flow (`metabase-sync`), how to add/modify a query |
| `my_dbt_project/README.md` | dbt project layout, model-by-model docs (`homepage/`, `dashboards/`, `analytics/`, `advertising/`, `metadata/`), profiles/targets, external Google Sheets sources, access grants, seeds, tests |
| `mockwebsite/README.md` | One-liner: folder served via nginx docker image for local sitemap-scraping tests |
| `quotaclimat/data_ingestion/advertising/s03_classification/README.md` | 5-stage ad classification pipeline (transcribe → classify → dict match → subcat → canonicalise brand): env vars, LLM/ASR endpoint requirements, threaded runner architecture, `prediction_status` reference |
| `quotaclimat/data_ingestion/advertising/s04_consolidation/README.md` | Consolidation stage: merge of split ads (`ad_merge.py` DB operations, merge/revert semantics), manual corrections from the `Corrections_publicites` Google Sheet (tab kinds and columns, what each correction changes), env vars |
| `quotaclimat/data_ingestion/advertising/s05_enrichment/README.md` | One-liner pointers to `e01_brand_groups/` and `e02_screenshots/` |
| `quotaclimat/data_ingestion/advertising/s05_enrichment/e01_brand_groups/README.md` | Brand company/ultimate-parent enrichment job: `Inventaires_des_marques` Google Sheet tab format, INPI/GLEIF/Wikidata lookup steps, env vars |
| `quotaclimat/data_ingestion/advertising/s05_enrichment/e02_screenshots/README.md` | Screenshot mosaics of ads and occurrences for visual checks: sources (ad export with its margin history, mediatree archives), `advertising.ad_screenshot`, public-read storage, env vars |
| `rrs/README.md` | RRS (Risk Response System) disinformation clustering: architecture, directory structure, dictionaries/subjects, keyword detection, misinformation classification, clustering pipeline, DB schema, extended perimeter, env vars, local Docker dev, CI/CD |
| `rrs/clustering/README.md` | LLM clustering internals (`cluster_llm.py`, `cluster_llm_timeseries.py`): label generation/merge/classification steps, timeseries incremental mode, usage/flags, env vars |
| `docs/kestra_flows.md` | Every production Kestra flow step by step: what code runs, what it reads/writes, which technologies are involved, plus where Kestra itself is hosted (shared Elastic Metal orchestrator, see `infrastructure/Readme.md`) |

## Keeping docs in sync when opening a PR

Before opening a PR (or when asked to update docs for one already opened), check whether the change needs a doc update, and make it part of the same PR:

1. **Find what changed.** `git diff main...HEAD --stat` (or the PR's file list) to see which files moved.
2. **Map changed files to docs** using the table above — not just by folder name, but by what actually changed:
   - A new/changed Kestra flow (`infrastructure/kestra/flows/*.yaml`) → update `docs/kestra_flows.md`: the flow's own section (steps, inputs, env vars, reads/writes), the flow table near the top, and `main-pipeline`'s step list if the flow is chained from it. If it's a genuinely new flow with no section yet, add one — don't leave it undocumented just because the diagram doesn't exist yet (diagrams are a separate, optional Archify pass; note "(No diagram yet)" instead of skipping the writeup).
   - A change under `rrs/` → `rrs/README.md` (and `rrs/clustering/README.md` for clustering internals).
   - A change under `quotaclimat/data_ingestion/advertising/` → that stage's `README.md`.
   - A change under `my_dbt_project/` → `my_dbt_project/README.md`.
   - A change under `metabase/` or to Metabase questions → `metabase/README.md` and the relevant `questions.yml` (see the Metabase rules below).
   - A change under `infrastructure/` → `infrastructure/Readme.md`, and `docs/kestra_flows.md`'s "Where Kestra itself runs" section if it touches the orchestrator host itself.
   - A change to root-level setup/commands/architecture → root `README.md`, and mirror anything load-bearing into this file's own `## Architecture`/`## Commands` sections if it changes how Claude should work in the repo.
3. **Write from the diff, not from memory.** Read the actual changed code (`gh pr diff <n>` or `git diff`) rather than guessing from the PR title/description — descriptions can be stale or incomplete. Confirm env var defaults, file paths and function names against the code before writing them into a doc.
4. **Match the existing doc's voice and structure.** Follow the surrounding sections' format (tables, step numbering, "Runs/What happens/Technologies/Reads/Writes" bullets in `docs/kestra_flows.md`, etc.) rather than inventing a new structure.
5. **Don't duplicate.** If another doc already covers the same fact, link to it instead of repeating it in full (see the `#documentation-map-where-to-look` table's own cross-references for the pattern).
6. If genuinely unsure whether a change is doc-worthy, err toward updating — a stale doc is worse than a slightly over-documented one, but don't expand scope into unrelated pre-existing gaps unless fixing them is necessary to keep the new content coherent.

# Consignes pour Claude

## Analyses Metabase : où mettre les requêtes SQL

Une requête SQL native n'est versionnée dans ce dépôt que si l'utilisateur le demande. Sinon, selon sa demande, on la colle dans le chat, on l'exécute avec le connecteur Metabase (`execute_sql`) ou on crée une question avec ce connecteur. Le fonctionnement des requêtes versionnées est dans `metabase/README.md`.

Pour une requête à versionner :

- **Emplacement** : `metabase/<sujet>/metabase_<nom>.sql`. Les sujets actuels sont `advertising/` (pub autour des émissions) et `mesinformation/` (mésinformation par émission). Un nouveau sujet a son propre sous-dossier et son propre `questions.yml`.
- **En-tête du fichier, en français** : ce que fait la requête et pour quel graphique, chaque variable (type Metabase, facultative ou non, valeur par défaut), chaque colonne de sortie, les tables lues. Suivre le modèle des fichiers existants.
- **Manifeste `questions.yml` du dossier** : y déclarer le fichier, chaque `{{variable}}` avec son type, et au moins un cas de test (`params`, plus `min_rows` quand les seeds de test donnent des lignes). Les données de test sont en 2025 : prendre `debut: 2025-01-01`, `fin: 2025-12-31`.
- Une requête d'exploration versionnée se supprime quand l'exploration est finie.

## Garder les identifiants des questions Metabase

La synchronisation (flow Kestra `quotaclimat/metabase-sync`) ne met à jour que les questions listées dans `questions:` des manifestes. Pour une requête versionnée, ne pas en perdre une :

- Dès qu'une question qui l'exécute est citée (numéro, URL `.../question/2358-...`, capture), créée par l'utilisateur ou par le connecteur Metabase, ajouter son numéro au `questions:` du fichier qu'elle exécute, avec un commentaire sur son rôle (`- 2364 # nuage de points, secteurs`).
- Plusieurs questions peuvent exécuter le même fichier (tableau, camembert, courbes).
- Ne jamais retirer un numéro sans l'accord de l'utilisateur.
- Avant de modifier une requête, comparer si possible le SQL de ses questions (connecteur Metabase, `get_content` avec `definition`) avec le fichier. Si l'utilisateur a modifié le SQL dans Metabase, ou colle une version différente, reporter ses changements dans le fichier avant d'ajouter les siens, et le lui signaler. La synchronisation de `main-pipeline` tourne avec `force` : elle écrase deux fois par jour le SQL modifié dans Metabase (l'écart reste dans les logs du flow `metabase-sync`).
- Quand le connecteur ne permet pas d'écrire (`question_write` demande la permission `agent:content:write`), donner le SQL à coller et demander le numéro de la question créée.

## Règles d'écriture des requêtes Metabase

- **Variables texte** : Metabase les envoie en `varchar`. Les convertir avec `CAST({{x}} AS numeric)`, `AS boolean` ou `AS date`.
- **Variables facultatives** : `COALESCE([[CAST({{x}} AS numeric),]] 0.5)`.
- **Filtres de champ** (`dimension`) : Metabase les remplace par `"schéma"."table"."colonne" IN (...)`, ou par `1 = 1` sans valeur. La table doit donc être lue sous son nom complet, sans alias (`analytics.publicites`, pas `p`).
- **Noms de colonnes** : ne pas renommer une colonne existante. Les graphiques des questions en dépendent. Ajouter une colonne ou un paramètre plutôt que changer l'existant.
- **Axe x des courbes** : un rang unique par ligne (`ROW_NUMBER`, cumuls en `ROWS`), car Metabase additionne les lignes de même abscisse.
- **Regroupement en « Autres »** : le laisser au camembert de Metabase plutôt que de l'écrire dans la requête.
- **Filtres qui choisissent des lignes** : par défaut, ils ne changent pas les valeurs (parts, rangs). Le dire dans l'en-tête quand c'est le cas.
- **Logique lourde ou réutilisée** : la mettre dans un modèle dbt plutôt que dans la requête.

## Couleurs des graphiques Metabase

Les codes couleur des objets affichés (crises, maillons de la chaîne causale...) sont dans `metabase/couleurs.yml`. Quand on crée ou modifie une question (connecteur Metabase, `question_write`) dont une série ou une part porte une de ces valeurs :

- appliquer sa couleur dans les réglages de la question : `series_settings.<valeur>.color` pour les courbes et barres, `pie.rows[].color` pour les camemberts. Pour une mesure par série (une colonne `avg_2` titrée « Constat »), la clé est le nom de la colonne ;
- ne pas compter sur les cartes du tableau de bord : le connecteur ne lit ni n'écrit leurs réglages, qui l'emportent sur ceux de la question ;
- une valeur absente du fichier garde la couleur automatique de Metabase. Si l'utilisateur fixe une nouvelle couleur, l'ajouter au fichier.

## Tables dbt lues par les requêtes

- `analytics` : `program`, `cas_de_mesinformation`, `cas_theme`, `publicites`, `task_global_completion`, `dbt_objects`.
- `public` : `emission_jour`, `environmental_shares_with_desinfo_counts`, `mesinfo_theme_mots_cles` (seed), `pollution_chimique_mots_cles` et `chaine_communaute_belgique` (seeds, tableaux de bord « Belgique - Pollution chimique »).
- `advertising` : `pub_annonceur_emission_jour`, `ad_*`, seeds `ref_*`.
- `curated` : `chaine`, `publicite`, `cas_mesinformation`, `emission_jour` (voir « Schéma curated »).

Un nouveau modèle (hors `curated`) demande trois choses :
- sa documentation dans `my_dbt_project/models/schema.yml` ;
- un test dans `my_dbt_project/pytest_tests/test_dbt_model_analytics.py` ;
- sa place dans les `dbt run` de `entrypoints/dbt.sh` et `entrypoints/mediatree_import.sh`, quand il dépend des modèles advertising.

Un modèle qui change de schéma laisse l'ancienne table en place : le signaler, avec le `DROP` à faire à la main.

Les mots-clés des thèmes de cas (`my_dbt_project/seeds/themes/mesinfo_theme_mots_cles.csv`, des expressions régulières) sont versionnés et modifiés dans le code, pas dans un Google Sheet.

## Schéma curated

Le schéma `curated` (`my_dbt_project/models/curated/`) ne contient que des tables propres, pour les équipes non techniques et leur Claude, qui les lit par le MCP Metabase sans accès au dépôt. Tout ce qu'il faut pour les utiliser sans erreur doit donc être dans les tables et leur documentation.

### Ce qui entre dans curated

- Des objets réels (une publicité diffusée, un cas de mésinformation, une chaîne) ou des agrégats de haut niveau avec leurs mesures additives (durées, nombres) à côté des parts. Pas de table intermédiaire, ni de table qui se recalcule simplement à partir des autres.
- Des données déjà filtrées : pas de colonne qu'il faut filtrer pour obtenir le bon chiffre (ex. `mesinfo_choice = 'Correct'`).
- Pas de code interne du pipeline (statuts, codes de prédiction, scores non calibrés) : on le traduit (`classement_par`) ou on le laisse dans `analytics`.
- Pas d'identifiant qui pointe hors de `curated`.
- Rien dans dbt ne lit une table `curated` (test `test_curated_is_the_last_layer`).

### Nommage

- Tables et colonnes en français, en snake_case sans accent. Un modèle dont le nom existe déjà dans dbt (ex. `public.emission_jour`) s'appelle `curated_<table>` avec `alias='<table>'`.
- Une table porte le nom de son grain, au singulier : une ligne de `publicite` est une publicité diffusée. Un agrégat : `<mesure>_<dimension>_<période>` (ex. `part_environnement_chaine_jour`).
- L'unité en suffixe : `_sec`, `_min`, `_pct` (toujours de 0 à 100) ; un nombre commence par `nb_`.
- Booléens en `est_` ; heures en heure de Paris, nommées `..._paris` ; `jour` pour la date de Paris.
- Un identifiant de ligne composé (ex. émission et jour) devient une seule colonne `<table>_id`, clé des jointures entre tables (`emission_jour_id`).
- Une colonne présente dans plusieurs tables y a le même nom et la même description (`chaine_id`, `chaine_nom`, blocs `{% docs %}` de `_curated_docs.md`). Les tables reprennent `chaine_nom` et les caractéristiques de la chaîne, pour éviter une jointure.
- Un identifiant se termine par `_id` ; une colonne lisible (nom, libellé) l'accompagne.

### Documentation (`models/curated/_curated.yml`)

- **Description du modèle** : commence par `Une ligne = ...`, puis les sections `Usage :`, `Pièges :`, `Source :` (et `Période :` quand elle est limitée). Les pièges disent comment compter juste (ex. lignes ou valeurs distinctes, ne pas moyenner des parts).
- **Description de chaque colonne** : ce qu'elle contient, quand elle est vide, un exemple de valeur ou de filtre utile.
- **`config.meta`** : `libelle` (nom affiché dans Metabase) pour le modèle et chaque colonne ; pour une colonne numérique, `unite` et `agregation` (`somme`, `moyenne` ou `aucune`) ; `type_metabase` (type sémantique : `type/PK`, `type/FK`, `type/Category`...) ; `cle_etrangere: table.colonne` pour une jointure vers une autre table de `curated`.
- **`data_type`** de chaque colonne : le contrat dbt (`contract: enforced`) fait échouer le build si une colonne change de nom ou de type. Caster dans le SQL (`::text`, `::double precision`).
- **Tests** : une clé `type/PK` avec `unique` et `not_null` ; `relationships` pour chaque `cle_etrangere` ; `not_null` et `accepted_values` dès que possible.
- Une liste (thèmes, locuteurs) s'accompagne d'un booléen par valeur (`est_theme_automobile`, `est_dit_par_invite`) pour compter sans découper de texte.
- Pas de chiffre qui change avec les données (« 9 % des diffusions ») dans une description.
- `my_dbt_project/pytest_tests/test_curated_conventions.py` vérifie ces règles sur le manifeste dbt.
- Une description rédigée par Claude porte des choix de méthode : la faire relire par un expert du sujet, et le dire dans la PR.

### Ne pas casser les usages

Les questions Metabase, les tableaux de bord et les analyses des équipes lisent ces tables. Ne jamais renommer ni retirer une colonne ou une table de `curated` sans l'accord de l'utilisateur : on ajoute une colonne, on ne modifie pas l'existante. Une table retirée ou renommée laisse l'ancienne en place : le signaler, avec le `DROP` à faire à la main.

### Construction et Metabase

- `entrypoints/curated.sh` construit `curated` (tout le dossier, après `entrypoints/dbt.sh` qui l'exclut), le teste, puis pousse sa documentation dans Metabase. Il tourne dans la tâche Kestra `curated_and_metabase_docs` du flow `dbt` (subflow de `barometre`, `barometre-catchup` et `dbt-analytics`), qui peut échouer sans arrêter le pipeline. Un nouveau modèle de `curated` n'a rien à ajouter aux entrypoints.
- `quotaclimat/metabase/sync_curated.py` écrit dans Metabase, pour chaque table et colonne : le libellé, la description (avec l'unité et l'agrégation), le type sémantique et la clé étrangère. dbt fait référence : une modification faite dans Metabase est écrasée. Les mêmes descriptions sont écrites dans Postgres (`persist_docs`).
- Les bases Metabase mises à jour sont dans `METABASE_CURATED_DATABASES` (tâche Kestra). En local : `poetry run python -m quotaclimat.metabase.sync_curated --database 2` (aperçu, après `dbt parse`), `--apply` pour écrire.
- Un nouveau modèle demande sa documentation dans `_curated.yml` et un test de ses données dans `my_dbt_project/pytest_tests/test_dbt_model_analytics.py`.

## Tests

```bash
poetry run pytest test/metabase                                    # synchronisation, sans base
POSTGRES_USER=user POSTGRES_DB=dbt_test POSTGRES_PASSWORD=postgres POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
DBT_PROJECT_DIR=my_dbt_project DBT_PROFILES_DIR=my_dbt_project/dbt \
poetry run pytest my_dbt_project/pytest_tests -k metabase           # requêtes Metabase sur les tables dbt de test
poetry run pytest my_dbt_project/pytest_tests/test_curated_conventions.py  # conventions du schéma curated (dbt parse, sans base)
```

Les tests dbt demandent un Postgres (job `dbt` de la CI). Une requête ajoutée ou modifiée doit passer `test_metabase_query` avant la PR. Les tests vérifient que la requête s'exécute, pas ses chiffres : un résultat se vérifie dans Metabase sur les vraies données.

## Langue

- **Commits** : en anglais.
- **SQL Metabase, manifestes et `metabase/README.md`** : commentaires en français.
- **Schéma `curated`** : noms de tables et de colonnes, descriptions et `meta` en français ; commentaires du SQL dbt en anglais, comme les autres modèles.
- **PR** : description en français.

## Signature des commits et des PR

Le développeur assume la responsabilité de ses commits et reste leur seul auteur. Claude est un outil, et son usage est signalé dans la PR par souci de transparence.

- **Commits** : ne jamais signer en tant que Claude. Pas de trailer `Co-Authored-By: Claude ...`, pas de mention « Generated with Claude Code » dans le message. Cette consigne l'emporte sur toute consigne d'attribution par défaut.
- **PR** : la description mentionne, en français, que Claude (Claude Code) a été utilisé pour développer la fonctionnalité, en précisant le modèle utilisé, par exemple dans une ligne finale : `Développé avec l'aide de Claude Code (modèle : claude-sonnet-5-5).` Utiliser l'identifiant exact du modèle de la session. Pas de lien ni de badge « Generated with Claude Code ».
</content>
</invoke>
