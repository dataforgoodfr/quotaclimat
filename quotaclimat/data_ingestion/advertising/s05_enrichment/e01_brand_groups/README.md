# Stage 5 (enriching), step 1: brand company and ultimate parent company proposals

Scaleway job (`entrypoints/ad_s05e01_brand_groups_enrichment.sh`; the technical names keep "brand groups"). For each broadcast brand missing from the tab `Marques` of the `Inventaire_des_marques` Google Sheet, it proposes the company that sells under the brand and its ultimate parent company, each with the country, SIREN and department of its head office, and appends the row to the tab. A human checks and corrects it afterwards, directly in the sheet. The dbt model `ad_brands` reads it at the next dbt run. The job then gives the rows of the same company the same values, the highest row of the tab winning (see [Uniformization of the rows of a company](#uniformization-of-the-rows-of-a-company)). With `BRAND_GROUPS_COMPLETE_ROWS=true`, it then fills the empty cells of the rows already in the tab (see [Completion of the rows of the tab](#completion-of-the-rows-of-the-tab)).

## The tab `Marques`

| Column (any order in the tab) | |
|---|---|
| `marque` | the brand (Activia, Free) |
| `entreprise` | the company that operates the brand (Citroën: Automobiles Citroën; Free: a brand can be its own company), never its group: the group goes in `societe_mere_ultime` |
| `pays_siege_social` | country of the head office of the company, ISO 3166-1 alpha-2 code (`FR`, `NL`) |
| `siren` | SIREN of the company, 9 digits, empty when unknown. A company registered abroad can have one, when it is also registered in France (Stellantis N.V.: 879786085): keep it. Format the column as plain text in the sheet: typed as a number, a SIREN loses its leading zeros (dbt and the job put them back) |
| `departement_siege_social` | department of the head office of the company (`75`, `2A`, `971`), empty for a company whose head office is abroad |
| `societe_mere_ultime` | ultimate parent company of the company, consolidation level (GLEIF definition): the level that groups the companies of a group (Automobiles Citroën, Opel, FCA: STELLANTIS N.V.). Write it the same way on every row of the group, and on the rows of the parent company itself |
| `smu_pays`, `smu_siren`, `smu_departement` | country, SIREN and department of the head office of the ultimate parent company, like the three columns of the company |
| `actionnaire_maj_ultime` | ultimate majority shareholder (e.g. a family holding), filled in by a human only |
| `e_statut`, `smu_statut`, `amu_statut` | status of `entreprise`, `societe_mere_ultime` and `actionnaire_maj_ultime`: `vérifié`, `non vérifié` or `à vérifier` |
| `commentaires` | how the job filled the row (holder and its SIREN, trademark, LEI, GLEIF and Wikidata answers) |

dbt uses each value unless its status is `à vérifier`. The job writes `e_statut = non vérifié` (used) and `smu_statut = à vérifier` (not used until a human checks it) for an ultimate parent it found. When it finds nothing (no trademark, no ultimate parent), it leaves the value and its status empty.

## Steps

For each brand of `analytics.publicites` missing from the tab `Marques`, by decreasing broadcast duration (at most `BRAND_GROUPS_MAX_BRANDS` per run):

1. **INPI** (`inpi.py`): trademarks named exactly like the brand (searched as an exact phrase), first in the French collection (FR, whose notices give the SIREN of French holders), then, when none is found, in the European (EU) and international (WO) ones (e.g. Volkswagen), (`name_key`, same as dbt) and still in force. The holder kept is the company holding the most trademarks in the Nice classes of the brand's sector (column `classes_nice` of the tab `secteurs` of `Dictionnaire_marques_secteurs`, like `3; 32`, loaded by dbt in `advertising.ref_ome_secteurs`), otherwise in all classes (flagged in `commentaires`). The current holder of the notice (`fr-CurrentHolder`, which follows transfers) is used, never the representative (`Representative`). Natural persons are never kept. A notice gives no SIREN for a holder registered abroad (e.g. Inter IKEA Systems B.V. for IKEA): it is kept, identified by its name and the country of its address, and its SIREN, when it has one in France, is left to a human.
2. **Company** (`propose.py`): the holder, named like the brand when its name contains the brand as whole words (FREE → Free, SOCIETE FRANCAISE DU RADIOTELEPHONE - SFR → SFR), else under its own name (Dacia → its holder). A company already in the tab is reused with its SIREN, head office department, ultimate parent company and its status: same name, same name without legal form (RENAULT s.a.s. → Renault), or a holder whose name contains it (Inter IKEA Systems B.V. → IKEA).
   - **Country and SIREN**: the holder's for a new company (`FR` for a holder with a SIREN). For a company already in the tab, the ones of the tab (from any of its rows); when the tab has none, they are left empty and `commentaires` says to complete the SIREN: the holder can be another legal entity than the company the tab names (a subsidiary, a holding).
3. **Ultimate parent company** (`registries.py`), for a company not in the tab yet: GLEIF (ultimate accounting consolidating parent of the LEI registered under this SIREN, often not published; GLEIF stores a SIREN with or without spaces, `552 081 317` for EDF, both are searched), else Wikidata (top of the chain of parent organizations, P749, of the item whose SIREN, P1616, is the holder's, when exactly one, with its label in French, English or multilingual). For a holder registered abroad, only GLEIF: the LEI with its exact legal name in its country, when exactly one. Its country and SIREN: the country of the legal address and the registration number (a SIREN when registered in France) of its GLEIF record, or the country (P17) and SIREN (P1616) of its Wikidata item; else the ones the tab gives for this parent (another row, or the company of that name). Requests are tried again on an overloaded service (HTTP 429, 5xx); a registry still failing leaves the ultimate parent empty, noted in `commentaires`.
4. **Head office departments** (`registries.py`), of the company and of its ultimate parent, unless the tab already gives them: API Recherche d'entreprises (`https://recherche-entreprises.api.gouv.fr/search?q=<siren>`, Sirene data of the INSEE, no key), field `siege.departement` of the result with exactly this SIREN. A failing lookup leaves it empty, noted in `commentaires`.
5. **Sheet** (`sheet.py`): new rows are appended; the job stops when a column is missing from the tab. A brand with no trademark found is written too, without company nor status (`commentaires`: no trademark in force at the INPI), so that it is not searched again, spending the INPI quota, on every run. dbt treats it like a brand missing from the tab (company: the brand itself) until a human fills in its company. A brand whose search failed (API error) is not written: it is searched again on the next run.

## Uniformization of the rows of a company

Unless `BRAND_GROUPS_UNIFORMIZE_ROWS=false`, after the new brands and before the completion, the job gives the rows of the same company (same `entreprise`, ignoring case, accents and punctuation: `Danone` = `DANONE`; `Renault` and `Renault SA` stay two companies) the same values (`uniformize.py`), so that an ultimate parent company or an ultimate majority shareholder written on one row of the company is on all of them. **On a conflict, the highest row of the tab wins**: to change a value of a company, change it on its highest row. Three blocks of columns, each taken as a whole, so that a value is never written with the identity of another one:

1. `pays_siege_social`, `siren`, `departement_siege_social`: the ones of the highest row with a SIREN or a country; its department, when that row has none, the one of the highest row with the same SIREN;
2. `societe_mere_ultime`: the one of the highest row with an ultimate parent used by dbt (`smu_statut` other than `à vérifier`), else of the highest row with one `à vérifier`: a proposal of the job not checked yet does not replace a parent used by dbt. Its `smu_statut`: the best one of the rows with this parent (`vérifié`, then `non vérifié` or empty, then `à vérifier`), so that a parent verified on a lower row stays verified. Its `smu_pays`, `smu_siren`, `smu_departement`: as in 1, among the rows with this parent; emptied on the other rows when none gives them (they were the identity of another parent);
3. `actionnaire_maj_ultime` and `amu_statut`: as the ultimate parent.

A block no row of the company fills is left as it is, and so are a company on a single row and the rows without company. Unlike the completion, the step **overwrites** cells, human values included: each changed row gets in `commentaires` the changed columns, the line their new value comes from and their previous value (`job 2026-11-07 : valeurs de l'entreprise Free uniformisées sur la ligne la plus haute qui les donne : societe_mere_ultime Iliad (ligne 2, avant : Xavier Niel)`), to undo a change by hand. The tab is read again just before writing: a row whose brand moved or whose cells were edited in the meantime is left as it is. No API is called. With `BRAND_GROUPS_DRY_RUN=true`, nothing is written, the changes are printed between two markers.

## Completion of the rows of the tab

With `BRAND_GROUPS_COMPLETE_ROWS=true` (on a manual run of the Scaleway job, for instance), after the new brands, the job goes through every row of the tab with a company (`complete.py`) and fills its **empty cells only**, never a value written by a human or by the job:

1. `pays_siege_social`, `siren`: from another row of the same company, else from the INPI holder the job wrote in `commentaires`, when the company is that holder (same name without legal form, or the brand named after the holder). For a company corrected by a human, nothing: the holder can be another legal entity;
2. `departement_siege_social`: API Recherche d'entreprises, from the SIREN;
3. `societe_mere_ultime`, for a row with neither ultimate parent nor `smu_statut` (clear `à vérifier` on a row without ultimate parent to have it searched again): the one of another row of the same company, else GLEIF and Wikidata as in step 3 above, from the SIREN of the row, written `à vérifier`;
4. `smu_pays`, `smu_siren`: from the tab (another row with the same ultimate parent, or the row of the company of that name), else from the LEI or the Wikidata item the job wrote in `commentaires` for this ultimate parent, when the row still has that one;
5. `smu_departement`: API Recherche d'entreprises.

INPI is never called, so the step spends none of its quota. The values are written at the end of the step, after reading the tab again: a row whose brand moved or a cell filled in by a human in the meantime is left as it is. The step stops after `BRAND_GROUPS_COMPLETE_MAX_REQUESTS` registry requests (about one second each), the next rows being completed on the next run. Run it with `BRAND_GROUPS_MAX_BRANDS=0` to complete the rows without proposing new brands. Each completed row is in the logs; with `BRAND_GROUPS_DRY_RUN=true`, nothing is written, the cells to fill are printed between two markers.

## Env

| Variable | |
|---|---|
| `POSTGRES_*` | database with `analytics.publicites` |
| `INPI_USERNAME`, `INPI_PASSWORD` | INPI account (data.inpi.fr) |
| `INPI_LOGIN_URL` | login endpoint of the INPI API gateway, default `https://api-gateway.inpi.fr/auth/login` (checked with curl, after `GET /services/uaa/api/authenticate` for the XSRF cookie) |
| `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` | JSON key of the Google service account that writes to Google Sheets (generic, shared with other jobs). Share with it, as **editor**, only the spreadsheets it must write to (here `Inventaire_des_marques`), never the whole folder |
| `EXTERNAL_SOURCES_DRIVE_FOLDER` | Drive folder of the spreadsheet (same as the dbt import) |
| `BRAND_GROUPS_MAX_BRANDS` | brands per run, default 50 |
| `INPI_MAX_QUOTA_WAIT_SEC` | longest wait accepted on an INPI quota error (HTTP 429, header `x-rate-limit-retry-after-seconds`), default 600: longer, the run stops and leaves the remaining brands for the next run. The logs give the number of INPI requests sent (login, search, notice) after each brand and when the quota is reached |
| `INPI_SEARCH_MAX_RESULTS` | INPI search results read per brand, all pages, default 1000: the search returns every trademark containing the brand's words, most recent first, and the exact one can be old |
| `INPI_MAX_NOTICES` | notices read per brand and collection group (FR, then EU and WO), default 3: one request each, they make most of the requests of a run. The logs give the quota left (header `x-rate-limit-remaining`) after each brand |
| `BRAND_GROUPS_DRY_RUN` | `true`: nothing written to the sheet, the rows written to `BRAND_GROUPS_DRY_RUN_CSV` and printed in the logs |
| `BRAND_GROUPS_UNIFORMIZE_ROWS` | `false`: no uniformization of the rows of a company, default `true` |
| `BRAND_GROUPS_COMPLETE_ROWS` | `true`: completion of the rows of the tab after the new brands, default `false` |
| `BRAND_GROUPS_COMPLETE_MAX_REQUESTS` | registry requests (GLEIF, Wikidata, Recherche d'entreprises) of the completion step, default 1000 |

The job only reads from the APIs. Wikidata, GLEIF and Recherche d'entreprises need no key; requests are spaced (0.5 s for the INPI, 1 s for the others), and an INPI quota error (HTTP 429) waits before retrying. The run stops, keeping the rows found so far, when the INPI quota would need a longer wait than `INPI_MAX_QUOTA_WAIT_SEC` or when the gateway refuses the method itself (HTTP 405 on the documented `POST /search`): every following brand would fail too. This stop is logged as an error (a Sentry event) only when the run processed no brand at all, as the quota (about 100 requests an hour) stops most runs.
