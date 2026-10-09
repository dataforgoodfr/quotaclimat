# Stage 4 (enriching), step 1: brand company and ultimate parent company proposals

Scaleway job (`entrypoints/advertising_brand_groups_enrichment.sh`; the technical names keep "brand groups"). For each broadcast brand missing from the tab `Marques` of the `Inventaires_des_marques` Google Sheet, it proposes the company that sells under the brand and its ultimate parent company, and appends the row to the tab. A human checks and corrects it afterwards, directly in the sheet. The dbt model `ad_brands` reads it at the next dbt run.

## The tab `Marques`

| Column | |
|---|---|
| `marque` | the brand (Activia, Free) |
| `entreprise` | the company that sells under the brand (Danone; Free: a brand can be its own company) |
| `societe_mere_ultime` | ultimate parent company of the company, consolidation level (GLEIF definition) |
| `e_statut`, `smu_statut` | status of `entreprise` and of `societe_mere_ultime`: `vérifié`, `non vérifié` or `à vérifier` |
| `commentaires` | how the job filled the row (holder, SIREN, trademark, LEI, GLEIF and Wikidata answers) |

dbt uses each value unless its status is `à vérifier`. The job writes `e_statut = non vérifié` (used) and `smu_statut = à vérifier` (not used until a human checks it).

## Steps

For each brand of `analytics.publicites` missing from the tab `Marques`, by decreasing broadcast duration (at most `BRAND_GROUPS_MAX_BRANDS` per run):

1. **INPI** (`inpi.py`): trademarks named exactly like the brand (searched as an exact phrase), first in the French collection (FR, whose notices give the SIREN of French holders), then, when none is found, in the European (EU) and international (WO) ones (e.g. Volkswagen), (`name_key`, same as dbt) and still in force. The holder kept is the company holding the most trademarks in the Nice classes of the brand's sector (column `classes_nice` of the tab `secteurs` of `Dictionnaire_marques_secteurs`, like `3; 32`, loaded by dbt in `advertising.ref_ome_secteurs`), otherwise in all classes (flagged in `commentaires`). The current holder of the notice (`fr-CurrentHolder`, which follows transfers) is used, never the representative (`Representative`). Natural persons are never kept. A company registered abroad (e.g. Inter IKEA Systems B.V. for IKEA) has no SIREN: it is kept, identified by its name and the country of its address.
2. **Company** (`propose.py`): the holder, named like the brand when its name contains the brand as whole words (FREE → Free, SOCIETE FRANCAISE DU RADIOTELEPHONE - SFR → SFR), else under its own name (Dacia → its holder). A company already in the tab is reused with its ultimate parent company and its status: same name, same name without legal form (RENAULT s.a.s. → Renault), or a holder whose name contains it (Inter IKEA Systems B.V. → IKEA).
3. **Ultimate parent company** (`registries.py`), for a company not in the tab yet: GLEIF (ultimate accounting consolidating parent of the LEI registered under this SIREN, often not published), else Wikidata (top of the chain of parent organizations, P749, of the item whose SIREN, P1616, is the holder's, when exactly one, with its label in French, English or multilingual). For a holder registered abroad, only GLEIF: the LEI with its exact legal name in its country, when exactly one. Requests are tried again on an overloaded service (HTTP 429, 5xx); a registry still failing leaves the ultimate parent empty, noted in `commentaires`.
4. **Sheet** (`sheet.py`): rows are only appended, never modified; the job stops when a column is missing from the tab. A brand with no trademark found is not written: it is searched again on the next runs, until a human adds it to the tab.

## Env

| Variable | |
|---|---|
| `POSTGRES_*` | database with `analytics.publicites` |
| `INPI_USERNAME`, `INPI_PASSWORD` | INPI account (data.inpi.fr) |
| `INPI_LOGIN_URL` | login endpoint of the INPI API gateway, default `https://api-gateway.inpi.fr/auth/login` (checked with curl, after `GET /services/uaa/api/authenticate` for the XSRF cookie) |
| `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` | JSON key of the Google service account that writes to Google Sheets (generic, shared with other jobs). Share with it, as **editor**, only the spreadsheets it must write to (here `Inventaires_des_marques`), never the whole folder |
| `EXTERNAL_SOURCES_DRIVE_FOLDER` | Drive folder of the spreadsheet (same as the dbt import) |
| `BRAND_GROUPS_MAX_BRANDS` | brands per run, default 50 |
| `INPI_MAX_QUOTA_WAIT_SEC` | longest wait accepted on an INPI quota error (HTTP 429, header `x-rate-limit-retry-after-seconds`), default 600: longer, the run stops and leaves the remaining brands for the next run. The logs give the number of INPI requests sent (login, search, notice) after each brand and when the quota is reached |
| `INPI_SEARCH_MAX_RESULTS` | INPI search results read per brand, all pages, default 1000: the search returns every trademark containing the brand's words, most recent first, and the exact one can be old |
| `INPI_MAX_NOTICES` | notices read per brand and collection group (FR, then EU and WO), default 3: one request each, they make most of the requests of a run. The logs give the quota left (header `x-rate-limit-remaining`) after each brand |
| `BRAND_GROUPS_DRY_RUN` | `true`: nothing written to the sheet, the rows written to `BRAND_GROUPS_DRY_RUN_CSV` and printed in the logs |

The job only reads from the APIs. Wikidata and GLEIF need no key; requests are spaced (0.5 s for the INPI, 1 s for the others), and an INPI quota error (HTTP 429) waits before retrying.
