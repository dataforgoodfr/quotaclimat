# Stage 4: brand group proposals

Scaleway job (`entrypoints/advertising_brand_groups.sh`). It proposes the group of the broadcast brands that are missing from the tab `Marques` of the `Inventaires_des_marques` Google Sheet, and appends them to this tab with `statut = non vérifié`. A human checks and corrects them afterwards in the sheet. The dbt model `ad_brands` reads them at the next dbt run.

## Steps

For each brand of `advertising.ad_occurrences_classified` missing from the tab `Marques`, by decreasing broadcast duration (at most `BRAND_GROUPS_MAX_BRANDS` per run):

1. **INPI** (`inpi.py`): French trademarks (collection FR, whose notices give the SIREN) named exactly like the brand (`name_key`, same as dbt) and still in force. The holder kept is the company holding the most trademarks in the Nice classes of the brand's sector (column `classes_nice` of the tab `secteurs` of `Dictionnaire_marques_secteurs`, like `3; 32`, loaded by dbt in `advertising.ref_ome_secteurs`), otherwise in all classes. The current holder of the notice (`fr-CurrentHolder`, which follows transfers) is used, never the representative (`Representative`). Natural persons are never kept.
2. **Parent company** (`registries.py`): Wikidata (P749 of the item whose SIREN, P1616, is the holder's), else GLEIF (direct parent of the LEI registered under this SIREN, often not published), else the holder itself.
3. **Group** (`propose.py`): a candidate already in the tab `Groupes` (its canonical name), else the first candidate above.
4. **Sheet** (`sheet.py`): rows are only appended, never modified, and only under the columns that exist in the tab. `commentaire` keeps the trace (holder, SIREN, trademark, Wikidata and GLEIF answers, other holders). The optional columns `titulaire`, `siren`, `lei` and `numero_marque` are filled when they exist in the tab. A brand with no trademark found is appended with an empty group, so that it is not searched again.

## Env

| Variable | |
|---|---|
| `POSTGRES_*` | database with `advertising.ad_occurrences_classified` |
| `INPI_USERNAME`, `INPI_PASSWORD` | INPI account (data.inpi.fr) |
| `INPI_LOGIN_URL` | login endpoint of the INPI API gateway, default `https://api-gateway.inpi.fr/auth/login` (checked with curl, after `GET /services/uaa/api/authenticate` for the XSRF cookie) |
| `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` | JSON key of the Google service account that writes to Google Sheets (generic, shared with other jobs). Share with it, as **editor**, only the spreadsheets it must write to (here `Inventaires_des_marques`), never the whole folder |
| `EXTERNAL_SOURCES_DRIVE_FOLDER` | Drive folder of the spreadsheet (same as the dbt import) |
| `BRAND_GROUPS_MAX_BRANDS` | brands per run, default 50 |
| `BRAND_GROUPS_DRY_RUN` | `true`: the rows are written to `BRAND_GROUPS_DRY_RUN_CSV` instead of the sheet |

The job only reads from the APIs. Wikidata and GLEIF need no key; requests are spaced (0.5 s for the INPI, 1 s for the others), and an INPI quota error (HTTP 429) waits before retrying.
