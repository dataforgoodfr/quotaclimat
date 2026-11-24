# Advertising pipeline diagrams

Two [Archify](https://github.com/tt-a1i/archify) `workflow` diagrams (3.3.0), in French, drawn from the code of
`quotaclimat/data_ingestion/advertising/` at the commit pinned in `meta.repository` of each JSON source. Each node
lists the lines it was drawn from (`sources`), and its details (`explanation`) open from the node in the HTML.

| Diagram | Shows |
|---|---|
| [`chaine_publicite.html`](chaine_publicite.html) | Run order of the advertising jobs, from the detection to dbt: why the exportation, the classification and the merge of split ads run twice (second pass on the merged ads left without classification and on what the manual corrections send back), the Google Sheets read by each step, the brand inventory loop with dbt. |
| [`recalcul_categories.html`](recalcul_categories.html) | What a change of the brand dictionary recomputes (`e3_dict_match` `FULL_RERUN=1`, `e4_classify_subcat`, `e5_canonicalise_brand` `RECANONICALISE=1`, see `s03_classification/sector_and_brand/README.md`) and the ads it does not reach: merged ads, ads corrected by hand, ads out of the classification pool (`CLASSIFICATION_START_DATE`). Also the second copy of the dictionary, read by the merge from the dbt seeds. |

The order shown is the one the code dependencies require: the advertising Scaleway jobs are not chained by an
orchestrator, and their schedules are not in the repository.

## Updating

Edit the JSON source, then render it from the repo root with the Archify skill package:

```bash
node <archify>/bin/archify.mjs finalize workflow docs/diagrams/advertising/<diagram>.archify.json docs/diagrams/advertising/<diagram>.html --repo-root . --quality showcase --json
```

`meta.translations` holds the French strings of the Viewer (from Archify's `examples/locales/fr.json`, plus the keys
it lacks). Keep only the `.archify.json` and `.html` files: delete the receipts (`*.finalize*.json`,
`*.delivery*.json`, `*.browser-check.json`) the command writes next to them.
