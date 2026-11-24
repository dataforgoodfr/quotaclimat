# Advertising pipeline diagrams

Four [Archify](https://github.com/tt-a1i/archify) diagrams (3.3.0), in French, drawn from the code of
`quotaclimat/data_ingestion/advertising/` at the commit pinned in `meta.repository` of each JSON source. Each node
lists the lines it was drawn from (`sources`), and its details (`explanation`) open from the node in the HTML.

| Diagram | Shows |
|---|---|
| [`chaine_publicite.html`](chaine_publicite.html) | Run order of the advertising jobs, from the detection to dbt, in three passes: detection, exportation and classification; the merge of split ads, which opens a second pass (exportation and classification of the merged ads left without classification, next merge run retrying the inheritance); the optional manual corrections, with the classification they may need before dbt. Also the inputs of each step and the brand inventory loop with dbt. |
| [`vie_pub_fusionnee.html`](vie_pub_fusionnee.html) | `lifecycle` diagram of a merged ad: inherits the classification of its pieces (retried by every merge run), or is exported and classified on its own video, whichever comes first; the `no_data` trap when the classification runs before the exportation; reverted by a manual fusion. |
| [`corrections_manuelles.html`](corrections_manuelles.html) | The optional manual corrections (`s04_consolidation/e02_manual_corrections`): for each tab, the flows to run afterwards (classification, dbt) and the optional ones (merge over the corrected period, screenshots). |
| [`recalcul_categories.html`](recalcul_categories.html) | What a change of the brand dictionary recomputes (`e3_dict_match` `FULL_RERUN=1`, `e4_classify_subcat`, `e5_canonicalise_brand` `RECANONICALISE=1`, see `s03_classification/sector_and_brand/README.md`) and the ads it does not reach: merged ads, ads corrected by hand, ads out of the classification pool (`CLASSIFICATION_START_DATE`). Also the second copy of the dictionary, read by the merge from the dbt seeds. |

The order shown is the one the code dependencies require: the advertising Scaleway jobs are not chained by an
orchestrator, and their schedules are not in the repository.

## Updating

Edit the JSON source, then render it from the repo root with the Archify skill package (`<type>`: the `diagram_type` of the JSON, `workflow` or `lifecycle`):

```bash
node <archify>/bin/archify.mjs finalize <type> docs/diagrams/advertising/<diagram>.archify.json docs/diagrams/advertising/<diagram>.html --repo-root . --quality showcase --json
```

`meta.translations` holds the French strings of the Viewer (from Archify's `examples/locales/fr.json`, plus the keys
it lacks). Keep only the `.archify.json` and `.html` files: delete the receipts (`*.finalize*.json`,
`*.delivery*.json`, `*.browser-check.json`) the command writes next to them.
