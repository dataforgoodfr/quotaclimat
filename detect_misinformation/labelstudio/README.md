# Label Studio interfaces

Labelling interfaces (Label Studio XML configs) of the misinformation review projects, migrated from `climateguard/deployments/labelstudio`. The tasks they display are created by the `label_misinformation` job (`../jobs/label_misinformation`), whose output fields (`channel_info`, `program_info`, `model_result`, `text_to_review_misinformation`, `audio`...) these interfaces read.

| File | Project | Content |
|---|---|---|
| `interface.xml` | Every country except France | Misinformation review: span labelling, misinformation choice, correction, speaker, salience, claims, explanation, references, comments |
| `france.interface.xml` | France | Same, plus CARDS narrative labels, "emblematic for a narrative", double annotation and its comments |

The interfaces are not deployed automatically: paste the XML in the project's *Settings → Labeling Interface* in Label Studio. Keep the `name` of every tag: annotations are stored keyed by these names (`from_name`), and dbt reads them by name (e.g. `claim`, `explanation`, `Was Misinformation Corrected?` in `my_dbt_project/models/analytics/task_global_completion.sql`, after ingestion by `quotaclimat/data_ingestion/labelstudio/`). Renaming a tag breaks those columns.
