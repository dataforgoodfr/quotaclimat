# Misinformation jobs

Two jobs, migrated from the `climateguard` repository:

| Job | What it does | Poetry group (root `pyproject.toml`) |
|---|---|---|
| `label_misinformation/` | Labels misinformation in TV/radio transcripts with an LLM and pushes the extracts to Label Studio | `misinformation` (the `shadow` service adds `rrs` for CPU torch + transformers) |
| `get_audio_video/` | Collects German audio/video for annotation (GCS, Google Drive, Label Studio) | `germany-audio-video-collector` |

## Dependencies and images
Dependencies are not in `requirements.txt` files anymore: each job installs its own Poetry group of the root `pyproject.toml` / `poetry.lock` (`poetry install --no-root --only <group>`). To add a dependency:
```
poetry add --group misinformation <package>   # or --group germany-audio-video-collector
```
Docker images are built **from the repository root** (the build needs `pyproject.toml` and `poetry.lock`):
```
docker build -f detect_misinformation/jobs/label_misinformation/Dockerfile .
docker build -f detect_misinformation/jobs/get_audio_video/Dockerfile .
```
The `docker-compose.yml` of each job already points its build context at the repository root, so `docker compose` commands are still run from the job folder.

# Labelling misinformation
The app folder contains all the code for the job whilst the docs folder contains documentation on the job and general project infrastructure.

## Run
In order to run this app locally, a docker installation is required. Services are defined in the docker-compose.yml file where main logic is in the app service, a database can be deployed locally for testing, and tests can be run using the test service. There is also a testconsole service that allows you to run and test single commands:
```
cd detect_misinformation/jobs/label_misinformation
docker compose up testconsole -d
docker compose exec testconsole bash
> pytest -vv -k test_pg_insert_data # will insert misinformation data linked to rmc on 2025 march 10
> exit
docker compose up app
```

### Secrets
 You have to set up your secrets (password, key etc.) inside your secrets folder
 ```
label_misinformation
├── app
├── docs
└── secrets
 ```

## Testing
Tests are located inside tests folder
### Run all test
```
docker compose up test
```

### Target one test
```
docker compose up testconsole -d
docker compose exec testconsole bash
> pytest --log-level INFO -vv -k s3_utils 
> pytest --log-level INFO -vv -k get_new_plaintext_from_whisper_mp4
```

## Configuration
The app config is setup via environment variables, here is an exhaustive list of the required environment variables needed:

* "LLM_PROVIDER" - which pipeline/API to run misinformation detection with: `mistral` (default) or `openai`. See "LLM provider" below.
* "MODEL_NAME" - the model used for the selected LLM_PROVIDER (e.g. `mistral-small-2603` for mistral, `gpt-4o-mini` for openai)
* "MODEL_NAME_BRAZIL" - same as MODEL_NAME, but for the brazil country
* "PROMPT_VERSION" - overrides the prompt version used by the mistral pipeline (see `app/prompts/prompts.py`). Defaults to the per-country value in `app/country.py` (the prompt translated into the country's language, e.g. `"0.1.0-nl"` for belgium-flanders). Only applies when LLM_PROVIDER=mistral; the openai pipeline always uses prompt version `"0.0.1"`.
*  "APP_NAME" - will be used as folder inside bucket_output bucket
* "MISTRAL_API_KEY" - required when LLM_PROVIDER=mistral (the default)
* "OPENAI_API_KEY" - required when LLM_PROVIDER=openai
*  "DATE" : to query a date with format YYYY-MM-DD, if none the date is yesterday
*  "BUCKET_OUTPUT" - bucket name with partitions : year,month,day,channel
*  "BUCKET_OUTPUT_BRAZIL" - bucket name with partitions : country,year,month,day,channel
*  "BUCKET_OUTPUT_BELGIUM" - bucket name with partitions : country,year,month,day,channel
* "MIN_MISINFORMATION_SCORE": 10 # the minimum score to have to be kept (10 out of 10). Only applies to non-binary prompts (the openai pipeline / prompt version "0.0.1"); the mistral pipeline's prompt outputs a binary misinformation flag, so this setting is ignored when LLM_PROVIDER=mistral.
* "CHANNEL" : mediatree former channel name (tf1 for TF1, itele for cnews, bfmtv for BFMTv ...)
* NUMBER_OF_PREVIOUS_DAYS (integer): default 7 days to check if something missing - in case production servers had an issue
* COUNTRY: The country or country collection for which to run the job. by default this should be `prod`. Possible values are france, brazil, belgium, all (france, brazil and belgium) and prod (france and brazil). For more details see label_misinformation/app/country.py.

### LLM provider
The job can run misinformation detection through two interchangeable pipelines, selected via `LLM_PROVIDER`:
* `mistral` (default) - uses `MistralSinglePromptPipeline` with the Mistral API (`mistralai` SDK) and the structured prompt of the country (`country.prompt_version`, `"0.1.0"` translated into the country's language). The model returns a `reasoning` string and a `misinformation` boolean (structured JSON output); the boolean is mapped to a score of 10 (misinformation) or 0 (not misinformation), and `MIN_MISINFORMATION_SCORE` is not applied - any score > 0 is flagged.
* `openai` - uses the original `SinglePromptPipeline` with the OpenAI API and prompt version `"0.0.1"` (English, plain 0-10 score). `MIN_MISINFORMATION_SCORE` applies as before.

Requires the matching secret (`MISTRAL_API_KEY` or `OPENAI_API_KEY`) and `secrets/mistral_key` / `secrets/openai_key` file to be set up (see Secrets above).

### To sync data after each process
This can be done manually by clicking on UI, but if you set this it will be done after each cron job if LABEL_STUDIO_PROJECT_ID is set :
* LABEL_STUDIO_URL = container url
* LABEL_STUDIO_PROJECT_ID = Storage ID (not project id) inside label studio --> new to see labelstudio logs when cliking on "sync" it can be different from the one inside the label interface. @see https://api.labelstud.io/api-reference/api-reference/import-storage/s-3/sync ( [2025-05-06 12:41:25,789] [django.server::log_message::213] [INFO] "POST /api/storages/s3/5/sync HTTP/1.1" 200 799 )
* LABEL_STUDIO_PROJECT = The actual project ID
* LABEL_STUDIO_PROJECT_ID_BRAZIL = Storage ID for the brazil project
* LABEL_STUDIO_PROJECT_BRAZIL = Project ID for the brazil project
* API_LABEL_STUDIO_KEY = label studio API key

### To reparse
* LABEL_MISINFORMATION_OVERWRITE = Boolean. If activated the job will reparse any extract that has not been included in labelstudio already and add save it to s3.

## Deployment

App image is pushed on the Scaleway Container Registry, and then deployed on the Scaleway Serverless service.

The GitHub Actions workflow `.github/workflows/label-misinformation.yml` (triggered by changes under `detect_misinformation/jobs/label_misinformation/`, `pyproject.toml` or `poetry.lock`) does it:
- **pull request**: runs the tests (`docker compose up test`), then builds and pushes the image tagged `dev` (GitHub environment `test`);
- **push on main**: bumps the patch version with bump2version, then builds and pushes the image tagged `<version>` and `latest` (GitHub environment `production`).

Registry secrets, set in both GitHub environments: `LABEL_MISINFORMATION_SCW_SECRET_KEY`, `LABEL_MISINFORMATION_REGISTRY_ENDPOINT`.

For production, to verify no days have been forgotten, NUMBER_OF_PREVIOUS_DAYS is used to check it. In case a day is missing it's recalculated.

### Bump
The job keeps its own version (`label_misinformation/.bumpversion.cfg`, `pyproject.toml`, `__init__.py`), independent of quotaclimat's. The CI bumps the patch version on main; for a minor/major bump, run from `detect_misinformation/jobs/label_misinformation`:
```
bump2version patch  # 0.1.0 → 0.1.1
bump2version minor  # 0.1.0 → 0.2.0
bump2version major  # 0.1.0 → 1.0.0
```
