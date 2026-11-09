# Windmill flows

[Windmill](https://www.windmill.dev) runs on the orchestrator next to Kestra (deployment: `infrastructure/Readme.md`, "Orchestrator deployment"). The 21 Kestra flows of `infrastructure/kestra/flows/` have a Windmill copy in `infrastructure/windmill/`, with the same steps, inputs and secrets. This page covers how they are built and deployed. What each flow does is in [`kestra_flows.md`](kestra_flows.md): the Windmill flows run the same code.

**Status:** Kestra still runs production on its schedules. The Windmill flows have **no schedule**: they are run by hand, to be tested before the cutover. Avoid starting a prod flow during Kestra's slots (00:30, 01:10, 03:00, 04:07, 12:30): the same job would run twice.

## Layout

One workspace (`WINDMILL_WORKSPACE`), one folder per Kestra namespace:

| Folder | Kestra namespace | Contents |
|---|---|---|
| `f/quotaclimat` | `quotaclimat` | `barometre`, `barometre_catchup`, `barometre_i18n`, `dbt`, `dbt_analytics`, `main_pipeline`, `metabase_sync` |
| `f/quotaclimat_dev` | `quotaclimat-dev` | `barometre`, `main_pipeline`, `alert_test`, `glitchtip_log_test` |
| `f/rrs` | `rrs` | `extended_perimeter`, `rrs_climate`, `rrs_insecurity`, `rrs_pollution`, `pulsar_themes` |
| `f/rrs_dev` | `rrs-dev` | same as `f/rrs` |
| `f/shared` | — | `docker_run` (runs a container) and `mattermost_alert` (failure alert) |
| `f/secrets` | — | one secret variable per Kestra secret (not in the repository, see [Secrets](#secrets)) |

Flow names are the Kestra ids with `_` instead of `-` (`rrs/rrs-pollution` → `f/rrs/rrs_pollution`). In the repository, a flow is a folder `f/<folder>/<name>__flow/` holding `flow.yaml` and, for inline Python steps, the `.py` file and its `.lock`. A shared script is `f/shared/<name>.py` with `<name>.script.yaml` (arguments, description) and `<name>.script.lock` (pinned dependencies).

## Deployment

`make tags=windmill ansible` (role `ansible/roles/windmill`) deploys everything, after starting Windmill:

1. **Secrets** (`tasks/secrets.yml`): every entry of `kestra_secrets` and `kestra_secrets_dev` (`ansible/playbook.yml`, read from `.env.secrets`) is written as the secret variable `f/secrets/<NAME>`, through the Windmill API. A variable is written only when it is missing or its value changed. Entries left empty in `.env.secrets` are skipped and listed ("Empty, skipped: ...").
2. **Flows and scripts** (`tasks/flows.yml`): `infrastructure/windmill/` is copied to the server and pushed with the Windmill CLI (`wmill sync push`, version pinned to the server's), run in a `node` container on the `windmill-internal` network.

Both need `WINDMILL_TOKEN` (a token of an admin of the workspace: account settings → Tokens) and `WINDMILL_WORKSPACE` (the workspace **id**, e.g. `quotaclimat`, not its display name) in `.env.secrets`. The workspace is created once by hand in the UI.

`wmill sync push` makes the workspace match the repository, **deletions included**, but only in the folders listed in `infrastructure/windmill/wmill.yaml` (`includes`). Variables, resources and schedules are left out of the sync. So:

- never create or edit a flow by hand in these folders: the next deploy overwrites or deletes it. Edit the files, then deploy;
- a second deploy with nothing changed reports `remote (...) <- local: 0 changes to apply`.

## From Kestra to Windmill

The flows were converted once from the Kestra files; they are now maintained by hand.

| Kestra | Windmill |
|---|---|
| `io.kestra.plugin.docker.Run` | step calling the script `f/shared/docker_run` (see [Running a container](#running-a-container)) |
| `{{ secret('X') }}` | `secret_env: {ENV_NAME: "f/secrets/X"}`: the path, read by the runner at run time |
| `{{ inputs.x }}`, ternaries, `timestamp()` | JavaScript input transform: `flow_input.x`, `cond ? a : b`, `Math.floor(Date.now() / 1000)` (epoch seconds, as Kestra's `timestamp()`) |
| `Subflow` (`wait: true`, `transmitFailed: true`) | step of type `flow` (`path: f/...`): waits, and its failure fails the parent |
| `runIf` | `skip_if: {expr: "!(...)"}` |
| `allowFailure: true` | `continue_on_error: true` |
| `ForEach` / `EachParallel` | `forloopflow` (`parallel: false` / `true`) |
| `errors:` posting to Mattermost | `failure_module` calling `f/shared/mattermost_alert` |
| `concurrency: limit 1` (`dbt`) | `tag_override: dbt` on its steps (see [dbt](#dbt-one-run-at-a-time)) |
| `logLevel: WARN` | `stdout_logs: false` (only stderr is kept) |
| `Fail`, Python `Script` | inline Python steps (`rawscript`, `content: '!inline file.py'`) |
| flow `inputs` | flow `schema` (same names and defaults) |

**Arguments computed at run time.** A step argument is either `static` (fixed value) or `javascript` (an expression evaluated when the step starts, with `flow_input`, `results.<step_id>`, or `flow_input.iter.value` inside a loop). `env` and `secret_env` are one argument each: when one key depends on an input, the whole object is a JavaScript expression, fixed values included.

**`barometre_i18n`.** The per-country settings (name, bucket, ...) are the table of the `i18n_loop` step's iterator, filtered on the `countries` input. Each country's credentials are keys of the JSON secret `I18N_COUNTRY_SECRETS` (`infrastructure/Readme.md`, "Per-country secret"), read with `f/secrets/I18N_COUNTRY_SECRETS#<code>.<key>`.

## Running a container

`f/shared/docker_run` does what Kestra's `docker.Run` did: it pulls the image (every run), starts the container on the host's Docker, streams its logs into the step's logs and fails the step when the container exits with a non-zero code. It talks to the Docker API through `/var/run/docker.sock`, mounted in the workers (`compose/windmill/docker-compose.yml.j2`). As with Kestra, a job can therefore control the host's Docker.

| Argument | Default | Use |
|---|---|---|
| `image` | (required) | image to run |
| `entrypoint` | image's | replaces the entrypoint (list) |
| `command` | image's | arguments given to the entrypoint (list) |
| `env` | — | environment variables; booleans are passed as `true`/`false`, objects and lists as JSON, as Kestra did |
| `secret_env` | — | `{NAME: "f/secrets/X"}`, or `"f/secrets/X#a.b"` for a key of a JSON secret |
| `registry` | — | private registry of the image (user `nologin`) |
| `registry_password_secret` | — | variable holding the registry password (e.g. `f/secrets/SCW_SECRET`) |
| `shm_size` | `6g` | size of `/dev/shm` |
| `stdout_logs` | `true` | `false` keeps only stderr (for jobs logging every row) |

Secrets are read by the runner when the step starts: they never appear in the job's arguments (stored in the database and shown in the UI) nor in the logs. The container is named `windmill-<job id>`. Cancelling the step stops and removes it.

Jobs are stopped after `TIMEOUT` (`windmill_timeout_secs`, 6 h, `ansible/roles/windmill/defaults/main.yml`).

## dbt: one run at a time

Kestra's `concurrency` limit is an Enterprise feature in Windmill. The steps of `f/quotaclimat/dbt` carry the `dbt` tag instead, and only one worker serves it (`windmill-worker-dbt`, `WORKER_TAGS=dbt`, one worker): only one dbt container runs at a time, the others wait in the queue.

The queue is per step, not per run: for two runs A and B started together, the order can be A `dbt.sh`, B `dbt.sh`, A `curated.sh`, B `curated.sh`. Two dbt containers never overlap, which is what matters (they drop and rebuild the same tables).

## Secrets

The flows read the secrets as `f/secrets/<NAME>`, the names of the Kestra secrets. To add one:

1. add it to `.env.secrets` (and, empty, to `.env.secrets.dist`) and to `kestra_secrets` (or `kestra_secrets_dev`) in `ansible/playbook.yml`;
2. use it in the flow: `secret_env: {ENV_NAME: "f/secrets/<NAME>"}`;
3. `make tags=windmill ansible`.

Values are changed the same way (edit `.env.secrets`, deploy): a value edited in the UI is overwritten by the next deploy.

## Failure alerts

`main_pipeline` (prod and dev) has a `failure_module`: when a step fails, `f/shared/mattermost_alert` posts the failed step, the error and a link to the run to each webhook of `MATTERMOST_WEBHOOK_URLS` (comma-separated). `metabase_sync` has `continue_on_error`: its failure sends no alert. To test the alert, run `f/quotaclimat_dev/main_pipeline` with `test_alert=true`: it calls `f/quotaclimat_dev/alert_test`, which always fails.

## Adding or changing a flow

1. Edit or add the files under `infrastructure/windmill/f/...`. For a new folder, add its `folder.meta.yaml` and its line in `wmill.yaml` (`includes`).
2. Check them: `npx windmill-cli@1.864.1 lint` from `infrastructure/windmill/` (same version as the server).
3. When a Python script or inline step changes its imports, regenerate its `.lock` with `wmill generate-metadata` against a Windmill instance (a local one is enough: `docker run` of the same image), and commit it. Without it, the CLI warns about stale metadata.
4. `make tags=windmill ansible`.

Keep the Kestra flow in sync while Kestra runs production.

## Running a flow by hand

In the UI, open the flow, fill the inputs (defaults as in Kestra) and run it. Test in this order after a deploy:

1. `f/quotaclimat_dev/alert_test`: fails, and the alert reaches Mattermost (through `main_pipeline` with `test_alert=true`);
2. `f/quotaclimat_dev/glitchtip_log_test`: its logs and error reach GlitchTip, filtered on its `run_id`;
3. a dev flow running a container, e.g. `f/rrs_dev/rrs_climate` with `run_clustering=false`;
4. `f/quotaclimat_dev/main_pipeline`, compared with the same Kestra dev run.
