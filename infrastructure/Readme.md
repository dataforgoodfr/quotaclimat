# Safeguards Infrastructure
This folder contains the framework and infrastructure definition for the safeguards projects.

## The Stack
In order to deploy the infrastructure we use `brew` and `npm` to install dependencies (`npm` not used now but will be used to pull secrets automatically from Vaultwarden - install it now!). These need to be installed before running setup.

We deploy using `OpenTofu` and `Terragrunt`, both managed via `mise`. `make` is used as a wrapper for all commands, so it must also be installed before working with this repository.

## Project Structure

```
insfrastructure/
├── mise.toml                  # OpenTofu and Terragrunt version pins (read by mise)
├── bin/
│   ├── setup.sh               # Installs all dependencies
│   └── terragrunt_wrapper.sh  # Sources .env / .env.secrets before running terragrunt
└── live/
    ├── terragrunt.hcl         # Root config: backend, provider, terraform_binary
    └── advertising/
        ├── dev/
        │   ├── terragrunt.hcl   # Dev environment inputs
        │   └── .env.secrets     # Dev secrets (not committed)
        ├── prod/
        │   ├── terragrunt.hcl   # Prod environment inputs
        │   └── .env.secrets     # Prod secrets (not committed)
        └── template/            # Shared OpenTofu module (resources defined here)
```

The root `live/terragrunt.hcl` generates the S3 backend and Scaleway provider configuration for every environment automatically, so there is no duplicated `backend.tf` or `provider.tf` to maintain. Each environment folder only needs a `terragrunt.hcl` with its specific input values.

## Installing dependencies
* `brew` can be installed at [brew.sh](https://brew.sh).
* `brew install make`
* `brew install node`

## Setting up your workspace
You need a Scaleway account and to be added as a member of the Quotaclimat organisation. From there, create an API key pair with the default project set as the preferred project. Add the Scaleway access key and secret key to the `.env` file (create it from `.env.dist`):

```
SCW_ACCESS_KEY=<your key>
SCW_SECRET_KEY=<your secret>
```

Once the `.env` file is ready, run:
```bash
make setup
```
This installs `mise` (if needed) and then runs `mise install`, which installs OpenTofu and Terragrunt at the versions pinned in `mise.toml`. Make sure `mise` is activated in your shell profile so the tools are on your PATH (`eval "$(mise activate zsh)"` in `~/.zshrc` or the bash equivalent).

Each environment also requires an `.env.secrets` file. Create it from the `.env.secrets.dist` template provided in the environment folder. The secrets are available in the [DataForGood Vaultwarden Password Manager](https://vaultwarden.services.dataforgood.fr/) under the collection `Quotaclimat - Désinformation/`.

*TODO: RECOVER THESE DYNAMICALLY FROM VAULTWARDEN*

At minimum you need:
* `.env`
* `live/advertising/dev/.env.secrets` (for dev)
* `live/advertising/prod/.env.secrets` (for prod)

## Running commands
All commands are run via `make` using `env` (default: `dev`) and `target` (default: `advertising`) as arguments.

Initialise the target directory first, then plan, apply, or destroy:
```bash
make env=dev target=advertising tg-init
make env=dev target=advertising tg-plan
make env=dev target=advertising tg-apply
make env=dev target=advertising tg-destroy
```

Other available commands:
```bash
make env=dev target=advertising tg-fmt         # Format .hcl files
make env=dev target=advertising tg-state-list  # List resources in state
make env=dev target=advertising tg-state-pull  # Pull raw state
```

## A note on passwords
When deploying for the first time you may need to create passwords and tokens. Use the following to generate a secure password:
```bash
LC_ALL=C tr -dc 'A-Za-z0-9!@#$%^&*' < /dev/urandom | head -c 32; echo
```
To generate a Label Studio token (40-character alphanumeric string):
```bash
openssl rand -base64 30 | tr -dc 'A-Za-z0-9' | head -c 40; echo
```
