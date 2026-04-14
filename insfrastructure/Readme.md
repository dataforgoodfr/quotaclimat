# Safeguards Infrastructure
This folder contains the framework and infrastructure definition for the safeguards projects. 

## The Stack
In order to deploy the infrastructure we use `brew` and `npm`to install dependencies (`npm` not used now but will be used to pull secrets automatically from Vaultwarden - Install it now !). These need to be installed before running setup.

We deploy using `OpenTofu` which is managed using `tofuenv`. In order to run setup and to run tofu command we use `make` as a wrapper, hence it has to be installed before the . This allows as to source the correct environment variables and to select the correct target directory. Install `make` as well before working with this repository.

## Project Structure
TODO

## Installing dependencies
* `brew` can be installed at [brew.sh](https://brew.sh).
* `brew install make`
* `brew install node`

## Setting up your workspace
In order to setup your workspace you need a Scaleway account, and to be added as a member to the Quotaclimat organisation. From there you can setup you credentials. You will need an API key pair for your account, with the default project selected as the preferred project. The scaleway access key and scaleway secret key need to be added into the `.env` file (which you can create basing yourself on the `.env.dist` file), as `SCW_ACCESS_KEY` and `SCW_SECRET_KEY`. 

Once the `.env` file has been setup, run 
```bash
make setup
```
to install tofu and all dependencies.

In order to deploy our infrastructure, we target individual directories that import the modules into different environments. These environments need different API keys in order to deploy all resources, so create an `.env.secrets` file, based on the `.env.secrets.dist` file provided.
The API keys and rest of the variables in `.env.secret` will be recovered from the [DataForGood Vaultwarden Password Manager](https://vaultwarden.services.dataforgood.fr/). The secrets are available in the collection `Quotaclimat - Désinformation/`.

*TODO: RECOVER THESE DYNAMICALLY FROM VAULTWARDEN*

In order to deploy or modify the infrastructure in an environment, at the very least you need to populate the base `.env` file and the `.env.secrets` file in the country specific folder. So for example you need:
* `.env``
* `live/dev/advertising/.env.secrets`


## Running commands
All commands are ran via `make`. 

In order to run a command, you need to specify the subject (`climate` by default), the environment (`dev` by default) and the country (`common` by default), as the first arguments of the make command. Finally you must run the tofu command separated by a `-`instead of whitespace.
So you will first have to initialise the target folder you are working with using the `make env=dev target=advertising tofu-init` command, and then in a similar manner run `tofu-plan`, `tofu-apply` and `tofu-destroy`. For example to deploy and then destroy the project for the climate topic, on the dev environment for france you would run:
```bash
make env=dev target=advertising tofu-init
make env=dev target=advertising tofu-plan
make env=dev target=advertising tofu-apply
make env=dev target=advertising tofu-destroy
```

## A note on passwords
When deploying a country for the first time you might need to create passwords for the different users and set tokens. You may use the command 
```bash
openssl rand -base64 16
```
to generate a secure password. 
To generate the labelstudio token a 40 character string needs to be generated with:
```bash
openssl rand -base64 30 | tr -dc 'A-Za-z0-9' | head -c 40; echo
```