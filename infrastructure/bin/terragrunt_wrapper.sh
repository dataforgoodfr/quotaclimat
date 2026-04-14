#! /bin/bash

tg_cmd() {
    source .env && source live/$1/$2/.env.secrets && terragrunt $3 --terragrunt-working-dir live/$1/$2
}
