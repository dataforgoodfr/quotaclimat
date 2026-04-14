#! /bin/bash

tofu_cmd() {
    source .env && source live/$1/$2/.env.secrets && export TF_VAR_environment=$2 && tofu -chdir=live/$1/$2 $3
}