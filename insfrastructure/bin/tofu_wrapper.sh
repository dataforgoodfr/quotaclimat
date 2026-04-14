#! /bin/bash

tofu_cmd() {
    source .env && source live/$1/$2/.env.secrets && tofu -chdir=live/$1/$2 $3
}