import logging
import os

from typing import Optional


def get_secret_docker(secret_name: str, default: Optional[str] = None):
    secret_value: str = os.environ.get(secret_name, default)

    if secret_value and os.path.exists(secret_value):
        with open(secret_value, "r") as file:
            return file.read().strip()
    return secret_value
