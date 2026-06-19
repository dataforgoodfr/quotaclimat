"""Configuration for the Pulsar pipeline, read from the environment.

Light by design — the whole pipeline (download → parse → store → fetch posts)
runs in the lean image, so no embedding/LLM config here.
"""

import os
from dataclasses import dataclass

from dotenv import load_dotenv

DEFAULT_BASE_URL = "https://lessurligneurs.pulsarplatform.com"


@dataclass
class PulsarSettings:
    email: str
    password: str
    search_id: str
    folder_id: str
    base_url: str = DEFAULT_BASE_URL
    themes_xlsx: str = "pulsar_themes.xlsx"
    top_n: int = 30
    headless: bool = True

    @classmethod
    def from_env(cls) -> "PulsarSettings":
        load_dotenv()
        email = os.getenv("PULSAR_EMAIL")
        password = os.getenv("PULSAR_PASSWORD")
        search_id = os.getenv("PULSAR_SEARCH_ID")
        folder_id = os.getenv("PULSAR_FOLDER_ID")
        missing = [
            n
            for n, v in (
                ("PULSAR_EMAIL", email),
                ("PULSAR_PASSWORD", password),
                ("PULSAR_SEARCH_ID", search_id),
                ("PULSAR_FOLDER_ID", folder_id),
            )
            if not v
        ]
        if missing:
            raise EnvironmentError(
                f"Missing Pulsar config: {', '.join(missing)}. "
                "Set them in your environment (1Password locally; Vaultwarden→Kestra secret in prod)."
            )
        return cls(
            email=email,
            password=password,
            search_id=search_id,
            folder_id=folder_id,
            base_url=os.getenv("PULSAR_BASE_URL", DEFAULT_BASE_URL),
            themes_xlsx=os.getenv("PULSAR_THEMES_OUTPUT", f"pulsar_themes_{search_id}.xlsx"),
            top_n=int(os.getenv("PULSAR_TOP_N", "30")),
            headless=os.getenv("PULSAR_HEADLESS", "true").lower() not in ("0", "false", "no"),
        )
