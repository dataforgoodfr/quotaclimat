"""
Configuration for Factiva data ingestion.
"""

import logging
import os


def get_factiva_service_account_id() -> str:
    """Get Factiva service account ID from environment or secrets file"""
    # Try environment variable first
    service_account_id = os.environ.get("FACTIVA_SERVICE_ACCOUNT_ID")

    if not service_account_id:
        # Try reading from secrets file
        secrets_path = os.environ.get(
            "FACTIVA_SERVICE_ACCOUNT_FILE", "/run/secrets/factiva_service_account"
        )
        try:
            with open(secrets_path, "r") as f:
                service_account_id = f.read().strip()
        except FileNotFoundError:
            logging.warning(f"Factiva service account file not found at {secrets_path}")

    return service_account_id


def get_factiva_subscription_id() -> str:
    """Get Factiva subscription ID from environment"""
    return os.environ.get("FACTIVA_SUBSCRIPTION_ID", "")


def is_mock_mode() -> bool:
    """Check if we should use mock Factiva client (for local testing)"""
    return os.environ.get("FACTIVA_MOCK_MODE", "false").lower() == "true"


def get_max_messages_per_pull() -> int:
    """Get maximum number of messages to pull at once"""
    return int(os.environ.get("FACTIVA_MAX_MESSAGES", "100"))


def get_poll_interval() -> int:
    """Get interval between polls in seconds"""
    return int(os.environ.get("FACTIVA_POLL_INTERVAL", "60"))


def get_batch_size() -> int:
    """Get batch size for database commits"""
    return int(os.environ.get("FACTIVA_BATCH_SIZE", "50"))
