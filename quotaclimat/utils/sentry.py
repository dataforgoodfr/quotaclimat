
import ray
import os
import logging
from quotaclimat.utils.healthcheck_config import get_app_version
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

# read SENTRY_DSN from env
functions_to_trace = [
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.get_cts_in_ms_for_keywords"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.filter_keyword_with_same_timestamp"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.get_themes_keywords_duration"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.count_keywords_duration_overlap"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.filter_and_tag_by_theme"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.add_primary_key"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.api_import.extract_api_sub"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.api_import.parse_reponse_subtitle"},
]

def sentry_init():
    if(os.environ.get("SENTRY_DSN", None) != None):
        logging.info("Sentry init")
        logging_kwargs = {}
        if os.getenv("SENTRY_LOGGING") == "true":
            logging_kwargs = dict(
                enable_logs=True,
                integrations=[
                    # Only send WARNING (and higher) logs to Sentry logs,
                    # even if the logger is set to a lower level.
                    LoggingIntegration(sentry_logs_level=logging.INFO),
                ]
            )
        sentry_sdk.init(
            traces_sample_rate=0.3,
            # To set a uniform sample rate
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production,
            profiles_sample_rate=0.3,
            release=get_app_version(),
            # functions_to_trace=functions_to_trace,
            # integrations=[ # TODO : https://docs.sentry.io/platforms/python/integrations/ray/
            #     RayIntegration(),
            # ],
            **logging_kwargs
        )
    else:
        logging.info("Sentry not init - SENTRY_DSN not found")