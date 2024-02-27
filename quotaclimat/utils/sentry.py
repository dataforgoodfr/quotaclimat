
import os
import logging
from quotaclimat.utils.healthcheck_config import get_app_version
import sentry_sdk

# read SENTRY_DSN from env
functions_to_trace = [
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.get_cts_in_ms_for_keywords"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.filter_keyword_with_same_timestamp"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.get_themes_keywords_duration"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.count_keywords_duration_overlap_without_indirect"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.filter_and_tag_by_theme"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.detect_keywords.add_primary_key"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.api_import.extract_api_sub"},
    {"qualified_name": "quotaclimat.data_processing.mediatree.api_import.parse_reponse_subtitle"},
]

def sentry_init():
    if(os.environ.get("SENTRY_DSN", None) != None):
        logging.info("Sentry init")
        sentry_sdk.init(
            enable_tracing=False,
            traces_sample_rate=0.7,
            # To set a uniform sample rate
            # Set profiles_sample_rate to 1.0 to profile 100%
            # of sampled transactions.
            # We recommend adjusting this value in production,
            profiles_sample_rate=0.7,
            release=get_app_version(),
            functions_to_trace=functions_to_trace,
        )
    else:
        logging.info("Sentry not init - SENTRY_DSN not found")