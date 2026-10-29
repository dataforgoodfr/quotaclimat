"""Values of advertising.ad.prediction_status written by the classification pipeline (see README.md,
"Status reference"). Every status written by a stage must be in one of the two lists:
test/advertising_classification/test_prediction_status.py reads the pipeline code and fails otherwise.
"""

# The classification gives a brand and a sector usable in the analyses: the ads shown in
# analytics.publicites (dbt var final_prediction_statuses, checked equal by the test above) and in the
# screenshots (s05_enrichment/e02_screenshots). dict_miss, dict_tier2_no_kw and dict_tier3_no_kw still go
# through stage 4 (sub-category), but already have their sector.
FINAL_PREDICTION_STATUSES = (
    "dict_miss",
    "dict_tier1",
    "dict_tier2",
    "dict_tier2_no_kw",
    "dict_tier3",
    "dict_tier3_no_kw",
    "subcat_done",
)

# Waiting for the next stage, or to be inspected: left out of the analyses.
# NULL (not classified yet) is not a status of this list.
NON_FINAL_PREDICTION_STATUSES = (
    "ad_type_done",
    "ad_type_parse_error",
    "subcat_parse_error",
    "no_data",
)
