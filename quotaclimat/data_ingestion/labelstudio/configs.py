from quotaclimat.data_processing.mediatree.i8n.country import EXTENDED_FRANCE

db_config = [
    {"database": "labelstudio", "countries": {6: "france", 9: "brazil"}},
    {"database": "labelstudio-climate-poland-prod-db", "countries": {1: "poland"}},
    {"database": "labelstudio-climate-spain-prod-db", "countries": {1: "spain"}},
    {"database": "labelstudio-climate-belgium-prod-db", "countries": {2: "belgium"}},
    # {"database": "labelstudio-climate-belgium-flanders-prod-db", "countries": {1: "belgium"}},
    {"database": "labelstudio-climate-germany-prod-db", "countries": {1: "germany"}},
]

# Used instead of db_config when EXTENDED_PERIMETER is true - see ingest_labelstudio.py
db_config_extended_perimeter = [
    {"database": "labelstudio", "countries": {23: EXTENDED_FRANCE.name}},
]
