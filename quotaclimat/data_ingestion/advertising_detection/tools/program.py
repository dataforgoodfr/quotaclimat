import json


def get_program():
    with open("postgres/program_metadata.json", "r", encoding="utf-8") as f:
        return json.load(f)
