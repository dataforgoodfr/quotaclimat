# to generate keywords.py file
import pandas as pd
import json
import logging

# Need to import these files - slack #keywords
excels_files = ["document-experts/Dictionnaire - OME.xlsx"]
output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"
language = "fr"

# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}
for excel_file_path in excels_files:
    print(f"Reading {excel_file_path}")
    df = pd.read_excel(excel_file_path, sheet_name='Catégorisation Finale')

    df['category'] = df['Secteur'].fillna('')
    df['crise'] = df['Crise'].fillna('') 

    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        theme_name = row['Category_legacy'].strip()
        keyword = row['keyword'].lower().strip()
        category = row['category'].strip()

        high_risk_of_false_positive = row['HRFP']
        crisis_climate = row['crise'] == "Climat"
        crisis_biodiversity = row['crise'] == "Biodiversité"
        crisis_resource = row['crise'] == "Ressources"

        solution = "solution" in theme_name
        consequence = "consequence " in theme_name
        cause = "cause" in theme_name
        general_concepts = "concepts_generaux" in theme_name
        statement = "constat" in theme_name

        # Check if the theme_name already exists in THEME_KEYWORDS
        if(theme_name not in THEME_KEYWORDS):
            print(f"Adding theme {theme_name}")
            THEME_KEYWORDS[theme_name] = []

        # filter # keyword with # (paused or removed)
        if ("#" not in keyword):
            THEME_KEYWORDS[theme_name].append(
                {
                    "keyword": keyword,
                    "category": category,
                    "high_risk_of_false_positive": high_risk_of_false_positive,
                    "crisis_climate": crisis_climate,
                    "crisis_biodiversity": crisis_biodiversity,
                    "crisis_resource": crisis_resource,
                    "solution": solution,
                    "consequence": consequence,
                    "cause": cause,
                    "general_concepts": general_concepts,
                    "statement": statement,
                    "language": language,
                 }
            )

# Sort keywords alphabetically for each theme
for theme_name in THEME_KEYWORDS:
    THEME_KEYWORDS[theme_name] = sorted(THEME_KEYWORDS[theme_name], key=lambda x: x["keyword"])

# Convert the THEME_KEYWORDS dictionary to a JSON string
with open(output_file, 'w', encoding='utf-8') as f:
    logging.info(f"Json written  - {len(THEME_KEYWORDS)} themes inside {output_file}")
    print(f"Json written {len(THEME_KEYWORDS)} in {output_file}")
    f.write("THEME_KEYWORDS = ")
    json.dump(THEME_KEYWORDS, f, ensure_ascii=False, indent=4)

# Read the file with JSON-style boolean values
with open(output_file, 'r', encoding='utf-8') as f:
    content = f.read()

    # Replace JSON boolean values with Python boolean values
    content = content.replace('true', 'True')
    content = content.replace('false', 'False')

# Write the modified content back to a Python file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(content)
    logging.info(f"Python dictionary written to {output_file}")
    print(f"Python dictionary written to {output_file}")