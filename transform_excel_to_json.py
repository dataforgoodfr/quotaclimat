# to generate keywords.py file
import pandas as pd
import json
import logging

# Need to import these files - slack #keywords
excels_files = ["document-experts/Dictionnaire de mots-clés.xlsx", "document-experts/Ressources_feuille de travail.xlsx"]
output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"
# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}
for excel_file_path in excels_files:
    print(f"Reading {excel_file_path}")
    if  "Ressources_feuille" in excel_file_path:
        df = pd.read_excel(excel_file_path)
    else:
        df = pd.read_excel(excel_file_path, sheet_name='Catégorisation Finale')

    df['category'] = df['category'].fillna('')

    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        theme_name = row['theme'].strip()
        keyword = row['keyword'].lower().strip()
        category = row['category'].strip()

        # Check if the theme_name already exists in THEME_KEYWORDS

        if(theme_name not in THEME_KEYWORDS):
            THEME_KEYWORDS[theme_name] = []

        # filter # keyword with # (paused or removed)
        if ("#" not in keyword):
            THEME_KEYWORDS[theme_name].append({"keyword": keyword, "category": category})

# Sort keywords alphabetically for each theme
for theme_name in THEME_KEYWORDS:
    THEME_KEYWORDS[theme_name] = sorted(THEME_KEYWORDS[theme_name], key=lambda x: x["keyword"])

# Convert the THEME_KEYWORDS dictionary to a JSON string
with open(output_file, 'w', encoding='utf-8') as f:
    logging.info(f"Json written  - {len(THEME_KEYWORDS)} themes inside {output_file}")
    print(f"Json written {len(THEME_KEYWORDS)} in {output_file}")
    f.write("THEME_KEYWORDS = ")
    json.dump(THEME_KEYWORDS, f, ensure_ascii=False, indent=4)