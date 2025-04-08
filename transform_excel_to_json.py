# to generate keywords.py file
import pandas as pd
import json
import logging

# Need to import these files - slack #metabase-keywords
excels_files = ["document-experts/Dictionnaire - OME.xlsx"]
i8n_dictionary = "document-experts/Dictionnaire_Multilingue.xlsx"
output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"
language = "fr"

# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}
for excel_file_path in excels_files:
    print(f"Reading {excel_file_path}")
    df = pd.read_excel(excel_file_path, sheet_name='Catégorisation Finale')
    i18n_df = pd.read_excel(i8n_dictionary)
    i18n_df['French'] = i18n_df['French'].str.lower().str.strip()  # Normalize for matching

    df['category'] = df['Secteur'].fillna('')
    df['crise'] = df['Crise'].fillna('') 

    # merge between i8n_dictionary and df based on keyword and French to get German, Spanish etc.
    df = df.merge(i18n_df, how='left', left_on='keyword', right_on='French')
    
    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        theme_name = row['Category_legacy'].strip()
        keyword = row['keyword'].lower().strip()
        category = row['category'].strip()

        # get for each language the translation, it can be None
        keyword_english = row.get('English')
        keyword_german = row.get('German')
        keyword_spanish = row.get('Spanish')
        keyword_portuguese = row.get('Portuguese')
        keyword_polish = row.get('Polish')
        keyword_danish = row.get('Danish') 
        keyword_italian = row.get('Italian')
        keyword_arabic = row.get('Arabic')
        keyword_greek = row.get('Greek')
        keyword_dutch = row.get('Dutch')
        keyword_latvian = row.get('Latvian')

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
                    "keyword_english" : keyword_english,
                    "keyword_german" : keyword_german,
                    "keyword_spanish" : keyword_spanish,
                    "keyword_portuguese" : keyword_portuguese,
                    "keyword_polish" : keyword_polish,
                    "keyword_danish" : keyword_danish,
                    "keyword_italian" : keyword_italian,
                    "keyword_arabic" : keyword_arabic,
                    "keyword_greek" : keyword_greek,
                    "keyword_dutch" : keyword_dutch,
                    "keyword_latvian" : keyword_latvian,
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