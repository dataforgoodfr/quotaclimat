# to generate keywords.py file
import json
import logging

import pandas as pd

from postgres.schemas.models import Keyword_Macro_Category
from quotaclimat.data_processing.mediatree.i8n.country import *

# Need to import these files - slack #metabase-keywords
i8n_dictionary = "document-experts/Dictionnaire_Multilingue_no_HRFP.xlsx"
french_dictionary = "document-experts/Dictionnaire - OME.xlsx"
macro_category_file = "document-experts/Dictionnaire - OME.xlsx - Catégories Transversales.tsv"
excels_files = [french_dictionary, i8n_dictionary]
plain_keyword_sheet = 'Catégorisation Finale'
output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"
output_file_macro_category = "quotaclimat/data_processing/mediatree/keyword/macro_category.py"

# Excel columns - must be lower to match country.py norms
french = 'French'
english = 'English'
german = 'German'
spanish = 'Spanish'
portuguese = 'Portuguese'
polish = 'Polish'
danish = 'Danish'
italian = 'Italian'
arabic = 'Arabic'
greek = 'Greek'
dutch = 'Dutch'
latvian = 'Latvian'

class TranslatedKeyword:   
    def __init__(self, language: str, keyword: str):
        self.language = language
        self.keyword = keyword

def process_i8n_dict(df_i8n : pd.DataFrame):
    print(df_i8n.head())

    # split the dataframe into portuguese and non-portuguese
    df_portuguese = df_i8n[['Portuguese', 'HRFP_Portuguese']].copy()
    df_non_portuguese = df_i8n[[col for col in df_i8n.columns if col not in ['Portuguese', 'HRFP_Portuguese']]].copy().dropna()

    # # apply the HRFP_Portuguese and HRFP to the respective dataframes
    # df_portuguese['Category_legacy'] = df_portuguese['HRFP_Portuguese'].apply(
    #     lambda x: 'changement_climatique_constat' if x == 0 else 'changement_climatique_constat_indirectes'
    # )
    # df_non_portuguese['Category_legacy'] = df_non_portuguese['HRFP'].apply(
    #     lambda x: 'changement_climatique_constat' if x == 0 else 'changement_climatique_constat_indirectes'
    # )

    # TODO: remove this once we want to implement HRFP for i8n
    df_portuguese['Category_legacy'] = 'changement_climatique_constat'
    df_non_portuguese['Category_legacy'] = 'changement_climatique_constat'

    df = pd.concat([df_non_portuguese, df_portuguese]).reset_index(drop=True)
    df['Secteur'] = pd.NA
    df['crise'] = pd.NA
    df['category'] = ''
    
    return df


def set_up_macro_category():
    logging.info(f"Reading macro categories from {macro_category_file}...")

    df = pd.read_csv(macro_category_file, sep='\t', encoding='utf-8')
    logging.info(df.columns)
    records = []
    expected_columns = [
        "keyword", "is_empty", "general", "agriculture", "transport",
        "batiments", "energie", "industrie", "eau", "ecosysteme", "economie_ressources"
    ]

    for _, row in df.iterrows():
        keyword=row["keyword"]
        if pd.isna(keyword) or keyword.startswith("#"):
            continue
        record = Keyword_Macro_Category(
            keyword=row["keyword"].strip(),  # required
            is_empty=bool(row.get("is_empty", False)),
            general=bool(row.get("general", False)),
            agriculture=bool(row.get("agriculture", False)),
            transport=bool(row.get("transport", False)),
            batiments=bool(row.get("batiments", False)),
            energie=bool(row.get("energie", False)),
            industrie=bool(row.get("industrie", False)),
            eau=bool(row.get("eau", False)),
            ecosysteme=bool(row.get("ecosysteme", False)),
            economie_ressources=bool(row.get("economie_ressources", False))
        )
        logging.info(f"Processing record: {record.keyword}")
        records.append(record)

    logging.info(f"Macro categories is being written to {output_file_macro_category}...")
    with open(output_file_macro_category, "w", encoding="utf-8") as f:
        f.write("# Auto-generated from Excel file\n")
        f.write("MACRO_CATEGORIES = [\n")
        for record in records:
            f.write("    {\n")
            for col in expected_columns:
                val = getattr(record, col)
                val_str = f"\"{val}\"" if isinstance(val, str) else str(bool(val))
                f.write(f"        \"{col}\": {val_str},\n")
            f.write("    },\n")
        f.write("]\n")
        logging.info(f"{len(records)} macro categories written to {output_file_macro_category} successfully.")


# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}
for excel_file_path in excels_files:
    print(f"Reading {excel_file_path}")
    if "Dictionnaire - OME" in excel_file_path:
        df = pd.read_excel(excel_file_path, sheet_name=plain_keyword_sheet)
        df = df.dropna(subset=['keyword'])
        df['category'] = df['Secteur'].fillna('')
        df['crise'] = df['Crise'].fillna('')
        df['Category_legacy'] = df['Category_legacy'].fillna('')
    else:
        df = pd.read_excel(i8n_dictionary)
        df = process_i8n_dict(df)
    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        print(f"Processing row {index + 1} - {row}")
        theme_name = row['Category_legacy'] #.strip()
        category = row['category'].strip()
        keyword_french =  None if pd.isna(row.get('keyword')) else TranslatedKeyword(french.lower(),row.get('keyword').lower().strip()) 
        
        # get for each language the translation, it can be None (pandas return NaN...)
        # TODO i8n: each translated keyword should be independant (we still use metadata of the french translation)
        keyword_english = None if pd.isna(row.get(english)) else TranslatedKeyword(english.lower(),row.get(english)) 
        keyword_german = None if pd.isna(row.get(german)) else TranslatedKeyword(german.lower(),row.get(german)) 
        keyword_spanish = None if pd.isna(row.get(spanish)) else TranslatedKeyword(spanish.lower(),row.get(spanish)) 
        keyword_portuguese = None if pd.isna(row.get(portuguese)) else TranslatedKeyword(portuguese.lower(),row.get(portuguese)) 
        keyword_polish = None if pd.isna(row.get(polish)) else TranslatedKeyword(polish.lower(),row.get(polish)) 
        keyword_danish = None if pd.isna(row.get(danish)) else TranslatedKeyword(danish.lower(),row.get(danish))  
        keyword_italian = None if pd.isna(row.get(italian)) else TranslatedKeyword(italian.lower(),row.get(italian)) 
        keyword_arabic = None if pd.isna(row.get(arabic)) else TranslatedKeyword(arabic.lower(),row.get(arabic)) 
        keyword_greek = None if pd.isna(row.get(greek)) else TranslatedKeyword(greek.lower(),row.get(greek)) 
        keyword_dutch = None if pd.isna(row.get(dutch)) else TranslatedKeyword(dutch.lower(),row.get(dutch)) 
        keyword_latvian = None if pd.isna(row.get(latvian)) else TranslatedKeyword(latvian.lower(),row.get(latvian)) 

        # Check if the theme_name already exists in THEME_KEYWORDS
        if(theme_name not in THEME_KEYWORDS):
            print(f"Adding theme {row['Category_legacy']} - {theme_name}")
            THEME_KEYWORDS[theme_name] = []
        keywords_list = [keyword_french, keyword_english, keyword_german, keyword_spanish, keyword_portuguese, keyword_polish, keyword_danish, keyword_italian, keyword_arabic, keyword_greek, keyword_dutch, keyword_latvian]
        # filter # keyword with # (paused or removed)
        for translated_keyword in keywords_list:
            if (translated_keyword == None):
                continue

            if translated_keyword.language.lower() not in [french.lower(), portuguese.lower()]:
                crisis_climate = False
                crisis_biodiversity = False
                crisis_resource = False
                high_risk_of_false_positive = row['HRFP'] == 1.0
                solution = False
                consequence = False
                cause = False
                general_concepts = False
                statement = False

            elif (translated_keyword.language == portuguese.lower()):
                # special HRFP for Portuguese
                crisis_climate = False
                crisis_biodiversity = False
                crisis_resource = False
                high_risk_of_false_positive = row['HRFP_Portuguese'] == 1.0
                solution = False
                consequence = False
                cause = False
                general_concepts = False
                statement = False

            else:
                # metadata only for French keywords
                crisis_climate = row['crise'] == "Climat"
                crisis_biodiversity = row['crise'] == "Biodiversité"
                crisis_resource = row['crise'] == "Ressources"
                high_risk_of_false_positive = row['HRFP'] == 1.0
                solution = "solution" in theme_name
                consequence = "consequence" in theme_name
                cause = "cause" in theme_name
                general_concepts = "concepts_generaux" in theme_name
                statement = "constat" in theme_name

            if ("#" not in translated_keyword.keyword):
                THEME_KEYWORDS[theme_name].append(
                    {
                        "keyword": translated_keyword.keyword,
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
                        "language" : translated_keyword.language # based on variable name
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

    # # Replace JSON boolean values with Python boolean values
    content = content.replace('true,', 'True,')
    content = content.replace('false,', 'False,')

    # Replace JSON null values with Python None values
    content = content.replace('null', 'None')

# Write the modified content back to a Python file
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(content)
    logging.info(f"Python dictionary written to {output_file}")
    print(f"Python dictionary written to {output_file}")

set_up_macro_category()