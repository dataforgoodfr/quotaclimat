# to generate keywords.py file
import pandas as pd
import json
import logging
from quotaclimat.data_processing.mediatree.i8n.country import *

# Need to import these files - slack #metabase-keywords
excels_files = ["document-experts/Dictionnaire - OME.xlsx"]
i8n_dictionary = "document-experts/Dictionnaire_Multilingue.xlsx"
output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"

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

# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}
for excel_file_path in excels_files:
    print(f"Reading {excel_file_path}")
    df = pd.read_excel(excel_file_path, sheet_name='Catégorisation Finale')
    df = df.dropna(subset=['keyword'])
    i18n_df = pd.read_excel(i8n_dictionary)
    i18n_df[french] = i18n_df[french].str.lower().str.strip()  # Normalize for matching

    df['category'] = df['Secteur'].fillna('')
    df['crise'] = df['Crise'].fillna('') 

    # merge between i8n_dictionary and df based on keyword and French to get German, Spanish etc.
    df = df.merge(i18n_df, how='left', left_on='keyword', right_on='French')
    
    # Iterate over the rows of the DataFrame
    for index, row in df.iterrows():
        
        print(f"Processing row {index + 1} - {row['keyword']}")
        theme_name = row['Category_legacy'] #.strip()
        keyword_french = TranslatedKeyword(french.lower(), row['keyword'].lower().strip())
        category = row['category'].strip()
        print(f"row['HRFP'] is {row['HRFP']} for {keyword_french.keyword} so must be {row['HRFP'] == 1.0}")
        high_risk_of_false_positive = row['HRFP'] == 1.0

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
            print(f"Adding theme {row['Category_legacy']} - {theme_name}")
            THEME_KEYWORDS[theme_name] = []
        keywords_list = [keyword_french, keyword_english, keyword_german, keyword_spanish, keyword_portuguese, keyword_polish, keyword_danish, keyword_italian, keyword_arabic, keyword_greek, keyword_dutch, keyword_latvian]
        # filter # keyword with # (paused or removed)
        for translated_keyword in keywords_list:
            if (translated_keyword == None):
                logging.info(f"No translation for {keyword_french.keyword}")
                continue

            if (translated_keyword.language != french.lower()):
                high_risk_of_false_positive = False

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