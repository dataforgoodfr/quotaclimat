# to generate keywords.py file
import pandas as pd
import json

excel_file_path = "cc-biodiv.xlsx"

df = pd.read_excel(excel_file_path, sheet_name='Catégorisation Finale')
df['category'] = df['category'].fillna('')
# Initialize the THEME_KEYWORDS dictionary
THEME_KEYWORDS = {}

# Iterate over the rows of the DataFrame
for index, row in df.iterrows():
    theme_name = row['theme']
    keyword = row['keyword']
    category = row['category']

    # Check if the theme_name already exists in THEME_KEYWORDS
    if theme_name not in THEME_KEYWORDS and "ressources" not in theme_name:
        THEME_KEYWORDS[theme_name] = []

    # filter # keywor
    if "#" not in keyword and "ressources" not in theme_name:
        THEME_KEYWORDS[theme_name].append({"keyword": keyword, "category": category})


# Convert the THEME_KEYWORDS dictionary to a JSON string
json_string = json.dumps(THEME_KEYWORDS, ensure_ascii=False, indent=4)

# Print the JSON string with UTF-8 encoding
print(json_string.encode('utf-8').decode())