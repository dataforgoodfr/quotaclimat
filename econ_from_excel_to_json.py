import json
import pandas as pd

input_file_path = 'C:/Users/RaphaëldiPIAZZA/OneDrive - eleven/0_ADMIN/EXTERNE/Baromètre/eco/mots_cles_econ.xlsx'
output_file_path = 'keywords_economie.json'

df = pd.read_excel(input_file_path)

KEY_WORDS_ECONOMIE = []

for index, row in df.iterrows():
    theme_name = 'economie'
    if pd.isna(row['Thème']):
        row['Thème'] = ''
    cur_dict = {
        'keyword': row['Mot clé']
        , 'category': row['Thème']
    }
    KEY_WORDS_ECONOMIE.append(cur_dict)

with open(output_file_path, 'w', encoding='utf-8') as file:
    json.dump(KEY_WORDS_ECONOMIE, file, ensure_ascii=False, indent=4)

print(f"Keywords written in {output_file_path}")