# %%

import pandas as pd
import openai
import json
import re
from tqdm import tqdm
import dateparser

# api_key = 'placeholder'

with open("secrets/api_key.txt") as f:
    api_key = f.read().strip()

# %%

openai_client = openai.OpenAI(api_key=api_key)

# %%

def merge_joint_ads_chunks(source:str):
    """
    Merge following chunks containing ads (2 minutes close)
    """

    df = pd.read_csv(source, sep=';')

    # Convert start to datetime if it's not already

    df['start'] = df['start'].apply(lambda x: dateparser.parse(x, languages=["fr"]))

    # Step 2: Merge rows with same channel_title and near-identical start times (within 2 min)
    df.sort_values(by=['channel_title', 'start'], inplace=True)
    merged_rows = []
    i = 0

    while i < len(df):
        current_row = df.iloc[i]
        j = i + 1
        merged_text = current_row['plaintext']
        
        while j < len(df):
            next_row = df.iloc[j]
            if (current_row['channel_title'] == next_row['channel_title'] and
                0 <= (next_row['start'] - current_row['start']).total_seconds() <= 120):
                merged_text += " " + next_row['plaintext']
                j += 1
            else:
                break
        
        merged_rows.append({
            'channel_title': current_row['channel_title'],
            'start': current_row['start'],
            'plaintext': merged_text.strip()
        })
        
        i = j

    return pd.DataFrame(merged_rows)

def split_ads(plaintext:str, model_name:str = "gpt-4.1-mini"):
    """
    # Step 3: Split each merged plaintext into separate ads using an LLM
    """

    prompt = """Tu es un assistant qui lit des transcriptions de séquences publicitaires provenant de la télévision française.

    Chaque séquences peut contenir une ou plusieurs publicités. Ton rôle est de découper les différentes publicités.

    Tu peux quand c'est possible extraire le topic parmi :

    - Energie
    - Corporate
    - Appareils Ménagers
    - Automobile Transport
    - Culture & Loisirs
    - Alimentation
    - Mode et accessoires
    - Hygiène beauté
    - Distribution
    - Ameublement-Décoration
    - Pharmacie-Médecine
    - Télécommunication
    - Voyage-Tourisme
    - Informatique bureautique
    - Immobilier

    Si la séquence n'est pas une publicité mais le contenu télévision / radio non pub, mets "Information" comme topic.

    Idéalement, extrait également le product.

    Je veux un output au format json suivant :

    { text : ,
    topic : ,
    product:
    }"""

    try:
        response = openai_client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": plaintext}
            ],
            temperature=0,
            max_tokens=2000
        )
        return [response.choices[0].message.content]
    except Exception as e:
        print(f"Error processing text: {e}")
        return [plaintext]

def apply_models(df:pd.DataFrame):

    expanded_rows = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Splitting ads"):
        ads = split_ads(row['plaintext'])
        for ad in ads:
            expanded_rows.append({
                'channel_title': row['channel_title'],
                'start': row['start'],
                'output': ad
            })
    
    return pd.DataFrame(expanded_rows)

def flatten_output_dataframe(df:pd.DataFrame):
    """
    Read the json output from the LLM
    """
    rows = []

    for _, row in df.iterrows():
        output_str = row['output']
        
        # Nettoyage : enlever les balises ```json ... ```
        cleaned_output = re.sub(r"^```json\n|```$", "", output_str.strip(), flags=re.MULTILINE).strip()

        try:
            # Tenter un parsing JSON
            items = json.loads(cleaned_output)

            # Si items est une chaîne, alors le JSON était doublement encodé
            if isinstance(items, str):
                items = json.loads(items)

            # S'assurer que c'est une liste de dictionnaires
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        rows.append({
                            'channel_title': row['channel_title'],
                            'start': row['start'],
                            'text': item.get('text', None),
                            'topic': item.get('topic', None),
                            'product': item.get('product', None)
                        })
                    else:
                        print(f"⚠️ Élément ignoré (pas un dict) : {item}")
            else:
                print(f"⚠️ Output non-list pour start={row['start']}")

        except json.JSONDecodeError as e:
            print(f"❌ Erreur JSON à la ligne {row['start']}: {e}")

    return pd.DataFrame(rows)

# %%

# Script to apply a specified model and segment ads

source = "data/raw/keywords_ads_all.csv"

# At this stage, the source is a csv file, coming from Metabase SQL, accessible here:
# https://barometre7kfudatm-metabase-barometre.functions.fnc.fr-par.scw.cloud/model/969-pub-x-keywords-2025
# It shall become a Mediatree api query to collect the selected days
# Hence it will incorporate the SQL query to join between the Advertising list and the Mediatree query output

merged_df = merge_joint_ads_chunks(source)

initial_sample = merged_df[merged_df.channel_title == 'France 3-idf']

# %%

# You might want to apply the model only to a month of data, to limit excessive costs (mini: 1€ / month x media)

transformed_df = apply_models(initial_sample)

final_df = flatten_output_dataframe(transformed_df)

final_df.to_csv("data/processed_ads_with_4-1_mini.csv", index=False)
