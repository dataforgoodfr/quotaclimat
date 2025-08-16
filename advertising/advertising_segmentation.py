# %%

import json
import re

import dateparser
import openai
import pandas as pd
from tqdm import tqdm

# api_key = 'placeholder'

with open("secrets/api_key.txt") as f:
    api_key = f.read().strip()

# %%

openai_client = openai.OpenAI(api_key=api_key)

# %%

prompt = """Tu es un assistant qui lit des transcriptions de séquences publicitaires provenant de la télévision française.

    Chaque séquences peut contenir une ou plusieurs publicités. Ton rôle est de découper les différentes publicités.

    Tu peux quand c'est possible extraire le topic parmi :

    - Actions Humanitaires
    - Agriculture-Jardinage
    - Alimentation
    - Ameublement-Décoration
    - Appareils Ménagers
    - Audiovisuel Photo Cinema
    - Batiments Travaux Publics
    - Boissons
    - Automobile Transport
    - Corporate
    - Culture et Loisirs
    - Distribution
    - Edition
    - Education
    - Energie
    - Entretien
    - ETS Financiers Assurance
    - Hygiène beauté
    - Immobilier
    - Industrie
    - Information media
    - Informatique bureautique
    - Mode et accessoires
    - Santé
    - Sécurité
    - Service
    - Télécommunication
    - Tourisme-Restauration
    - Autre
    
    Si la séquence n'est pas une publicité mais le contenu télévision / radio non pub, mets "Information" comme topic.

    Idéalement, extrait également le product.

    Je veux un output au format json suivant :

    { text : ,
    topic : ,
    product:
    }"""

prompt_2 = """
Tu es un expert en analyse de séquences publicitaires issues de transcriptions ASR de la télévision française.

Chaque séquence peut contenir une ou plusieurs publicités. 
Ton rôle est de découper la séquence en segments correspondant à une publicité unique, et pour chaque segment :
- Déterminer le topic parmi la liste ci-dessous.
- Identifier le produit principal (product) s'il est clairement mentionné.
- Reproduire textuellement le contenu du segment (tel qu'il apparaît dans la transcription).

Si un segment correspond à du contenu TV/radio non publicitaire (journal, documentaire, bande-annonce, etc.), 
alors mets "Information" comme topic et product = null.

Topics possibles :
- Actions Humanitaires
- Agriculture-Jardinage
- Alimentation
- Ameublement-Décoration
- Appareils Ménagers
- Audiovisuel Photo Cinema
- Batiments Travaux Publics
- Boissons
- Automobile Transport
- Corporate
- Culture et Loisirs
- Distribution
- Edition
- Education
- Energie
- Entretien
- ETS Financiers Assurance
- Hygiène beauté
- Immobilier
- Industrie
- Information media
- Informatique bureautique
- Mode et accessoires
- Santé
- Sécurité
- Service
- Télécommunication
- Tourisme-Restauration
- Autre
- Information  (pour contenu non publicitaire)

IMPORTANT :
- Ne pas inventer de marques ou produits s'ils ne sont pas dans le texte.
- Ne jamais produire de texte hors du JSON demandé.
- Le JSON doit être un tableau de plusieurs objets si la séquence contient plusieurs segments.

Format de sortie attendu strictement :

[
  {
    "text": "...",
    "topic": "...",
    "product": "..." 
  },
  {
    "text": "...",
    "topic": "...",
    "product": "..."
  }
]

Si un champ n'est pas connu, mettre null.
"""


def merge_joint_ads_chunks(df:pd.DataFrame):
    """
    Merge following chunks containing ads (2 minutes close)
    """
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

def split_ads(plaintext:str, prompt:str, model_name:str):
    """
    # Step 3: Split each merged plaintext into separate ads using an LLM
    """


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

def apply_models(df:pd.DataFrame, prompt:str, model_name:str):
    '''
    Model name can be "gpt-4.1-mini" or "gpt-4o"
    '''

    expanded_rows = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Splitting ads"):
        ads = split_ads(row['plaintext'], prompt, model_name)
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

def find_positions(row):
    full_text = row['plaintext']
    snippet = row['text']
    
    start = full_text.find(snippet)
    end = start + len(snippet) if start != -1 else -1
    
    return pd.Series({'start': start, 'end': end})


# %%

def main():
    df = pd.read_csv('data/labelled_ads_france3.csv', index_col=0)
    merged_df = merge_joint_ads_chunks(df)
    transformed_df = apply_models(merged_df, prompt_2, model_name="gpt-4o")
    final_df = flatten_output_dataframe(transformed_df)
    final_df = final_df.merge(df, on=['channel_title', 'start'], how='left')
    final_df[['start', 'end']] = final_df.apply(find_positions, axis=1)
    final_df.rename(columns={'topic':'label'}, inplace=True)
    final_df.to_csv('labellisation_4o-prompt2.csv')

if __name__ == "__main__":
    main()