from openai import OpenAI
import os
import logging
import csv
import json
import asyncio

from postgres.schemas.models import create_tables, get_sitemap, connect_to_db
from postgres.insert_data import save_to_pg
from typing import List
from dataclasses import dataclass
from asyncio import Semaphore
import pandas as pd
import glob

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# Semaphore(5) gives 30 out of 1000 :429 errors
semaphore = Semaphore(4)  # Limit the number of concurrent API calls

# get from env variable
SCW_SECRET_KEY = os.getenv("SCW_SECRET")
SCW_API_URL = os.getenv("SCW_API_URL")
# https://www.scaleway.com/en/docs/ai-data/generative-apis/reference-content/rate-limits/
client = OpenAI(
    base_url=SCW_API_URL, # # Scaleway's Generative APIs service URL
    api_key=SCW_SECRET_KEY # Your unique API secret key from Scaleway
)

class Claim:
  def __init__(self, claim:str, note:int, sentiment:str, theme:str,keyword_id:str):
    self.claim = claim
    self.note = note
    self.sentiment = sentiment
    self.theme = theme
    self.keyword_id = keyword_id

def read_csv(file_path: str)-> list:
    logging.info("Reading csv")
    data = []
    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)

    return data
    
def save_to_csv(file_path: str, data: list[Claim]):
    logging.warning(f"Saving into csv {len(data)} claims")
    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerow(["claim", "note", "sentiment", "theme", "keyword_id"])
        for claim in data:
            writer.writerow([claim.claim, claim.note, claim.sentiment, claim.theme, claim.keyword_id])

async def get_claim_sentiment_async(plaintext: str, keyword: str):
    prompt = f"""
                    Tu es un expert des enjeux climat, énergie, et crise de la biodiversité.
                    Les contenus ci-dessous proviennent de transcription imparfaite en Speech to text de contenus médiatique Télévision ou Radio.
                    Dans cet extrait, il est fait mention du mot clé {keyword}.
                    Identifie les différentes claims relatives autour de ce mot clé ({keyword}), et attribue une note de 1 à 5
                    1 étant un contenu positif à l'égard des écologistes, par exemple soutenant l'importance de défendre la cause écologique.
                    3 étant un contenu neutre, présentant un invité par exemple ou le fait que des écologistes se soient exprimés.
                    5 étant un contenu à charge, diffamant, voire criminalisant, par exemple dramatisant des actions violentes, ou expliquant que l'écologie est une religion, voire un risque démocratique.
                    En rajoutant le sentiment principal autour de l'utilisation de ce mot clé avec le champs "sentiment".
                    Et en rajoutant le theme principal de la claim avec le champs "theme".
                    Tu ne réponds que sur ce format json sans aucun commentaire car c'est un programme python qui lit la réponse, et en échappant les caractères spéciaux dans l'objet "claims" comme les guilletmets avec \" :
                    {{
                    "claims": [
                        {{
                        "claim": "les écologistes veulent la décroissance et le déclin de la civilisation",
                        "note": 5,
                        "sentiment": "négatif",
                        "theme": "décroissance"
                        }},
                        {{
                        "claim": "les écologistes violent qui détruisent des méga bassines",
                        "note": 5,
                        "sentiment": "ironique",
                        "theme": "violence"
                        }},
                        {{
                        "claim": "vous êtes représentante des écologistes",
                        "note": 3,
                        "sentiment": "neutre",
                        "theme": "politique"
                        }},
                        {{
                        "claim": "les écologistes qui se battent pour la défense de l'environnement et la protection de la santé de nos concitoyens",
                        "note": 1,
                        "sentiment": "positif",
                        "theme": "militantisme"
                        }},
                        {{
                        "claim": "l'écologiste Marie Toussaint estime que le discours d'Emmanuel Macron promeut un \"grand productivisme vert\" qui ignore les limites de la planète",
                        "note": 1,
                        "sentiment": "négatif",
                        "theme": "écologie"
                        }}
                        ]
                    }}
    """
    try:
        async with semaphore:
            completion = await asyncio.to_thread(
                client.chat.completions.create,
                model="llama-3.1-70b-instruct",
                messages=[
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": f"le mot clé est {keyword} et l'extrait à analyser {plaintext}"}
                ],
                temperature=0.7,
                max_tokens=500
            )
            logging.info(f"API call completed for {plaintext[0:20]}...")
            return completion.choices[0].message.content
    except Exception as e:
        logging.info(f"Error during API call: {e}")
        return None

async def process_batch(batch: List[dict], keyword: str) -> List[Claim]:
    """Processes a batch of rows with parallel API calls."""
    tasks = [
        asyncio.create_task(
            get_claim_sentiment_async(row["plaintext"], keyword)
        )
        for row in batch
    ]
    results = await asyncio.gather(*tasks)

    claims = []
    for response, row in zip(results, batch):
        if response:
            claims.extend(parse_json_response_llm_to_claim(response, row["id"]))
    return claims


async def process_batches(data: List[dict], batch_size: int, keyword: str, output_dir: str):
    """Processes data in batches, saving the results to separate CSV files."""
    total_len = len(data)
    for start_index in range(0, len(data), batch_size):
        batch = data[start_index:start_index + batch_size]
        logging.info(f"Processing batch {start_index} to {start_index + len(batch)}... out of {total_len}")
        claims = await process_batch(batch, keyword)
        output_file = f"{output_dir}/claims-{start_index}.csv"
        save_to_csv(output_file, claims)
        logging.info(f"Batch {start_index} saved with {len(claims)} claims.")
        
###
# {
# "claims": [
# {
# "claim": "c' est quand même très écologique tout ça",
# "note": 1,
# "sentiment": "positif",
# "theme": "écologie urbaine"
# },
# {
# "claim": "la ville de lyon et très écologiste est en fête des lumières",
# "note": 1,
# "sentiment": "positif",
# "theme": "politique environnementale"
# }
# ]
# }
def parse_json_response_llm_to_claim(response: str, keyword_id:str)-> dict:
    logging.info(f"Parse response {response}")
    if response is not None:
        try:
            response = json.loads(response)
            logging.info(f"Length responses {response}")
            output = []
            # parse it to Claim
            for claim in response["claims"]:
                logging.info(f"claim : {claim}")
                claim_parsed = Claim(claim["claim"], claim["note"], claim["sentiment"], claim["theme"], keyword_id)
                output.append(claim_parsed)

            return output
        except Exception as e:
            logging.error(f"Error during parsing response: {e}")
            return []
    else:
        logging.warning(f"Empty response - ignore keyword_id: {keyword_id}")
        return []

# TODO store in PG instead of CSV
# use for now head -n 1 file1.csv > combined.out && tail -n+2 -q *.csv >> combined.out.csv
def save_csv_to_pg():
    path = "/app/llm/output/"
    all_files = glob.glob(os.path.join(path , "*.csv"))
    logging.info(f"Reading all_files {all_files}")
    li = []

    for filename in all_files:
        logging.info(f"Reading file {filename}")
        df = pd.read_csv(filename, header=1)
        li.append(df)

    frame = pd.concat(li, axis=0, ignore_index=True)
    conn = connect_to_db()
    save_to_pg(frame, "claims", conn)
    conn.close()


if __name__ == "__main__":
    file_path = "llm/ecologist_claims_raw_data_april_2023_dec_2024.csv"
    output_dir = "llm/output"
    batch_size = 500  # Batch size
    keyword = "écologiste"

    data = read_csv(file_path)
    asyncio.run(process_batches(data, batch_size, keyword, output_dir))

    logging.info("Processing completed.")

