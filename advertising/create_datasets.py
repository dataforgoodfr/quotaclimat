#%%

import pandas as pd

filename_ads = 'data/raw/keywords_ads_all.csv'

# At this stage, the source is a csv file, coming from Metabase SQL, accessible here:
# https://barometre7kfudatm-metabase-barometre.functions.fnc.fr-par.scw.cloud/model/969-pub-x-keywords-2025
# It shall become a Mediatree api query to collect the selected days
# Hence it will incorporate the SQL query to join between the Advertising list and the Mediatree query output

filename_keywords = 'data/raw/base_keywords_all.csv'

# This file is a manual extration from Keywords on Metabase

df_ads = pd.read_csv(filename_ads, sep=';')
df = pd.read_csv(filename_keywords)

df = df[['Channel Title', 'Start', 'Plaintext', 'Number Of Keywords']]
mapper = dict(zip(df.columns.tolist(), df_ads.columns.tolist()))

df_ads['ads_ground_truth'] = True
df = df.rename(columns=mapper)

df = pd.concat([df, df_ads]).sort_values(by='plaintext')

df['ads_ground_truth'] = df['ads_ground_truth'].fillna(False)  

df = df.sort_values(by='ads_ground_truth', ascending=False)

df = df.drop_duplicates(subset=['channel_title', 'start'], keep='first')

df.to_csv('data/intermediate/labeled_ads_dataset.csv')
