# %%

import pandas as pd

def merge_output_from_models(output_model_pathfile_1:str, output_model_pathfile_2:str, media:str = None):
    
    df_1 = pd.read_csv(output_model_pathfile_1)
    df_2 = pd.read_csv(output_model_pathfile_2)

    if media is not None:
        df_1 = df_1[df_1.channel_title == media]
        df_2 = df_2[df_2.channel_title == media]

    df_1['model'] = '4o'
    df_2['model'] = '4.1-mini'

    return pd.concat([df_1, df_2])

# Script to merge two differents outputs from different models for comparison purposes

output_model_pathfile_1 = 'data\processed_ads_2.csv'
output_model_pathfile_2 = 'data\processed_ads_with_4-1_mini.csv'

media_target = 'France 3-idf'

df = merge_output_from_models(output_model_pathfile_1, output_model_pathfile_2, media_target)

df.to_csv('data/processed/merged_with_different_models.csv')