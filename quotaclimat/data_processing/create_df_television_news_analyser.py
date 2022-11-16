# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 18:57:23 2022

@author: Elise
"""

import os
import pandas as pd
from tqdm.notebook import tqdm

def create_csv(path_qclimat="C:/Users/elise/Documents/Data for good/Quota climat/"):
    path_data_tele = os.path.join(path_qclimat, "data", "television-news-analyser", "data-news-json")
    
    data_medias = pd.DataFrame(columns=[
           'title', 'description', 'date', 'order', 'presenter',
           'authors', 'editor', 'editorDeputy', 'url', 'urlTvNews',
           'containsWordGlobalWarming','media','year','month','day',
    ])

    #lire les fichiers de chaque dossier
    list_medias = os.listdir(path_data_tele)
    for media in tqdm(list_medias):
        list_year = os.listdir(os.path.join(path_data_tele, media))
        for year in tqdm(list_year):
            list_month = os.listdir(os.path.join(path_data_tele, media, year))
            for month in list_month:
                list_day = os.listdir(os.path.join(path_data_tele, media, year, month))
                for day in list_day:

                    #ouvrir le json et en faire un dataframe
                    file = os.listdir(os.path.join(path_data_tele, media, year, month, day))
                    df = pd.read_json(os.path.join(path_data_tele, media, year, month, day, file[0]), lines=True)

                    #ajouter les infos du média et de la date
                    df['media'] = media[6:] #enlever "media="
                    df['year'] = year[5:]
                    df['month'] = month[6:]
                    df['day'] = day[4:]
                    
                    #pour résoudre un futurewarning concernant une colonne avec uniquement des booléens
                    df["containsWordGlobalWarming"] = df["containsWordGlobalWarming"].astype("boolean")
                    
                    #ajouter les données de ce fichier json au dataframe global
                    data_medias = pd.concat([data_medias,df])

    data_medias.to_csv(os.path.join(path_qclimat, 'data', 'df_3medias.csv'),index=False)