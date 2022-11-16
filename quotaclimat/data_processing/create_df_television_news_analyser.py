# -*- coding: utf-8 -*-
"""
Created on Wed Nov 16 18:57:23 2022

@author: Elise
"""

import pandas as pd
import os
import json

path_qclimat = "C:/Users/elise/Documents/Data for good/Quota climat/"
path_data_tele = path_qclimat + "data/television-news-analyser-main/data-news-json/"


data_medias = pd.DataFrame(columns=['title', 'description', 'date', 'order', 'presenter', 'authors',
       'editor', 'editorDeputy', 'url', 'urlTvNews',
       'containsWordGlobalWarming','media','year','month','day'])

#lire les fichiers de chaque dossier
list_medias = os.listdir(path_data_tele)
for media in list_medias:
    list_year = os.listdir(path_data_tele + media + '/')
    for year in list_year:
        list_month = os.listdir(path_data_tele + media + '/' + year + '/')
        for month in list_month:
            list_day = os.listdir(path_data_tele + media + '/' + year + '/' + month + '/')
            for day in list_day:
                
                #ouvrir le json et en faire un dataframe
                file = os.listdir(path_data_tele + media + '/' + year + '/' + month + '/' + day + '/')
                f = open(path_data_tele + media + '/' + year + '/' + month + '/' + day + '/' + file[0], encoding='utf-8')
                json_file = [json.loads(line) for line in f]
                df = pd.DataFrame.from_dict(json_file, orient='columns')
                
                #ajouter les infos du média et de la date
                df['media'] = media[6:] #enlever "media="
                df['year'] = year[5:]
                df['month'] = month[6:]
                df['day'] = day[4:]
                
                #ajouter les données de ce fichier json au dataframe global
                data_medias = pd.concat([data_medias,df])
                
                
data_medias.to_csv(path_qclimat+'data/df_3medias.csv',index=False)
