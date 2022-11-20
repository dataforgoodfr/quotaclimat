"""
@author: Pierre-Loic
"""

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

def extract_COP_occurences(path="D:\__dossier_essais\DataForGood\data\df_3medias.csv"):
    """
    Get date, media and COP number when COP is mentioned in the description
    Returns:
            cops (pandas.DataFrame): dataframe with date, media and COP number
    """
    
    df = pd.read_csv(path)
    interesting_columns = [
        "title", "description", "date", "media",
    ]
    
    # Clean dataframe
    df = df[interesting_columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=['description'])
    
    # Vectorize descriptions
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(df["description"])
    
    # Select only description with COPXX (from 21 to 27) in it
    cops = pd.DataFrame(columns=["date", "media", "cop"])
    for nb_cop in range(21, 28):
        temp_df = df[["date", "media"]].iloc[X[:,vectorizer.vocabulary_[f"cop{nb_cop}"]].nonzero()[0]]
        temp_df["cop"] = nb_cop
        cops = pd.concat([cops, temp_df])
    
    return cops