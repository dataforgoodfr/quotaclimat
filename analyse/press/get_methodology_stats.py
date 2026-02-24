import pandas as pd
from sklearn.metrics import classification_report


df = pd.read_csv('data/output/instagram_annotated_samples.csv')

df[[
    "is_changement_climatique_gt",
    "is_changement_climatique_causes_gt",
    "is_changement_climatique_consequences_gt",
    "is_climatique_solutions_gt",
    "is_biodiversite_concepts_generaux_gt",
    "is_biodiversite_causes_gt",
    "is_biodiversite_consequences_gt",
    "is_biodiversite_solutions_gt",
    "is_ressources_gt",
    "is_ressources_solutions_gt",
]] = df [[
    "is_changement_climatique_gt",
    "is_changement_climatique_causes_gt",
    "is_changement_climatique_consequences_gt",
    "is_climatique_solutions_gt",
    "is_biodiversite_concepts_generaux_gt",
    "is_biodiversite_causes_gt",
    "is_biodiversite_consequences_gt",
    "is_biodiversite_solutions_gt",
    "is_ressources_gt",
    "is_ressources_solutions_gt",
]].applymap(lambda x: True if x=='yes' else False)

df["is_climatique"] = df[[
    "is_adaptation_climatique_solutions",
    "is_attenuation_climatique_solutions",
    "is_changement_climatique_constat",
    "is_changement_climatique_causes",
    "is_changement_climatique_consequences",
]].max(axis=1)
df["is_climatique_gt"] = df[[
    "is_changement_climatique_gt",
    "is_changement_climatique_causes_gt",
    "is_changement_climatique_consequences_gt",
    "is_climatique_solutions_gt",
]].max(axis=1)

df["is_biodiversite"] = df[[
    "is_biodiversite_concepts_generaux",
    "is_biodiversite_causes",
    "is_biodiversite_consequences",
    "is_biodiversite_solutions",
]].max(axis=1)
df["is_biodiversite_gt"] = df[[
    "is_biodiversite_concepts_generaux_gt",
    "is_biodiversite_causes_gt",
    "is_biodiversite_consequences_gt",
    "is_biodiversite_solutions_gt",

]].max(axis=1)


df["is_ressources"] = df[[
    "is_ressources",
    "is_ressources_solutions",
]].max(axis=1)
df["is_ressources_gt"] = df[[
    "is_ressources_gt",
    "is_ressources_solutions_gt",
]].max(axis=1)

df["is_environment"] = df[[
    "is_climatique",
    "is_biodiversite",
    "is_ressources",
]].max(axis=1)

df["is_environment_gt"] = df[[
    "is_climatique_gt",
    "is_biodiversite_gt",
    "is_ressources_gt",
]].max(axis=1)

categories = [
    "is_climatique",
    "is_biodiversite",
    "is_ressources",
]

for category in categories:
    print("Analysing: "+category)
    print(classification_report(df[category+"_gt"], df[category]))

category = "is_environment"
print("Analysing: "+category)
print(classification_report(df[category+"_gt"], df[category]))