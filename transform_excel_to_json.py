# to generate keywords.py file
import json
import logging

import pandas as pd

from postgres.schemas.models import Keyword_Macro_Category
from typing import Union


output_file = "quotaclimat/data_processing/mediatree/keyword/keyword.py"
output_file_macro_category = (
    "quotaclimat/data_processing/mediatree/keyword/macro_category.py"
)


DICTIONARY_COLUMNS = {
    "french": ["keyword", "Catégories", "HRFP", "Crise", "Secteur"],
    "english": ["keyword", "Categories", "HRFP", "Crisis", "Sector"],
}
DICTIONARY_CATEGORIES = {
    "french": ["Climat", "Biodiversité", "Ressources"],
    "english": ["Climate", "Biodiversity", "Resources"],
}

MACROCATEGORY_COLUMNS = {
    "french": [
        "keyword",
        "is_empty",
        "general",
        "agriculture",
        "transport",
        "batiments",
        "energie",
        "industrie",
        "eau",
        "ecosysteme",
        "economie_ressources",
    ],
    "english": [
        "keyword",
        "is_empty",
        "general",
        "agriculture",
        "transport",
        "buildings",
        "energy",
        "industry",
        "water",
        "ecosystem",
        "resource_economy",
    ],
}

THEME_MAP = {
    "biodiversity_general_concepts": "biodiversite_concepts_generaux",
    "climate_change_causes": "changement_climatique_causes",
    "biodiversity_solutions": "biodiversite_solutions",
    "natural_resources_solutions": "ressources_solutions",
    "climate_change_consequences": "changement_climatique_consequences",
    "climate_adaptation_solutions": "adaptation_climatique_solutions",
    "climate_change_assessment": "changement_climatique_constat",
    "biodiversity_causes": "biodiversite_causes",
    "natural_resources": "ressources",
    "biodiversity_consequences": "biodiversite_consequences",
    "climate_mitigation_solutions": "attenuation_climatique_solutions",
}

CATEGORY_MAP = {
    "Transport": "Transport",
    "Buildings": "Batiments",
    "Energy": "Energie",
    "Industry": "Industrie",
    "Agriculture": "Agriculture",
    "Water": "Eau",
    "Air": "Air",
    "Soils": "Sols",
    "Metals and minerals": "Métaux et minerais",
    "Forest": "Forêt",
    "Ecosystem": "Ecosystème",
    "Circular economy": "Economie circulaire",
    "Resource economy": "Economie des ressources",
    "Changes in practices and regulations": "Changements de pratiques et de réglementations",
    "Conservation and protection action (preventive solutions)": "Action de conservation et de protection (solutions préventives)",
    "Restoration action (remedial solution)": "Action de restauration (solution réparatrice)",
    "Climate change": "Changement climatique",
    "Habitat destruction": "Destruction des habitats",
    "Invasive alien species": "Espèces exotiques envahissantes",
    "Resource overexploitation": "Surexploitation des ressources",
    "Pollution": "Pollution",
    "General concepts": "Concepts généraux",
    "General": "General"
}


class TranslatedKeyword:
    def __init__(self, language: str, keyword: str):
        self.language = language
        self.keyword = keyword


class DictionaryProcessor:
    def __init__(
        self,
        source: str,
        sheet_name: str,
        file_language: str,
        dictionary_language: str,
    ):
        df = pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl")
        self.df = df[DICTIONARY_COLUMNS[file_language]]
        self.file_language = file_language
        self.theme_column = DICTIONARY_COLUMNS[file_language][1]
        self.dictionary_language = dictionary_language

    def process(self):
        THEME_KEYWORDS = {}
        for idx, row in self.df.iterrows():
            category = row.get(DICTIONARY_COLUMNS[self.file_language][-1], "")
            category = "" if pd.isna(category) else category
            crisis_climate = (
                row[DICTIONARY_COLUMNS[self.file_language][3]]
                == DICTIONARY_CATEGORIES[self.file_language][0]
            )
            crisis_biodiversity = (
                row[DICTIONARY_COLUMNS[self.file_language][3]]
                == DICTIONARY_CATEGORIES[self.file_language][1]
            )
            crisis_resource = (
                row[DICTIONARY_COLUMNS[self.file_language][3]]
                == DICTIONARY_CATEGORIES[self.file_language][2]
            )
            high_risk_of_false_positive = row["HRFP"] == 1.0
            solution = "solution" in row[self.theme_column]
            consequence = "consequence" in row[self.theme_column]
            cause = "cause" in row[self.theme_column]
            general_concepts = "concepts_generaux" in row[self.theme_column]
            statement = "constat" in row[self.theme_column]
            theme_name = row[self.theme_column]

            if self.file_language == "english":
                theme_name = THEME_MAP.get(theme_name, theme_name)
                category = CATEGORY_MAP.get(category, category)
            
            if high_risk_of_false_positive and not theme_name.endswith("_indirectes"):
                theme_name = theme_name + "_indirectes"

            if theme_name not in THEME_KEYWORDS:
                print(f"Found new theme {theme_name}")
                THEME_KEYWORDS[theme_name] = []
            THEME_KEYWORDS[theme_name].append(
                {
                    "keyword": row["keyword"],
                    "category": category,
                    "high_risk_of_false_positive": high_risk_of_false_positive,
                    "crisis_climate": crisis_climate,
                    "crisis_biodiversity": crisis_biodiversity,
                    "crisis_resource": crisis_resource,
                    "solution": solution,
                    "consequence": consequence,
                    "cause": cause,
                    "general_concepts": general_concepts,
                    "statement": statement,
                    "language": self.dictionary_language,  # based on variable name
                }
            )
        return THEME_KEYWORDS


class MacroCategoryProcessor:
    def __init__(
        self, source: str, sheet_name: str, file_language: str, dictionary_language: str
    ):
        df = pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl")
        self.df = df[MACROCATEGORY_COLUMNS[file_language]]
        self.file_language = file_language
        self.dictionary_language = dictionary_language

    def process(self):
        records = []
        self.df["keyword"] = self.df["keyword"].str.strip()
        df = self.df.drop_duplicates()
        for _, row in df.iterrows():
            keyword = row["keyword"]
            if pd.isna(keyword) or keyword.startswith("#"):
                continue
            record = dict(
                keyword=row["keyword"].lower().strip(),  # required
                is_empty=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][1], False)
                ),  # is_empty
                general=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][2], False)
                ),  # general
                agriculture=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][3], False)
                ),  # agriculture
                transport=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][4], False)
                ),  # transport
                batiments=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][5], False)
                ),  # batiments
                energie=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][6], False)
                ),  # energie
                industrie=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][7], False)
                ),  # industrie
                eau=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][8], False)
                ),  # eau
                ecosysteme=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][9], False)
                ),  # ecosysteme
                economie_ressources=bool(
                    row.get(MACROCATEGORY_COLUMNS[self.file_language][10], False)
                ),  # economie_ressources
                language=self.dictionary_language,
            )
            records.append(record)
        return records


class i18nDictionaryProcessor:
    def __init__(self, source: str, sheet_name: Union[str, int]):
        self.french = "French"
        self.english = "English"
        self.german = "German"
        self.spanish = "Spanish"
        self.portuguese = "Portuguese"
        self.polish = "Polish"
        self.danish = "Danish"
        self.italian = "Italian"
        self.arabic = "Arabic"
        self.greek = "Greek"
        self.dutch = "Dutch"
        self.latvian = "Latvian"
        self.df = pd.read_excel(source, sheet_name=sheet_name, engine="openpyxl")

    def process_i8n_dict(self, df_i8n: pd.DataFrame):
        # split the dataframe into portuguese and non-portuguese
        df_portuguese = df_i8n[["Portuguese", "HRFP_Portuguese"]].copy()
        df_non_portuguese = (
            df_i8n[
                [
                    col
                    for col in df_i8n.columns
                    if col not in ["Portuguese", "HRFP_Portuguese"]
                ]
            ]
            .copy()
            .dropna()
        )

        df_portuguese["Category_legacy"] = "changement_climatique_constat"
        df_non_portuguese["Category_legacy"] = "changement_climatique_constat"

        df = pd.concat([df_non_portuguese, df_portuguese]).reset_index(drop=True)
        df["Secteur"] = pd.NA
        df["crise"] = pd.NA
        df["category"] = ""
        return df

    def process(self):
        THEME_KEYWORDS = {}
        df = self.process_i8n_dict(self.df)
        for index, row in df.iterrows():
            print(f"Processing row {index + 1} - {row}")
            theme_name = row["Category_legacy"]  # .strip()
            category = row["category"].strip()

            # get for each language the translation, it can be None (pandas return NaN...)
            # TODO i8n: each translated keyword should be independant (we still use metadata of the french translation)
            keyword_english = (
                None
                if pd.isna(row.get(self.english))
                else TranslatedKeyword(self.english.lower(), row.get(self.english))
            )
            keyword_german = (
                None
                if pd.isna(row.get(self.german))
                else TranslatedKeyword(self.german.lower(), row.get(self.german))
            )
            keyword_spanish = (
                None
                if pd.isna(row.get(self.spanish))
                else TranslatedKeyword(self.spanish.lower(), row.get(self.spanish))
            )
            keyword_portuguese = (
                None
                if pd.isna(row.get(self.portuguese))
                else TranslatedKeyword(
                    self.portuguese.lower(), row.get(self.portuguese)
                )
            )
            keyword_polish = (
                None
                if pd.isna(row.get(self.polish))
                else TranslatedKeyword(self.polish.lower(), row.get(self.polish))
            )
            keyword_danish = (
                None
                if pd.isna(row.get(self.danish))
                else TranslatedKeyword(self.danish.lower(), row.get(self.danish))
            )
            keyword_italian = (
                None
                if pd.isna(row.get(self.italian))
                else TranslatedKeyword(self.italian.lower(), row.get(self.italian))
            )
            keyword_arabic = (
                None
                if pd.isna(row.get(self.arabic))
                else TranslatedKeyword(self.arabic.lower(), row.get(self.arabic))
            )
            keyword_greek = (
                None
                if pd.isna(row.get(self.greek))
                else TranslatedKeyword(self.greek.lower(), row.get(self.greek))
            )
            keyword_latvian = (
                None
                if pd.isna(row.get(self.latvian))
                else TranslatedKeyword(self.latvian.lower(), row.get(self.latvian))
            )

            if theme_name not in THEME_KEYWORDS:
                print(f"Adding theme {row['Category_legacy']} - {theme_name}")
                THEME_KEYWORDS[theme_name] = []
            keywords_list = [
                keyword_english,
                keyword_german,
                keyword_spanish,
                keyword_portuguese,
                keyword_polish,
                keyword_danish,
                keyword_italian,
                keyword_arabic,
                keyword_greek,
                keyword_latvian,
            ]
            for translated_keyword in keywords_list:
                if translated_keyword == None:
                    continue

                if translated_keyword.language.lower() != self.portuguese.lower():
                    crisis_climate = False
                    crisis_biodiversity = False
                    crisis_resource = False
                    high_risk_of_false_positive = row["HRFP"] == 1.0
                    solution = False
                    consequence = False
                    cause = False
                    general_concepts = False
                    statement = False

                else:
                    # special HRFP for Portuguese
                    crisis_climate = False
                    crisis_biodiversity = False
                    crisis_resource = False
                    high_risk_of_false_positive = row["HRFP_Portuguese"] == 1.0
                    solution = False
                    consequence = False
                    cause = False
                    general_concepts = False
                    statement = False

                if "#" not in translated_keyword.keyword:
                    THEME_KEYWORDS[theme_name].append(
                        {
                            "keyword": translated_keyword.keyword,
                            "category": category,
                            "high_risk_of_false_positive": high_risk_of_false_positive,
                            "crisis_climate": crisis_climate,
                            "crisis_biodiversity": crisis_biodiversity,
                            "crisis_resource": crisis_resource,
                            "solution": solution,
                            "consequence": consequence,
                            "cause": cause,
                            "general_concepts": general_concepts,
                            "statement": statement,
                            "language": translated_keyword.language,  # based on variable name
                        }
                    )
        return THEME_KEYWORDS


french_processor = DictionaryProcessor(
    source="document-experts/Dictionnaire - OME.xlsx",
    sheet_name="Catégorisation Finale",
    file_language="french",
    dictionary_language="french",
)
dutch_processor = DictionaryProcessor(
    source="document-experts/Dictionary_OME_Dutch.xlsx",
    sheet_name="Final Categorization",
    file_language="english",
    dictionary_language="dutch",
)
i18n_processor = i18nDictionaryProcessor(
    source="document-experts/Dictionnaire_Multilingue.xlsx",
    sheet_name=0,  # "Multilingual_dictionnary_without_HRFP"
)

french_keywords = french_processor.process()
dutch_keywords = dutch_processor.process()
i18n_keywords = i18n_processor.process()
themes = set(
    list(french_keywords.keys())
    + list(dutch_keywords.keys())
    + list(i18n_keywords.keys())
)
theme_keywords = {}
for theme in themes:
    theme_keywords[theme] = sorted(
        french_keywords.get(theme, [])
        + dutch_keywords.get(theme, [])
        + i18n_keywords.get(theme, []),
        key=lambda x: x["keyword"],
    )

with open(output_file, "w", encoding="utf-8") as f:
    logging.info(f"Json written  - {len(theme_keywords)} themes inside {output_file}")
    print(f"Json written {len(theme_keywords)} in {output_file}")
    f.write("# Auto-generated from Excel file\n")
    f.write(
        "THEME_KEYWORDS = "
        + json.dumps(theme_keywords, ensure_ascii=False, indent=4)
        .replace("null", "None")
        .replace("true", "True")
        .replace("false", "False")
    )


french_macro_categories_processor = MacroCategoryProcessor(
    source="document-experts/Dictionnaire - OME.xlsx",
    sheet_name="Catégories Transversales",
    file_language="french",
    dictionary_language="french",
)
dutch_macro_categories_processor = MacroCategoryProcessor(
    source="document-experts/Dictionary_OME_Dutch.xlsx",
    sheet_name="Cross-cutting Categories",
    file_language="english",
    dictionary_language="dutch",
)

french_macro_categories = french_macro_categories_processor.process()
dutch_macro_categories = dutch_macro_categories_processor.process()

macro_categories = french_macro_categories + dutch_macro_categories
with open(output_file_macro_category, "w", encoding="utf-8") as f:
    logging.info(
        f"Json written  - {len(macro_categories)} keywords inside {output_file_macro_category}"
    )
    print(f"Json written {len(macro_categories)} in {output_file_macro_category}")
    f.write("# Auto-generated from Excel file\n")
    f.write(
        "MACRO_CATEGORIES = "
        + json.dumps(macro_categories, ensure_ascii=False, indent=4)
        .replace("null", "None")
        .replace("true", "True")
        .replace("false", "False")
    )
