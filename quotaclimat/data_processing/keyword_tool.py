# Libraries
import os
import random

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import seaborn as sns
from dotenv import load_dotenv
from flashtext import KeywordProcessor
from inflect import engine as inflect_engine
from IPython.display import clear_output, display
from ipywidgets import widgets
from pyairtable import Table
from spacy import displacy
from tqdm.auto import tqdm


class KeywordsTool:
    """Helper tool to manipulate a list of keywords from different sources (file, database, Airtable)"""

    def __init__(self, case_sensitive: bool = False, lowercase=True):

        # Store as arguments
        self.case_sensitive = case_sensitive
        self.inflect = inflect_engine()

        self.lowercase = lowercase

    def load_from_dict(self, keywords: dict):
        def remove_symbol(text):
            return [x.replace("-", " ") for x in text]

        def add_singular_and_plural(text: str):
            new_l = []
            for x in text:
                x_singular = self.inflect.singular_noun(x)
                if x_singular:
                    new_l.append(x_singular)

                x_plural = self.inflect.plural(x)
                if x_plural:
                    new_l.append(x_plural)

            return new_l

        # Process data
        # We add the keywords itself and remove duplicates to list of variants
        self.keywords = keywords
        self.keywords = {k: list(set([k] + v)) for k, v in self.keywords.items()}
        self.keywords = {
            k: list(set(v + remove_symbol(v))) for k, v in self.keywords.items()
        }
        self.keywords = {
            k: list(set(v + add_singular_and_plural(v)))
            for k, v in self.keywords.items()
        }
        self.keywords = {
            k: [x for x in v if len(x) > 2] for k, v in self.keywords.items()
        }

        if self.lowercase:
            self.keywords = {
                k: list(set([x.lower() for x in v])) for k, v in self.keywords.items()
            }

        # Create keywords processor
        self.processor = KeywordProcessor(case_sensitive=self.case_sensitive)
        self.processor.add_keywords_from_dict(self.keywords)

        # Prepare visualization color roulette
        self._prepare_colors()

    def load_from_yaml(self):
        pass

    def load_from_airtable(
        self,
        airtable_api_key=None,
        airtable_base_id=None,
        airtable_table_name=None,
        keyword_col="name",
        variants_col="variants",
    ):

        if airtable_api_key is not None:

            # Load data from Airtable using PyAirtable
            self.airtable = Table(
                airtable_api_key, airtable_base_id, airtable_table_name
            )
            self.data = (
                pd.DataFrame([x["fields"] for x in self.airtable.all()])
                .dropna(subset=[keyword_col])
                .set_index(keyword_col)
            )

            # Handle no variants cases

            def process_col_keywords(col):

                isnull = self.data[col].isnull()
                fillna_value = pd.DataFrame([[[]] * isnull.sum()]).T.values[:, 0]
                self.data.loc[self.data[col].isnull(), col] = fillna_value

            if isinstance(variants_col, list):

                for col in variants_col:
                    process_col_keywords(col)

                self.data["variants_col"] = self.data[variants_col].sum(axis=1)
                self.keywords = self.data["variants_col"].to_dict()

            else:
                process_col_keywords(variants_col)
                self.keywords = self.data[variants_col].to_dict()

            # Load from dict with find dict
            self.load_from_dict(self.keywords)

        else:

            # Load from env file with dotenv
            assert load_dotenv()

            self.load_from_airtable(
                airtable_api_key=os.environ["AIRTABLE_API_KEY"]
                if airtable_api_key is None
                else airtable_api_key,
                airtable_base_id=os.environ["AIRTABLE_BASE_ID"]
                if airtable_base_id is None
                else airtable_base_id,
                airtable_table_name=os.environ["AIRTABLE_TABLE_NAME"]
                if airtable_table_name is None
                else airtable_table_name,
                keyword_col=keyword_col,
                variants_col=variants_col,
            )

        pass

    def save_as_yaml(self):
        pass

    def _prepare_colors(self):

        # Helper function to convert to hexadecimal colors
        convert_hex = lambda r, g, b: "#%02x%02x%02x" % tuple(
            map(lambda x: int(x * 255), (r, g, b))
        )
        concepts = list(self.keywords.keys())

        self.colors = sns.color_palette("husl", len(concepts))
        self.colors = {
            concepts[i]: convert_hex(*self.colors[i]) for i in range(len(concepts))
        }

    def extract_keywords(self, text, unique=False, span_info=False, as_dataframe=False):
        keywords = self.processor.extract_keywords(text, span_info=span_info)
        if unique:
            keywords = list(set(keywords))

        if as_dataframe and span_info:
            keywords = pd.DataFrame(keywords, columns=["name", "start", "end"])
        return keywords

    def count_keywords(self, text, as_dict=False, as_category=True):
        count = (
            self.extract_keywords(text, span_info=True, as_dataframe=True)
            .assign(count=lambda x: 1)
            .groupby("name", as_index=True)[["count"]]
            .sum()
        )

        if hasattr(self, "data"):
            count = count.join(self.data)

        return count

    def count_keywords_on_corpus(self, texts, as_melted=False, as_category=False):

        data = []

        if as_melted:

            for i, text in enumerate(tqdm(texts)):

                text_count = self.count_keywords(text)[
                    ["category", "count"]
                ].reset_index()
                text_count.insert(0, "text_id", i)

                data.append(text_count)

            data = pd.concat(data, ignore_index=True, axis=0)
            return data

        else:

            for i, text in enumerate(tqdm(texts)):

                text_count = self.count_keywords(text)[
                    ["category", "count"]
                ].reset_index()

                if as_category:
                    data.append(text_count.groupby("category")["count"].sum().to_dict())
                else:
                    data.append(text_count.set_index("name")["count"].to_dict())

            data = pd.DataFrame(data).fillna(0.0)
            return data

    def count_keywords_by_paragraph(self, text):

        paragraphs = text.split("\n")
        count = []

        for i, paragraph in enumerate(paragraphs):
            if len(paragraph) > 3:
                keywords = self.extract_keywords(
                    paragraph, span_info=True, as_dataframe=True
                )
                keywords["paragraph_number"] = i
                keywords["paragraph"] = paragraph

                count.append(keywords)

        count = pd.concat(count, ignore_index=True, axis=0)
        count = (
            count.assign(count=lambda x: 1)
            .groupby(["keyword", "paragraph_number"], as_index=False)["count"]
            .sum()
            .set_index("keyword")
        )

        if hasattr(self, "data"):
            count = count.join(self.data)

        return count.sort_values("paragraph_number")

    def shuffle_explore_keywords(self, texts):

        button = widgets.Button(description="Shuffle")
        output = widgets.Output()

        display(button, output)

        def on_button_clicked(b):
            with output:
                clear_output()
                text = random.choice(texts)
                self.show_keywords(text)

        button.on_click(on_button_clicked)

    def show_keywords(self, text, jupyter=True):
        """Show entities in Jupyter notebook
        Documentation at https://spacy.io/usage/visualizers
        """
        if isinstance(text, str):

            # Extract all entities
            keywords = self.extract_keywords(text, span_info=True)
            keywords = [
                {"start": keyword[1], "end": keyword[2], "label": keyword[0]}
                for keyword in keywords
            ]

            # Build input dictionary
            data = {"text": text, "ents": keywords, "title": None}

            # Build options dictionary
            options = {}
            options["colors"] = self.colors
            options["ents"] = list(self.colors.keys())

            # Render Displacy
            return displacy.render(
                data, style="ent", manual=True, jupyter=jupyter, options=options
            )

        else:
            for document in text:
                self.show_keywords(document)
                print("")
