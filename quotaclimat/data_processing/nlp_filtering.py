import pandas as pd
import numpy as np
import plotly.express as px
from deepmultilingualpunctuation import PunctuationModel
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import TextClassificationPipeline



class NLPFilteringModel:
    def __init__(self):

        self.punct_model = PunctuationModel()
        self.model_name = 'lincoln/flaubert-mlsum-topic-classification'
        self.category_loaded_tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.category_loaded_model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.category_predictor = TextClassificationPipeline(model=self.category_loaded_model, tokenizer=self.category_loaded_tokenizer,return_all_scores = True)

    def restore_punctuation(self,text):
        text = self.punct_model.restore_punctuation(text)
        text = [x.strip().capitalize() for x in text.split(".") if len(x.strip()) > 0]
        return text

    def predict_sentence(self,sent):
        return self.category_predictor(sent,truncation = True)[0]

    def predict_sentences(self,sentences,dedup = True):
        
        scores = []

        for i,x in enumerate(sentences):
            d = pd.DataFrame(self.predict_sentence(x))
            d["sentence_id"] = i
            d["sentence_len"] = len(x)
            d["sentence"] = x
            d["label"] = d["label"].replace("Environement","Environnement")
            scores.append(d)

        scores = pd.concat(scores,axis = 0,ignore_index = True)

        if dedup:
            scores = self.dedup_scores(scores)

        return scores


    def dedup_scores(self,scores):

        scores = (scores
        .sort_values(["sentence_id","score"],ascending = [True,False])
        .reset_index(drop = True)
        .drop_duplicates(subset = ["sentence_id"])
        .reset_index(drop = True)
        )

        scores["sentence_percentage"] = scores["sentence_len"] / scores["sentence_len"].sum()
        scores["sentence_duration"] = scores["sentence_percentage"] * 60 * 2

        return scores

    def analyze_topic_change(self,scores,th = 0.6):
        scores["topic_change"] = (scores["score"] > 0.6).astype(int)
        scores.loc[scores["topic_change"] == 0,"label"] = np.NaN
        scores.loc[scores["topic_change"] == 1,"label"] = scores["label"]
        scores["label"] = scores["label"].fillna(method = "ffill").fillna(method = "bfill")
        return scores

    def show_scores(self,scores,height = 400,**kwargs):
        return px.bar(scores,x = "sentence_id",y = "score",color = "label",height = height,**kwargs)


    def predict(self,text,topic_change = True,as_percent_environment = True,th = 0.6):

        sentences = self.restore_punctuation(text)
        scores = self.predict_sentences(sentences,dedup = True)

        if topic_change:
            scores = self.analyze_topic_change(scores,th = th)

        if as_percent_environment:
            group = scores.groupby("label")["sentence_percentage"].sum()
            group = group.get("Environnement")
            return group

        return scores



