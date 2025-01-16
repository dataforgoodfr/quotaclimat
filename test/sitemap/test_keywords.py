import logging
import os
import pandas as pd
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.detect_keywords import *

def test_get_remove_stopwords_recycler():
    stop_words_list = [
        "recycler"
    ]
    ad = "nous les recycler pour en faire de nouvelles en fabriquant nous-mêmes du plastique recyclé pour cela nous avons créé trois usines exclusivement dédié au recyclage dès cette année cristallines est capable de recycler autant de bouteilles"

    assert remove_stopwords(ad, stop_words_list) == " de nouvelles en fabriquant  pour cela nous avons créé  dès cette année  autant de bouteilles"

def test_get_remove_stopwords_no_modification():
    stop_words_list = [
        "recycler"
    ]
    ad = "no keywords"

    assert remove_stopwords(ad, stop_words_list) == ad

def test_remove_stopwords_huile():
    stop_words_list = [
        "recycler",
        "huile de coude était aussi une énergie renouvelable",
        "est à fond sur le tri sélectif"
    ]
    assert remove_stopwords("l' huile de coude était aussi une énergie renouvelable stéphane est à fond sur le tri sélectif",stop_words_list) \
          == "l'  stéphane "
