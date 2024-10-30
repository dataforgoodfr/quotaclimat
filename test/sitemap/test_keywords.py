import logging
import os
import pandas as pd
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.detect_keywords import *

def test_get_remove_stopwords_recycler():
    ad = "nous les recycler pour en faire de nouvelles en fabriquant nous-mêmes du plastique recyclé pour cela nous avons créé trois usines exclusivement dédié au recyclage dès cette année cristallines est capable de recycler autant de bouteilles"

    assert remove_stopwords(ad) == " de nouvelles en fabriquant  pour cela nous avons créé  dès cette année  autant de bouteilles"

def test_get_remove_stopwords_no_modification():
    ad = "no keywords"

    assert remove_stopwords(ad) == ad
