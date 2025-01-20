import logging
import os
import pandas as pd
from quotaclimat.data_processing.mediatree.utils import *
from quotaclimat.data_processing.mediatree.detect_keywords import *
from quotaclimat.data_processing.mediatree.keyword.stop_words import STOP_WORDS

def test_get_remove_stopwords_recycler():
    stop_words_list = [
        "recycler"
    ]
    ad = "nous les recycler pour en faire de nouvelles en fabriquant nous-mêmes du plastique recyclé pour cela nous avons créé trois usines exclusivement dédié au recyclage dès cette année cristallines est capable de recycler autant de bouteilles"

    assert remove_stopwords(ad, stop_words_list) == "nous les  pour en faire de nouvelles en fabriquant nous-mêmes du plastique recyclé pour cela nous avons créé trois usines exclusivement dédié au recyclage dès cette année cristallines est capable de  autant de bouteilles"

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


def test_remove_stopwords_energie():
    plaintext = "quand le prix de l' énergie augmente il y a ceux qui se couvre plus ceux qui sortent moins et il y a ceux qui choisissent d' optimiser leurs énergies panneaux solaires isolations thermique pompes à chaleur chaque jour fleuron industrie parcourt la france pour vous aider à optimiser votre énergie florent industries point com en ce moment la centrale photovoltaïque de trois kilowatts et deux mille cinq cents euros et oui deux deux mille cinq cents euros cents dépêchez euros vous dépêchez vous de réserver votre kit sur fleuron industries point com <unk> <unk> la rénovation énergétique avec ici pour changer de maison sans changer de maison isolation chauffage solaire plus de confort et d' économie avec ici pas à mal casser pas mal vous avez fait une toute la pâte à modeler la je fais comment une tartine de pâte à modeler sans pâte à modeler c' est pas interdit ça s' appelle dupin juste merci pour le partage le jour où vous aimerez la pâte"
    output = remove_stopwords(plaintext,STOP_WORDS)
    # plantext does not contain photovoltaïque
    assert "photovoltaïque" not in output
    assert "rénovation énergétique" not in output
    assert "chauffage" not in output

def test_remove_stopwords_fleuron():
    plaintext = "chaque jour fleuron industrie parcourt"
    output = remove_stopwords(plaintext,STOP_WORDS)
    # plantext does not contain photovoltaïque
    assert output == ""

def test_remove_stopwords_photovoltaique():
    plaintext = "point com en ce moment la centrale photovoltaïque de trois kilowatt et à deux m"
    output = remove_stopwords(plaintext,STOP_WORDS)
    # plantext does not contain photovoltaïque
    assert "photovoltaïque" not in output
    assert len(output) == 0


def test_replace_word_with_context_unk():
    plaintext="<unk> <unk> quand le prix de l' énergie augmente il y a ceux qui se couvren"
    output = replace_word_with_context(text=plaintext, word="<unk> ", length_to_remove=0)
    assert output == "quand le prix de l' énergie augmente il y a ceux qui se couvren"
