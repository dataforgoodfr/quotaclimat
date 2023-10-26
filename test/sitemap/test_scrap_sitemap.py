import logging

import numpy as np
import pandas as pd
import pytest
import os 
from quotaclimat.data_ingestion.scrap_sitemap import (find_sections, query_one_sitemap_and_transform, get_sections_from_url, normalize_section)
from quotaclimat.data_ingestion.config_sitemap import (SITEMAP_CONFIG)

from quotaclimat.data_ingestion.ingest_db.ingest_sitemap_in_db import get_sitemap_list

def test_normalize_section():
    assert normalize_section(["test", "pizza"]) == ["test", "pizza"]
    assert normalize_section(["test", "12312"]) == ["test"]
    assert normalize_section(["test_15", "12312"]) == ["test-15"]

def test_get_sections_from_url():
    sitemap_config = get_sitemap_list()
    url = "https://www.lefigaro.fr/international/en-direct-conflit-hamas-israel-l-etat-hebreu-poursuit-son-pilonnage-de-la-bande-de-gaza-en-promettant-de-detruire-le-hamas-20231012"
    output = get_sections_from_url(url, sitemap_config["lefigaro"], default_output=[""]) 

    assert output == ['international']

def test_get_sitemap_list():
    sitemap = list(get_sitemap_list())[0]
    # locally we test only a few items
    sitemap_url = sitemap
    sitemap_url == "http://nginxtest:80/sitemap_news_figaro_3.xml"

@pytest.mark.asyncio
async def test_query_one_sitemap_and_transform():
    sitemap_config = get_sitemap_list()

    media = "lefigaro"
    url_to_parse = ""
    if(os.environ.get("ENV") == "docker"):
        url_to_parse ="http://nginxtest:80/mediapart_website.html"
    else:
        url_to_parse = "http://localhost:8000/mediapart_website.html"
    output = await query_one_sitemap_and_transform(media, sitemap_config[media])

    expected_result = pd.DataFrame([{
        "url" :url_to_parse,
        "lastmod" :pd.Timestamp("2023-10-12 15:34:28"),
        "publication_name" :"Le Figaro",
        "publication_language" :"fr",
        "news_publication_date" : pd.Timestamp("2023-10-12T06:13:00"),
        "news_title" :"EN DIRECT - Conflit Hamas-Israël : l’armée israélienne dit avoir frappé Gaza avec 4000 tonnes d’explosifs depuis samedi",
        "news_keywords" :"Israël, Hamas, conflit israélo-palestinien, International, actualité internationale, affaires étrangères, ministère des affaires étrangères, politique étrangère",
        "news_genres" :"Blog",
        "image_loc" :"https://i.f1g.fr/media/cms/orig/2023/10/12/eccf7495cede8869a8a35d6fd70a1635759a12dbef68dd16e82e34162f69ec4f.jpg",
        "image_caption" :"Explosion dans le centre de la ville de Gaza ce jeudi 12 octobre.",
        "sitemap" :sitemap_config[media]["sitemap_url"],
        "sitemap_last_modified" :pd.Timestamp("2023-10-12 15:52:41+00:00"),
        "download_date": pd.Timestamp.now(),
        "section" :["unknown"],
        "media_type" :"webpress",
        "news_description": "description could be parsed with success",
        "media":"lefigaro",
    }])

    # warning : hard to compare almost the same timestamp
    expected_result['download_date'] = output['download_date']
    expected_result['sitemap_last_modified'] = output['sitemap_last_modified']
    expected_result['lastmod'] = output['lastmod']
    expected_result['news_publication_date'] = output['news_publication_date']

    pd.testing.assert_frame_equal(output.head(1), expected_result)


def test_find_sections():
    sitemap_config = SITEMAP_CONFIG

    url_franceinfo = "https://www.francetvinfo.fr/monde/proche-orient/israel-palestine/direct-guerre-entre-israel-et-le-hamas-l-occupation-de-la-bande-de-gaza-serait-une-grave-erreur-previent-joe-biden_6125127.html"
    output = find_sections(url_franceinfo, "francetvinfo", sitemap_config["francetvinfo"])
    assert output == ['monde', 'proche-orient', 'israel-palestine']

    url_lemonde = "https://www.lemonde.fr/emploi/article/2023/10/16/que-sait-on-du-travail-plus-d-un-tiers-des-salaries-ont-un-rythme-de-travail-impose-par-un-controle-informatise_6194727_1698637.html"
    assert find_sections(url_lemonde, "lemonde", sitemap_config["lemonde"]) == ["emploi"]

    url_lefigaro = "https://www.lefigaro.fr/sports/rugby/coupe-du-monde/coupe-du-monde-de-rugby-une-bagarre-eclate-dans-les-tribunes-pendant-angleterre-fidji-20231015"
    assert find_sections(url_lefigaro, "lefigaro", sitemap_config["lefigaro"]) == ['sports', 'rugby', 'coupe-du-monde']

    url_lesechos = "https://www.lesechos.fr/finance-marches/marches-financiers/limpensable-resurrection-de-ftx-en-plein-proces-de-sam-bankman-fried-1987274"
    assert find_sections(url_lesechos, "lesechos", sitemap_config["lesechos"]) == ["finance-marches", "marches-financiers"]

    url_midilibre = "https://www.midilibre.fr/2023/10/15/une-rixe-a-lentree-de-la-boite-de-nuit-a-saint-jean-de-vedas-ce-dimanche-15-octobre-11520905.php"
    assert find_sections(url_midilibre, "midilibre", sitemap_config["midilibre"]) == ["unknown"]

    url_liberation = "https://www.liberation.fr/international/moyen-orient/guerre-hamas-israel-a-ramallah-siege-de-lautorite-palestinienne-lintifada-qui-ne-vient-pas-20231016_37VO7KNPJJAZXFFCVBDQ24KTME/"
    assert find_sections(url_liberation, "liberation", sitemap_config["liberation"]) == ["international", "moyen-orient"]

    url_20minutes = "https://www.20minutes.fr/sport/rugby/coupe_du_monde_de_rugby/4057884-20231016-angleterre-fidji-tres-bon-travail-xv-rose-trop-confiant-malgre-victoire-poussive"
    assert find_sections(url_20minutes, "20_minutes", sitemap_config["20_minutes"]) == ["sport", "rugby", "coupe-du-monde-de-rugby"]

    url_laprovence = "https://www.laprovence.com/article/sports/6599160924963815/coupe-du-monde-de-rugby-antoine-dupont-larbitrage-na-pas-ete-a-la-hauteu"
    assert find_sections(url_laprovence, "laprovence", sitemap_config["laprovence"]) == ["sports"]
