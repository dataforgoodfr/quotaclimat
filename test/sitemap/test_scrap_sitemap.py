import logging

import numpy as np
import pandas as pd
import pytest
import os 
from quotaclimat.data_ingestion.scrap_sitemap import (filter_on_date, find_sections, get_consistent_hash, get_diff_from_df, query_one_sitemap_and_transform, get_sections_from_url, normalize_section)
from quotaclimat.data_ingestion.config_sitemap import (SITEMAP_CONFIG)
from datetime import datetime, timedelta

from quotaclimat.data_ingestion.ingest_db.ingest_sitemap_in_db import get_sitemap_list

url_to_parse = ""
if(os.environ.get("ENV") == "docker"):
    url_to_parse ="http://nginxtest:80/"
else:
    url_to_parse = "http://localhost:8000/"

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
    pg_df =pd.DataFrame(
        [
            {
                "id": "unknown"
            }
        ]
    )
    media = "lefigaro"
    output = await query_one_sitemap_and_transform(media, sitemap_config[media], pg_df)
    title = "EN DIRECT - Conflit Hamas-Israël : l’armée israélienne dit avoir frappé Gaza avec 4000 tonnes d’explosifs depuis samedi"
    expected_result = pd.DataFrame([{
        "url" : f"{url_to_parse}mediapart_website.html",
        "lastmod" :pd.Timestamp("2023-10-12 15:34:28"),
        "publication_name" :"Le Figaro",
        "publication_language" :"fr",
        "news_publication_date" : pd.Timestamp("2023-10-12T06:13:00"),
        "news_title" : title,
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
        "id": get_consistent_hash("Le Figaro" + title),
        "media":"lefigaro",
        }])

    # warning : hard to compare almost the same timestamp
    expected_result['download_date'] = output['download_date']
    expected_result['sitemap_last_modified'] = output['sitemap_last_modified']
    expected_result['lastmod'] = output['lastmod']
    expected_result['news_publication_date'] = output['news_publication_date']
    expected_result.reset_index(drop=True, inplace=True)
    output.reset_index(drop=True, inplace=True)

    # Reorder the columns of df2 to match df1
    output = output[expected_result.columns]
    pd.testing.assert_frame_equal(output.iloc[[0]], expected_result)

@pytest.mark.asyncio
async def test_query_one_sitemap_and_transform_hat_parsing():
    # here, the sitemap does not have "title" information so we parse it from the website scrapping
    sitemap_config = get_sitemap_list()
    pg_df =pd.DataFrame(
        [
            {
                "id": "unknown"
            }
        ]
    )
    media = "20_minutes"
    output = await query_one_sitemap_and_transform(media, sitemap_config["lefigaro"], pg_df)
    title = "Grève du 13 octobre : SNCF, RATP, aérien, médecins… Retrouvez le détail des perturbations à prévoir"
    publication_name = "Le Figaro"
    expected_result = pd.DataFrame([{
        "url" : f"{url_to_parse}20minutes_website.html",
        "lastmod" :pd.Timestamp("2023-10-12 15:34:21"),
        "publication_name" :"Le Figaro",
        "publication_language" :"fr",
        "news_publication_date" : pd.Timestamp("2023-10-11T16:16:00"),
        "news_title" : title, # magic should be here: this title is parsed using a web scrapper and not the sitemap.xml "title"
        "news_keywords" :"grève, salaires, social, RH, ressources humaines, primes, conjoncture, entreprise, œuvres sociales, trséorerie, finance, comoité d'entreprise, elections syndicales, gestion entreprise, TPE, PME, PMI, CAC 40, fiscalité des entreprises, actualités sociales",
        "image_loc" :"https://i.f1g.fr/media/cms/orig/2023/10/09/8f1062e1948f5c0abb930b0665ec4958613a74853c8fba9dfb7f374b3ec82065.jpg",
        "image_caption" :"Grève: à quoi faut-il s’attendre ce 13 octobre ?",
        "sitemap" :sitemap_config["lefigaro"]["sitemap_url"],
        "sitemap_last_modified" :pd.Timestamp("2023-10-12 15:52:41+00:00"),
        "download_date": pd.Timestamp.now(),
        "section" :["unknown"],
        "media_type" :"webpress",
        "news_description": "howdy there",
        "id": get_consistent_hash(publication_name + title),
        "media":media,
        }])

    # warning : hard to compare almost the same timestamp
    output = output.iloc[[1]] # second element of figaro's fake sitemap.xml
    # Reorder the columns of df2 to match df1
    output = output[expected_result.columns]

    # warning : hard to compare almost the same timestamp
    output.drop("lastmod",axis=1, inplace=True)
    output.drop("news_publication_date",axis=1, inplace=True)
    output.drop("sitemap_last_modified",axis=1, inplace=True)
    output.drop("download_date",axis=1, inplace=True)

    expected_result.drop("lastmod",axis=1, inplace=True)
    expected_result.drop("news_publication_date",axis=1, inplace=True)
    expected_result.drop("sitemap_last_modified",axis=1, inplace=True)
    expected_result.drop("download_date",axis=1, inplace=True)

    pd.testing.assert_frame_equal(output.reset_index(drop=True), expected_result.reset_index(drop=True))


def test_find_sections():
    sitemap_config = SITEMAP_CONFIG

    # url_jdd = "https://www.lejdd.fr/politique/arnaud-benedetti-la-loi-immigration-reintroduit-les-clivages-gauche/droite-1382860"
    # assert find_sections(url_jdd, "lejdd", sitemap_config["lejdd"]) == ["politique"]
    
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

def test_get_diff_from_df():
    df = pd.DataFrame([{
        "id": "id_unknown"
    },{
        "id": "id2"
    },{
        "id": "id1"
    }])

    df_pg = pd.DataFrame([{
        "id": "id1"
    },{
        "id": "id2"
    }])

    expected = pd.DataFrame([{
        "id": "id_unknown"
    }])
    output = get_diff_from_df(df, df_pg)

    pd.testing.assert_frame_equal(output, expected)

def test_filter_on_date():
    now = datetime.now()
    old = pd.Timestamp("2020-01-01 15:34:28")
    old_df = pd.DataFrame([{
        "url" : "test",
        "lastmod" :old,
    }])
    assert len(filter_on_date(old_df)) == 0

    old_df = pd.DataFrame([{
        "url" : "test",
        "news_publication_date" : old,
    }])
    assert len(filter_on_date(old_df, date_label="news_publication_date"))  == 0

    recent_df = pd.DataFrame([{
        "url" : "test",
        "lastmod" : now,
    }])
    assert len(filter_on_date(recent_df)) == 1

    recent_df = pd.DataFrame([{
        "url" : "test",
        "news_publication_date" : now,
    }])
    assert len(filter_on_date(recent_df, date_label="news_publication_date")) == 1