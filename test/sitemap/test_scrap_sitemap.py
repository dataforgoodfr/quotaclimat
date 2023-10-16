import logging

import numpy as np
import pandas as pd

from quotaclimat.data_ingestion.scrap_sitemap import (find_sections)
from quotaclimat.data_ingestion.config_sitmap import (MEDIA_CONFIG,
                                                      SITEMAP_CONFIG)
                                                      
def test_find_sections():
    url_franceinfo = "https://www.francetvinfo.fr/monde/proche-orient/israel-palestine/direct-guerre-entre-israel-et-le-hamas-l-occupation-de-la-bande-de-gaza-serait-une-grave-erreur-previent-joe-biden_6125127.html"
    output = find_sections(url_franceinfo, "francetvinfo")
    assert output == ['monde', 'proche-orient', 'israel-palestine']

    url_lemonde = "https://www.lemonde.fr/emploi/article/2023/10/16/que-sait-on-du-travail-plus-d-un-tiers-des-salaries-ont-un-rythme-de-travail-impose-par-un-controle-informatise_6194727_1698637.html"
    assert find_sections(url_lemonde, "lemonde") == ["emploi"]

    url_lefigaro = "https://www.lefigaro.fr/sports/rugby/coupe-du-monde/coupe-du-monde-de-rugby-une-bagarre-eclate-dans-les-tribunes-pendant-angleterre-fidji-20231015"
    assert find_sections(url_lefigaro, "lefigaro") == ['sports', 'rugby', 'coupe-du-monde']

    url_lesechos = "https://www.lesechos.fr/finance-marches/marches-financiers/limpensable-resurrection-de-ftx-en-plein-proces-de-sam-bankman-fried-1987274"
    assert find_sections(url_lesechos, "lesechos") == ["finance-marches", "marches-financiers"]

    url_midilibre = "https://www.midilibre.fr/2023/10/15/une-rixe-a-lentree-de-la-boite-de-nuit-a-saint-jean-de-vedas-ce-dimanche-15-octobre-11520905.php"
    assert find_sections(url_midilibre, "midilibre") == ["unknown"]

    url_liberation = "https://www.liberation.fr/international/moyen-orient/guerre-hamas-israel-a-ramallah-siege-de-lautorite-palestinienne-lintifada-qui-ne-vient-pas-20231016_37VO7KNPJJAZXFFCVBDQ24KTME/"
    assert find_sections(url_liberation, "liberation") == ["international", "moyen-orient"]

    url_20minutes = "https://www.20minutes.fr/sport/rugby/coupe_du_monde_de_rugby/4057884-20231016-angleterre-fidji-tres-bon-travail-xv-rose-trop-confiant-malgre-victoire-poussive"
    assert find_sections(url_20minutes, "20_minutes") == ["sport", "rugby", "coupe-du-monde-de-rugby"]

    url_laprovence = "https://www.laprovence.com/article/sports/6599160924963815/coupe-du-monde-de-rugby-antoine-dupont-larbitrage-na-pas-ete-a-la-hauteu"
    assert find_sections(url_laprovence, "laprovence") == ["sports"]
