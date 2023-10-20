SITEMAP_CONFIG = {
    "bfmtv": {
        "sitemap_index": None,
        "sitemap_url": "https://www.bfmtv.com/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.bfmtv\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lefigaro.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "francetvinfo": {
        "sitemap_index": None,
        "sitemap_url": "https://www.francetvinfo.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.francetvinfo\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lemonde": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lemonde.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lemonde\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "20_minutes": {
        "sitemap_index": None,
        "sitemap_url": "https://www.20minutes.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.20minutes\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "liberation": {
        "sitemap_index": None,
        "sitemap_url": "https://www.liberation.fr/arc/outboundfeeds/sitemap_news.xml?outputType=xml",
        "regex_section": r"^https:\/\/www\.liberation\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lesechos": {
        "sitemap_index": "https://sitemap.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://www.lesechos.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "investir.lesechos": {
        "sitemap_index": "https://sitemap-investir.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://investir.lesechos.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.investir.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lamarseillaise": {
        "sitemap_index": "https://www.lamarseillaise.fr/sitemap.xml",
        "sitemap_url": "https://www.lamarseillaise.fr/sitemapforgoogle.xml",  # lien recursif en fait
        "regex_section": r"^https:\/\/www\.lamarseillaise\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "laprovence": {
        "sitemap_index": None,
        "sitemap_url": "https://www.laprovence.com/googlenews.xml",
        "regex_section": r"^https:\/\/www\.laprovence\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lequipe": {
        "sitemap_index": "https://www.lequipe.fr/sitemap.xml",
        "sitemap_url": "https://www.lequipe.fr/sitemap/sitemap_google_news_premium.xml",  # https://www.lequipe.fr/sitemap/sitemap_google_news_gratuit.xml aussi ?
        "regex_section": r"^https:\/\/www\.lequipe\.fr\/(?P<section>[\/\w-]*)\/.+\/\d+$",  # quelques chiffres à la fin
    },
    "lopinion": {
        "sitemap_index": "https://www.lopinion.fr/sitemap.xml",
        "sitemap_url": "https://www.lopinion.fr/news-sitemap-latest.xml",
        "regex_section": r"^https:\/\/www\.lopinion\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "midilibre": {
        "sitemap_index": "https://www.midilibre.fr/sitemap.xml",
        "sitemap_url": "https://www.midilibre.fr/sitemap_articles_1.xml.gz",  # sans le .gz pour afficher dans le wew browser
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    "lindependant": {
        "sitemap_index": "https://www.lindependant.fr/sitemap.xml",
        "sitemap_url": "https://www.lindependant.fr/sitemap_articles_1.xml.gz",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    "ladepeche": {
        "sitemap_index": "https://www.ladepeche.fr/sitemap.xml",
        "sitemap_url": "https://www.ladepeche.fr/sitemap_articles_1.xml.gz",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    "centrepresseaveyron": {
        "sitemap_index": "https://www.centrepresseaveyron.fr/sitemap.xml",
        "sitemap_url": "https://www.centrepresseaveyron.fr/sitemap_articles_1.xml.gz",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    "petitbleu": {
        "sitemap_index": "https://www.petitbleu.fr/sitemap.xml",
        "sitemap_url": "https://www.petitbleu.fr/sitemap_articles_1.xml.gz",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    # "letelegramme": { #TODO fix me
    #     "sitemap_index": None,
    #     "sitemap_url": "https://www.letelegramme.fr/metasitemap_news.xml",
    #     "regex_section": r"^https:\/\/www\.letelegramme\.fr\/(?P<section>[\/\w-]*)\/.+$",
    # },
}

MEDIA_CONFIG = {
    "bfmtv": {
        "site_url": "https://www.bfmtv.com/",
        "type": "tv",
        "coverage": "national",
    },
    "lefigaro": {
        "site_url": "https://www.lefigaro.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "francetvinfo": {
        "site_url": "https://www.francetvinfo.fr/",
        "type": "tv",
        "coverage": "national",
    },
    "lemonde": {
        "site_url": "https://www.lemonde.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "20_minutes": {
        "site_url": "https://www.20minutes.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "liberation": {
        "site_url": "https://www.liberation.fr/arc/outboundfeeds/",
        "type": "webpress",
        "coverage": "national",
    },
    "nouvel_obs": {
        "site_url": "https://www.nouvelobs.com/",
        "type": "webpress",
        "coverage": "national",
    },
    # "letelegramme": { #TODO Fix me
    #     "site_url": "https://www.letelegramme.fr/",
    #     "type": "webpress",
    #     "coverage": "regional",
    # },
    "le_point": {
        "site_url": "https://www.lepoint.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "lexpress": {
        "site_url": "https://www.lexpress.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "lesechos": {
        "site_url": "https://www.lesechos.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "investir.lesechos": {
        "site_url": "https://investir.lesechos.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "lamarseillaise": {
        "site_url": "http://www.lamarseillaise.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lequipe": {
        "site_url": "https://www.lequipe.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "lopinion": {
        "site_url": "https://www.lopinion.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "midilibre": {
        "site_url": "https://www.midilibre.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lindependant": {
        "site_url": "https://www.lindependant.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "ladepeche": {
        "site_url": "https://www.ladepeche.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "laprovence": {
        "site_url": "https://www.laprovence.com/googlenews.xml",
        "type": "webpress",
        "coverage": "regional",
    },
    "centrepresseaveyron": {
        "site_url": "https://www.centrepresseaveyron.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "petitbleu": {
        "site_url": "https://www.petitbleu.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
}

SITEMAP_TEST_CONFIG = {
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/sitemap_news_figaro_3.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
    }
}
    
SITEMAP_DOCKER_CONFIG = {
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/sitemap_news_figaro_3.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
    }
}