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
    "nouvel_obs": {
        "sitemap_index": None,
        "sitemap_url": "https://www.nouvelobs.com/sitemap-articles-news.xml",
        "regex_section": r"^https:\/\/www\.nouvelobs\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "le_point": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lepoint.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.lepoint\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lexpress": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lexpress.fr/sitemap_actu_1.xml",
        "regex_section": r"^https:\/\/www\.lexpress\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lesechos": {
        "sitemap_index": "https://sitemap.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://www.lesechos.fr/sitemap_news.xml",
        "regex_section" : r"^https:\/\/www\.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$"
    },
    "investir.lesechos": {
        "sitemap_index": "https://sitemap-investir.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://investir.lesechos.fr/sitemap_news.xml",
        "regex_section" : r"^https:\/\/www\.investir.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$"
    },
    "lamarseillaise": {
        "sitemap_index": "https://www.lamarseillaise.fr/sitemap.xml",
        "sitemap_url": "https://www.lamarseillaise.fr/sitemapforgoogle.xml", # lien recursif en fait
        "regex_section" : r"^https:\/\/www\.lamarseillaise\.fr\/(?P<section>[\/\w-]*)\/.+$"
    },
    "lequipe": {
        "sitemap_index": "https://www.lequipe.fr/sitemap.xml",
        "sitemap_url": "https://www.lequipe.fr/sitemap/sitemap_google_news_premium.xml", # https://www.lequipe.fr/sitemap/sitemap_google_news_gratuit.xml aussi ?
        "regex_section" : r"^https:\/\/www\.lequipe\.fr\/(?P<section>[\/\w-]*)\/.+\/\d+$" # quelques chiffres à la fin
    }, 
    "lopinion": {
        "sitemap_index": "https://www.lopinion.fr/sitemap.xml",
        "sitemap_url": "https://www.lopinion.fr/news-sitemap-latest.xml", 
        "regex_section" : r"^https:\/\/www\.lopinion\.fr\/(?P<section>[\/\w-]*)\/.+$"
    },
    "midilibre": {
        "sitemap_index": "https://www.midilibre.fr/sitemap.xml",
        "sitemap_url": "https://www.midilibre.fr/sitemap_articles_1.xml.gz", #sans le .gz pour afficher dans le wew browser
        "regex_section" : None, #  articles en year/month/day/titre
        "filter_date_label" : "lastmod"
    }, 
    "lindependant": {
        "sitemap_index": "https://www.lindependant.fr/sitemap.xml",
        "sitemap_url": "https://www.lindependant.fr/sitemap_articles_1.xml.gz",
        "regex_section" : None, #  articles en year/month/day/titre
        "filter_date_label" : "lastmod"
    }, 
    "ladepeche": {
        "sitemap_index": "https://www.ladepeche.fr/sitemap.xml",
        "sitemap_url": "https://www.ladepeche.fr/sitemap_articles_1.xml.gz",
        "regex_section" : None, #  articles en year/month/day/titre
        "filter_date_label" : "lastmod"
    },
    "centrepresseaveyron": {
        "sitemap_index": "https://www.centrepresseaveyron.fr/sitemap.xml",
        "sitemap_url": "https://www.centrepresseaveyron.fr/sitemap_articles_1.xml.gz",
        "regex_section" : None, #  articles en year/month/day/titre
        "filter_date_label" : "lastmod"
    },
    "petitbleu": {
        "sitemap_index": "https://www.petitbleu.fr/sitemap.xml",
        "sitemap_url": "https://www.petitbleu.fr/sitemap_articles_1.xml.gz",
        "regex_section" : None, #  articles en year/month/day/titre
        "filter_date_label" : "lastmod"
    },
}

MEDIA_CONFIG = {
    "bfmtv": {"site_url": "https://www.bfmtv.com/", "type": "tv"},
    "lefigaro": {"site_url": "https://www.lefigaro.fr/", "type": "webpress"},
    "francetvinfo": {"site_url": "https://www.francetvinfo.fr/", "type": "tv"},
    "lemonde": {"site_url": "https://www.lemonde.fr/", "type": "webpress"},
    "20_minutes": {"site_url": "https://www.20minutes.fr/", "type": "webpress"},
    "liberation": {
        "site_url": "https://www.liberation.fr/arc/outboundfeeds/",
        "type": "webpress",
    },
    "nouvel_obs": {"site_url": "https://www.nouvelobs.com/", "type": "webpress"},
    "le_point": {"site_url": "https://www.lepoint.fr/", "type": "webpress"},
    "lexpress": {"site_url": "https://www.lexpress.fr/", "type": "webpress"},
    "lesechos": {"site_url": "https://www.lesechos.fr/", "type": "webpress"},
    "investir.lesechos": {"site_url": "https://investir.lesechos.fr/", "type": "webpress"},
    "lamarseillaise": {"site_url": "http://www.lamarseillaise.fr/", "type": "webpress"},
    "lequipe": {"site_url": "https://www.lequipe.fr/", "type": "webpress"},
    "lopinion": {"site_url": "https://www.lopinion.fr/", "type": "webpress"},
    "midilibre": {"site_url": "https://www.midilibre.fr/", "type": "webpress"},
    "lindependant": {"site_url": "https://www.lindependant.fr/", "type": "webpress"},
    "ladepeche": {"site_url": "https://www.ladepeche.fr/", "type": "webpress"},
    "centrepresseaveyron": {"site_url": "https://www.centrepresseaveyron.fr/", "type": "webpress"},
    "petitbleu": {"site_url": "https://www.petitbleu.fr/", "type": "webpress"},    
}
