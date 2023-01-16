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
    "letelegramme": {
        "sitemap_index": None,
        "sitemap_url": "https://www.letelegramme.fr/metasitemap_news.xml",
        "regex_section": r"^https:\/\/www\.letelegramme\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
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
    "letelegramme": {
        "site_url": "https://www.letelegramme.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
}
