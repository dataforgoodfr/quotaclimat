SITEMAP_CONFIG = {
    "bfmtv": {
        "sitemap_index": None,
        "sitemap_url": "https://www.bfmtv.com/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.bfmtv\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    # "lejdd": { # need custom parsing as sitemap is missing a lot of attributes
    #     "sitemap_index": None,
    #     "sitemap_url": "https://www.lejdd.fr/sitemap.xml",
    #     "regex_section": r"^https:\/\/www\.lejdd\.fr\/(?P<section>[\/\w]*)\/.+$",
    #     "filter_date_label": "lastmod",
    # },
    "cnews": {
        "sitemap_index": None,
        "sitemap_url": "https://www.cnews.fr/googlenews.xml",
        "regex_section": r"^https:\/\/www\.cnews\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "leparisien": {
        "sitemap_index": None,
        "sitemap_url": "https://www.leparisien.fr/arc/outboundfeeds/news-sitemap/?from=0&outputType=xml&_website=leparisien",
        "regex_section": r"^https:\/\/www\.leparisien\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lexpress": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lexpress.fr/arc/outboundfeeds/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.lexpress\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lepoint": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lepoint.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.lepoint\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "nicematin": {
        "sitemap_index": None,
        "sitemap_url": "https://www.nicematin.com/googlenews.xml",
        "regex_section": r"^https:\/\/www\.nicematin\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "corsematin": {
        "sitemap_index": None,
        "sitemap_url": "https://www.corsematin.com/googlenews.xml",
        "regex_section": r"^https:\/\/www\.corsematin\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "larepubliquedespyrenees": {
        "sitemap_index": None,
        "sitemap_url": "https://www.larepubliquedespyrenees.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.larepubliquedespyrenees\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lamontagne": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lamontagne.fr/sitemap.xml",
        "regex_section": r"^https:\/\/www\.lamontagne\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "telerama": {
        "sitemap_index": None,
        "sitemap_url": "https://www.telerama.fr/sitemaps/sitemap_news.php",
        "regex_section": r"^https:\/\/www\.telerama\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "challenges": {
        "sitemap_index": None,
        "sitemap_url": "https://www.challenges.fr/sitemap.news.xml",
        "regex_section": r"^https:\/\/www\.challenges\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "ouest-france": {
        "sitemap_index": None,
        "sitemap_url": "https://www.ouest-france.fr/googlenews-0.xml",
        "regex_section": r"^https:\/\/www\.ouest-france\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date"
    },
    "lunion": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lunion.fr/sites/default/files/sitemaps/abonnes_lunion_fr/sitemapnews-0.xml",
        "regex_section": r"^https:\/\/www\.lunion\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date"
    },
    "la-croix": {
        "sitemap_index": None,
        "sitemap_url": "https://www.la-croix.com/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.la-croix\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lefigaro.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "francetvinfo": {
        "sitemap_index": None,
        "sitemap_url": "https://www.francetvinfo.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.francetvinfo\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "franceinter": {
        "sitemap_index": None,
        "sitemap_url": "https://www.radiofrance.fr/franceinter/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.radiofrance\.fr\/franceinter\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "lemonde": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lemonde.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lemonde\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "20_minutes": {
        "sitemap_index": None,
        "sitemap_url": "https://www.20minutes.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.20minutes\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "liberation": {
        "sitemap_index": None,
        "sitemap_url": "https://www.liberation.fr/arc/outboundfeeds/sitemap_news.xml?outputType=xml",
        "regex_section": r"^https:\/\/www\.liberation\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lesechos": {
        "sitemap_index": "https://sitemap.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://www.lesechos.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "investir.lesechos": {
        "sitemap_index": "https://sitemap-investir.lesechos.fr/sitemap_index.xml",
        "sitemap_url": "https://investir.lesechos.fr/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.investir.lesechos\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "lamarseillaise": {
        "sitemap_index": "https://www.lamarseillaise.fr/sitemap.xml",
        "sitemap_url": "https://www.lamarseillaise.fr/sitemapforgoogle.xml",
        "regex_section": r"^https:\/\/www\.lamarseillaise\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "laprovence": {
        "sitemap_index": "https://www.laprovence.com/sitemap_index.xml",
        "sitemap_url": "https://www.laprovence.com/sitemap_news.xml",
        "regex_section": r"^https:\/\/www\.laprovence\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lequipe": {
        "sitemap_index": "https://www.lequipe.fr/sitemap.xml",
        "sitemap_url": "https://www.lequipe.fr/sitemap/sitemap_google_news_premium.xml",  # https://www.lequipe.fr/sitemap/sitemap_google_news_gratuit.xml aussi ?
        "regex_section": r"^https:\/\/www\.lequipe\.fr\/(?P<section>[\/\w-]*)\/.+\/\d+$",  # quelques chiffres à la fin
        "filter_date_label": "lastmod",
    },
    "lopinion": {
        "sitemap_index": "https://www.lopinion.fr/sitemap.xml",
        "sitemap_url": "https://www.lopinion.fr/news-sitemap-content.xml",
        "regex_section": r"^https:\/\/www\.lopinion\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "midilibre": {
        "sitemap_index": "https://www.midilibre.fr/sitemap.xml",
        "sitemap_url": "https://www.midilibre.fr/sitemap-news.xml",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "news_publication_date",
    },
    "varmatin": {
        "sitemap_index": "https://www.varmatin.com/sitemap.xml",
        "sitemap_url": "https://www.varmatin.com/googlenews.xml",
        "regex_section": r"^https:\/\/www\.varmatin\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "lindependant": {
        "sitemap_index": "https://www.lindependant.fr/sitemap.xml",
        "sitemap_url": "https://www.lindependant.fr/sitemap-news.xml",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "news_publication_date",
    },
    "ladepeche": {
        "sitemap_index": "https://www.ladepeche.fr/sitemap.xml",
        "sitemap_url": "https://www.ladepeche.fr/sitemap-news.xml",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "news_publication_date",
    },
    "centrepresseaveyron": {
        "sitemap_index": "https://www.centrepresseaveyron.fr/sitemap.xml",
        "sitemap_url": "https://www.centrepresseaveyron.fr/sitemap-news.xml",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "news_publication_date",
    },
    "petitbleu": {
        "sitemap_index": "https://www.petitbleu.fr/sitemap.xml",
        "sitemap_url": "https://www.petitbleu.fr/sitemap-news.xml",
        "regex_section": None,  # articles en year/month/day/titre
        "filter_date_label": "lastmod",
    },
    "sudouest": {
        "sitemap_index": "https://www.sudouest.fr/sitemap.xml",
        "sitemap_url": "https://www.sudouest.fr/sitemap-news.xml",
         "regex_section": r"^https:\/\/www\.sudouest\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "lavoixdunord": {
        "sitemap_index": "https://www.lavoixdunord.fr/sites/default/files/sitemaps/www_lavoixdunord_fr/sitemapindex.xml",
        "sitemap_url": "https://www.lavoixdunord.fr/sites/default/files/sitemaps/www_lavoixdunord_fr/sitemapnews-0.xml",
         "regex_section": r"^https:\/\/www\.lavoixdunord\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "courrierpicard": {
        "sitemap_index": "https://www.courrier-picard.fr/sites/default/files/sitemaps/abonne_courrier_picard_fr/sitemapindex.xml",
        "sitemap_url": "https://www.courrier-picard.fr/sites/default/files/sitemaps/abonne_courrier_picard_fr/sitemapnews-0.xml",
        "regex_section": r"^https:\/\/www\.courrier-picard\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "parisnormandie": {
        "sitemap_index": "https://www.paris-normandie.fr/sites/default/files/sitemaps/www_paris_normandie_fr/sitemapindex.xml",
        "sitemap_url": "https://www.paris-normandie.fr/sites/default/files/sitemaps/www_paris_normandie_fr/sitemapnews-0.xml",
         "regex_section": r"^https:\/\/www\.paris-normandie\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "lesteclair": {
        "sitemap_index": None,
        "sitemap_url": "https://www.lest-eclair.fr/sites/default/files/sitemaps/abonnes_lest_eclair_fr/sitemapnews-0.xml",
         "regex_section": None,
        "filter_date_label": "news_publication_date",
    },
    "charentelibre": {
        "sitemap_index": None,
        "sitemap_url": "https://www.charentelibre.fr/sitemap-news.xml",
         "regex_section": r"^https:\/\/www\.charentelibre\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "elle": {
        "sitemap_index": None,
        "sitemap_url": "https://www.elle.fr/sitemaps/sitemap_elle.xml",
         "regex_section": r"^https:\/\/www\.elle\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "letelegramme": {
        "sitemap_index": None,
        "sitemap_url": "https://www.letelegramme.fr/sitemaps/sitemap-news/urlset.xml",
        "regex_section": r"^https:\/\/www\.letelegramme\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "nouvelobs": {
        "sitemap_index": None,
        "sitemap_url": "https://www.nouvelobs.com/sitemap-articles-news.xml",
        "regex_section": r"^https:\/\/www\.nouvelobs\.com\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "huffingtonpost": {
        "sitemap_index": None,
        "sitemap_url": "https://www.huffingtonpost.fr/sitemaps/news.xml",
        "regex_section": r"^https:\/\/www\.huffingtonpost\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "lastmod",
    },
    "mediapart": {
        "sitemap_index": None,
        "sitemap_url": "https://www.mediapart.fr/news_sitemap_editor_choice.xml",
        "regex_section": r"^https:\/\/www\.mediapart\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
    "francebleu": {
        "sitemap_index": None,
        "sitemap_url": "https://www.francebleu.fr/sitemap-news.xml",
        "regex_section": r"^https:\/\/www\.francebleu\.fr\/(?P<section>[\/\w-]*)\/.+$",
        "filter_date_label": "news_publication_date",
    },
}

MEDIA_CONFIG = {
    "bfmtv": {
        "site_url": "https://www.bfmtv.com/",
        "type": "tv",
        "coverage": "national",
    },
    "lejdd": {
        "site_url": "https://www.lejdd.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "cnews": {
        "site_url": "https://www.cnews.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "la-croix": {
        "site_url": "https://www.la-croix.com",
        "type": "webpress",
        "coverage": "national",
    },
    "leparisien": {
        "site_url": "https://www.leparisien.fr",
        "type": "webpress",
        "coverage": "national",
    },
    "nicematin": {
        "site_url": "https://www.nicematin.com",
        "type": "webpress",
        "coverage": "regional",
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
    "franceinter": {
        "site_url": "https://www.radiofrance.fr/",
        "type": "radio",
        "coverage": "national",
    },
    "francebleu": {
        "site_url": "https://www.francebleu.fr/",
        "type": "radio",
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
    "nouvelobs": {
        "site_url": "https://www.nouvelobs.com/",
        "type": "webpress",
        "coverage": "national",
    },
    "mediapart": {
        "site_url": "https://www.mediapart.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "reporterre": {
        "site_url": "https://www.reporterre.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "letelegramme": {
        "site_url": "https://www.letelegramme.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "courrierpicard": {
        "site_url": "https://www.courrier-picard.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lavoixdunord": {
        "site_url": "https://www.lavoixdunord.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lunion": {
        "site_url": "https://www.lunion.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lesteclair": {
        "site_url": "https://www.lest-eclair.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "parisnormandie": {
        "site_url": "https://www.paris-normandie.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "charentelibre": {
        "site_url": "https://www.charentelibre.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "varmatin": {
        "site_url": "https://www.varmatin.com/",
        "type": "webpress",
        "coverage": "regional",
    },
    "sudouest": {
        "site_url": "https://www.sudouest.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lepoint": {
        "site_url": "https://www.lepoint.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "elle": {
        "site_url": "https://www.elle.fr/",
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
    "telerama": {
        "site_url": "https://www.telerama.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "huffingtonpost": {
        "site_url": "https://www.huffingtonpost.fr/",
        "type": "webpress",
        "coverage": "national",
    },
    "challenges": {
        "site_url": "https://www.challenges.fr/",
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
    "ouest-france": {
        "site_url": "https://www.ouest-france.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "larepubliquedespyrenees": {
        "site_url": "https://www.larepubliquedespyrenees.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "lamontagne": {
        "site_url": "https://www.lamontagne.fr/",
        "type": "webpress",
        "coverage": "regional",
    },
    "corsematin": {
        "site_url": "https://www.corsematin.com/",
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
    "bfmtv": {
        "sitemap_index": None,
        "sitemap_url" : "http://localhost:8000/bfmtv_sitemap.xml",
        "regex_section": r"^https:\/\/www\.bfmtv\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "cnews": {
        "sitemap_index": None,
        "sitemap_url" : "http://localhost:8000/cnews_sitemap.xml",
        "regex_section": r"^https:\/\/www\.cnews\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "leparisien": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/leparisien_sitemap.xml",
        "regex_section": r"^https:\/\/www\.leparisien\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lexpress": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/lexpress_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lexpress\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lepoint": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/lepoint_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lepoint\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "nicematin": {
        "sitemap_index": None,
        "sitemap_url" : "http://localhost:8000/nicematin_sitemap.xml",
        "regex_section": r"^https:\/\/www\.nicematin\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "larepubliquedespyrenees": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/republiquepyrenees_sitemap.xml",
        "regex_section": r"^https:\/\/www\.larepubliquedespyrenees\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lamontagne": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/lamontagne_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lamontagne\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "telerama": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/telerama_sitemap.xml",
        "regex_section": r"^https:\/\/www\.telerama\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "challenges": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/challenges_sitemap.xml",
        "regex_section": r"^https:\/\/www\.challenges\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "ouest-france": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/ouestfrance_sitemap.xml",
        "regex_section": r"^https:\/\/www\.ouest-france\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "la-croix": {
        "sitemap_index": None,
        "sitemap_url" :"http://localhost:8000/lacroix_sitemap.xml",
        "regex_section": r"^https:\/\/www\.la-croix\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "francetvinfo": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/franceinfo_sitemap.xml",
        "regex_section": r"^https:\/\/www\.francetvinfo\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "franceinter": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/franceinter_sitemap.xml",
        "regex_section": r"^https:\/\/www\.radiofrance\.fr\/franceinter\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/lefigaro_localhost_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "midilibre": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/midilibre_sitemap.xml",
        "regex_section": r"^https:\/\/www\.midilibre\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "liberation": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/liberation_sitemap.xml",
        "regex_section": r"^https:\/\/www\.liberation\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "letelegramme": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/letelegramme_sitemap.xml",
        "regex_section": r"^https:\/\/www\.letelegramme\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "20_minutes": {
        "sitemap_index": None,
        "sitemap_url": "http://localhost:8000/20minutes_sitemap.xml",
        "regex_section": r"^https:\/\/www\.20minutes\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
}
    
SITEMAP_DOCKER_CONFIG = {
    "ouest-france": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/ouestfrance_sitemap.xml",
        "regex_section": r"^https:\/\/www\.ouest-france\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lefigaro": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/lefigaro_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lefigaro\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
     "bfmtv": {
        "sitemap_index": None,
        "sitemap_url" : "http://nginxtest:80/bfmtv_sitemap.xml",
        "regex_section": r"^https:\/\/www\.bfmtv\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "cnews": {
        "sitemap_index": None,
        "sitemap_url" : "http://nginxtest:80/cnews_sitemap.xml",
        "regex_section": r"^https:\/\/www\.cnews\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "leparisien": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/leparisien_sitemap.xml",
        "regex_section": r"^https:\/\/www\.leparisien\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lexpress": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/lexpress_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lexpress\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lepoint": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/lepoint_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lepoint\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "nicematin": {
        "sitemap_index": None,
        "sitemap_url" : "http://nginxtest:80/nicematin_sitemap.xml",
        "regex_section": r"^https:\/\/www\.nicematin\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "larepubliquedespyrenees": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/republiquepyrenees_sitemap.xml",
        "regex_section": r"^https:\/\/www\.larepubliquedespyrenees\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lamontagne": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/lamontagne_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lamontagne\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "telerama": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/telerama_sitemap.xml",
        "regex_section": r"^https:\/\/www\.telerama\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "challenges": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/challenges_sitemap.xml",
        "regex_section": r"^https:\/\/www\.challenges\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "la-croix": {
        "sitemap_index": None,
        "sitemap_url" :"http://nginxtest:80/lacroix_sitemap.xml",
        "regex_section": r"^https:\/\/www\.la-croix\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "francetvinfo": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/franceinfo_sitemap.xml",
        "regex_section": r"^https:\/\/www\.francetvinfo\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "franceinter": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/franceinter_sitemap.xml",
        "regex_section": r"^https:\/\/www\.radiofrance\.fr\/franceinter\/(?P<section>[\/\w-]*)\/.+$",
    },
    "lemonde": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/lemonde_sitemap.xml",
        "regex_section": r"^https:\/\/www\.lemonde\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "midilibre": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/midilibre_sitemap.xml",
        "regex_section": r"^https:\/\/www\.midilibre\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "liberation": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/liberation_sitemap.xml",
        "regex_section": r"^https:\/\/www\.liberation\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "letelegramme": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/letelegramme_sitemap.xml",
        "regex_section": r"^https:\/\/www\.letelegramme\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "francebleu": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/francebleu_sitemap.xml",
        "regex_section": r"^https:\/\/www\.francebleu\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "nouvelobs": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/nouvelobs_sitemap.xml",
        "regex_section": r"^https:\/\/www\.nouvelobs\.com\/(?P<section>[\/\w-]*)\/.+$",
    },
    "mediapart": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/mediapart_sitemap.xml",
        "regex_section": r"^https:\/\/www\.mediapart\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
    "20_minutes": {
        "sitemap_index": None,
        "sitemap_url": "http://nginxtest:80/20minutes_sitemap.xml",
        "regex_section": r"^https:\/\/www\.20minutes\.fr\/(?P<section>[\/\w-]*)\/.+$",
    },
}