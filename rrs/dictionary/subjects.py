from quotaclimat.data_processing.mediatree.keyword.keyword import THEME_KEYWORDS
from rrs.dictionary.subject import insecurity, environmental_health, purchase_power

subjects = {
    "climate": {"keywords": THEME_KEYWORDS, "title": "Climat"},
    "insecurity": {"keywords": insecurity.keywords, "title": "Migrations"},
    "environmental_health": {"keywords": environmental_health.keywords, "title": "Santé environnementale"},
    "purchase_power": {"keywords": purchase_power.keywords, "title": "Pouvoir d'achat"},
}
