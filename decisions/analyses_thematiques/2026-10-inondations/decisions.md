# Inondations 2025-2026

- Responsable : Renan
- Statut : en cours, étape 0 (cadrage) ; dictionnaire noyau validé le 26/10/2026
- Entrée de `thematic_jobs_config.yml` : `inondations` (mots-clés mis à jour le 26/10/2026 ; périodes à fixer après la courbe journalière ; job pas encore lancé)
- Tables : `thematic_extracts.inondations_articles_fr`, `thematic_extracts.inondations_keywords_fr`
- Collection Metabase : `03. Analyses thématiques/21. Inondations 2026` (id 469)
- Document de résultats (avant publication) : 🔴 [lien document Metabase à créer]

## 1. Méthode spécifique

Méthode commune, sans écart à ce stade.

- Événements recensés (étape 0) : 🔴 [tableau début, fin, lieu, impact, source datée, à remplir par recherche en ligne puis compléter avec les pics de la courbe]
- Courbe journalière (étape 0) : modèle de la méthode commune (§1, étape 0.3), audiovisuel du 01/01/2024 au 26/10/2026, presse du 01/01/2025 (début des données) au 26/10/2026, mots du §2 ; presse : « PE - V2 - Courbe journalière inondations » (2394) ; 🔴 [numéro de la question Metabase audiovisuel]
- Période d'analyse : 🔴 [à fixer après la courbe ; le YAML contient pour l'instant `hiver_2025_2026` et `automne_2026`, fenêtres saisonnières à resserrer autour des pics]
- Période de comparaison : 🔴 [à fixer après la courbe]
- Contrôles : 🔴 [précision et rappel, après extraction]

**Presse.** Presque tous les termes qui décrivent l'événement (`inondation`, `Crue`, `coulée de boue`, `pluie torrentielle`...) sont HRFP. Seuls les mots non HRFP font entrer un article dans `factiva_articles` (méthode commune §3.10) : le corpus presse sera tiré vers les articles qui contiennent déjà du vocabulaire environnemental, ce qui gonfle les taux de recontextualisation. Comparer les périodes entre elles en presse, ne pas comparer la presse à l'audiovisuel en niveau absolu, et le dire dans l'encadré méthodologique. 🔴 [à valider par l'expert]

## 2. Mots-clés

Dictionnaire OME : `quotaclimat/data_processing/mediatree/keyword/keyword.py`, entrées en français. Critère du dictionnaire noyau (méthode commune §3.1) : le terme désigne l'inondation ou la crue sans ambiguïté. Aucun terme n'est écarté parce qu'il est HRFP.

**Dictionnaire noyau retenu** (forme exacte du dictionnaire OME, pour l'audiovisuel)

| Terme | HRFP | Remarque |
|---|---|---|
| `inondation` | oui | |
| `inondations à répétition` | non | |
| `Crue` | oui | faux positifs à contrôler : participe de « croire », adjectif (« lumière crue ») |
| `crue centennale` | oui | |
| `coulée de boue` | oui | |
| `pluie extrême` | oui | |
| `pluie torrentielle` | oui | |
| `zone inondable` | oui | ramène aussi de l'urbanisme hors événement |
| `Inondable` | oui | idem |
| `augmentation du risque inondation` | non | |
| `anticiper les inondations` | non | |
| `anticiper les risques d'inondation` | non | |
| `gestion des inondations` | non | |
| `prévention des inondations` | non | |

En presse, les expressions qui contiennent `inondation` ou `crue` n'ajoutent aucun article ; elles sont gardées parce qu'elles ne sont pas HRFP (méthode commune §3.1). L'entrée `presse` du YAML déclare aussi les pluriels.

**Termes écartés et raison**

| Terme | Raison |
|---|---|
| `submergé par les eaux`, `englouti par les eaux`, `emporté par les eaux`, `cerné par les eaux`, `piégé par les eaux`, `sous les eaux`, `les eaux sont montées` | trop génériques, ne désignent pas l'inondation sans contexte |
| `eaux déchainées`, `eau boueuse`, `eaux boueuses` | trop génériques |
| `forte pluie`, `pluie intense`, `importante précipitation` | météo générique, ne désigne pas l'inondation |
| `déluge` | souvent figuré (« déluge de critiques ») |
| `méga-orage` | désigne l'orage, pas l'inondation |
| `régulation des crues` | 🔴 [raison à préciser] |
| `submersion`, `submersion marine` | inondations côtières liées aux tempêtes 🔴 [raison à confirmer] |
| `cyclone`, `ouragan` | autre aléa, surtout en outre-mer 🔴 [raison à confirmer] |
| `augmentation des précipitations`, `intensification des précipitations`, `le plus pluvieux`, `printemps le plus pluvieux` | constats climatiques sur la pluie, pas l'aléa |
| `améliorer les systèmes d'alerte et d'évacuation` | pas propre aux inondations |
| `renforcer les digues`, `Digue` | les digues peuvent être maritimes ou fluviales sans inondation en cours |
| `montée des eaux`, `montée du niveau de la mer`, `hausse du niveau de la mer`, `élévation du niveau de la mer`, `augmentation du niveau de la mer` | élévation du niveau marin, autre sujet |
| `rivière`, `cours d'eau`, `Bassin versant` | concepts généraux |
| `ruissellement`, `ruissellement de l'eau`, `ruissellement des eaux`, `eau de ruissellement`, `eaux de ruissellement` | ressources et sols ; à réutiliser pour un axe de causes (aménagement, imperméabilisation) |
| `déficit de pluie` | sécheresse |
| `eau de pluie`, `eaux pluviales` et autres mots de gestion de l'eau | gestion de la ressource, pas l'aléa |
| `jamais un tel phénomène` | générique |

Correspondance avec le dictionnaire OME vérifiée le 26/10/2026.

## 3. Axes

🔴 [à remplir à l'étape 2]

## 4. Résultats

Après publication uniquement.
