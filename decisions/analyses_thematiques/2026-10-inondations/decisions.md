# Inondations 2025-2026

- Responsable : Renan
- Référence pour les axes causes : [Bon Pote, « Inondations : est-ce la faute du changement climatique ? »](https://bonpote.com/inondations-est-ce-la-faute-du-changement-climatique/) (14/09/2021), méthode commune §5
- Statut : en cours, étape 2 (axes) ; dictionnaire noyau validé le 26/10/2026, révisé le 28/10/2026
- Entrée de `thematic_jobs_config.yml` : `inondations` (mots-clés mis à jour le 28/10/2026 ; périodes au §1 ; job lancé)
- Tables : `thematic_extracts.inondations_articles_fr`, `thematic_extracts.inondations_keywords_fr`
- Collection Metabase : `03. Analyses thématiques/21. Inondations 2026` (id 469)

## Angle

- Destination : rapport OMÉ S2 2026, bilan annuel, parution prévue en février 2027
- Ce que couvre l'analyse : les plus grosses inondations de 2026 en France, comparées à des épisodes antérieurs et à des épisodes à l'étranger

Questions auxquelles l'analyse répond (29/10/2026), avec les axes du §3 qui y répondent :

- Relie-t-on les inondations au réchauffement climatique, et en explique-t-on le mécanisme ? (A1 à A5)
- Les causes citées sont-elles météorologiques, locales ou politiques ? (B2, B6, B4, B5, B3)
- Renvoie-t-on la faute aux écologistes ou aux agriculteurs ? (B5 en partie : l'axe mesure la mention du modèle agricole, pas la mise en cause)
- Parle-t-on davantage des secours que de la prévention et de l'adaptation ? (curatif : D1, D2 ; préventif : D3 à D7 ; colonnes `pct_curatif` et `pct_preventif`)
- De quelles conséquences parle-t-on : humaines, matérielles et reconstruction, assurances, agriculture, routes et transports, économie, tourisme ? (C1 à C7)
- Parle-t-on davantage de la météo que du climat ? (X1)
- Quels épisodes ont le plus ému les médias, en France ou à l'étranger, et est-ce proportionné à leur gravité ? (V1, V2)

Plan de comparaison (30/10/2026), périodes de l'entrée `inondations` du YAML (dates au §1) :

| `id` | Épisode | Rôle |
|---|---|---|
| `202601_aude` | Aude, janvier 2026 | analyse |
| `202602_sud_ouest_fr` | Sud-Ouest, février 2026 | analyse |
| `202609_gard` | Gard et Hérault, septembre et octobre 2026 | analyse |
| `202610_espagne` | Espagne, octobre 2026 | comparaison à l'étranger, même année |
| `202608_nepal` | Népal, août et septembre 2026 | comparaison à l'étranger, même année |
| `202507_texas` | Texas (États-Unis), juillet 2025 | comparaison à l'étranger, antérieure |
| `202401_pas_de_calais` | Pas-de-Calais, janvier 2024 | comparaison antérieure, France |
| `202403_gard` | Gard, mars 2024 🔴 [épisode à décrire et sourcer] | comparaison antérieure, France 🔴 [rôle à confirmer] |
| `202404_yonne` | Yonne, fin mars et début avril 2024 🔴 [épisode à décrire et sourcer] | comparaison antérieure, France 🔴 [rôle à confirmer] |
| `202410_ardeche_valence` | Ardèche et Valence (Espagne), octobre 2024 | comparaison antérieure, France et étranger dans la même fenêtre, non séparés (regex de lieu retirée de V1 le 03/11/2026) |
| `202501_bretagne` | Bretagne, janvier 2025 | comparaison antérieure, France |

Le Gard et l'Espagne 2026, d'abord réunis dans une même fenêtre (`202609_gard_espagne`), sont séparés en deux fenêtres consécutives. Gard 03/24 et Yonne 04/24 ont été ajoutés ensuite. 🔴 [dates des deux changements à compléter]

## 1. Méthode spécifique

Méthode commune, sans écart à ce stade.

- Événements recensés (étape 0) : 🔴 [tableau début, fin, lieu, impact, source datée, à remplir par recherche en ligne puis compléter avec les pics de la courbe]
- Courbe journalière (étape 0) : modèles 7.1 et 7.2 de la méthode commune, audiovisuel du 01/01/2024 au 26/10/2026, presse par semaine, du lundi 06/01/2025 à la dernière semaine complète ingérée, mots du §2 ; audiovisuel : « AV - V2 - Courbe journalière inondations » (2329) ; presse : « PE - V2 - Courbe hebdomadaire inondations » (2394)
- Périodes (entrée `inondations` du YAML, fin incluse ci-dessous) :

  Onze fenêtres, chacune avec une tolérance d'une à deux semaines avant et après (méthode commune §3.2, événement météorologique). L'Ardèche et Valence, proches dans le temps, partagent une même fenêtre ; le Gard et l'Espagne 2026 sont séparés en deux fenêtres consécutives. Rôle de chaque période : plan de comparaison de l'Angle.

  | `id` | Du | Au | Épisodes | Presse |
  |---|---|---|---|---|
  | `202401_pas_de_calais` | 25/12/2023 | 14/01/2024 | Pas-de-Calais, second épisode | non |
  | `202403_gard` | 04/03/2024 | 17/03/2024 | Gard | non |
  | `202404_yonne` | 25/03/2024 | 08/04/2024 | Yonne | non |
  | `202410_ardeche_valence` | 07/10/2024 | 09/11/2024 | Ardèche et Espagne (Valence), fusionnés | non |
  | `202501_bretagne` | 20/01/2025 | 02/02/2025 | Bretagne | oui |
  | `202507_texas` | 30/06/2025 | 13/07/2025 | Texas (États-Unis) | oui |
  | `202601_aude` | 12/01/2026 | 25/01/2026 | Aude | oui |
  | `202602_sud_ouest_fr` | 09/02/2026 | 28/02/2026 | Sud-Ouest | oui |
  | `202608_nepal` | 24/08/2026 | 05/09/2026 | Népal | oui |
  | `202609_gard` | 21/09/2026 | 10/10/2026 | Gard et Hérault | oui |
  | `202610_espagne` | 11/10/2026 | 29/10/2026 | Espagne | oui |

  Libellés courts des périodes dans les questions (colonne `display_period`) : Pas-de-Calais 01/24, Gard 03/24, Yonne 04/24, Ardèche et Valence 10/24, Bretagne 01/25, Texas 07/25, Aude 01/26, Sud-Ouest 02/26, Népal 08/26, Gard 09/26, Espagne 10/26. 🔴 [libellé à trancher : Yonne 04/24 ou 03/24, la fenêtre commençant le 25/03]

  Le premier épisode du Pas-de-Calais (31/10 au 19/11/2023) est retiré : il précède les données S3 (01/12/2023), donc sans segments adjacents.

  🔴 [sources datées des épisodes à ajouter au tableau des événements]

- Période d'analyse et périodes de comparaison : plan de comparaison de l'Angle
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
| `augmentation du risque inondation` | non | |
| `anticiper les inondations` | non | |
| `anticiper les risques d'inondation` | non | |
| `gestion des inondations` | non | |
| `prévention des inondations` | non | |

Les expressions longues (`prévention des inondations`, `gestion des inondations`, `anticiper les inondations`...) sont gardées : en audiovisuel, la détection n'enregistre que l'expression la plus longue, et les retirer ferait sortir des segments du corpus. Les axes contextuels (prévention, solutions) utiliseront d'autres termes, pour éviter la circularité. En presse, les expressions qui contiennent `inondation` ou `crue` n'ajoutent aucun article ; elles sont gardées parce qu'elles ne sont pas HRFP (méthode commune §3.1). L'entrée `presse` du YAML déclare aussi les pluriels.

**Termes écartés et raison**

| Terme | Raison |
|---|---|
| `submergé par les eaux`, `englouti par les eaux`, `emporté par les eaux`, `cerné par les eaux`, `piégé par les eaux`, `sous les eaux`, `les eaux sont montées` | trop génériques, ne désignent pas l'inondation sans contexte |
| `eaux déchainées`, `eau boueuse`, `eaux boueuses` | trop génériques |
| `pluie extrême`, `pluie torrentielle` (retirés le 28/10/2026, après le premier lancement du job) | décrivent une cause de l'inondation, pas l'inondation ; les garder rendrait circulaire la mesure des axes causes et météo (B2) |
| `zone inondable`, `Inondable` (retirés le 28/10/2026, après le premier lancement du job) | relève de l'aménagement ; le garder rendrait circulaire la mesure d'un axe aménagement (D4) |
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

Termes écrits sous forme normalisée (méthode commune §3.5), en regex de mots entiers ; `s?` marque le pluriel. Aucun terme d'axe n'appartient au dictionnaire noyau (§3.1). Statut de tous les axes : à tester. Termes relus le 03/11/2026 dans les questions Metabase, qui font foi.

Audiovisuel : chunks thématiques, fenêtre de ±2 minutes (colonnes `_sans_fenetre` en contrôle). Presse : articles d'au moins 100 mots, regex sur le titre, le chapô et le corps, parts rapportées au corpus ; une ligne par média (`media_all` de `source_classification`, éditions papier et web additionnées avant le calcul des parts, comme pour l'analyse incendies, question 2220 ; `source_name` pour une source non classée) ; la part de couverture (V1 presse) est rapportée au total de `stats_factiva_articles`. Mêmes regex dans les deux médias. Une ligne par période et par chaîne ou titre, sans ligne cumulée sur les périodes (retirée le 03/11/2026) : regrouper les chunks ou articles de toutes les périodes pondérerait chaque période par son volume.

| Code | Axe | Origine | Termes | Graphique | Question Metabase (id) | Résultat |
|---|---|---|---|---|---|---|
| V1 | Volume (chunks et minutes) ; en presse, part des articles inondations dans le total des articles du titre. Colonne Espagne (`espagne`, `valence`, `barcelone`, `andalousie`) retirée le 03/11/2026 | banque V1 | corpus | G7 | 2427 (AV), 2480 (PE) | |
| V2 | Courbe journalière (presse : hebdomadaire) | banque V2 | corpus | G2 | 2329 / 2394 | testé |
| V4 | Part de couverture : part du temps d'antenne consacrée à l'environnement, au climat et à chaque maillon du climat, par chaîne, pendant chaque période du YAML face à ses 52 semaines précédentes (parts pondérées par les minutes monitorées). Forme retenue le 30/10/2026 : une ligne par période (`period`, mêmes id que le YAML) et par chaîne, avec côte à côte la part de la période (`pct_<part>`), celle des 52 semaines précédentes (`pct_<part>_reference`) et l'écart en points (`ecart_pts_<part>`) ; sans variable, sans ligne Ensemble ni cumul (retirées le 03/11/2026). La table est hebdomadaire : une semaine entre dans une période si son lundi est dans ses dates. Les périodes sont recopiées du YAML dans la requête, à mettre à jour quand il change | banque V4 | `public.core_query_environmental_shares` | Barres avant / pendant (G8), donut de répartition (G5) | 2432 (AV) | |
| V3 | Mots les plus employés, en Pareto : occurrences dans le texte de chaque terme du dictionnaire noyau et des axes, chunks thématiques et voisins ; variable `maillon` (all, noyau, constat, causes, consequences, solutions), all sans le noyau ni B1 (qui répète A1) | banque V3, étendue aux termes des axes | noyau (avec pluriels) et termes des axes 🔴 [Pareto à resynchroniser avec les termes ci-dessous] | Pareto, barres et part cumulée | 2430 (AV) | |
| X1 | Occurrences des mentions du changement climatique (regex de A1) et de la météo (regex de B2), plusieurs possibles par chunk, en valeur et par heure de grille (`program_metadata`), chunks thématiques seuls et avec leurs voisins à ±2 minutes (`_avec_fenetre`) | spécifique | ceux de A1 et B2 | 🔴 [graphique à choisir] | 2457 (AV) | |
| A1 | Mentions explicites du changement climatique | banque A1 | ci-dessous | G7 | 2429 (AV), 2463 (PE) | |
| A2 | Mécanismes physiques de l'inondation | spécifique | ci-dessous | G7 | 2429 (AV), 2463 (PE) | |
| A3 | Voix scientifiques | banque A3, + hydrologues et météorologues | ci-dessous | G7 | 2429 (AV), 2463 (PE) | |
| A4 | Mise en série | banque A2, réécrit | ci-dessous | G7 | 2429 (AV), 2463 (PE) | |
| A5 | Causes du réchauffement climatique, dictionnaire OME hors HRFP (ancien A6, renuméroté le 03/11/2026) | spécifique | construit dans la requête depuis public.dictionary, ci-dessous | G7 | 2429 (AV), 2463 (PE) | |
| B1 | Mentions explicites du changement climatique | banque A1, reprise dans les causes | même liste que A1 | G7 | 2426 (AV), 2464 (PE) | |
| B2 | Causes météorologiques directes | variantes de « forte pluie » du dictionnaire OME | ci-dessous | G7 | 2426 (AV), 2464 (PE) | |
| B3 | Gouvernance et politique locale | spécifique | ci-dessous | G7 | 2426 (AV), 2464 (PE) | |
| B4 | Urbanisation et aménagement (colonne `b4_urbanisation_amenagement`, anciennement facteurs locaux aggravants) | spécifique | ci-dessous | G7 | 2426 (AV), 2464 (PE) | |
| B5 | Modèle agricole (colonne `b5_modele_agricole`) | spécifique | ci-dessous | G7 | 2426 (AV), 2464 (PE) | |
| B6 | Sécheresse (colonne `b6_secheresse`, ancien A5 du constat, passé dans les causes le 03/11/2026) | banque B3, réduite | ci-dessous | G7 | 2426 (AV), 2464 (PE) | |
| C1 | Conséquences humaines | spécifique | ci-dessous | G7 | 2436 (AV), 2465 (PE) | |
| C2 | Dommages matériels et reconstruction | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| C3 | Assurances | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| C4 | Agriculture (mentions larges) | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| C5 | Routes et transports (mentions larges, colonne `c5_routes_transports`) | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| C6 | Économie (mentions larges, génériques) | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| C7 | Tourisme (mentions larges, génériques) | banque C1 (secteur) | ci-dessous | G6 | 2436 (AV), 2465 (PE) | |
| D1 | Secours (curatif) | banque D3 | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D2 | Alerte des habitants et préparation (curatif) | banque D3 | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D3 | Aménagement (préventif) | banque D3 | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D4 | Végétalisation (préventif) | banque D3 | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D5 | Entretien des cours d'eau (préventif) | spécifique | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D6 | Politiques et financements d'adaptation (préventif) | spécifique | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |
| D7 | Atténuation à la racine (préventif) | banque D1, liste atténuation | ci-dessous | G11 | 2434 (AV), 2466 (PE) | |

Croisements prévus, faits dans le Google Sheets des résultats : lien CC (A1) et causes du réchauffement (A5), colonnes `pct_a1_lien_cc` et `pct_a5_causes_rechauffement` de la question 2429 ; lien CC (A1) et mécanismes (A2) (G13, nuage de points, un point par média) ; curatif ou préventif (banque D3) : curatif = D1 ou D2, préventif = D3 à D7 (colonnes `pct_curatif` et `pct_preventif`, un chunk ou un article peut être les deux ; G11, barres empilées par média). D6 ajouté au préventif le 03/11/2026.

**Axes retirés** : climat d'après le dictionnaire OME explicite (redondant avec A1, les mentions explicites) ; adaptation d'après le dictionnaire OME (redondant avec D3) ; absence de solution, fatalité (ancien D7) ; reconstruction, temps long (ancien C5, fusionné dans C2) ; emploi et activité économique (ancien C6, remplacé par l'économie locale) ; agriculture large, sans « inondés » (trop large).

**Axes de la banque non exploités** : B2 co-aléa chaleur (sans lien direct avec l'inondation) ; B6 forces de l'ordre et justice (pas de cause criminelle). X3 proportionnalité : couvert par la courbe journalière (V2), rapprochée des événements. 🔴 [à décider : B5 aléas composés, C2 santé, C3 précarité du ménage, C4 personnes vulnérables, D2 solutions individuelles ou systémiques]

**Termes des axes spécifiques**

- **A1 Mentions explicites du changement climatique** (et B1, et regex CC de X1) : `changements? climatiques?`, `dereglement climatique`, `dereglement du climat`, `rechauffement climatique`, `rechauffement de la planete`, `rechauffement planetaire`, `crise climatique`, `urgence climatique`, `climat se rechauffe`, `le climat change`, `risques climatiques`
- **A2 Mécanismes physiques** : `vapeur d eau`, `atmosphere plus chaude`, `evaporation`, `cycle de l eau`, `oceans? plus chauds?`, `mediterranee plus chaude`, `temperature de surface de la mer`, `clausius clapeyron`. `evaporation` est large : à contrôler.
- **A3 Voix scientifiques** : `climatologues?`, `meteorologues?`, `hydrologues?`, `hydrogeologues?`, `scientifiques?`, `chercheu(r|se)s?`, `giec`, `cnrs`, `universite`, `expert(e)?s? du climat`. `scientifiques?` et `universite` sont larges : à contrôler.
- **A4 Mise en série** (réécrit dans la question 2429) : `(crues?|inondations?|episodes?) (plus|de plus en plus) frequent(e)?s?`, `(crues?|inondations?|episodes?) de plus en plus intenses?`, `(crues?|inondations?|episodes?) a repetitions?`, `(multiplication|intensification) des (inondations|crues|episodes)`. Chaque terme est ancré sur l'aléa : `record`, `jamais vu`, `nouvelle norme` et `comme en 2024` ne sont plus dans l'axe ; `inondations? sans precedent` et `(crues?|inondations?|episodes?) historiques?` retirés le 03/11/2026.
- **A5 Causes du réchauffement climatique** (ancien A6) : mots français du dictionnaire OME de thème changement_climatique_causes (direct et indirect), hors HRFP, lus dans public.dictionary et normalisés dans la requête (54 mots le 28/10/2026). Liste tenue à jour par le dictionnaire, plus besoin de la maintenir ici. Mots hors sujet possibles : a69, mercosur, greenwashing, bouilloire thermique.
- **B2 Causes météorologiques directes** (et regex météo de X1) : variantes de « forte pluie » du dictionnaire OME, `fortes? pluies?`, `pluies? (intenses?|extremes?|torrentielles?)`, `importantes precipitations`, `mega orages?`. `orages?`, `cumuls? de pluie`, `millimetres de pluie`, `episodes? pluvieux`, `un mois de pluie` et `pluies? diluviennes?` ne sont plus dans l'axe (pluie seule jugée trop large).
- **B3 Gouvernance et politique locale** : `choix politiques?`, `defaillance de l etat`, `manque d anticipation`, `sous investissement`, `responsabilite des elus`, `cout de l inaction` (`court termisme` retiré le 29/10/2026 : jugement de valeur, jamais employé) ; ajoutés le 29/10/2026 depuis l'axe « absence de gouvernance » de l'analyse incendies 2026 (première liste sans résultat) : `prevention insuffisante`, `manque de (moyens|prevention)`, `aurai(en)?t pu etre evitee?s?` (accordé à « les inondations », 29/10/2026), `alertes? ignorees?` (`risques? connus?` et `risques? annonces?` retirés ensuite dans la question 2426). Écartés de cette liste incendies : termes propres au feu (`interdiction de fauchage`, `celex`, interdictions), arrêtés (ambigus avec les arrêtés de catastrophe naturelle et d'évacuation), `aurait du` (trop large).
- **B4 Urbanisation et aménagement** (renommé le 29/10/2026 dans la question 2426) : `artificialisation`, `impermeabilisation`, `sols? impermeabilises?`, `beton(isation|nage)`, `etalement urbain`, `saturation des sols`, `constructions? en zone a risque`, `vetuste des reseaux`, `gestion des eaux pluviales`. Limite : l'axe détecte la mention, pas ce qu'on en dit (méthode commune §3.10, valence). `curage`, `entretien des cours d eau` et `embacles?` retirés le 03/11/2026 : ils recoupaient D5. `arrachage des haies` et `remembrement` passés en B5 le 03/11/2026.
- **B5 Modèle agricole** (03/11/2026) : modèle agricole, `agriculture (intensive|industrielle|productiviste)`, `monocultures?`, `grandes cultures` ; haies et bocage, `arrachage des haies`, `(suppression|destruction|disparition) des haies`, `haies arrachees`, `remembrement`, `disparition du bocage`, `talus arases` ; sols, `tassement des sols`, `sols? compactes?`, `compaction des sols`, `sols? nus?`, `battance`, `sols? battants?`, `erosion des sols` ; ruissellement agricole, `ruissellement (sur les|des) (champs|terres|parcelles)`, `ruissellement agricole`. Écartés : prairies et drainage (familles non retenues), `mais` (confondu avec la conjonction une fois normalisé), `coulee de boue` (noyau), `pesticides`, `engrais`, `nitrates` (pollution, pas cause), `agriculteurs?` seul (recoupe C4). `grandes cultures` et `erosion des sols` peuvent relever des conséquences : à contrôler. Limite : comme B4, l'axe détecte la mention, pas la mise en cause. 🔴 [lien entre ces pratiques et le ruissellement à faire valider par l'expert, avec source]
- **B6 Sécheresse** (ancien A5, passé du constat aux causes le 03/11/2026 ; liste de la banque arrêtée aux termes qui nomment explicitement la sécheresse) : `secheresses?`, `deficit hydrique`, `sols? secs?`, `sol tres sec`, `manque d eau`, `penurie d eau`. L'ancien A6 (causes du réchauffement) est renuméroté A5.
- **C1 Conséquences humaines** : `evacue(e)?s?`, `sinistre(e)?s?`, `bilan humain`, `vies? humaines?`, `personnes? disparues?`, `disparu(e)?s`, `personnes? deplacee?s`, `deplacements? de (personnes|populations?)`, `morts?`, `deces`, `victimes?`. Féminins, disparus et déplacés ajoutés le 29/10/2026, inspirés des posts LinkedIn de QuotaClimat ; les termes de détresse psychologique (`detresse`, `traumatisme`, `traumatise(e)?s?`, `angoisse`, `stress post traumatique`) ne sont plus dans l'axe.
- **C2 Dommages matériels et reconstruction** : `degats materiels`, `milliards d euros de degats`, `millions d euros de degats`, `coupures? d electricite`, `reconstruire`, `reconstruction` ; ajoutés le 03/11/2026 : `infrastructures? (detruites?|endommagees?)`, `(maisons?|habitations?|logements?) (detruite?s?|devastee?s?|endommagee?s?|sinistree?s?)`, `degats (considerables|importants)`, `ampleur des degats`, `estimation des degats`, `cout des degats`, `bilan materiel`. `pertes economiques` passé en C6 ; `retour a la normale` retiré.
- **C3 Assurances** : `regime cat nat`, `cat( )?nat`, `arretes? de catastrophe naturelle`, `indemnis(ations?|er?)`, `assureurs?`, `france assureurs`, `surprime`, `inassurable`, `reassurance`, `primes d assurance` (`franchise` retiré le 29/10/2026 : trop large).
- **C4 à C7, mentions larges du secteur** (03/11/2026) : les listes d'impacts explicites (cultures inondées, routes coupées, commerces sinistrés, campings évacués...) sont remplacées par des mentions larges, sans terme propre aux inondations, pour réutiliser les axes sur d'autres épisodes ; le contexte vient du corpus noyau. Les axes mesurent qu'on parle du secteur, pas qu'on parle de ses dégâts.
- **C4 Agriculture** : `agriculture`, `agricoles?`, `agriculteurs?`, `agricultrices?`, `paysans?`, `eleveurs?`, `elevages?`, `maraichers?`, `maraichage`, `viticulteurs?`, `vignerons?`, `arboriculteurs?`, `cerealiers?`, `recoltes?`. Retirés le 03/11/2026, jugés trop larges ou peu utiles : `exploitations? agricoles?`, `exploitants? agricoles?` (déjà couverts par `agricoles?`), `semis`, `betail`, `cheptel`, `fnsea`, `chambres? d agriculture`, `confederation paysanne`.
- **C5 Routes et transports** : `routes?`, `axes? routiers?`, `autoroutes?`, `routiers?`, `circulation`, `automobilistes?`, `deviations?`, `ponts?`, `transports?`, `trains?`, `sncf`, `tgv`, `ferroviaires?`, `voies? ferrees?`. Faux positifs possibles : « feuille de route », « faire le pont », « libre circulation ».
- **C6 Économie** : `economie`, `economiques?`, `entreprises?`, `commerces?`, `commercants?`, `artisans?`, `pme`, `tpe`, `chiffre d affaires`, `emplois?`, `salaries?`, `cci`, `chambres? de commerce` (`activite economique` retiré le 03/11/2026, déjà couvert par `economiques?`). `economiques?`, `emplois?` et `salaries?` sont très fréquents hors sujet.
- **C7 Tourisme** : `tourisme`, `touristiques?`, `touristes?`, `vacanciers?`, `campings?`, `hotels?`, `hoteliers?`, `restaurateurs?`, `gites?`, `office de tourisme`, `saison estivale`, `arriere saison`. `hotels?` compte aussi « hôtel de ville », « hôtel de région », « hôtel de police » : à contrôler.
- **D1 Secours** : `secours`, `sapeurs? pompiers?`, `sauveteurs?`, `gendarmes?`, `helicopteres?`, `cellule de crise`, `plan orsec`, `securite civile`
- **D2 Alerte des habitants et préparation** : `batardeaux?`, `calfeutrer`, `sacs de sable`, `kit d urgence`, `plan communal de sauvegarde`, `exercices? d evacuation`, `systemes? d alerte`, `fr alert`, `vigilance (orange|rouge|crues)`, `alerte precoce`, `evacuation preventive`, `vigicrues( flash)?`, `service de prevision des crues`, `hydrometrie`, `stations? hydrometriques?`, `reseau d observation`, `avertissement automatique`. `apic` n'est plus dans l'axe.
- **D3 Aménagement** (ancien D4) : `relocalisation`, `demolition des habitations`, `rachat des maisons`, `expropriation`, `non constructible`, `zone inondable`, `inconstructible`, `interdiction de construire`, `desartificialisation`, `surelevation`, `digues?`, `systemes? d endiguement`, `bassins? de retention`, `espaces? verts?`. Ne sont plus dans l'axe : `repli strategique`, `recul strategique`, `fonds barnier`, `pilotis`, `piece refuge`, `bassins? ecreteurs?`, `ecretement`, `ouvrages? de protection`, `amenagement hydraulique`. `espaces? verts?` est large.
- **D4 Végétalisation** (ancien D5) : `desimpermeabilisation`, `desimpermeabiliser`, `vegetalisation`, `renaturation`, `zones? humides?`, `agroforesterie`, `ville eponge`, `bio retention`, `zan`, `zero artificialisation nette`, `loi climat (et )?resilience`, `replanter (d|l)es haies`, `replantage des haies`. Ne sont plus dans l'axe : `remeandrage`, `ripisylve` ; `restauration des zones? humides?` élargi à `zones? humides?`.
- **D5 Entretien des cours d'eau** : `(curage|cure(r|ent)?) (d|l)es (cours d eau|fosses|rivieres)`, `entret(ien|enir) (d|l)es (cours d eau|fosses|rivieres|berges)`, `nettoy(age|er) (l|d)es (cours d eau|rivieres|fosses)`, `(retrait|enlevement) des embacles`, `desenvasement`, `nettoyage du lit (de la|des|du) (cours d eau|rivieres?)`, `nettoyer le lit (de la|des|du) (cours d eau|rivieres?)` (ajoutés le 03/11/2026). Les formes en « du », « de la » (« curage du ruisseau », « entretien du cours d'eau ») ne sont pas détectées : élargissement proposé le 03/11/2026, non retenu. Limite : comme B4, l'axe détecte la mention, pas la position (l'entretien des cours d'eau est un argument contesté).
- **D6 Politiques et financements d'adaptation** : `fonds vert`, `plan national d adaptation`, `pnacc`, `fonds? barnier`, `loi climat (et )?resilience`, `fonds de prevention des risques naturels`, `fprnm`, `politiques? d adaptation`, `strategies? d adaptation`, `plans? d adaptation`, `finance(r|ment) (de )?l adaptation`, `plans? de prevention des risques( naturels)?` (élargi le 03/11/2026). La loi Climat et résilience est comptée en D4 et en D6, sous ses deux formes. Non retenus : `ppri`, `papi`, `gemapi`, `tracc`, directive inondation. 🔴 [noms officiels des dispositifs (fonds de prévention des risques naturels majeurs, plans de prévention des risques) à faire vérifier par l'expert]
- **D7 Atténuation à la racine** (ancien D3, liste atténuation de la banque) : `reduire les emissions`, `reduction des emissions`, `baisse des emissions`, `decarbonation`, `decarboner`, `neutralite carbone`, `zero emission nette`, `sortie des energies fossiles`, `sortie du charbon`, `energies fossiles`, `puits de carbone`, `budget carbone`, `objectifs climatiques`, `accord de paris`, `green deal`, `plan climat`, `pniec`, `snbc`

**Termes de X1** : regex CC = A1, regex météo = B2, comptées avec `regexp_count` (plusieurs occurrences par chunk). Le ratio météo / CC est un rapport de totaux, pas une moyenne de ratios.

## 4. Résultats

Après publication uniquement.
