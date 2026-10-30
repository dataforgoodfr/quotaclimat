# Inondations 2025-2026

- Responsable : Renan
- Référence pour les axes causes : [Bon Pote, « Inondations : est-ce la faute du changement climatique ? »](https://bonpote.com/inondations-est-ce-la-faute-du-changement-climatique/) (14/09/2021), méthode commune §5
- Statut : en cours, étape 2 (axes) ; dictionnaire noyau validé le 26/10/2026, révisé le 28/10/2026
- Entrée de `thematic_jobs_config.yml` : `inondations` (mots-clés mis à jour le 28/10/2026 ; périodes au §1 ; job lancé)
- Tables : `thematic_extracts.inondations_articles_fr`, `thematic_extracts.inondations_keywords_fr`
- Collection Metabase : `03. Analyses thématiques/21. Inondations 2026` (id 469)
- Document de résultats (avant publication) : 🔴 [lien document Metabase à créer]

## Angle

- Destination : rapport OMÉ S2 2026, bilan annuel, parution prévue en février 2027
- Ce que couvre l'analyse : les plus grosses inondations de 2026 en France, comparées à des épisodes antérieurs et à des épisodes à l'étranger

Questions auxquelles l'analyse répond (29/10/2026), avec les axes du §3 qui y répondent :

- Relie-t-on les inondations au réchauffement climatique, et en explique-t-on le mécanisme ? (A1 à A6)
- Les causes citées sont-elles météorologiques, locales ou politiques ? (B2, B4, B3)
- Renvoie-t-on la faute aux écologistes ou aux agriculteurs ?
- Parle-t-on davantage des secours que de la prévention et de l'adaptation ? (D1 face à D2, D4 et D5)
- De quelles conséquences parle-t-on : humaines, matérielles, assurances, agriculture, reconstruction ? (C1 à C5)
- Quels épisodes ont le plus ému les médias, en France ou à l'étranger, et est-ce proportionné à leur gravité ? (V2, colonne `espagne` de V1)

Plan de comparaison (30/10/2026), périodes de l'entrée `inondations` du YAML (dates au §1) :

| `id` | Épisode | Rôle |
|---|---|---|
| `202601_aude` | Aude, janvier 2026 | analyse |
| `202602_sud_ouest_fr` | Sud-Ouest, février 2026 | analyse |
| `202609_gard_espagne` | Gard, Hérault et Espagne, septembre et octobre 2026 | analyse (France) et comparaison à l'étranger (Espagne), dans la même fenêtre : à séparer par une regex de lieu (piste X7, colonne `espagne` de V1) |
| `202608_nepal` | Népal, août et septembre 2026 | comparaison à l'étranger, même année |
| `202507_texas` | Texas (États-Unis), juillet 2025 | comparaison à l'étranger, antérieure |
| `202401_pas_de_calais` | Pas-de-Calais, janvier 2024 | comparaison antérieure, France |
| `202410_ardeche_valence` | Ardèche et Valence (Espagne), octobre 2024 | comparaison antérieure, France et étranger dans la même fenêtre (regex de lieu) |
| `202501_bretagne` | Bretagne, janvier 2025 | comparaison antérieure, France |

## 1. Méthode spécifique

Méthode commune, sans écart à ce stade.

- Événements recensés (étape 0) : 🔴 [tableau début, fin, lieu, impact, source datée, à remplir par recherche en ligne puis compléter avec les pics de la courbe]
- Courbe journalière (étape 0) : modèles 7.1 et 7.2 de la méthode commune, audiovisuel du 01/01/2024 au 26/10/2026, presse par semaine, du lundi 06/01/2025 à la dernière semaine complète ingérée, mots du §2 ; audiovisuel : « AV - V2 - Courbe journalière inondations » (2329) ; presse : « PE - V2 - Courbe hebdomadaire inondations » (2394)
- Périodes (entrée `inondations` du YAML, fin incluse ci-dessous) :

  Huit fenêtres, chacune avec une tolérance d'une à deux semaines avant et après (méthode commune §3.2, événement météorologique). Deux épisodes proches dans le temps partagent une même fenêtre. Rôle de chaque période : plan de comparaison de l'Angle.

  | `id` | Du | Au | Épisodes | Presse |
  |---|---|---|---|---|
  | `202401_pas_de_calais` | 25/12/2023 | 14/01/2024 | Pas-de-Calais, second épisode | non |
  | `202410_ardeche_valence` | 07/10/2024 | 09/11/2024 | Ardèche et Espagne (Valence), fusionnés | non |
  | `202501_bretagne` | 20/01/2025 | 02/02/2025 | Bretagne | oui |
  | `202507_texas` | 30/06/2025 | 13/07/2025 | Texas (États-Unis) | oui |
  | `202601_aude` | 12/01/2026 | 25/01/2026 | Aude | oui |
  | `202602_sud_ouest_fr` | 09/02/2026 | 28/02/2026 | Sud-Ouest | oui |
  | `202608_nepal` | 24/08/2026 | 05/09/2026 | Népal | oui |
  | `202609_gard_espagne` | 21/09/2026 | 26/10/2026 | Gard et Hérault, et Espagne, fusionnés | oui |

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

Termes écrits sous forme normalisée (méthode commune §3.5), en regex de mots entiers ; `s?` marque le pluriel. Aucun terme d'axe n'appartient au dictionnaire noyau (§3.1). Statut de tous les axes : à tester.

| Code | Axe | Origine | Termes | Graphique | Question Metabase (id) | Résultat |
|---|---|---|---|---|---|---|
| V1 | Volume, dont segments évoquant l'Espagne | banque V1 | corpus ; Espagne : espagne, valence, barcelone, andalousie | G7 | 2427 (AV) | |
| V2 | Courbe journalière (presse : hebdomadaire) | banque V2 | corpus | G2 | 2329 / 2394 | testé |
| V4 | Part de couverture : part du temps d'antenne consacrée à l'environnement, au climat et à chaque maillon du climat, par chaîne et pour l'ensemble, semaines du 21/09 au 25/10/2026 face aux 52 semaines précédentes (parts pondérées par les minutes monitorées) | banque V4 | `public.core_query_environmental_shares` | Barres avant / pendant (G8), donut de répartition (G5) | 2432 (AV) | |
| V3 | Mots les plus employés, en Pareto : occurrences dans le texte de chaque terme du dictionnaire noyau et des axes, chunks thématiques et voisins ; variable `maillon` (all, noyau, constat, causes, consequences, solutions), all sans le noyau ni B1 (qui répète A1) | banque V3, étendue aux termes des axes | noyau (avec pluriels) et termes de A1 à D6 | Pareto, barres et part cumulée | 2430 (AV) | |
| A1 | Mentions explicites du changement climatique | banque A1 | [banque](../decisions.md#lien-avec-le-changement-climatique) | G7 | 2429 (AV) | |
| A2 | Mécanismes physiques de l'inondation | spécifique | ci-dessous | G7 | 2429 (AV) | |
| A3 | Voix scientifiques | banque A3, + hydrologues et météorologues | ci-dessous | G7 | 2429 (AV) | |
| A4 | Mise en série | banque A2, sans `comme en 2022` ni `saison des feux` | ci-dessous | G7 | 2429 (AV) | |
| A5 | Sécheresse | banque B3, réduite | ci-dessous | G7 | 2429 (AV) | |
| A6 | Causes du réchauffement climatique, dictionnaire OME hors HRFP | spécifique | construit dans la requête depuis public.dictionary, ci-dessous | G7 | 2429 (AV) | |
| B1 | Mentions explicites du changement climatique | banque A1, reprise dans les causes | même liste que A1 | G7 | 2426 (AV) | |
| B2 | Causes météorologiques directes | banque B4, termes remplacés | ci-dessous | G7 | 2426 (AV) | |
| B3 | Gouvernance et politique locale | spécifique | ci-dessous | G7 | 2426 (AV) | |
| B4 | Urbanisation et aménagement (colonne `b4_urbanisation_amenagement`, anciennement facteurs locaux aggravants) | spécifique | ci-dessous | G7 | 2426 (AV) | |
| C1 | Conséquences humaines | spécifique | ci-dessous | G7 | | |
| C2 | Dommages matériels | banque C1 (secteur) | ci-dessous | G6 | | |
| C3 | Assurances | banque C1 (secteur) | ci-dessous | G6 | | |
| C4 | Agriculture | banque C1 (secteur) | ci-dessous | G6 | | |
| C5 | Reconstruction, temps long | spécifique | ci-dessous | G7 | | |
| C6 | Emploi et activité économique | banque C1 (secteur) | ci-dessous | G6 | | |
| C7 | Routes et transports | banque C1 (secteur) | ci-dessous | G6 | | |
| D1 | Secours (curatif) | banque D3 | ci-dessous | G11 | 2434 (AV) | |
| D2 | Alerte des habitants et préparation | banque D3 | ci-dessous | G11 | 2434 (AV) | |
| D3 | Atténuation du changement climatique | banque D1 | [banque](../decisions.md#solutions), liste atténuation | G11 | 2434 (AV) | |
| D4 | Aménagement | banque D3 | ci-dessous | G11 | 2434 (AV) | |
| D5 | Végétalisation | banque D3 | ci-dessous | G11 | 2434 (AV) | |
| D6 | Politiques et financements d'adaptation | spécifique | ci-dessous | G11 | 2434 (AV) | |
| D7 | Absence de solution, fatalité | spécifique | ci-dessous | G11 | 2434 (AV) | |

Croisements prévus : `A1 x A6` (colonnes `pct_a1_lien_cc` et `pct_a6_causes_rechauffement` de la question 2429) et `A1 x A2` (G13, nuage de points, un point par média) ; curatif ou préventif (banque D3) : D1 face à D2, D4 et D5 (G11, barres empilées par média).

**Axes retirés** : climat d'après le dictionnaire OME explicite (redondant avec A1, les mentions explicites).

**Axes de la banque non exploités** : B2 co-aléa chaleur (sans lien direct avec l'inondation) ; B6 forces de l'ordre et justice (pas de cause criminelle). X3 proportionnalité : couvert par la courbe journalière (V2), rapprochée des événements. 🔴 [à décider : B5 aléas composés, C2 santé, C3 précarité du ménage, C4 personnes vulnérables, D2 solutions individuelles ou systémiques]

**Termes des axes spécifiques**

- **A2 Mécanismes physiques** : `vapeur d eau`, `atmosphere plus chaude`, `evaporation`, `cycle de l eau`, `oceans? plus chauds?`, `mediterranee plus chaude`, `temperature de surface de la mer`, `clausius clapeyron`. `evaporation` est large : à contrôler.
- **A3 Voix scientifiques** : `climatologues?`, `meteorologues?`, `hydrologues?`, `hydrogeologues?`, `scientifiques?`, `chercheu(r|se)s?`, `giec`, `meteo france`, `cnrs`, `universite`, `expert(e)?s? du climat`. `scientifiques?` et `universite` sont larges : à contrôler.
- **A4 Mise en série** : `plus frequent(e)?s?` (couvre aussi « de plus en plus fréquents »), `de plus en plus intenses?`, `record`, `jamais vu`, `sans precedent`, `nouvelle norme`, `crue historique`, `comme en 2024`, `multiplication des (inondations|crues|episodes)` (ajouté le 29/10/2026, inspiré des posts LinkedIn de QuotaClimat). `record` est large : à contrôler.
- **A5 Sécheresse** (liste de la banque arrêtée aux termes qui nomment explicitement la sécheresse) : `secheresses?`, `deficit hydrique`, `sols? secs?`, `sol tres sec`, `manque d eau`, `penurie d eau`
- **A6 Causes du réchauffement climatique** : mots français du dictionnaire OME de thème changement_climatique_causes (direct et indirect), hors HRFP, lus dans public.dictionary et normalisés dans la requête (54 mots le 28/10/2026). Liste tenue à jour par le dictionnaire, plus besoin de la maintenir ici. Mots hors sujet possibles : a69, mercosur, greenwashing, bouilloire thermique.
- **B2 Causes météorologiques directes** : `pluies? (diluviennes?|torrentielles?|extremes?|intenses?)`, `fortes? pluies?`, `precipitations? extremes?`, `orages?`, `cumuls? de pluie`, `millimetres de pluie`, `episodes? pluvieux`, `un mois de pluie` (ajouté le 29/10/2026, posts LinkedIn)
- **B3 Gouvernance et politique locale** : `choix politiques?`, `defaillance de l etat`, `manque d anticipation`, `sous investissement`, `responsabilite des elus`, `cout de l inaction` (`court termisme` retiré le 29/10/2026 : jugement de valeur, jamais employé) ; ajoutés le 29/10/2026 depuis l'axe « absence de gouvernance » de l'analyse incendies 2026 (première liste sans résultat) : `prevention insuffisante`, `manque de (moyens|prevention)`, `aurai(en)?t pu etre evitee?s?` (accordé à « les inondations », 29/10/2026), `alertes? ignorees?` (`risques? connus?` et `risques? annonces?` retirés ensuite dans la question 2426). Écartés de cette liste incendies : termes propres au feu (`interdiction de fauchage`, `celex`, interdictions), arrêtés (ambigus avec les arrêtés de catastrophe naturelle et d'évacuation), `aurait du` (trop large).
- **B4 Urbanisation et aménagement** (renommé le 29/10/2026 dans la question 2426) : `artificialisation`, `impermeabilisation`, `sols? impermeabilises?`, `beton(isation|nage)`, `etalement urbain`, `saturation des sols`, `arrachage des haies`, `remembrement`, `curage`, `entretien des cours d eau`, `embacles?`, `constructions? en zone a risque`, `vetuste des reseaux`, `gestion des eaux pluviales`. Limite : l'axe détecte la mention, pas ce qu'on en dit (méthode commune §3.10, valence) ; `curage` et `entretien des cours d eau` sont aussi des arguments contestés.
- **C1 Conséquences humaines** : `evacue(e)?s?`, `sinistre(e)?s?`, `bilan humain`, `vies? humaines?`, `personnes? disparues?`, `disparu(e)?s`, `personnes? deplacee?s`, `deplacements? de (personnes|populations?)`, `morts?`, `deces`, `victimes?`, `detresse`, `traumatisme`, `traumatise(e)?s?`, `angoisse`, `stress post traumatique`. Féminins, disparus, déplacés et détresse psychologique ajoutés le 29/10/2026, inspirés des posts LinkedIn de QuotaClimat (question 2433).
- **C2 Dommages matériels** : `degats materiels`, `milliards d euros de degats`, `millions d euros de degats`, `pertes economiques`, `coupures? d electricite`
- **C3 Assurances** : `regime cat nat`, `catnat`, `arretes? de catastrophe naturelle`, `indemnisation`, `assureurs?`, `france assureurs`, `surprime`, `inassurable`, `reassurance` (`franchise` retiré le 29/10/2026 : trop large).
- **C4 Agriculture** : `cultures inondees`, `parcelles inondees`, `champs inondes`, `terres agricoles inondees`, `recoltes? detruites?`, `pertes? de recolte`, `recolte compromise`, `semis detruits`, `semis retardes`, `impossibilite de semer`, `retard des semis`, `pertes? de rendement`, `baisse des rendements`, `cereales noyees`, `vergers inondes`, `vignes inondees`, `maraichage`, `cultures maraicheres`
- **C5 Reconstruction, temps long** : `reconstruire`, `reconstruction`, `retour a la normale`
- **C6 Emploi et activité économique** (29/10/2026) : `chomage (partiel|technique)`, `activite partielle`, `pertes? d emplois?`, `emplois? menaces?`, `perte d exploitation`, `entreprises? sinistrees?`, `commerces? (fermes?|sinistres?|detruits?)`, `commercants?`, `artisans?`, `salaries?`, `jours? de travail perdus?`. `salaries?`, `commercants?` et `artisans?` sont larges.
- **C7 Routes et transports** (29/10/2026) : `routes? (coupees?|barrees?|fermees?|inondees?)`, `axes? routiers?`, `autoroutes? (coupees?|fermees?)`, `circulation (interrompue|coupee|perturbee)`, `trafic (interrompu|perturbe|ferroviaire)`, `trains? (supprimes?|annules?)`. Retirés car trop larges : `sncf`, `voies? ferrees?`, `ponts? (effondres?|emportes?|fermes?)`, `vehicules? emportes?`, `voitures? emportees?`, `automobilistes?`, `deviations?`, `transports? scolaires?`, `vols? annules?`
- **D1 Secours** : `secours`, `sapeurs? pompiers?`, `sauveteurs?`, `helicopteres?`, `cellule de crise`, `plan orsec`, `securite civile`
- **D2 Alerte des habitants et préparation** : `batardeaux?`, `calfeutrer`, `sacs de sable`, `kit d urgence`, `plan communal de sauvegarde`, `exercices? d evacuation`, `systemes? d alerte`, `fr alert`, `vigilance orange`, `vigilance rouge`, `alerte precoce`, `evacuation preventive`, `vigicrues`, `vigicrues flash`, `apic`, `service de prevision des crues`, `hydrometrie`, `stations? hydrometriques?`, `reseau d observation`, `vigilance crues`, `avertissement automatique`
- **D4 Aménagement** : `repli strategique`, `recul strategique`, `relocalisation`, `demolition des habitations`, `rachat des maisons`, `expropriation`, `non constructible`, `zone inondable`, `inconstructible`, `interdiction de construire`, `desartificialisation`, `fonds barnier`, `surelevation`, `pilotis`, `piece refuge`, `digues?`, `systeme d endiguement`, `bassins? de retention`, `bassins? ecreteurs?`, `ecretement`, `ouvrages? de protection`, `amenagement hydraulique`
- **D5 Végétalisation** : `desimpermeabilisation`, `vegetalisation`, `renaturation`, `remeandrage`, `restauration des zones? humides?`, `ripisylve`, `agroforesterie`, `ville eponge`, `bio retention`, `zan`, `zero artificialisation nette`, `loi climat resilience`
- **D6 Politiques et financements d'adaptation** : `fonds vert`, `plan national d adaptation`, `pnacc`
- **D7 Absence de solution, fatalité** : `fatalite`, `il faut subir`, `rien a faire`, `phenomenes? naturels?`, `caprices? de la nature`. Axe exploratoire (29/10/2026), inspiré des posts LinkedIn de QuotaClimat (« il n'y a rien à faire, il faut subir », inondations « présentées comme une fatalité ») : l'inondation présentée comme naturelle ou inévitable, sans solution. `rien a faire` est large.

## 4. Résultats

Après publication uniquement.
