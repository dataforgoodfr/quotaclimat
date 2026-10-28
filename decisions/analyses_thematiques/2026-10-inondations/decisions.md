# Inondations 2025-2026

- Responsable : Renan
- Statut : en cours, étape 2 (axes) ; dictionnaire noyau validé le 26/10/2026, révisé le 28/10/2026
- Entrée de `thematic_jobs_config.yml` : `inondations` (mots-clés mis à jour le 26/10/2026 ; périodes à fixer après la courbe journalière ; job pas encore lancé)
- Tables : `thematic_extracts.inondations_articles_fr`, `thematic_extracts.inondations_keywords_fr`
- Collection Metabase : `03. Analyses thématiques/21. Inondations 2026` (id 469)
- Document de résultats (avant publication) : 🔴 [lien document Metabase à créer]

## 1. Méthode spécifique

Méthode commune, sans écart à ce stade.

- Événements recensés (étape 0) : 🔴 [tableau début, fin, lieu, impact, source datée, à remplir par recherche en ligne puis compléter avec les pics de la courbe]
- Courbe journalière (étape 0) : modèles 7.1 et 7.2 de la méthode commune, audiovisuel du 01/01/2024 au 26/10/2026, presse par semaine, du lundi 06/01/2025 à la dernière semaine complète ingérée, mots du §2 ; audiovisuel : « AV - V2 - Courbe journalière inondations » (2329) ; presse : « PE - V2 - Courbe hebdomadaire inondations » (2394)
- Périodes (entrée `inondations` du YAML, fin incluse ci-dessous) :

  Cinq fenêtres, chacune avec une tolérance d'une à deux semaines avant et après (méthode commune §3.2, événement météorologique). Deux épisodes proches dans le temps partagent une même fenêtre.

  | `id` | Du | Au | Épisodes | Presse |
  |---|---|---|---|---|
  | `pas_de_calais_novembre_2023` | 31/10/2023 | 19/11/2023 | Pas-de-Calais, premier épisode (crues de 2023-2024, divisées en deux fenêtres) | non (données presse à partir de 2025) |
  | `pas_de_calais_janvier_2024` | 25/12/2023 | 14/01/2024 | Pas-de-Calais, second épisode | non |
  | `ardeche_valence_octobre_2024` | 07/10/2024 | 09/11/2024 | Ardèche et Espagne (Valence), fusionnés | non |
  | `sud_ouest_fevrier_2026` | 09/02/2026 | 28/02/2026 | Sud-Ouest | oui |
  | `sud_espagne_septembre_octobre_2026` | 21/09/2026 | 26/10/2026 | Gard et Hérault, et Espagne, fusionnés | oui |

  🔴 [sources datées des épisodes à ajouter au tableau des événements]

- Période d'analyse et périodes de comparaison : 🔴 [préciser laquelle est la période d'analyse]
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
| `pluie extrême`, `pluie torrentielle` (retirés le 28/10/2026, après le premier lancement du job) | décrivent une cause de l'inondation, pas l'inondation ; les garder rendrait circulaire la mesure des axes causes et météo (C2) |
| `zone inondable`, `Inondable` (retirés le 28/10/2026, après le premier lancement du job) | relève de l'aménagement ; le garder rendrait circulaire la mesure d'un axe aménagement (C2) |
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
| V1 | Volume | banque V1 | corpus | G7 | | |
| V2 | Courbe journalière (presse : hebdomadaire) | banque V2 | corpus | G2 | 2329 / 2394 | testé |
| A1 | Climat, dictionnaire OME explicite | spécifique | ci-dessous | G7 | | |
| A2 | Mentions explicites du changement climatique | banque A1 | [banque](../decisions.md#lien-avec-le-changement-climatique) | G7 | | |
| A3 | Mécanismes physiques de l'inondation | spécifique | ci-dessous | G7 | | |
| A4 | Voix scientifiques | banque A3 | banque, sans doublon de `giec`, + `hydrologues?`, `hydrogeologues?`, `meteorologues?` | G7 | | |
| A5 | Mise en série | banque A2 | banque, sans `comme en 2022` ni `saison des feux`, + `episode mediterraneen`, `episode cevenol`, `crue historique`, `comme en 2024` | G7 | | |
| A6 | Sécheresse | banque B3 | [banque](../decisions.md#co-aléa-sécheresse) | G7 | | |
| B1 | Causes du changement climatique, dictionnaire OME explicite | spécifique | ci-dessous | G7 | | |
| B2 | Causes météorologiques directes | banque B4, termes remplacés | ci-dessous | G7 | | |
| B3 | Gouvernance et politique locale | spécifique | ci-dessous | G7 | | |
| B4 | Facteurs locaux aggravants | spécifique | ci-dessous | G7 | | |
| C1 | Conséquences humaines | spécifique | ci-dessous | G7 | | |
| C2 | Dommages matériels | banque C1 (secteur) | ci-dessous | G6 | | |
| C3 | Assurances | banque C1 (secteur) | ci-dessous | G6 | | |
| C4 | Agriculture | banque C1 (secteur) | ci-dessous | G6 | | |
| C5 | Reconstruction, temps long | spécifique | ci-dessous | G7 | | |
| D1 | Secours (curatif) | banque D3 | ci-dessous | G11 | | |
| D2 | Alerte des habitants et préparation | banque D3 | ci-dessous | G11 | | |
| D3 | Atténuation du changement climatique | banque D1 | [banque](../decisions.md#solutions), liste atténuation | G11 | | |
| D4 | Aménagement | banque D3 | ci-dessous | G11 | | |
| D5 | Végétalisation | banque D3 | ci-dessous | G11 | | |
| D6 | Politiques et financements d'adaptation | spécifique | ci-dessous | G11 | | |

Croisements prévus : `A1 x B1` et `A2 x A3` (G13, nuage de points, un point par média) ; curatif ou préventif (banque D3) : D1 face à D2, D4 et D5 (G11, barres empilées par média).

**Axes de la banque non exploités** : B2 co-aléa chaleur (sans lien direct avec l'inondation) ; B6 forces de l'ordre et justice (pas de cause criminelle). X3 proportionnalité : couvert par la courbe journalière (V2), rapprochée des événements. 🔴 [à décider : V3 mots les plus employés, V4 part de couverture, B5 aléas composés, C2 santé, C3 précarité du ménage, C4 personnes vulnérables, D2 solutions individuelles ou systémiques]

**Termes des axes spécifiques**

- **A1 Climat, dictionnaire OME explicite** (termes du dictionnaire OME qui nomment le climat sans ambiguïté) : `changement climatique`, `rechauffement climatique`, `rechauffement planetaire`, `urgence climatique`, `dereglement du climat`, `un climat qui change`, `evolution du climat`, `effet de serre`, `risques? climatiques?`, `rechauffement des oceans`, `rechauffement de l atmosphere`, `rechauffement des temperatures`, `seuil de rechauffement`, `equilibre climatique`, `le climat du futur`, `anthropocene`
- **A3 Mécanismes physiques** : `vapeur d eau`, `atmosphere plus chaude`, `evaporation`, `cycle de l eau`, `oceans? plus chauds?`, `mediterranee plus chaude`, `temperature de surface de la mer`, `plus frequentes et plus intenses`, `clausius clapeyron`. `evaporation` est large : à contrôler.
- **B1 Causes du changement climatique, dictionnaire OME explicite** : `gaz a effet de serre`, `emissions? de carbone`, `emissions anthropiques`, `emissions de methane`, `emissions mondiales de co2`, `emissions mondiales de co deux`, `co2`, `co deux`, `methane`, `combustibles? fossiles?`, `energies? fossiles?`, `deforestation`
- **B2 Causes météorologiques directes** : `pluies? diluviennes?`, `fortes? pluies?`, `pluies? torrentielles?`, `pluies? extremes?`, `pluies? intenses?`, `precipitations? extremes?`, `orages?`, `cumuls? de pluie`, `millimetres de pluie`, `episodes? pluvieux`
- **B3 Gouvernance et politique locale** : `choix politiques?`, `defaillance de l etat`, `manque d anticipation`, `sous investissement`, `responsabilite des elus`, `cout de l inaction`, `court termisme`
- **B4 Facteurs locaux aggravants** : `artificialisation`, `impermeabilisation`, `sols? impermeabilises?`, `betonisation`, `betonnage`, `etalement urbain`, `saturation des sols`, `arrachage des haies`, `remembrement`, `rectification des cours d eau`, `curage`, `entretien des cours d eau`, `embacles?`, `fonds de vallee`, `constructions? en zone a risque`, `subsidence`, `affaissement des sols`, `pompage des nappes`, `vetuste des reseaux`, `gestion des eaux pluviales`. Limite : l'axe détecte la mention, pas ce qu'on en dit (méthode commune §3.10, valence) ; `curage` et `entretien des cours d eau` sont aussi des arguments contestés.
- **C1 Conséquences humaines** : `evacues?`, `sinistres?`, `bilan humain`, `vies? humaines?`, `personnes? disparues?`, `morts?`, `deces`, `victimes?`. Termes fréquents : si l'axe dépasse 70 % du corpus, prévoir une version stricte (`bilan humain`, `morts?`, `deces`, `personnes? disparues?`) (§3.6).
- **C2 Dommages matériels** : `degats materiels`, `milliards d euros de degats`, `millions d euros de degats`, `pertes economiques`, `coupures? d electricite`
- **C3 Assurances** : `regime cat nat`, `catnat`, `arretes? de catastrophe naturelle`, `indemnisation`, `assureurs?`, `france assureurs`, `franchise`, `surprime`, `inassurable`, `reassurance`. `franchise` est large : à contrôler.
- **C4 Agriculture** : `cultures inondees`, `parcelles inondees`, `champs inondes`, `terres agricoles inondees`, `recoltes? detruites?`, `pertes? de recolte`, `recolte compromise`, `semis detruits`, `semis retardes`, `impossibilite de semer`, `retard des semis`, `pertes? de rendement`, `baisse des rendements`, `cereales noyees`, `vergers inondes`, `vignes inondees`, `maraichage`, `cultures maraicheres`
- **C5 Reconstruction, temps long** : `reconstruire`, `reconstruction`, `retour a la normale`
- **D1 Secours** : `secours`, `sapeurs? pompiers?`, `sauveteurs?`, `helicopteres?`, `cellule de crise`, `plan orsec`
- **D2 Alerte des habitants et préparation** : `batardeaux?`, `calfeutrer`, `sacs de sable`, `kit d urgence`, `plan familial de mise en surete`, `plan communal de sauvegarde`, `dicrim`, `culture du risque`, `exercices? d evacuation`, `systemes? d alerte`, `fr alert`, `vigilance rouge`, `alerte precoce`, `evacuation preventive`, `journee nationale de la resilience`, `vigicrues`, `vigicrues flash`, `apic`, `service de prevision des crues`, `hydrometrie`, `stations? hydrometriques?`, `reseau d observation`, `vigilance crues`, `avertissement automatique`
- **D4 Aménagement** : `repli strategique`, `recul strategique`, `relocalisation`, `demolition des habitations`, `rachat des maisons`, `expropriation`, `non constructible`, `zone inondable`, `inconstructible`, `interdiction de construire`, `desartificialisation`, `fonds barnier`, `surelevation`, `pilotis`, `piece refuge`, `digues?`, `systeme d endiguement`, `bassins? de retention`, `bassins? ecreteurs?`, `ecretement`, `ouvrages? de protection`, `amenagement hydraulique`
- **D5 Végétalisation** : `desimpermeabilisation`, `vegetalisation`, `renaturation`, `remeandrage`, `zones? humides?`, `ripisylve`, `agroforesterie`, `ville eponge`, `noues?`, `bio retention`, `zan`, `zero artificialisation nette`, `loi climat resilience`
- **D6 Politiques et financements d'adaptation** : `fonds vert`, `plan national d adaptation`, `pnacc`

## 4. Résultats

Après publication uniquement.
