# Incendies, été 2026

Fichier reconstitué à partir de la fiche d'analyse, des questions Metabase et de l'article publié.

**Méthode propre à cette analyse, non reconduite.** Le corpus noyau a été construit ad hoc, hors du dictionnaire OME, et la normalisation différait légèrement de celle du job (apostrophes supprimées au lieu d'être remplacées par un espace). Les prochaines analyses incendies suivent la méthode commune ([decisions.md](../decisions.md) §3) et l'entrée `incendies` de `thematic_jobs_config.yml`. Les chiffres publiés restent exploitables et comparables ; les regex et requêtes ci-dessous documentent ce qui a été fait, elles ne sont pas un modèle à réutiliser.

- Statut : publié, [article du 22/09/2026](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/) ; à intégrer au Bilan OMÉ S2 2026 (la méthode doit être compatible avec les deux)
- Entrée de `thematic_jobs_config.yml` : `incendies` (pour les prochaines analyses incendies)
- Tables : `public.keywords` (audiovisuel) et `public.factiva_articles` (presse)
- Collection Metabase : `03. Analyses thématiques/19. Incendies 2026` (id 362)
- Inspiration : [article vague de chaleur de mai 2026](https://quotaclimat.org/actualites/vague-chaleur-mai-2026-couverture-mediatique/) ; méthode audiovisuelle alignée sur l'analyse faite pour le BBIE, étendue à la presse

## 1. Méthode spécifique

**Corpus construit hors dictionnaire OME.** Le dictionnaire OME est pauvre en vocabulaire du feu : `incendie`, `incendie de forêt` et `feux de forêt` sont HRFP et classés dans `changement_climatique_consequences_indirectes`. Les compteurs `*_no_hrfp` les ignorent, et le modèle `core_query_thematics_keywords`, qui filtre les thèmes `%indirect%`, ne les contient pas. Le corpus a donc été construit par regex sur le texte (§2). À l'inverse, le vocabulaire de la canicule est dans le dictionnaire (`changement_climatique_consequences`, non HRFP).

**Normalisation (propre à cette analyse).** Minuscules, accents retirés, apostrophes supprimées (`d'eau` donne `deau`, contre `d eau` dans le job). Les regex des axes (§3) sont écrites dans cette forme : à adapter avant toute réutilisation.

**Périmètre.** 17 sources audiovisuelles et 55 sources de presse du périmètre de l'OMÉ.

**Sources.**

- Audiovisuel : `public.keywords` (segments de 2 minutes), `public.program_metadata` (grille), `public.time_monitored` (temps d'antenne), `core_query_environmental_shares` (référence habituelle de chaque chaîne).
- Presse : `factiva_articles` (détection et référence), `source_classification` (regroupement des éditions papier et web), `stats_factiva_articles` (volumes de référence). Articles de moins de 100 mots exclus, hors `DUP`.

**Dénominateurs.**

| Usage | Audiovisuel | Presse |
|---|---|---|
| Part de couverture (Q1) | Durée monitorée × jours captés, sur les créneaux déclarés dans `program_metadata` | Nombre total d'articles |
| Ratios éditoriaux (Q2 à Q5) | Séquences du corpus incendie | Articles du corpus incendie |
| Courbe journalière (Q7) | Durée théorique de grille par jour | Nombre total d'articles par jour |

Le filtre sur les créneaux de `program_metadata` n'est appliqué qu'à Q1, pour rester comparable aux métriques publiées par l'OMÉ.

**Périodes.**

- Analyse : 1er juillet au 30 août 2026.
- Comparaison : profil habituel de chaque média sans filtre incendie (Q6), et comparaison janvier-avril / mai-août 2026 (AV-Q6 2259).
- Canicules de 2026 utilisées pour le croisement avec les incendies : 21 au 30 mai, 17 au 29 juin, 4 au 19 juillet, 11 au 14 août.

**Contexte de l'été 2026** (fiche d'analyse, sources : Office national des forêts et Copernicus). Surfaces brûlées : 72 000 ha en 2022, 15 000 en 2023, 5 000 en 2024, 30 000 en 2025, 96 000 en 2026. Plus grands feux de 2026 : Aude (Pouzols-Minervois, 1-3 juillet, 806 ha), Drôme (Solaure-en-Diois, 3-13 juillet, 4 192 ha), Pyrénées-Orientales (Montalba-le-Château, 4-13 juillet, 4 000 ha), Seine-et-Marne (Fontainebleau, 12-14 juillet, 1 956 ha), Corse (Corte, 14-31 juillet, 1 081 ha), Var (Pontevès, 21-28 juillet, 6 300 ha), Gironde (Le Porge, Cap Ferret, 22 juillet-1er août, 42 000 ha), Landes (Luglon, 13-17 août, 1 813 ha ; Biscarrosse, 23-27 août, 3 513 ha).

## 2. Mots-clés

**Détection stricte** (corpus) : incendie(s), mégafeu(x), brasier, feu(x) de forêt, feu(x) de végétation, feu de tourbe, feu de lande, feu de broussailles, départ(s) de feu, reprise(s) de feu, front de flammes, front du feu, hectares brûlés, parti(s) en fumée, ravagé(e) par le feu, ravagée par les flammes, soldats du feu, bombardier(s) d'eau, Canadair.

**Détection large** (contrôle) : terme de feu générique accompagné d'un marqueur végétal dans le même segment.

**Exclusions** (feux domestiques, de véhicule ou industriels) : incendie, feu ou brand(...) suivi à moins de 25 caractères de immeuble, maison, habitation, appartement, entrepôt, usine, hangar, garage, voiture, bus, camion, hôpital, école.

## 3. Axes

Les codes (A1, B2...) sont ceux de la fiche et des requêtes. L'article ne contient pas de graphique : les résultats sont donnés dans le texte.

| Code | Axe | Origine | Termes | Question Metabase (AV / PE) | Résultat |
|---|---|---|---|---|---|
| Q1 | Volumétrie | socle | | 2204 / 2218 | publié |
| Q6 | Référence habituelle et comparaison janvier-avril / mai-août | socle | | 2259, 2237, 2216, 2225 / 2230, 2232 | publié |
| Q7 | Courbe journalière | socle | | 2217, 2226 / 2261 | testé |
| Q8 | Couverture par feu : localisation, ampleur ou durée ? | socle | | — / 2253 | testé |
| A1 | Vocabulaire climat du dictionnaire (constat, causes, conséquences ; variante hors HRFP) | banque | [banque](../decisions.md#lien-avec-le-changement-climatique) | 2209 / 2220 | publié |
| A2 | Mention explicite du changement climatique | banque | [banque](../decisions.md#lien-avec-le-changement-climatique) | 2209 / 2220 | publié |
| A1 sans A2 | Vocabulaire climat présent sans que le lien soit nommé | spécifique | A1 et non A2 | 2209 | testé |
| A3 | Mise en série | banque | [banque](../decisions.md#mise-en-série) + `comme en 2022`, `saison des feux` | 2209 / 2220 | testé |
| A4 | Voix scientifiques | banque | [banque](../decisions.md#voix-scientifiques) | 2209 / 2220 | testé |
| B1 | Co-aléa chaleur | banque | [banque](../decisions.md#co-aléa-chaleur) | 2209 / 2220 | testé |
| B2 | Co-aléa sécheresse | banque | [banque](../decisions.md#co-aléa-sécheresse) | 2209 / 2220 | publié |
| B3 | Co-aléa vent et météo | banque | [banque](../decisions.md#co-aléa-vent-et-météo) | 2209 / 2220 | testé |
| B4 | Aléas composés | banque | [banque](../decisions.md#aléas-composés) | 2209 | testé |
| C1a | Causes directes : allumage | spécifique | ci-dessous | 2214, 2266 / 2221 | publié |
| C1b | Causes judiciaires | banque | [banque](../decisions.md#forces-de-lordre-et-justice) + `responsable du depart`, `sanctions` | 2214, 2266 / 2221 | testé |
| C2a | Cause climatique structurelle | banque | [banque](../decisions.md#causes-climatiques) | 2214, 2266 / 2221 | publié |
| C2b | Sécheresse comme cause | spécifique | liste du [co-aléa sécheresse](../decisions.md#co-aléa-sécheresse) | 2214, 2266 / 2221 | publié |
| C2c | Gestion du milieu | spécifique | ci-dessous | 2214, 2266 / 2221 | sans résultat publié |
| C2d | Absence de gouvernance | spécifique | ci-dessous | 2214, 2266 / 2221 | sans résultat publié |
| C3 | Quadrant cause immédiate seule / structurelle seule / les deux / aucune | spécifique | C1 croisé avec C2 | 2214 / 2221 | testé |
| C5 | Causes du changement climatique lui-même | banque | thème `changement_climatique_causes` | 2214 / 2221 | testé |
| D2 | Santé | banque | [banque](../decisions.md#santé) + `fumee`, `fumees`, `panache de fumee` | non retrouvée dans Metabase | testé |
| D3 | Écologie et biodiversité | banque | biodiversité, espèces, faune, flore, habitat, Natura 2000 (dictionnaire et regex) | non retrouvée dans Metabase | testé |
| D4 | Tourbières et carbone | spécifique | tourbière, tourbe, croisés avec carbone, CO2, puits de carbone | non retrouvée dans Metabase | testé |
| E1 / E2 | Solutions du dictionnaire, atténuation ou adaptation | banque | [banque](../decisions.md#solutions) | 2215, 2254 / 2222 | publié |
| E3 | Neuf familles de solutions opérationnelles | spécifique | ci-dessous | 2215 / 2222 | publié (curatif et préventif) |
| Biais curatif | Famille 6 (moyens de lutte) rapportée aux familles 1 à 4 (prévention) | spécifique | E3 | 2215 / 2222 | publié |
| Pyromanes | Responsabilité individuelle : occurrences, recontextualisation, pyromanes face à la sécheresse | spécifique | C1a | 2283, 2284, 2285, 2286 / 2287 (collection « Pyromanes ») | testé |
| Canicules | Recontextualisation des épisodes caniculaires de 2026 | spécifique | [co-aléa chaleur](../decisions.md#co-aléa-chaleur) | non retrouvée dans Metabase | testé |
| Validation | Vocabulaire de contextualisation réellement employé, par épisode et par mot | contrôle | | non retrouvée dans Metabase | testé |
| Longueur x lien CC | Les articles longs recontextualisent-ils davantage ? | banque | | — / 2294 | testé |

Ventilations publiées : public / privé, presse nationale / régionale.

**Axes de la banque non exploités** : non recensés, l'analyse est antérieure au recensement des axes.

**Termes des axes spécifiques** (forme normalisée de la méthode commune)

🔴 [termes à risque de faux positifs, à contrôler avant réutilisation : `la defense`, `troupeau`, `moutons`, `chevres`, `combustible`]

- **C1a Allumage** : `pyromane`, `pyromanes`, `megot`, `megots`, `cigarette`, `barbecue`, `feu de camp`, `imprudence`, `negligence`, `malveillance`, `criminel`, `criminelle`, `incendiaire`, `foudre`, `etincelle`, `etincelles`, `engin forestier`, `engin agricole`, `machine forestiere`, `fauchage`, `faucheuse`, `debroussailleuse`, `broyeur`, `tracteur`, `origine du feu`, `point de depart du feu`, `cause du depart`
- **C2c Gestion du milieu** : `resineux`, `sapiniere`, `sapinieres`, `epicea`, `epiceas`, `monoculture`, `monocultures`, `gestion forestiere`, `entretien de la foret`, `combustible`, `biomasse seche`, `drainage`, `draine`, `drainee`, `assechement des tourbieres`, `abandon des terres`, `embroussaillement`
- **C2d Absence de gouvernance** : `interdiction de fauchage`, `pas d interdiction`, `absence d interdiction`, `aucune interdiction`, `prevention insuffisante`, `manque de moyens`, `manque de prevention`, `aurait pu etre evite`, `aurait du`, `alerte ignoree`, `risque connu`, `risque annonce`, `celex`, `arrete communal`, `arrete prefectoral`
- **E3 Neuf familles de solutions** (construites à partir du GIEC WGII, d'EFFIS/JRC et des retours d'expérience de l'ONF, le dictionnaire OME ne couvrant pas la gestion du risque incendie)
  1. Gouvernance du risque : `risque d incendie`, `risques d incendie`, `danger d incendie`, `indice de danger`, `niveau de danger`, `danger eleve`, `risque eleve`, `alerte incendie`, `code orange`, `code rouge`, `interdiction de feu`, `interdiction de faire du feu`, `interdiction de barbecue`, `interdiction de fumer`, `interdiction de fauchage`, `interdiction d acces`, `acces interdit`, `fermeture du massif`, `fermeture des sites`, `sites fermes au public`, `plan d urgence`, `plan communal de sauvegarde`, `plan orsec`, `plan national`, `arrete communal`, `arrete prefectoral`, `arrete municipal`, `prevention des incendies`, `prevention du risque incendie`
  2. Gestion du combustible : `pare feu`, `coupe feu`, `bande coupe feu`, `debroussaillement`, `debroussaillage`, `debroussailler`, `gyrobroyage`, `ecobuage`, `gestion du combustible`, `reduction du combustible`, `brulage dirige`, `brulage controle`, `feu dirige`, `feu controle`, `fauchage preventif`, `paturage`, `paturage extensif`, `ecopaturage`, `troupeau`, `moutons`, `chevres`, `entretien des layons`
  3. Sylviculture adaptée : `diversification des essences`, `melange des essences`, `essences feuillues`, `essences resistantes`, `essences indigenes`, `essences autochtones`, `feuillus`, `conversion des peuplements`, `conversion des sapinieres`, `futaie irreguliere`, `futaie melangee`, `foret mosaique`, `mosaique paysagere`, `mosaique forestiere`, `gestion durable des forets`, `gestion forestiere durable`, `preservation des forets`, `preserver la foret`, `preserver les forets`, `protection des forets`, `reboisement`, `reboiser`, `reforestation`, `replanter la foret`
  4. Tourbières et eau : `restauration des tourbieres`, `restaurer les tourbieres`, `protection des tourbieres`, `restauration des zones humides`, `restaurer les zones humides`, `protection des zones humides`, `remouillage`, `rehumidification`, `relevement de la nappe`, `remonter le niveau d eau`, `barrage de tourbe`, `batardeau`, `batardeaux`, `reboucher les drains`, `boucher les drains`, `arret du drainage`, `renaturation`, `renaturer`, `retenue collinaire`, `reserve d eau`, `reserve incendie`, `bassin de stockage`, `bassin tampon`, `point d eau`, `citerne`, `citerne d aspiration`, `projet life`
  5. Détection précoce : `detection precoce`, `detection satellite`, `image satellite`, `images satellites`, `imagerie satellite`, `copernicus`, `effis`, `camera de surveillance`, `cameras de surveillance`, `drone`, `drones`, `tour de guet`, `vigie`, `patrouille`, `patrouilles`, `surveillance aerienne`, `systeme d alerte`, `alerte precoce`, `fr alert`, `alerte a la population`
  6. Moyens de lutte et coopération : `pompier`, `pompiers`, `sapeur pompier`, `sapeurs pompiers`, `soldats du feu`, `caserne`, `service d incendie`, `service departemental d incendie et de secours`, `sdis`, `centre de secours`, `les secours`, `eteindre le feu`, `eteindre l incendie`, `extinction du feu`, `lutte contre le feu`, `lance a incendie`, `autopompe`, `moyens aeriens`, `bombardier d eau`, `bombardiers d eau`, `canadair`, `helicoptere`, `helicopteres`, `helicoptere bombardier`, `seau bambi`, `largage`, `largages`, `resceu`, `mecanisme europeen de protection civile`, `protection civile`, `renfort`, `renforts`, `armee`, `militaires`, `la defense`, `cooperation transfrontaliere`, `aide internationale`, `piste d acces`, `pistes d acces`, `chemin forestier`, `chemins forestiers`
  7. Interface habitat-forêt : `interface habitat`, `zone d interface`, `zone tampon`, `debroussaillement obligatoire`, `debroussaillement reglementaire`, `autour des habitations`, `materiaux resistants`, `materiaux ignifuges`, `toiture resistante`, `plan d evacuation`, `defendabilite`, `protection des habitations`
  8. Santé et information du public : liste [santé de la banque](../decisions.md#santé) + `fumee`, `fumees`, `panache de fumee`
  9. Atténuation racine : liste [atténuation de la banque](../decisions.md#solutions)

## 4. Résultats

**Takeaways publiés**

- Lien avec le changement climatique : 16 % des séquences audiovisuelles, 25 % des articles (contre 41 % et plus de 60 % pour la vague de chaleur de mai-juillet).
- Sécheresse citée dans 25,5 % des séquences et 32,4 % des articles : la cause la plus mise en avant.
- Causes directes (pyromanes, mégots, barbecues, foudre) en deuxième position : 20 % des séquences du service public, 25 % du privé, 24 % des articles.
- Écart public / privé : Arte 54 %, LCI environ 17 %, Franceinfo TV 14,7 %, BFMTV et CNews environ 8 %.
- Presse nationale 40,5 %, presse régionale 19 %.
- Secours et moyens de lutte : 78 % des séquences, 62,5 % des articles. Solutions curatives 41 % du temps d'antenne, préventives 5 %.

**Limites** : pas d'encadré méthodologique dans l'article (voir méthode commune §3.9). En presse, `incendie` étant HRFP, une partie des articles qui le mentionnent n'est pas dans `factiva_articles` (méthode commune §3.10).

**Axes remontés dans la banque** : fait.
