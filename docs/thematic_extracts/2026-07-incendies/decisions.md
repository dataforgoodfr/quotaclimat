# Incendies, été 2026

Fichier reconstitué à partir des questions Metabase et de l'article publié. 🔴 [à relire par l'équipe qui a mené l'analyse]

- Statut : publié, [article du 22/09/2026](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/)
- Entrée de `thematic_jobs_config.yml` : `incendies` (pour les prochaines analyses incendies)
- Tables : `public.keywords` (audiovisuel) et `public.factiva_articles` (presse)
- Collection Metabase : `03. Analyses thématiques/19. Incendies 2026` (id 362)

## 1. Méthode spécifique

Analyse menée avant la révision de la méthode :

- corpus noyau construit directement sur `public.keywords` et `public.factiva_articles`, par regex dans les questions Metabase, et non sur des tables thématiques ;
- termes du noyau non limités au dictionnaire OME (§2) ;
- presse : articles de moins de 100 mots exclus ;
- recontextualisation conforme à la méthode commune (±2 minutes en audiovisuel, article entier en presse).

Périodes :

- analyse : 1er juillet au 31 août 2026 ;
- comparaison : janvier-avril 2026 (AV-Q6). 🔴 [vérifier la période de comparaison effectivement utilisée pour les chiffres publiés]

## 2. Mots-clés

**Corpus noyau** (regex des questions AV-Q2 et PE-Q2) : incendie(s), mégafeu(x), brasier, feu(x) de forêt, feu(x) de végétation, feu de tourbe, feu de lande, feu de broussailles, départ(s) de feu, reprise(s) de feu, front de flammes, front du feu, hectares brûlés, parti(s) en fumée, ravagé(e) par le feu, ravagée par les flammes, soldats du feu, bombardier(s) d'eau, Canadair.

**Exclusion** (feux urbains) : incendie, feu ou brand(...) suivi à moins de 25 caractères de immeuble, maison, habitation, appartement, entrepôt, usine, hangar, garage, voiture, bus, camion, hôpital, école.

## 3. Axes contextuels

Questions Metabase : AV-Q2 (2209) / PE-Q2 (2220) pour le lien CC et les co-aléas ; AV-Q3 (2214, 2266) / PE-Q3 (2221) pour les causes ; AV-Q5 (2215, 2254) / PE-Q5 (2222) pour les solutions. Socle : Q1 volumétrie (2204 / 2218), Q6 comparaison (2259, 2237, 2216, 2225 / 2230, 2232), Q7 courbe journalière (2217, 2226 / 2261), Q8 par feu (PE 2253).

🔴 [reporter dans la colonne « Graphique » les graphiques effectivement publiés dans l'article]

| Axe | Origine | Termes | Graphique | Question Metabase | Résultat |
|---|---|---|---|---|---|
| Lien avec le changement climatique | banque | [banque](../decisions.md#lien-avec-le-changement-climatique) | | 2209 / 2220 | publié |
| Co-aléa chaleur | banque | [banque](../decisions.md#co-aléa-chaleur) | | 2209 / 2220 | testé |
| Co-aléa sécheresse | banque | [banque](../decisions.md#co-aléa-sécheresse) | | 2209 / 2220 | publié |
| Co-aléa vent et météo | banque | [banque](../decisions.md#co-aléa-vent-et-météo) | | 2209 / 2220 | testé |
| Aléas composés | banque | [banque](../decisions.md#aléas-composés) | | 2209 | testé |
| Mise en série | banque | [banque](../decisions.md#mise-en-série) + `comme en 2022`, `saison des feux` | | 2209 / 2220 | testé |
| Voix scientifiques | banque | [banque](../decisions.md#voix-scientifiques) | | 2209 / 2220 | testé |
| Causes climatiques | banque | [banque](../decisions.md#causes-climatiques) | | 2214, 2266 / 2221 | publié |
| Solutions | banque | [banque](../decisions.md#solutions) | | 2215, 2254 / 2222 | publié |
| Santé | banque | [banque](../decisions.md#santé) + `fumee`, `fumees`, `panache de fumee` | | 2215 / 2222 | testé |
| Forces de l'ordre et justice | banque | [banque](../decisions.md#forces-de-lordre-et-justice) + `responsable du depart` | | 2214, 2266 / 2221 | testé |
| Causes directes (origine du feu) | spécifique | ci-dessous | | 2214, 2266 / 2221 | publié |
| Causes : sécheresse | spécifique | liste du [co-aléa sécheresse](../decisions.md#co-aléa-sécheresse) | | 2214, 2266 / 2221 | testé 🔴 [vérifier si publié] |
| Causes forestières et paysagères | spécifique | ci-dessous | | 2214, 2266 / 2221 | testé 🔴 [vérifier si publié] |
| Défaillances de prévention | spécifique | ci-dessous | | 2214, 2266 / 2221 | testé 🔴 [vérifier si publié] |
| Solutions préventives | spécifique | ci-dessous | | 2215 / 2222 | publié (agrégé) |
| Secours et moyens de lutte (solutions curatives) | spécifique | ci-dessous | | 2215 / 2222 | publié |
| Responsabilité individuelle | spécifique | 🔴 [vérifier la collection Metabase « Pyromanes »] | | | idée |
| Croisement Lien CC x causes, par média | banque | | G13 | | publié |
| Croisement Longueur x lien CC (presse) | banque | | Barres par tranche de longueur | 2294 | testé |
| Croisement Comparaison entre épisodes (incendies vs chaleur) | banque | | | | publié |

Ventilations publiées : public / privé, presse nationale / régionale.

**Axes de la banque écartés** : désinformation, Unes, économie et assurance, biodiversité. 🔴 [raisons à préciser]

**Termes des axes spécifiques** (normalisés)

🔴 [termes à risque de faux positifs, à contrôler avant réutilisation : `la defense`, `troupeau`, `moutons`, `chevres`, `combustible`]

- **Causes directes (origine du feu)** : `pyromane`, `pyromanes`, `megot`, `megots`, `cigarette`, `barbecue`, `feu de camp`, `imprudence`, `negligence`, `malveillance`, `criminel`, `criminelle`, `incendiaire`, `foudre`, `etincelle`, `etincelles`, `engin forestier`, `engin agricole`, `machine forestiere`, `fauchage`, `faucheuse`, `debroussailleuse`, `broyeur`, `tracteur`, `origine du feu`, `point de depart du feu`, `cause du depart`
- **Causes forestières et paysagères** : `resineux`, `sapiniere`, `sapinieres`, `epicea`, `epiceas`, `monoculture`, `monocultures`, `gestion forestiere`, `entretien de la foret`, `combustible`, `biomasse seche`, `drainage`, `draine`, `drainee`, `assechement des tourbieres`, `abandon des terres`, `embroussaillement`
- **Défaillances de prévention** : `interdiction de fauchage`, `pas d interdiction`, `absence d interdiction`, `aucune interdiction`, `prevention insuffisante`, `manque de moyens`, `manque de prevention`, `aurait pu etre evite`, `aurait du`, `alerte ignoree`, `risque connu`, `risque annonce`, `celex`, `arrete communal`, `arrete prefectoral`
- **Solutions préventives**
  - Alerte et réglementation : `risque d incendie`, `risques d incendie`, `danger d incendie`, `indice de danger`, `niveau de danger`, `danger eleve`, `risque eleve`, `alerte incendie`, `code orange`, `code rouge`, `interdiction de feu`, `interdiction de faire du feu`, `interdiction de barbecue`, `interdiction de fumer`, `interdiction de fauchage`, `interdiction d acces`, `acces interdit`, `fermeture du massif`, `fermeture des sites`, `sites fermes au public`, `plan d urgence`, `plan communal de sauvegarde`, `plan orsec`, `plan national`, `arrete communal`, `arrete prefectoral`, `arrete municipal`, `prevention des incendies`, `prevention du risque incendie`
  - Gestion du combustible : `pare feu`, `coupe feu`, `bande coupe feu`, `debroussaillement`, `debroussaillage`, `debroussailler`, `gyrobroyage`, `ecobuage`, `gestion du combustible`, `reduction du combustible`, `brulage dirige`, `brulage controle`, `feu dirige`, `feu controle`, `fauchage preventif`, `paturage`, `paturage extensif`, `ecopaturage`, `troupeau`, `moutons`, `chevres`, `entretien des layons`
  - Gestion forestière : `diversification des essences`, `melange des essences`, `essences feuillues`, `essences resistantes`, `essences indigenes`, `essences autochtones`, `feuillus`, `conversion des peuplements`, `conversion des sapinieres`, `futaie irreguliere`, `futaie melangee`, `foret mosaique`, `mosaique paysagere`, `mosaique forestiere`, `gestion durable des forets`, `gestion forestiere durable`, `preservation des forets`, `preserver la foret`, `preserver les forets`, `protection des forets`, `reboisement`, `reboiser`, `reforestation`, `replanter la foret`
  - Eau et zones humides : `restauration des tourbieres`, `restaurer les tourbieres`, `protection des tourbieres`, `restauration des zones humides`, `restaurer les zones humides`, `protection des zones humides`, `remouillage`, `rehumidification`, `relevement de la nappe`, `remonter le niveau d eau`, `barrage de tourbe`, `batardeau`, `batardeaux`, `reboucher les drains`, `boucher les drains`, `arret du drainage`, `renaturation`, `renaturer`, `retenue collinaire`, `reserve d eau`, `reserve incendie`, `bassin de stockage`, `bassin tampon`, `point d eau`, `citerne`, `citerne d aspiration`, `projet life`
  - Détection et surveillance : `detection precoce`, `detection satellite`, `image satellite`, `images satellites`, `imagerie satellite`, `copernicus`, `effis`, `camera de surveillance`, `cameras de surveillance`, `drone`, `drones`, `tour de guet`, `vigie`, `patrouille`, `patrouilles`, `surveillance aerienne`, `systeme d alerte`, `alerte precoce`, `fr alert`, `alerte a la population`
  - Interface habitat-forêt : `interface habitat`, `zone d interface`, `zone tampon`, `debroussaillement obligatoire`, `debroussaillement reglementaire`, `autour des habitations`, `materiaux resistants`, `materiaux ignifuges`, `toiture resistante`, `plan d evacuation`, `defendabilite`, `protection des habitations`
- **Secours et moyens de lutte**
  - Secours au sol : `pompier`, `pompiers`, `sapeur pompier`, `sapeurs pompiers`, `soldats du feu`, `caserne`, `service d incendie`, `service departemental d incendie et de secours`, `sdis`, `centre de secours`, `les secours`, `eteindre le feu`, `eteindre l incendie`, `extinction du feu`, `lutte contre le feu`, `lance a incendie`, `autopompe`
  - Moyens aériens et renforts : `moyens aeriens`, `bombardier d eau`, `bombardiers d eau`, `canadair`, `helicoptere`, `helicopteres`, `helicoptere bombardier`, `seau bambi`, `largage`, `largages`, `resceu`, `mecanisme europeen de protection civile`, `protection civile`, `renfort`, `renforts`, `armee`, `militaires`, `la defense`, `cooperation transfrontaliere`, `aide internationale`, `piste d acces`, `pistes d acces`, `chemin forestier`, `chemins forestiers`

## 4. Résultats

**Takeaways publiés**

- Lien avec le changement climatique : 16 % des séquences audiovisuelles, 25 % des articles (contre 41 % et plus de 60 % pour la vague de chaleur de mai-juillet).
- Sécheresse citée dans 25,5 % des séquences et 32,4 % des articles.
- Écart public / privé : Arte 54 %, LCI environ 17 %, Franceinfo TV 14,7 %, BFMTV et CNews environ 8 %.
- Presse nationale 40,5 %, presse régionale 19 %.
- Secours et moyens de lutte : 78 % des séquences, 62,5 % des articles. Solutions curatives 41 % du temps d'antenne, préventives 5 %.

**Limites** : pas d'encadré méthodologique dans l'article (voir méthode commune §3.9).

**Axes remontés dans la banque** : fait.
