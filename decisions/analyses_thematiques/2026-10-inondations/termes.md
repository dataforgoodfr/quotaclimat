# Inondations 2026 : termes du noyau et des axes

État au 02/11/2026. Le noyau vient de l'entrée `inondations` de `thematic_jobs_config.yml` (branche `thematic-axes-inondations`). Les termes des axes viennent des questions Metabase de la collection `03. Analyses thématiques/21. Inondations 2026`, qui font foi.

Les termes des axes sont écrits en forme normalisée (sans accents, apostrophes et tirets remplacés par des espaces) et lus comme des regex de mots entiers : `s?` marque un pluriel facultatif, `(a|b)` une alternative.

## Dictionnaire noyau

**Audiovisuel** (forme exacte du dictionnaire OME) : `inondation`, `augmentation du risque inondation`, `inondations à répétition`, `gestion des inondations`, `anticiper les risques d'inondation`, `anticiper les inondations`, `prévention des inondations`, `crue`, `crue centennale`, `coulée de boue`.

**Presse** (avec pluriels) : `inondation`, `inondations`, `augmentation du risque inondation`, `augmentation du risque inondations`, `inondations à répétition`, `gestion des inondations`, `anticiper les risques d'inondation`, `anticiper les risques d'inondations`, `anticiper les inondations`, `prévention des inondations`, `crue`, `crues`, `crue centennale`, `crues centennales`, `coulée de boue`, `coulées de boue`.

## A. Constat (question 2429)

**A1 Mentions explicites du changement climatique** : `changements? climatiques?`, `dereglement climatique`, `dereglement du climat`, `rechauffement climatique`, `rechauffement de la planete`, `rechauffement planetaire`, `crise climatique`, `urgence climatique`, `climat se rechauffe`, `le climat change`, `risques climatiques`.

**A2 Mécanismes physiques** : `vapeur d eau`, `atmosphere plus chaude`, `evaporation`, `cycle de l eau`, `oceans? plus chauds?`, `mediterranee plus chaude`, `temperature de surface de la mer`, `clausius clapeyron`.

**A3 Voix scientifiques** : `climatologues?`, `meteorologues?`, `hydrologues?`, `hydrogeologues?`, `scientifiques?`, `chercheu(r|se)s?`, `giec`, `cnrs`, `universite`, `expert(e)?s? du climat`.

**A4 Mise en série** : `(crues?|inondations?|episodes?) (plus|de plus en plus) frequent(e)?s?`, `(crues?|inondations?|episodes?) de plus en plus intenses?`, `inondations? sans precedent`, `(crues?|inondations?|episodes?) a repetitions?`, `(crues?|inondations?|episodes?) historiques?`, `(multiplication|intensification) des (inondations|crues|episodes)`. `inondations a repetition` est aussi un terme du noyau.

**A5 Sécheresse** : `secheresses?`, `deficit hydrique`, `sols? secs?`, `sol tres sec`, `manque d eau`, `penurie d eau`.

**A6 Causes du réchauffement climatique** : mots du dictionnaire OME, thème `changement_climatique_causes` (direct et indirect), hors HRFP, lus dans `public.dictionary` au moment de la requête. Les 54 mots au 02/11/2026 : `A69`, `agriculture conventionnelle`, `agriculture extensive`, `agriculture industrielle`, `agriculture intensive`, `artificialisation des terres`, `bouilloire énergétique`, `bouilloire thermique`, `central à charbon`, `co2`, `co deux`, `combustibles fossiles`, `déforestation`, `destruction de la planète`, `dyoxide de carbone`, `élevage intensif`, `élevage XXL`, `émission de carbone`, `émissions anthropiques`, `émissions importées`, `émissions liées à la consommation`, `émissions territoriales`, `énergie fossile`, `exploitation forestière`, `exploitation gazière`, `exploitation minière des fonds marins`, `exploitation pétrolière`, `ferme-usine`, `Fuel lourd`, `fumée industrielle`, `gaz à effet de serre`, `greenwashing`, `inaction climatique`, `insuffisance climatique`, `logement bouilloire`, `mauvaise nouvelle pour la planète`, `mauvaise nouvelle pour le climat`, `Mercosur`, `méthane`, `nocif pour l'environnement`, `Pas très écolo`, `polluant`, `pollueur`, `pollution émise localement`, `pollution industrielle`, `pollution record`, `productivisme`, `productiviste`, `subsides aux combustibles fossiles`, `subsides aux énergies fossiles`, `subsides fossiles`, `Subventions aux énergies fossiles`, `subventions fossiles`, `voiture thermique`.

## B. Causes (question 2426)

**B1 Mentions explicites du changement climatique** : mêmes termes que A1.

**B2 Météo** : `fortes? pluies?`, `pluies? (intenses?|extremes?|torrentielles?)`, `importantes precipitations`, `mega orages?`.

**B3 Gouvernance** : `choix politiques?`, `defaillance de l etat`, `manque d anticipation`, `sous investissement`, `responsabilite des elus`, `cout de l inaction`, `prevention insuffisante`, `manque de (moyens|prevention)`, `aurai(en)?t pu etre evitee?s?`, `alertes? ignorees?`.

**B4 Urbanisation et aménagement** : `artificialisation`, `impermeabilisation`, `sols? impermeabilises?`, `beton(isation|nage)`, `etalement urbain`, `saturation des sols`, `arrachage des haies`, `remembrement`, `curage`, `entretien des cours d eau`, `embacles?`, `constructions? en zone a risque`, `vetuste des reseaux`, `gestion des eaux pluviales`.

## C. Conséquences (question 2436)

**C1 Humaines** : `evacue(e)?s?`, `sinistre(e)?s?`, `bilan humain`, `vies? humaines?`, `personnes? disparues?`, `disparu(e)?s`, `personnes? deplacee?s`, `deplacements? de (personnes|populations?)`, `morts?`, `deces`, `victimes?`.

**C2 Dommages matériels et reconstruction** : `degats materiels`, `milliards d euros de degats`, `millions d euros de degats`, `pertes economiques`, `coupures? d electricite`, `reconstruire`, `reconstruction`.

**C3 Assurances** : `regime cat nat`, `catnat`, `arretes? de catastrophe naturelle`, `indemnisation`, `assureurs?`, `france assureurs`, `surprime`, `inassurable`, `reassurance`.

**C4 Agriculture** : `cultures inondees`, `parcelles inondees`, `champs inondes`, `terres agricoles inondees`, `recoltes? detruites?`, `pertes? de recolte`, `recolte compromise`, `semis (detruits|retardes)`, `impossibilite de semer`, `retard des semis`, `pertes? de rendement`, `baisse des rendements`, `cereales noyees`, `vergers inondes`, `vignes inondees`, `maraichage`, `cultures maraicheres`.

**C5 Routes et transports** : `routes? (coupees?|barrees?|fermees?|inondees?)`, `axes? routiers?`, `autoroutes? (coupees?|fermees?)`, `circulation (interrompue|coupee|perturbee)`, `trafic (interrompu|perturbe|ferroviaire)`, `trains? (supprimes?|annules?)`.

## D. Solutions (question 2434)

**D1 Secours** : `secours`, `sapeurs? pompiers?`, `sauveteurs?`, `helicopteres?`, `cellule de crise`, `plan orsec`, `securite civile`.

**D2 Alerte et préparation** : `batardeaux?`, `calfeutrer`, `sacs de sable`, `kit d urgence`, `plan communal de sauvegarde`, `exercices? d evacuation`, `systemes? d alerte`, `fr alert`, `vigilance (orange|rouge|crues)`, `alerte precoce`, `evacuation preventive`, `vigicrues( flash)?`, `service de prevision des crues`, `hydrometrie`, `stations? hydrometriques?`, `reseau d observation`, `avertissement automatique`.

**D3 Aménagement** : `repli strategique`, `recul strategique`, `relocalisation`, `demolition des habitations`, `rachat des maisons`, `expropriation`, `non constructible`, `zone inondable`, `inconstructible`, `interdiction de construire`, `desartificialisation`, `surelevation`, `digues?`, `systemes? d endiguement`, `bassins? de retention`.

**D4 Végétalisation** : `desimpermeabilisation`, `desimpermeabiliser`, `vegetalisation`, `renaturation`, `restauration des zones? humides?`, `agroforesterie`, `ville eponge`, `bio retention`, `zan`, `zero artificialisation nette`, `loi climat resilience`, `replanter (d|l)es haies`, `replantage des haies`.

**D5 Entretien des cours d'eau** : `curage`, `curer`, `entretien des (cours d eau|fosses|rivieres|berges)`, `nettoyage des (cours d eau|rivieres|fosses)`, `recalibrage`, `recalibrer`, `(retrait|enlevement) des embacles`, `desenvasement`, `faucardage`, `wateringues?`. `curage` et `entretien des cours d eau` sont aussi dans B4.

**D6 Politiques d'adaptation** : `fonds vert`, `plan national d adaptation`, `pnacc`, `fond barnier`, `loi climat et resilience`.

**D7 Atténuation à la racine** : `reduire les emissions`, `reduction des emissions`, `baisse des emissions`, `decarbonation`, `decarboner`, `neutralite carbone`, `zero emission nette`, `sortie des energies fossiles`, `sortie du charbon`, `energies fossiles`, `puits de carbone`, `budget carbone`, `objectifs climatiques`, `accord de paris`, `green deal`, `plan climat`, `pniec`, `snbc`.

## Autres questions

**V1 Espagne** (question 2427) : `espagne`, `valence`, `barcelone`, `andalousie`.

**X1 Occurrences CC et météo** (question 2457) : changement climatique, mêmes termes que A1 ; météo, mêmes termes que B2.
