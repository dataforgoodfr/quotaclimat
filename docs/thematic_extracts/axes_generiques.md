# Banque d'axes génériques

Axes qui ne dépendent pas du sujet traité, réutilisables d'une analyse à l'autre, avec la représentation graphique conseillée pour chacun. Chaque analyse a son propre `axes.md` : elle y reprend les axes de cette banque qu'elle applique (avec un renvoi ici) et y décrit ses axes spécifiques.

Un axe spécifique remonte dans cette banque quand il ne dépend pas du sujet, ou dès qu'une deuxième analyse le réutilise.

Les termes sont écrits sous forme normalisée (`methodo.md` §5) : minuscules, sans accent, apostrophe remplacée par un espace. Une liste est un point de départ : à contrôler (`methodo.md` §6) avant réutilisation. Une analyse qui ajoute ou retire des termes l'écrit dans son `axes.md`, sans modifier la liste d'ici.

La colonne « Graphique » renvoie au [catalogue des représentations graphiques](#représentations-graphiques) en fin de fichier.

🔴 [termes à risque de faux positifs, à contrôler avant réutilisation : `record`, `universite`, `le vent`, `du vent`, `police`, `auteur`, `masque`, `atmo`]

## Mesures du socle (reprises à chaque analyse)

| Code | Mesure | Question posée | Graphique | Explorée dans |
|---|---|---|---|---|
| Q1 | Volumétrie | Combien en parle-t-on ? Nombre et part des segments, temps d'antenne, nombre d'articles | G7 Classement par famille | Incendies 2026, Chaleur 2026 |
| Q6 | Comparaison | La couverture est-elle exceptionnelle par rapport à un épisode similaire ? | G8 Avant / pendant | Incendies 2026, Chaleur 2026 |
| Q7 | Courbe journalière | Comment la couverture suit-elle l'événement ? | G2 Courbe annotée ; G1 Courbe moyenne, max, min | Incendies 2026 |
| Q7b | Reflux | La couverture retombe-t-elle sous son niveau habituel après le pic ? Couverture après l'épisode comparée à la période de comparaison | G2 Courbe annotée, avec le niveau de comparaison en référence | idée (Bilan OMÉ S1 2026) |
| Q8 | Par événement | Quels événements (lieux, départements...) sont couverts ? | G10 Barres horizontales avec n | Incendies 2026 |

Ventilations habituelles : chaîne ou titre, public / privé, presse nationale / régionale, TV / radio.

## Axes

| Axe | Question posée | Graphique | Explorée dans |
|---|---|---|---|
| [Lien avec le changement climatique](#lien-avec-le-changement-climatique) | Les médias recontextualisent-ils l'événement avec le changement climatique ? | G7 Classement par famille ; G3 Courbes par épisode ; G4 Boîte à moustaches par média | Incendies 2026, Chaleur 2026 |
| [Co-aléa chaleur](#co-aléa-chaleur) | L'événement est-il relié à la chaleur ? | G7 Classement par famille | Incendies 2026 |
| [Co-aléa sécheresse](#co-aléa-sécheresse) | L'événement est-il relié à la sécheresse ? | G7 Classement par famille | Incendies 2026 |
| [Co-aléa vent et météo](#co-aléa-vent-et-météo) | L'événement est-il relié aux conditions météo ? | G7 Classement par famille | Incendies 2026 |
| [Aléas composés](#aléas-composés) | L'événement est-il présenté comme la combinaison de plusieurs aléas ? | G10 Barres horizontales avec n | Incendies 2026 |
| [Mise en série](#mise-en-série) | L'événement est-il présenté comme une tendance ? | G7 Classement par famille | Incendies 2026 |
| [Voix scientifiques](#voix-scientifiques) | Qui est invité à expliquer ? | G7 Classement par famille | Incendies 2026 |
| [Causes climatiques](#causes-climatiques) | Le changement climatique est-il présenté comme une cause ? | G4 Boîte à moustaches par média ; croisement Lien CC x causes | Incendies 2026, Chaleur 2026 |
| [Solutions](#solutions) | Quelles solutions sont présentées : atténuation, adaptation ? | G11 Barres empilées par média | Incendies 2026, Chaleur 2026 |
| [Santé](#santé) | Les effets sanitaires sont-ils évoqués ? | G7 Classement par famille | Incendies 2026 |
| [Forces de l'ordre et justice](#forces-de-lordre-et-justice) | Le sujet est-il traité comme un fait divers ? | G7 Classement par famille | Incendies 2026 |
| Désinformation | Quels narratifs trompeurs circulent ? Cas détectés par climateguard, saisines Arcom. Pistes : taux normalisé (cas par heure d'information sur le climat), répartition par type de locuteur (invités politiques, journalistes, chroniqueurs) | G2 Histogramme hebdomadaire annoté ; G7 Classement des cas par heure | Chaleur 2026 ; pistes : idée (Bilan OMÉ S1 2026) |
| Unes | Le sujet fait-il la Une ? Présence en Une, par titre | G10 Barres horizontales avec n | Chaleur 2026 |
| Économie et assurance | Le coût est-il chiffré ? | | idée |
| Biodiversité | Les écosystèmes sont-ils mentionnés ? | | idée |
| Chaîne causale | Quel maillon est évoqué : constat, causes, conséquences, solutions d'atténuation ou d'adaptation ? Découpe le lien CC au lieu de le traiter en binaire | G5 Donut et évolution | idée (Bilan OMÉ S1 2026) |
| Conséquences par secteur et par population | Qui est montré comme touché : enfance et écoles, santé, travail, personnes âgées, agriculture, alimentation ? | G6 Barres groupées secteur × épisode | idée (Bilan OMÉ S1 2026) |
| Champ lexical et cadrage lexical | Quels termes portent un axe, et lesquels sont absents ? Exemple : « intempéries », « fatalité » face à « dérèglement climatique ». S'appuie sur `keywords_found` | G12 Barres terme × famille de presse | idée (Bilan OMÉ S1 2026, Acrimed) |
| Solutions individuelles ou systémiques | Les solutions présentées relèvent-elles du geste individuel et du court terme, ou de l'action collective et structurelle ? Généralise l'axe chaleur « adaptation palliative ou structurelle » | G11 Barres empilées par média ; G3 Courbes par épisode | idée (Bilan OMÉ S1 2026, Chaleur 2026) |
| Cadrages concurrents | Quel angle domine : pouvoir d'achat, souveraineté, géopolitique, industrie et emploi, santé, « écologie punitive » ? Codage booléen non exclusif, un dictionnaire par cadrage | G9 Courbes avec périodes grisées | idée (Bilan OMÉ S1 2026) |
| Politisation | Le sujet est-il présenté comme l'affaire d'un camp politique ? Demande une lecture humaine ; un codage par LLM impose les précautions de la charte IA (validation humaine, prompts versionnés) | G10 Barres horizontales avec n | idée (Bilan OMÉ S1 2026) |
| Couplage avec une autre actualité | Quelle part du corpus relie le sujet à un autre événement du moment (élection, crise énergétique...) ? | G7 Classement par famille | idée (Bilan OMÉ S1 2026) |
| Types d'intervenants | Qui est invité à expliquer : élus, secours, sinistrés, scientifiques, associations, monde agricole ? Élargit « voix scientifiques ». Limite : `plaintext` ne dit pas qui parle, on détecte la mention d'un type d'intervenant | G11 Barres empilées par média | idée (Acrimed) |
| Hiérarchie dans le JT | Le sujet ouvre-t-il le journal, à quelle position arrive-t-il ? 🔴 [vérifier si la grille des programmes permet de situer un segment dans l'émission] | | idée (Acrimed) |

## Croisements

| Croisement | Question posée | Graphique | Explorée dans |
|---|---|---|---|
| Lien CC x causes, par média | Faire le lien, est-ce expliquer ? | G13 Nuage de points | Incendies 2026, Chaleur 2026 |
| Volume x recontextualisation, par média | Ceux qui en parlent le plus recontextualisent-ils le plus ? | G13 Nuage de points | idée |
| Longueur x lien CC | Les articles longs recontextualisent-ils davantage ? | Barres par tranche de longueur | Incendies 2026 |
| Évolution du lien CC dans l'épisode | Le climat arrive-t-il au pic ou après ? | G2 Courbe annotée | idée |
| Comparaison entre épisodes | Un aléa est-il mieux recontextualisé qu'un autre ? | G3 Courbes par épisode ; G4 Boîte à moustaches par média | Incendies 2026 vs Chaleur 2026 |
| Spectacle ou explication | La couverture porte-t-elle sur l'opérationnel (images, dégâts, secours) plutôt que sur les causes, les enjeux et les solutions ? | G11 Barres empilées par média | idée (Acrimed) |

## Représentations graphiques

| Code | Graphique | Ce qu'il montre | Piège | Source |
|---|---|---|---|---|
| G1 | Courbe hebdomadaire avec moyenne, maximum et minimum | La volatilité de la couverture, pas seulement son niveau | | Bilan OMÉ S1 2026 |
| G2 | Courbe annotée d'événements (COP, publication d'un plan, début d'une crise) | Ce qui explique les pics | Trop d'annotations noient la courbe | Bilan OMÉ S1 2026 |
| G3 | Courbes avec un épisode par point de l'axe x | Évolution d'un indicateur d'un épisode à l'autre | Épisodes de durées différentes : normaliser | Bilan OMÉ S1 2026 |
| G4 | Boîte à moustaches par média sur plusieurs épisodes | Moyenne et dispersion d'un média d'un épisode à l'autre | Peu d'épisodes : la boîte ne veut rien dire | Bilan OMÉ S1 2026 |
| G5 | Donut, à côté de l'évolution mensuelle | Répartition entre plusieurs catégories, par exemple les maillons de la chaîne causale | Plus de cinq catégories : préférer des barres | Bilan OMÉ S1 2026 |
| G6 | Barres groupées catégorie × épisode | Comment une répartition évolue d'un épisode à l'autre | | Bilan OMÉ S1 2026 |
| G7 | Classement des médias en barres colorées par famille | Classement lisible et lecture par famille (nationale, régionale, économique, AFP, public, privé) | Médias sous l'effectif minimal (`methodo.md` §8) : les signaler ou les regrouper | Bilan OMÉ S1 2026 |
| G8 | Barres « avant / pendant » avec la variation en étiquette | Effet d'un événement sur la couverture | | Bilan OMÉ S1 2026 |
| G9 | Courbes avec périodes grisées, éventuellement double axe audiovisuel / presse | Synchronisation entre médias ou entre cadrages | Double axe : ne se lit qu'en tendance, jamais en valeur absolue | Bilan OMÉ S1 2026 |
| G10 | Barres horizontales simples avec n dans le titre | Un résultat unique avec son dénominateur affiché | | Bilan OMÉ S1 2026 |
| G11 | Barres empilées par média | Composition de la couverture de chaque média | Trop de segments : regrouper | |
| G12 | Barres terme × famille de presse | Quels termes portent un axe, selon le type de média | | Bilan OMÉ S1 2026 |
| G13 | Nuage de points, un point par média | Relation entre deux mesures | Petits effectifs : afficher n ou la taille du point | Incendies 2026, Chaleur 2026 |

À éviter : les barres groupées croisant une vingtaine de médias et plusieurs épisodes, illisibles.

## Termes

### Lien avec le changement climatique

- **Mention explicite** : `changement climatique`, `changements climatiques`, `dereglement climatique`, `rechauffement climatique`, `rechauffement de la planete`, `rechauffement planetaire`, `crise climatique`, `urgence climatique`, `climat se rechauffe`, `le climat change`
- **Via le dictionnaire OME** : thèmes `changement_climatique_constat`, `changement_climatique_causes`, `changement_climatique_consequences` (`keywords_with_timestamp` en audiovisuel, `predict_climat_*` en presse). Variante hors HRFP en audiovisuel.

### Co-aléa chaleur

`canicule`, `caniculaire`, `vague de chaleur`, `vagues de chaleur`, `forte chaleur`, `fortes chaleurs`, `chaleur extreme`, `chaleurs extremes`, `pic de chaleur`, `record de chaleur`, `temperatures records`, `35 degres`, `36 degres`, `37 degres`, `38 degres`, `39 degres`, `40 degres`, `plan canicule`

### Co-aléa sécheresse

`secheresse`, `secheresses`, `deficit hydrique`, `sol sec`, `sols secs`, `sol tres sec`, `manque d eau`, `penurie d eau`, `nappe phreatique`, `nappes phreatiques`, `restriction d eau`, `restrictions d eau`, `assechement`, `asseche`, `assechee`, `vegetation seche`, `tres sec`

### Co-aléa vent et météo

`le vent`, `les vents`, `du vent`, `rafale`, `rafales`, `bourrasque`, `direction du vent`, `vent du nord`, `vent du sud`, `vent fort`, `vents forts`, `vent violent`, `conditions meteo`, `meteo defavorable`, `meteo favorable`, `previsions meteo`

### Aléas composés

Au moins deux des co-aléas ci-dessus.

### Mise en série

`de plus en plus frequent`, `de plus en plus frequents`, `de plus en plus frequentes`, `de plus en plus intenses`, `plus frequents`, `record`, `jamais vu`, `sans precedent`, `nouvelle norme`

### Voix scientifiques

`climatologue`, `climatologues`, `scientifique`, `scientifiques`, `chercheur`, `chercheuse`, `chercheurs`, `giec`, `meteo france`, `cnrs`, `universite`, `expert du climat`

### Causes climatiques

Liste « mention explicite » de l'axe [Lien avec le changement climatique](#lien-avec-le-changement-climatique), et thème `changement_climatique_causes` du dictionnaire OME.

### Solutions

- **Via le dictionnaire OME** : thèmes `attenuation_climatique_solutions`, `adaptation_climatique_solutions`, `biodiversite_solutions`, `ressources_solutions` (`changement_climatique_solutions` en presse). Pour séparer atténuation et adaptation : mots de chaque thème dans `public.dictionary`, moins les mots communs aux deux.
- **Atténuation** : `reduire les emissions`, `reduction des emissions`, `baisse des emissions`, `decarbonation`, `decarboner`, `neutralite carbone`, `zero emission nette`, `sortie des energies fossiles`, `sortie du charbon`, `energies fossiles`, `puits de carbone`, `budget carbone`, `objectifs climatiques`, `accord de paris`, `green deal`, `plan climat`, `pniec`, `snbc`

### Santé

`qualite de l air`, `pollution de l air`, `particules fines`, `pm10`, `pm2`, `microparticules`, `masque`, `personnes vulnerables`, `personnes fragiles`, `problemes respiratoires`, `asthme`, `rester chez soi`, `rester a l interieur`, `consigne sanitaire`, `consignes sanitaires`, `recommandation sanitaire`, `fermer les fenetres`, `atmo`, `prevair`

### Forces de l'ordre et justice

`parquet`, `enquete judiciaire`, `ouvre une enquete`, `juge d instruction`, `police`, `auteur`, `poursuites`, `plainte`, `risque jusqu a`, `ans de prison`, `inculpe`
