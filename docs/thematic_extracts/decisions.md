# Analyses thématiques : méthode commune

Analyses ponctuelles de l'Observatoire des Médias sur l'Écologie (OMÉ) sur un sujet donné : incendies, vagues de chaleur, inondations, etc. Ce fichier décrit la méthode commune à toutes les analyses, la banque d'axes et les références. Chaque analyse a ensuite son propre `<AAAA-MM>-<sujet>/decisions.md`, qui ne contient que ce qui lui est propre.

Les analyses incendies et vague de chaleur 2026 servent de modèle pour la démarche, les axes contextuels et la restitution. Leur corpus noyau a été construit avant la révision de la méthode, directement sur `keywords` et `factiva_articles`. Leurs résultats restent réutilisables et comparables à ceux des analyses suivantes.

Statuts des règles : **adoptée** (s'applique), **provisoire** (s'applique, à valider).

## 1. Démarche

Quatre étapes, chacune validée explicitement par l'équipe avant de passer à la suivante.

| Étape | Ce qu'on produit | Où on le consigne |
|---|---|---|
| 1. Extraction | Entrée de `thematic_jobs_config.yml` (mots-clés, périodes), tables thématiques générées, contrôles | `decisions.md` de l'analyse, §1 et §2 |
| 2. Axes | Axes de la banque appliqués ou écartés, axes spécifiques, termes, graphiques prévus | `decisions.md` de l'analyse, §3 |
| 3. Requêtes | Questions Metabase sur les tables thématiques | Colonne « Question Metabase » du §3 |
| 4. Résultats | Takeaways triés (publiable, à creuser, rien à signaler), encadré méthodologique | Document Metabase avant publication, `decisions.md` de l'analyse §4 après publication |

**Le repo est public** : aucun chiffre ni takeaway non publié n'y entre. Avant publication, les résultats vivent dans un document Metabase `Résultats - <Sujet> <Année>` de la collection de l'analyse. Le statut d'un axe reste « testé » jusqu'à la publication.

**Conventions Metabase.** Collection `03. Analyses thématiques/NN. <Sujet> <Année>`. Préfixe `AV -` pour l'audiovisuel, `PE -` pour la presse. Numérotation `Qn - <axe>` (Q1 volumétrie, Q2 recontextualisation, Q3 causes, Q5 solutions, Q6 comparaison, Q7 courbe journalière, Q8 par événement). Versions abandonnées préfixées `OLD-`. On ne modifie jamais une question d'une autre analyse : on la duplique. Après publication, le SQL des questions utilisées est copié dans `<AAAA-MM>-<sujet>/queries/`.

**Nouvelle analyse.** Créer `<AAAA-MM>-<sujet>/decisions.md` à partir du [modèle](#6-modèle-pour-une-analyse) et remplir les §1 et §2 avant toute requête.

## 2. Données

Les analyses tournent sur des tables thématiques légères, générées par un job Scaleway lancé à la main (code : `quotaclimat/data_processing/thematic_extracts/`), et non plus directement sur `keywords` et `factiva_articles`.

**Configuration.** `quotaclimat/data_processing/thematic_extracts/thematic_jobs_config.yml`, une entrée par thème :
- `periodes.<pays>` (`france`, `belgium`, `belgium_flanders`) : liste des périodes à extraire, chacune avec un `id` (`[a-z0-9_]`), un `start` (inclus) et un `end` (exclu). Les périodes d'un même pays ne se chevauchent pas ;
- `keywords.<langue>.audiovisuel` : termes sous leur forme exacte dans `keywords_with_timestamp` (orthographe et accents du dictionnaire OME) ;
- `keywords.<langue>.presse` : toutes les variantes (genre, nombre) des termes OME, détectées par regex après normalisation.

Le job prend en entrée `THEMATIC` (l'entrée du fichier), `MODE` et `COUNTRY` (par défaut `france`). Modes : `new` crée la table, `resume` reprend chaque période à son dernier jour ingéré (c'est ainsi qu'on ajoute une période), `reset` vide la table et repart du début (obligatoire si l'on modifie les dates ou l'`id` d'une période déjà ingérée). `DRY_RUN` n'écrit rien.

**Tables.** Schéma `thematic_extracts`, nommées `<thème>_<articles|keywords>_<pays>`, sans date, où `<pays>` vaut `fr`, `be` ou `befl` (exemple : `incendies_keywords_fr`). Une table par thème, par type de média et par pays ; elle contient toutes les périodes de l'entrée, quel que soit leur nombre. On sélectionne une période en filtrant sur la colonne `period`, qui contient l'`id` de la période dans le YAML (par exemple `ete_2026`).

| | Presse écrite | Audiovisuel |
|---|---|---|
| Base | Print Media DB (Metabase id 8) | Baromètre (Scaleway Postgres, Metabase id 2) |
| Source | `public.factiva_articles`, hors `duplicate_status = 'DUP'` | `public.keywords` pour les segments thématiques, S3 Mediatree pour les segments adjacents |
| Une ligne | Un article dont le titre, le chapô ou le corps contient un terme de l'entrée `presse` | Un segment de 2 minutes : thématique (`thematic_chunk = true`, il contient un terme de l'entrée `audiovisuel`) ou adjacent d'un segment thématique de la même chaîne (`false`). Un segment à la fois thématique et adjacent compte comme thématique |
| Texte (normalisé, §3.5) | `title_normalized`, `snippet_normalized`, `body_normalized` | `plaintext_normalized` |
| Colonne de contrôle | `keywords_found` : termes détectés, distincts, sous forme normalisée | `keywords_found` : termes détectés, distincts, sous leur forme dans le dictionnaire OME (non normalisée) ; `themes` : thèmes OME détectés. Vides pour un segment adjacent |
| Période | `period` : `id` de la période dans le YAML | `period` : `id` de la période dans le YAML |
| Autres colonnes | `an` (clé), `source_code`, `source_name`, `publication_datetime`, `word_count`, `created_at` | `this_id` (clé : chaîne + `start` en UTC), `channel_name`, `channel_title`, `start` (heure de Paris), `source_keywords_id`, `country` (contrôle), `created_at` |

Les anciennes tables datées (par exemple `incendies_articles_fr_20260701_20260708`) sont vouées à disparaître ; aucune table n'est plus créée sous ce nom.

Volume du corpus audiovisuel : toujours filtrer sur `thematic_chunk = true`. Les segments adjacents ne servent qu'à la recontextualisation.

Classification des titres de presse (nationale, régionale...) : table `source_classification` (`media_all`, `source_type`), à joindre sur `source_code`.

## 3. Méthode

### 3.1 Corpus noyau (adoptée)

- Le corpus noyau d'un thème est construit uniquement à partir de mots du dictionnaire OME, directs (HRFP) ou indirects.
- La liste de chaque thème est fixe : un thème récurrent (incendies, inondations...) utilise toujours la même entrée de `thematic_jobs_config.yml`.
- En presse, une expression qui en contient une autre (« gestion des inondations » et « inondations ») n'ajoute aucun article au corpus. On la garde si elle est non-HRFP : `keywords_found` l'affiche alors en priorité et indique par quel terme l'article est entré dans `factiva_articles`.
- La correspondance entre `thematic_jobs_config.yml` et le dictionnaire OME est vérifiée à la main à la création ou à la modification d'une entrée.
- Le corpus noyau ne contient pas de mot absent du dictionnaire OME. Si le besoin se présente, se demander s'il devrait entrer dans le dictionnaire OME.
- On ne filtre pas le corpus sur les contenus validés « environnement » par l'OME, pour éviter la circularité : mesurer la recontextualisation environnementale sur des contenus déjà sélectionnés pour leur caractère environnemental.

### 3.2 Périodes (adoptée)

- **Période d'analyse** : une fenêtre d'actualité chaude sur le sujet, justifiée par une source datée (Météo-France, préfecture, presse), renseignée dans `periodes.<pays>`. C'est elle qui limite les faux positifs d'un dictionnaire noyau large.
- **Période de comparaison** : un épisode similaire (même aléa, actualité comparable), de durée équivalente ou normalisée, renseigné comme une autre période de la même entrée. Les requêtes comparent les périodes via la colonne `period`.

### 3.3 Axes contextuels (adoptée)

- Chaque axe a son propre dictionnaire de termes explicites. Ces termes n'ont pas à appartenir au dictionnaire OME, puisqu'un axe peut porter sur l'économie, les transports, la santé...
- Les axes qui ne dépendent pas du sujet sont dans la [banque d'axes génériques](#4-banque-daxes-génériques). Chaque analyse liste dans son §3 tous les axes appliqués : pour un axe de la banque, un renvoi et les éventuels ajouts ou retraits de termes ; pour un axe spécifique, ses termes. Les axes de la banque écartés sont listés avec leur raison.
- Un axe spécifique remonte dans la banque quand il ne dépend pas du sujet, ou dès qu'une deuxième analyse le réutilise.
- Dans le SQL, chaque axe est un drapeau (une regex). Réutiliser les regex existantes avant d'en écrire.

### 3.4 Recontextualisation (adoptée)

- **Audiovisuel** : un axe compte s'il apparaît dans le segment thématique ou dans un segment adjacent de la même chaîne (±2 minutes).
- **Presse** : l'article entier (titre, chapô, corps).

### 3.5 Normalisation du texte (adoptée)

Le job normalise le texte des tables thématiques et, en presse, les mots-clés recherchés, de la même manière :

1. minuscules ;
2. `œ` et `æ` remplacés par `oe` et `ae`, accents retirés ;
3. apostrophes (toutes variantes typographiques) et tirets remplacés par un espace.

Les regex des axes s'appliquent aux colonnes `*_normalized`, en mots entiers (`\y...\y` en Postgres). Leurs termes sont donc écrits sous la même forme : `secheresse`, `manque d eau`, `pare feu`.

### 3.6 Contrôles (étape 1)

- **Complétude (adoptée)** : comparer la table thématique avec la table source sur la même période et les mêmes termes. Tout écart doit être expliqué.
- **Précision (provisoire)** : sur un échantillon de contenus par terme fréquent, relire et compter les faux positifs à l'aide de `keywords_found`. Un terme trop bruité est discuté (exclusion ou contrainte) et le choix noté dans le §2 de l'analyse.
- **Rappel (adoptée)** : vérifier que quelques contenus connus de l'épisode (JT du jour du pic, Unes) sont dans le corpus.
- **Périmètre cadré (provisoire)** : pour distinguer le sujet principal de la mention incidente, calculer aussi les résultats sur les contenus où un terme du noyau est dans le titre ou apparaît au moins N fois. Le seuil N est noté dans l'analyse.
- **Discrimination d'un axe (provisoire)** : un axe présent dans plus de 70 % du corpus ne discrimine plus ; le redéfinir en une version large et une version stricte.
- **Robustesse (provisoire)** : recalculer les principaux résultats sur un sous-corpus plus strict (plusieurs mentions du noyau, articles longs) et vérifier qu'ils tiennent.
- **Lecture du bruit (provisoire)** : un bruit réparti uniformément dans le temps fausse les niveaux absolus mais pas les comparaisons entre périodes. Le vérifier avant de publier un niveau absolu.

Les contrôles provisoires viennent des annexes méthodologiques du Bilan OMÉ S1 2026 (§5).

### 3.7 Unités et effectif minimal (adoptée)

- Audiovisuel : unité = segment de 2 minutes ; temps d'antenne en minutes. Presse : unité = article.
- Ne jamais comparer une part de segments à une part de temps d'antenne.
- Chaque requête sort numérateurs et dénominateurs bruts en colonnes séparées, en plus des pourcentages.
- Un média dont le corpus compte moins de **30** segments ou articles reste dans les résultats, mais on n'en tire aucune conclusion individuelle. Il est exploité à l'intérieur de sa famille : presse nationale, presse régionale, audiovisuel public, audiovisuel privé.

### 3.8 Takeaway publiable (adoptée)

Un résultat est publiable si :

- le dénominateur est affiché et atteint l'effectif minimal ;
- l'écart entre périodes ou entre groupes est net, pas quelques points sur de petits effectifs ;
- la conclusion tient avec et sans la fenêtre de ±2 minutes (audiovisuel) ;
- les unités sont homogènes ;
- il est formulé avec chiffre, dénominateur et période.

Les pistes testées sans résultat sont notées dans le §3 de l'analyse, jamais effacées.

### 3.9 Encadré méthodologique et validation humaine (adoptée)

- Chaque article publié inclut : corpus (médias, nombre de segments et d'articles), période et comparaison, lien vers l'entrée de `thematic_jobs_config.yml` et vers le `decisions.md` de l'analyse, méthode de recontextualisation, limites (§3.10).
- Tout contenu diffusé est relu par une personne experte du sujet. Les dictionnaires sont versionnés dans le repo, le SQL dans Metabase puis dans `queries/` après publication.

### 3.10 Limites persistantes

- **Presse, mots HRFP** : sans réactiver l'API Factiva, on ne récupère pas tous les articles qui ne contiennent que des mots clés HRFP, car seuls les mots non-HRFP déclenchent leur recensement dans `factiva_articles`. Nous décidons de ne pas la réactiver pour l'instant.
- **Uniformité entre médias** : en audiovisuel, le corpus inclut tous les segments contenant un mot du noyau, direct ou indirect ; en presse, l'accès dépend des mots non-HRFP. À rappeler dans l'encadré méthodologique.
- **Segments adjacents** : récupérés sur S3 sous réserve qu'ils y soient présents.
- **Dictionnaires d'axes** : explicites mais non exhaustifs ; ni la précision ni le rappel ne sont garantis à 100 %.
- **Valence** : détecter un thème ne dit pas si le contenu le défend ou le critique.

## 4. Banque d'axes génériques

Axes qui ne dépendent pas du sujet, avec leur graphique conseillé (codes du [§4.4](#44-représentations-graphiques)). Les termes ([§4.5](#45-termes)) sont un point de départ : à contrôler avant réutilisation. Une analyse qui ajoute ou retire des termes l'écrit dans son §3, sans modifier la liste d'ici.

Statuts : **publié** (a donné un résultat publié), **testé** (requête faite, résultat non publié ou nul), **idée** (jamais testé).

🔴 [termes à risque de faux positifs, à contrôler avant réutilisation : `record`, `universite`, `le vent`, `du vent`, `police`, `auteur`, `masque`, `atmo`]

### 4.1 Mesures du socle (reprises à chaque analyse)

| Code | Mesure | Question posée | Graphique | Explorée dans |
|---|---|---|---|---|
| Q1 | Volumétrie | Combien en parle-t-on ? Nombre et part des segments, temps d'antenne, nombre d'articles | G7 | Incendies 2026, Chaleur 2026 |
| Q6 | Comparaison | La couverture est-elle exceptionnelle par rapport à un épisode similaire ? | G8 | Incendies 2026, Chaleur 2026 |
| Q7 | Courbe journalière | Comment la couverture suit-elle l'événement ? | G2, G1 | Incendies 2026 |
| Q7b | Reflux | La couverture retombe-t-elle sous son niveau habituel après le pic ? | G2, avec le niveau de comparaison en référence | idée (Bilan OMÉ S1 2026) |
| Q8 | Par événement | Quels événements (lieux, départements...) sont couverts ? | G10 | Incendies 2026 |

Ventilations habituelles : chaîne ou titre, public / privé, presse nationale / régionale, TV / radio.

### 4.2 Axes

| Axe | Question posée | Graphique | Explorée dans |
|---|---|---|---|
| [Lien avec le changement climatique](#lien-avec-le-changement-climatique) | Les médias recontextualisent-ils l'événement avec le changement climatique ? | G7, G3, G4 | Incendies 2026, Chaleur 2026 |
| [Co-aléa chaleur](#co-aléa-chaleur) | L'événement est-il relié à la chaleur ? | G7 | Incendies 2026 |
| [Co-aléa sécheresse](#co-aléa-sécheresse) | L'événement est-il relié à la sécheresse ? | G7 | Incendies 2026 |
| [Co-aléa vent et météo](#co-aléa-vent-et-météo) | L'événement est-il relié aux conditions météo ? | G7 | Incendies 2026 |
| [Aléas composés](#aléas-composés) | L'événement est-il présenté comme la combinaison de plusieurs aléas ? | G10 | Incendies 2026 |
| [Mise en série](#mise-en-série) | L'événement est-il présenté comme une tendance ? | G7 | Incendies 2026 |
| [Voix scientifiques](#voix-scientifiques) | Qui est invité à expliquer ? | G7 | Incendies 2026 |
| [Causes climatiques](#causes-climatiques) | Le changement climatique est-il présenté comme une cause ? | G4, croisement Lien CC x causes | Incendies 2026, Chaleur 2026 |
| [Solutions](#solutions) | Quelles solutions sont présentées : atténuation, adaptation ? | G11 | Incendies 2026, Chaleur 2026 |
| [Santé](#santé) | Les effets sanitaires sont-ils évoqués ? | G7 | Incendies 2026 |
| [Forces de l'ordre et justice](#forces-de-lordre-et-justice) | Le sujet est-il traité comme un fait divers ? | G7 | Incendies 2026 |
| Désinformation | Quels narratifs trompeurs circulent ? Cas détectés par climateguard, saisines Arcom. Pistes : taux normalisé (cas par heure d'information sur le climat), répartition par type de locuteur | G2, G7 | Chaleur 2026 ; pistes : idée (Bilan OMÉ S1 2026) |
| Unes | Le sujet fait-il la Une ? Présence en Une, par titre | G10 | Chaleur 2026 |
| Économie et assurance | Le coût est-il chiffré ? | | idée |
| Biodiversité | Les écosystèmes sont-ils mentionnés ? | | idée |
| Chaîne causale | Quel maillon est évoqué : constat, causes, conséquences, solutions d'atténuation ou d'adaptation ? | G5 | idée (Bilan OMÉ S1 2026) |
| Conséquences par secteur et par population | Qui est montré comme touché : enfance et écoles, santé, travail, personnes âgées, agriculture, alimentation ? | G6 | idée (Bilan OMÉ S1 2026) |
| Champ lexical et cadrage lexical | Quels termes portent un axe, et lesquels sont absents ? Exemple : « intempéries », « fatalité » face à « dérèglement climatique ». S'appuie sur `keywords_found` | G12 | idée (Bilan OMÉ S1 2026, Acrimed) |
| Solutions individuelles ou systémiques | Les solutions relèvent-elles du geste individuel et du court terme, ou de l'action collective et structurelle ? Généralise l'axe chaleur « adaptation palliative ou structurelle » | G11, G3 | idée (Bilan OMÉ S1 2026, Chaleur 2026) |
| Cadrages concurrents | Quel angle domine : pouvoir d'achat, souveraineté, géopolitique, industrie et emploi, santé, « écologie punitive » ? Codage booléen non exclusif, un dictionnaire par cadrage | G9 | idée (Bilan OMÉ S1 2026) |
| Politisation | Le sujet est-il présenté comme l'affaire d'un camp politique ? Demande une lecture humaine ; un codage par LLM impose les précautions de la charte IA | G10 | idée (Bilan OMÉ S1 2026) |
| Couplage avec une autre actualité | Quelle part du corpus relie le sujet à un autre événement du moment (élection, crise énergétique...) ? | G7 | idée (Bilan OMÉ S1 2026) |
| Types d'intervenants | Qui est invité à expliquer : élus, secours, sinistrés, scientifiques, associations, monde agricole ? Limite : on détecte la mention d'un type d'intervenant, pas qui parle | G11 | idée (Acrimed) |
| Hiérarchie dans le JT | Le sujet ouvre-t-il le journal, à quelle position arrive-t-il ? 🔴 [vérifier si la grille des programmes permet de situer un segment dans l'émission] | | idée (Acrimed) |

### 4.3 Croisements

| Croisement | Question posée | Graphique | Explorée dans |
|---|---|---|---|
| Lien CC x causes, par média | Faire le lien, est-ce expliquer ? | G13 | Incendies 2026, Chaleur 2026 |
| Volume x recontextualisation, par média | Ceux qui en parlent le plus recontextualisent-ils le plus ? | G13 | idée |
| Longueur x lien CC | Les articles longs recontextualisent-ils davantage ? | Barres par tranche de longueur | Incendies 2026 |
| Évolution du lien CC dans l'épisode | Le climat arrive-t-il au pic ou après ? | G2 | idée |
| Comparaison entre épisodes | Un aléa est-il mieux recontextualisé qu'un autre ? | G3, G4 | Incendies 2026 vs Chaleur 2026 |
| Spectacle ou explication | La couverture porte-t-elle sur l'opérationnel (images, dégâts, secours) plutôt que sur les causes, les enjeux et les solutions ? | G11 | idée (Acrimed) |

### 4.4 Représentations graphiques

| Code | Graphique | Ce qu'il montre | Piège | Source |
|---|---|---|---|---|
| G1 | Courbe hebdomadaire avec moyenne, maximum et minimum | La volatilité de la couverture, pas seulement son niveau | | Bilan OMÉ S1 2026 |
| G2 | Courbe annotée d'événements (COP, publication d'un plan, début d'une crise) | Ce qui explique les pics | Trop d'annotations noient la courbe | Bilan OMÉ S1 2026 |
| G3 | Courbes avec un épisode par point de l'axe x | Évolution d'un indicateur d'un épisode à l'autre | Épisodes de durées différentes : normaliser | Bilan OMÉ S1 2026 |
| G4 | Boîte à moustaches par média sur plusieurs épisodes | Moyenne et dispersion d'un média d'un épisode à l'autre | Peu d'épisodes : la boîte ne veut rien dire | Bilan OMÉ S1 2026 |
| G5 | Donut, à côté de l'évolution mensuelle | Répartition entre quelques catégories | Plus de cinq catégories : préférer des barres | Bilan OMÉ S1 2026 |
| G6 | Barres groupées catégorie × épisode | Comment une répartition évolue d'un épisode à l'autre | | Bilan OMÉ S1 2026 |
| G7 | Classement des médias en barres colorées par famille | Classement lisible et lecture par famille | Médias sous l'effectif minimal : les signaler ou les regrouper | Bilan OMÉ S1 2026 |
| G8 | Barres « avant / pendant » avec la variation en étiquette | Effet d'un événement sur la couverture | | Bilan OMÉ S1 2026 |
| G9 | Courbes avec périodes grisées, éventuellement double axe audiovisuel / presse | Synchronisation entre médias ou entre cadrages | Double axe : ne se lit qu'en tendance, jamais en valeur absolue | Bilan OMÉ S1 2026 |
| G10 | Barres horizontales simples avec n dans le titre | Un résultat unique avec son dénominateur affiché | | Bilan OMÉ S1 2026 |
| G11 | Barres empilées par média | Composition de la couverture de chaque média | Trop de segments : regrouper | |
| G12 | Barres terme × famille de presse | Quels termes portent un axe, selon le type de média | | Bilan OMÉ S1 2026 |
| G13 | Nuage de points, un point par média | Relation entre deux mesures | Petits effectifs : afficher n ou la taille du point | Incendies 2026, Chaleur 2026 |

À éviter : les barres groupées croisant une vingtaine de médias et plusieurs épisodes, illisibles.

### 4.5 Termes

#### Lien avec le changement climatique

- **Mention explicite** : `changement climatique`, `changements climatiques`, `dereglement climatique`, `rechauffement climatique`, `rechauffement de la planete`, `rechauffement planetaire`, `crise climatique`, `urgence climatique`, `climat se rechauffe`, `le climat change`
- **Via le dictionnaire OME** : thèmes `changement_climatique_constat`, `changement_climatique_causes`, `changement_climatique_consequences` (`themes` en audiovisuel, `predict_climat_*` de `factiva_articles` en presse). Variante hors HRFP en audiovisuel.

#### Co-aléa chaleur

`canicule`, `caniculaire`, `vague de chaleur`, `vagues de chaleur`, `forte chaleur`, `fortes chaleurs`, `chaleur extreme`, `chaleurs extremes`, `pic de chaleur`, `record de chaleur`, `temperatures records`, `35 degres`, `36 degres`, `37 degres`, `38 degres`, `39 degres`, `40 degres`, `plan canicule`

#### Co-aléa sécheresse

`secheresse`, `secheresses`, `deficit hydrique`, `sol sec`, `sols secs`, `sol tres sec`, `manque d eau`, `penurie d eau`, `nappe phreatique`, `nappes phreatiques`, `restriction d eau`, `restrictions d eau`, `assechement`, `asseche`, `assechee`, `vegetation seche`, `tres sec`

#### Co-aléa vent et météo

`le vent`, `les vents`, `du vent`, `rafale`, `rafales`, `bourrasque`, `direction du vent`, `vent du nord`, `vent du sud`, `vent fort`, `vents forts`, `vent violent`, `conditions meteo`, `meteo defavorable`, `meteo favorable`, `previsions meteo`

#### Aléas composés

Au moins deux des co-aléas ci-dessus.

#### Mise en série

`de plus en plus frequent`, `de plus en plus frequents`, `de plus en plus frequentes`, `de plus en plus intenses`, `plus frequents`, `record`, `jamais vu`, `sans precedent`, `nouvelle norme`

#### Voix scientifiques

`climatologue`, `climatologues`, `scientifique`, `scientifiques`, `chercheur`, `chercheuse`, `chercheurs`, `giec`, `meteo france`, `cnrs`, `universite`, `expert du climat`

#### Causes climatiques

Liste « mention explicite » de l'axe [Lien avec le changement climatique](#lien-avec-le-changement-climatique), et thème `changement_climatique_causes` du dictionnaire OME.

#### Solutions

- **Via le dictionnaire OME** : thèmes `attenuation_climatique_solutions`, `adaptation_climatique_solutions`, `biodiversite_solutions`, `ressources_solutions` (`changement_climatique_solutions` en presse). Pour séparer atténuation et adaptation : mots de chaque thème dans `public.dictionary`, moins les mots communs aux deux.
- **Atténuation** : `reduire les emissions`, `reduction des emissions`, `baisse des emissions`, `decarbonation`, `decarboner`, `neutralite carbone`, `zero emission nette`, `sortie des energies fossiles`, `sortie du charbon`, `energies fossiles`, `puits de carbone`, `budget carbone`, `objectifs climatiques`, `accord de paris`, `green deal`, `plan climat`, `pniec`, `snbc`

#### Santé

`qualite de l air`, `pollution de l air`, `particules fines`, `pm10`, `pm2`, `microparticules`, `masque`, `personnes vulnerables`, `personnes fragiles`, `problemes respiratoires`, `asthme`, `rester chez soi`, `rester a l interieur`, `consigne sanitaire`, `consignes sanitaires`, `recommandation sanitaire`, `fermer les fenetres`, `atmo`, `prevair`

#### Forces de l'ordre et justice

`parquet`, `enquete judiciaire`, `ouvre une enquete`, `juge d instruction`, `police`, `auteur`, `poursuites`, `plainte`, `risque jusqu a`, `ans de prison`, `inculpe`

## 5. Références

Notre sujet est l'écologie. Une référence qui traite d'un autre sujet ne sert qu'à s'inspirer pour les axes et les représentations graphiques ; on n'en reprend ni la méthode de constitution du corpus, ni les chiffres à des fins de comparaison. Les axes et graphiques repérés sont reportés dans la banque (§4).

| Référence | Organisation | Date | Ce qu'on peut en reprendre |
|---|---|---|---|
| [Vague de chaleur de mai 2026 : couverture médiatique](https://quotaclimat.org/actualites/vague-chaleur-mai-2026-couverture-mediatique/) | QuotaClimat | 🔴 [date de publication] | Analyse de référence. Voir `2026-05-chaleur/`. |
| [Incendies 2026 : analyse du traitement médiatique](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/) | QuotaClimat | 22/09/2026 | Analyse de référence. Voir `2026-07-incendies/`. |
| [Bilan OMÉ, 1er semestre 2026](https://observatoiremediaecologie.fr/app/uploads/2026/09/BILAN_OME_S1_2026.pdf) | OMÉ | 09/2026 | Axes (chaîne causale, conséquences par secteur, champ lexical, solutions individuelles ou systémiques, cadrages concurrents, politisation, couplage avec une autre actualité, reflux), graphiques G1 à G10 et G12, contrôles provisoires du §3.6. Le dédoublonnage des reprises de presse (titre normalisé, date, empreinte du contenu) y est décrit, non retenu pour l'instant. |
| [20h de France 2 : une télé de préfecture](https://www.acrimed.org/20h-de-France-2-Une-tele-de-prefecture) | Acrimed (hors écologie) | 21/10/2026 | Communiqués syndicaux critiquant un JT, sans protocole de mesure : inspiration seulement. Axes : spectacle ou explication, types d'intervenants, cadrage lexical, hiérarchie dans le JT. |

## 6. Modèle pour une analyse

À copier dans `<AAAA-MM>-<sujet>/decisions.md`.

```markdown
# <Sujet> <période>

- Statut : en cours | publié (<lien article ou bilan>)
- Entrée de `thematic_jobs_config.yml` : `<thème>`
- Tables : `thematic_extracts.<thème>_articles_<pays>`, `thematic_extracts.<thème>_keywords_<pays>`
- Collection Metabase : `03. Analyses thématiques/NN. <Sujet> <Année>`
- Document de résultats (avant publication) : <lien document Metabase>

## 1. Méthode spécifique

Seulement ce qui s'écarte de la méthode commune. Sinon : « Méthode commune, sans écart. »

- Période d'analyse : `<id>` dans le YAML, du ... au ... (justification, source datée)
- Période de comparaison : `<id>` dans le YAML, du ... au ... (justification)
- Contrôles : complétude, précision, rappel (résultat qualitatif et date ; aucun chiffre avant publication)

## 2. Mots-clés

- Entrée `<thème>` de `thematic_jobs_config.yml`, créée ou modifiée le ..., correspondance avec le dictionnaire OME vérifiée par ...
- Termes discutés (bruit, exclusions) et décision :

## 3. Axes contextuels

Chaque axe de la banque (méthode commune §4) est soit listé ici, soit écarté avec sa raison.

| Axe | Origine | Termes | Graphique | Question Metabase (id) | Résultat |
|---|---|---|---|---|---|
| Lien avec le changement climatique | banque | [banque](../decisions.md#lien-avec-le-changement-climatique) | G7 | | testé |
| <axe spécifique> | spécifique | ci-dessous | | | |

Résultat : publié, testé (jusqu'à publication), sans résultat (après publication, avec la raison).

**Axes de la banque écartés** : <axe> (raison), ...

**Termes des axes spécifiques**

- **<axe spécifique>** : `terme`, `terme`

## 4. Résultats

Après publication uniquement (le repo est public) : takeaways publiés avec chiffre, dénominateur et période ; limites ; axes à remonter dans la banque.
```
