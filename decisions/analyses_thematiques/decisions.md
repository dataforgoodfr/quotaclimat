# Analyses thématiques : méthode commune

Analyses ponctuelles de l'Observatoire des Médias sur l'Écologie (OMÉ) sur un sujet donné : incendies, vagues de chaleur, inondations, etc. Ce fichier décrit la méthode commune à toutes les analyses, la banque d'axes et les références. Chaque analyse a ensuite son propre `<AAAA-MM>-<sujet>/decisions.md`, qui ne contient que ce qui lui est propre.

Les analyses incendies et vague de chaleur 2026 servent de modèle pour la démarche, les axes et la restitution. Leur corpus noyau a été construit avant la révision de la méthode, directement sur `keywords` et `factiva_articles`. Leurs résultats restent réutilisables et comparables à ceux des analyses suivantes.

Statuts des règles : **adoptée** (s'applique), **provisoire** (s'applique, à valider).

## 1. Démarche

Cinq étapes, de 0 à 4, chacune validée explicitement par l'équipe avant de passer à la suivante.

| Étape | Ce qu'on produit | Où on le consigne |
|---|---|---|
| 0. Cadrage | Mots du dictionnaire OME retenus (liste stricte audiovisuel, regex presse), événements recensés, périodes candidates | `decisions.md` de l'analyse, §1 et §2 |
| 1. Extraction | Entrée de `thematic_jobs_config.yml` (mots-clés, périodes), tables thématiques générées, contrôles | `decisions.md` de l'analyse, §1 et §2 |
| 2. Axes | Axes de la banque appliqués ou non exploités, axes spécifiques, termes, graphiques prévus | `decisions.md` de l'analyse, §3 |
| 3. Requêtes | Questions Metabase sur les tables thématiques | Colonne « Question Metabase » du §3 |
| 4. Résultats | Takeaways triés (publiable, à creuser, rien à signaler), encadré méthodologique | Document Metabase avant publication, `decisions.md` de l'analyse §4 après publication |

**Étape 0, cadrage.** Elle précède l'entrée du YAML et se fait donc sur les tables sources (`public.keywords`, `public.factiva_articles`). Ses trois sous-étapes se valident une par une, comme les étapes : on ne propose pas la suivante (événements, courbe, périodes) tant que la précédente n'est pas validée par l'équipe.

1. **Dictionnaire noyau.** Recenser, dans le dictionnaire OME du repo (`quotaclimat/data_processing/mediatree/keyword/keyword.py`, généré depuis l'Excel par `transform_excel_to_json.py`, une entrée par mot avec sa `language`, son drapeau `high_risk_of_false_positive` et ses thèmes), tous les mots qui touchent au sujet, directs (HRFP) ou indirects. On produit deux listes : la liste stricte pour l'audiovisuel, sous la forme exacte du dictionnaire, et les regex pour la presse, avec les variantes de genre et de nombre, en forme normalisée (§3.5). Les mots écartés sont notés avec leur raison. Règles : §3.1.
2. **Recherche des événements.** Lister sur internet les événements marquants de la période envisagée (épisodes d'incendie, d'inondation, de chaleur, décisions politiques, procès...), un événement par ligne :

   | Début | Fin | Lieu (ville, département, région) | Impact | Source datée |
   |---|---|---|---|---|

   L'impact dépend de l'aléa : hectares brûlés, personnes évacuées ou déplacées, victimes, dégâts matériels ou coût assuré, vigilance orange ou rouge, arrêtés de catastrophe naturelle, pic de pollution... Il sert à comparer les épisodes entre eux et à tester si le traitement médiatique est proportionné aux conséquences de l'événement ou relève d'un choix éditorial (piste X3).
3. **Courbe des occurrences par jour.** La recherche sur internet laisse passer des événements. On trace donc le nombre d'occurrences par jour des mots du dictionnaire noyau sur une fenêtre large (plusieurs mois à deux ans), en audiovisuel (occurrences par heure de grille) et en presse (articles par jour). Chaque pic doit être rattaché à un événement de la liste ; un pic sans événement signale un oubli : on cherche ce qui s'est passé ce jour-là et on complète la liste. Modèles : §7.1 (audiovisuel) et §7.2 (presse).

La recherche donne les événements marquants, susceptibles d'être cités dans la publication ; les pics servent de tamis pour ce qu'elle a manqué. La liste fixe ensuite les périodes d'analyse et de comparaison (§3.2) déclarées dans le YAML.

**Le repo est public** : aucun chiffre ni takeaway non publié n'y entre. Avant publication, les résultats vivent dans un document Metabase `Résultats - <Sujet> <Année>` de la collection de l'analyse. Le statut d'un axe reste « testé » jusqu'à la publication.

**Conventions Metabase.** Collection `03. Analyses thématiques/NN. <Sujet> <Année>`. Préfixe `AV -` pour l'audiovisuel, `PE -` pour la presse. Après le préfixe, le code de l'axe ([§4.1](#41-catégories-et-codes)) : `AV - V1 - Volume`, `PE - C1 - Lien CC`. Versions abandonnées préfixées `OLD-`. On ne modifie jamais une question d'une autre analyse : on la duplique. Dans le SQL, `<sujet>` (en-tête) et `<thematique>` (commentaires, noms de CTE et de colonnes) se remplacent par le thème : `Courbe journalière inondations`, `dictionnaire_inondations`, `nb_articles_inondations`. On n'emploie pas « noyau » dans les requêtes. Après publication, le SQL des questions utilisées est copié dans `<AAAA-MM>-<sujet>/queries/`.

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

Profondeur des données : rien avant le 1er janvier 2023 en audiovisuel (`keywords`), rien avant le 1er janvier 2025 en presse (`factiva_articles`). Les articles de presse n'entrent en base qu'une semaine après leur parution : pas d'analyse presse au jour le jour, la dernière semaine est toujours incomplète. `time_monitored` ne remonte pas avant 2025 : en deçà, les heures se prennent dans la grille (`program_metadata`).

Volume du corpus audiovisuel : toujours filtrer sur `thematic_chunk = true`. Les segments adjacents ne servent qu'à la recontextualisation.

Corpus presse : à partir de l'étape 2 (axes), toujours filtrer sur `word_count >= 100` (articles de moins de 100 mots exclus). La courbe journalière de l'étape 0 n'applique pas ce filtre : elle mesure l'agitation des médias sur tous les articles, comme son dénominateur `stats_factiva_articles`. Le job garde les articles courts dans les tables thématiques, pour pouvoir y revenir.

Classification des titres de presse (nationale, régionale...) : table `source_classification` (`media_all`, `source_type`), à joindre sur `source_code`.

## 3. Méthode

### 3.1 Corpus noyau (adoptée)

- Le corpus noyau d'un thème est construit uniquement à partir de mots du dictionnaire OME, directs (HRFP) ou indirects.
- Un terme n'entre dans le dictionnaire noyau que s'il désigne le sujet sans ambiguïté : `crue centennale` oui, `piégé par les eaux` non.
- On n'écarte jamais un terme parce qu'il est HRFP : il est dans le dictionnaire OME parce que nous voulons qu'il y soit. Ses faux positifs se mesurent au contrôle de précision (§3.6).
- On garde toujours l'écriture du dictionnaire OME, même si elle paraît fautive ou incomplète (`augmentation du risque inondation`, sans « d' ») : elle peut reprendre un terme administratif. En presse, les variantes de genre et de nombre partent de cette écriture.
- La liste de chaque thème est fixe : un thème récurrent (incendies, inondations...) utilise toujours la même entrée de `thematic_jobs_config.yml`.
- En presse, une expression qui en contient une autre (« gestion des inondations » et « inondations ») n'ajoute aucun article au corpus. On la garde si elle est non-HRFP : `keywords_found` l'affiche alors en priorité et indique par quel terme l'article est entré dans `factiva_articles`.
- La correspondance entre `thematic_jobs_config.yml` et le dictionnaire OME est vérifiée à la main à la création ou à la modification d'une entrée.
- Le corpus noyau ne contient pas de mot absent du dictionnaire OME. Si le besoin se présente, se demander s'il devrait entrer dans le dictionnaire OME.
- On ne filtre pas le corpus sur les contenus validés « environnement » par l'OME, pour éviter la circularité : mesurer la recontextualisation environnementale sur des contenus déjà sélectionnés pour leur caractère environnemental.

### 3.2 Périodes (adoptée)

- **Période d'analyse** : une fenêtre d'actualité chaude sur le sujet, justifiée par une source datée (Météo-France, préfecture, presse), renseignée dans `periodes.<pays>`. C'est elle qui limite les faux positifs d'un dictionnaire noyau large.
- **Période de comparaison** : un épisode similaire (même aléa, actualité comparable), de durée équivalente ou normalisée, renseigné comme une autre période de la même entrée. Les requêtes comparent les périodes via la colonne `period`.

### 3.3 Axes (adoptée)

- Chaque axe a son propre dictionnaire de termes explicites. Ces termes n'ont pas à appartenir au dictionnaire OME, puisqu'un axe peut porter sur l'économie, les transports, la santé...
- Les axes qui ne dépendent pas du sujet sont dans la [banque d'axes génériques](#4-banque-daxes-génériques). Chaque analyse liste dans son §3 tous les axes appliqués : pour un axe de la banque, un renvoi et les éventuels ajouts ou retraits de termes ; pour un axe spécifique, ses termes. Les axes de la banque non exploités sont listés avec leur raison.
- Un axe entre dans la banque (catégories V et C) s'il remplit trois conditions :
  1. sa question se transpose à au moins deux aléas (incendies, inondations, chaleur...) sans changer de sens ; les termes peuvent varier, l'analyse note ses ajouts et retraits dans son §3 ;
  2. il a une trace : une analyse qui l'a testé, ou une référence datée (§5) pour une idée ;
  3. ses termes à risque de faux positifs sont signalés.
- Un axe qui ne remplit pas la condition 1 reste dans le `decisions.md` de son analyse ; une idée sans trace va en X (pistes exploratoires).
- Le recensement des axes non exploités commence avec les analyses menées après la révision ; les analyses antérieures (incendies et chaleur 2026) ne le contiennent pas.
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

**Frugalité (adoptée).** Un contrôle ne scanne jamais toute une table et ne fait pas relire tout un corpus par un LLM : 30 contenus au plus par contrôle, tirés au hasard avec `TABLESAMPLE SYSTEM (1) ... LIMIT 30` (un `ORDER BY random()` lirait toute la table). Pas de contrôle de complétude : les scripts du job vérifient déjà ce qu'ils ingèrent.

- **Précision (provisoire)** : sur un échantillon de 30 contenus au plus par terme fréquent, relire et compter les faux positifs à l'aide de `keywords_found`. Un terme trop bruité est discuté (exclusion ou contrainte) et le choix noté dans le §2 de l'analyse.
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

Statuts : **publié** (a donné un résultat publié), **présenté** (figure dans un livrable non public, par exemple pour un partenaire), **testé** (requête faite, résultat non publié ou nul), **idée** (jamais testé).

🔴 [termes à risque de faux positifs, à contrôler avant réutilisation : `record`, `universite`, `le vent`, `du vent`, `police`, `auteur`, `masque`, `atmo`]

### 4.1 Catégories et codes

Les axes se rangent en trois catégories, dans l'ordre où un article les présente généralement.

| Préfixe | Catégorie | Question type | Traitement |
|---|---|---|---|
| V | Volumétrie | Combien de données a-t-on ? Nombre d'articles, d'occurrences, de segments, et leur timeline. Sert surtout à savoir s'il faudra agréger (effectif minimal) ; la timeline sert aussi à fixer les périodes et à comparer l'agitation médiatique d'une thématique à l'autre | Quelques requêtes à chaque analyse ; l'article commence en général par la timeline |
| C | Chaîne causale | Quelle part de couverture des enjeux environnementaux, et que dit-on du constat, des causes, des conséquences, des solutions ? Chaque axe se range dans un maillon : les personnes vulnérables relèvent des conséquences, les pompiers des solutions | Le cœur de l'analyse |
| X | Inclassables | Angles qui n'entrent dans aucun maillon (Unes, intervenants, cadrages...) | Pas systématique ; réservoir d'idées pour sortir des analyses répétitives, à enrichir à chaque analyse |

**Codage.**

- V : `V` + numéro (`V1`, `V2`).
- C : le chiffre est le maillon (`C0` couverture globale, `C1` constat, `C2` causes, `C3` conséquences, `C4` solutions), la lettre le sous-axe (`C3b` santé). Le maillon se lit dans le code.
- X : `X` + numéro.
- Un croisement s'écrit avec les deux codes (`C1 x C2`). Les questions Metabase reprennent le code : `AV - C1 - Lien CC`.
- Un axe propre à une analyse prend la lettre suivante de son maillon dans le `decisions.md` de l'analyse, sans l'ajouter à la banque.
- Les analyses antérieures (incendies, chaleur et surchauffe 2026) gardent leurs anciens codes (Q1, A1, B1...).

Ventilations habituelles, pour toutes les catégories : chaîne ou titre, public / privé, presse nationale / régionale, TV / radio.

### 4.2 Axes

Statuts : voir plus haut. « Explorée dans » cite les analyses ; « idée » renvoie à la référence qui l'a inspirée (§5).

**V. Volumétrie**

| Code | Ancien code | Axe | Question posée | Graphique | Explorée dans |
|---|---|---|---|---|---|
| V1 | Q1 (en partie) | Volume | Combien d'articles, d'occurrences, de segments, par média ? Faudra-t-il agréger ? | Classement des médias en barres (G7) | Incendies 2026, Chaleur 2026 |
| V2 | Q7 | Timeline | Comment le volume évolue-t-il jour par jour ? Occurrences par heure de grille sur une fenêtre large (voir l'étape 0). Sert à fixer les périodes, à mesurer l'agitation des médias sur le sujet et à la comparer d'une thématique à l'autre (incendies face à inondations), pour formuler des hypothèses sur ce qui fait réagir les médias. Attention à la taille des dictionnaires noyau comparés | Courbe annotée d'événements (G2), courbe hebdomadaire (G1) | Incendies 2026 |
| V3 | | Mots les plus employés | Quels mots du dictionnaire noyau portent le volume, et lesquels restent peu utilisés ? Compter les occurrences dans le texte (`plaintext_normalized`, `title/snippet/body_normalized`, par exemple avec `regexp_matches(..., 'g')`), pas `keywords_found` : celui-ci ne donne que le nombre de segments ou d'articles où le mot apparaît, pas le nombre de fois où il est dit. Intéressant selon le contexte | Barres terme × famille de presse (G12) | Surchauffe 2026 |

**C. Chaîne causale**

Les axes de tête de chaque maillon (`C1` à `C4`) reprennent les thèmes du dictionnaire OME (`quotaclimat/data_processing/mediatree/detect_keywords.py`), ceux des tableaux de bord publics de l'OMÉ : en audiovisuel, colonne `themes` ; en presse, `predict_*` de `factiva_articles`. Ces thèmes couvrent climat, biodiversité et ressources. Les sous-axes (lettres) sont des listes de termes (§4.5).

| Code | Maillon | Axe | Thèmes du dictionnaire OME | Question posée | Graphique | Explorée dans |
|---|---|---|---|---|---|---|
| C0 | Global | Part de couverture, comparaison et répartition (ancien Q1 en partie, Q6) | tous les thèmes | Quelle part du temps d'antenne ou des articles porte sur les enjeux environnementaux, est-elle exceptionnelle par rapport au niveau habituel ou à un épisode similaire, et quel maillon domine ? Takeaway à en tirer : le reflux (ancien Q7b), la couverture retombe-t-elle sous son niveau habituel après le pic ? | Barres avant / pendant (G8), donut de répartition (G5) | Incendies 2026, Chaleur 2026 ; répartition : idée (Bilan OMÉ S1 2026) |
| C1 | Constat | [Lien avec le changement climatique](#lien-avec-le-changement-climatique) | `changement_climatique_constat` | Les médias relient-ils l'événement au changement climatique ? | G7, G3, G4 | Incendies 2026, Chaleur 2026 |
| C1a | Constat | [Mise en série](#mise-en-série) | | L'événement est-il présenté comme une tendance (« de plus en plus fréquent ») ? | G7 | Incendies 2026 |
| C1b | Constat | Intervenants : qui on appelle pour en parler ([voix scientifiques](#voix-scientifiques) et autres) | | Qui est mentionné pour expliquer : scientifiques, élus, secours, sinistrés, associations, monde agricole ? Limite : on détecte la mention d'un type d'intervenant, pas qui parle | G7, G11 | Incendies 2026 (voix scientifiques) ; autres intervenants : idée (Acrimed) |
| C2 | Causes | [Causes climatiques](#causes-climatiques) | `changement_climatique_causes` | Le changement climatique est-il présenté comme une cause ? | G4, croisement `C1 x C2` | Incendies 2026, Chaleur 2026 |
| C2a | Causes | [Co-aléa chaleur](#co-aléa-chaleur) | | L'événement est-il relié à la chaleur ? | G7 | Incendies 2026 |
| C2b | Causes | [Co-aléa sécheresse](#co-aléa-sécheresse) | | L'événement est-il relié à la sécheresse ? | G7 | Incendies 2026 |
| C2c | Causes | [Co-aléa vent et météo](#co-aléa-vent-et-météo) | | L'événement est-il relié aux conditions météo ? | G7 | Incendies 2026 |
| C2d | Causes | [Aléas composés](#aléas-composés) | | L'événement est-il présenté comme la combinaison de plusieurs aléas ? | G10 | Incendies 2026 |
| C2e | Causes | [Forces de l'ordre et justice](#forces-de-lordre-et-justice) | | Les causes humaines directes (origine criminelle, enquête) dominent-elles, le sujet est-il traité comme un fait divers ? | G7 | Incendies 2026 |
| C3 | Conséquences | Conséquences | `changement_climatique_consequences` | Les conséquences sont-elles évoquées ? | G7 | Chaleur 2026 (définit le corpus) |
| C3a | Conséquences | Par secteur | | De quelles conséquences de l'événement parle-t-on le plus : transports, économie, assurance, écoles, travail, agriculture, alimentation... (secteurs à choisir selon le sujet) ? | G6 | idée (Bilan OMÉ S1 2026) |
| C3b | Conséquences | [Santé](#santé) | | Les effets sanitaires sont-ils évoqués ? | G7 | Incendies 2026, Surchauffe 2026 |
| C3c | Conséquences | Précarité du ménage | | Les ménages précaires ou modestes sont-ils évoqués ? Termes larges (`populaire`, `fragile`) : à contrôler | G7 | Surchauffe 2026 |
| C3d | Conséquences | Personnes vulnérables | | Les personnes âgées, les enfants, les nourrissons sont-ils évoqués ? Termes larges (`enfant`) : à contrôler | G7 | Surchauffe 2026 |
| C4 | Solutions | [Solutions](#solutions) | `attenuation_climatique_solutions`, `adaptation_climatique_solutions` | Quelles solutions sont présentées : atténuation, adaptation ? | G11 | Incendies 2026, Chaleur 2026 |
| C4a | Solutions | Individuelles ou systémiques | | Les solutions relèvent-elles du geste individuel et du court terme, ou de l'action collective et structurelle ? Généralise l'axe chaleur « adaptation palliative ou structurelle » | G11, G3 | Chaleur 2026 (adaptation) ; idée en général (Bilan OMÉ S1 2026) |
| C4b | Solutions | Curatif ou préventif | | La couverture porte-t-elle sur la lutte et les secours (pompiers, moyens aériens) ou sur la prévention ? | G11 | Incendies 2026 |

La désinformation n'entre pas dans ces analyses : elle fait l'objet d'analyses annexes, avec leur propre méthode.

**X. Inclassables**

| Code | Axe | Question posée | Graphique | Explorée dans |
|---|---|---|---|---|
| X1 | Unes | Le sujet fait-il la Une ? Présence en Une, par titre | G10 | Chaleur 2026 |
| X2 | Hiérarchie dans le JT | Le sujet ouvre-t-il le journal, à quelle position arrive-t-il ? 🔴 [vérifier si la grille des programmes permet de situer un segment dans l'émission] | | idée (Acrimed) |
| X3 | Par événement et proportionnalité (ancien Q8) | Quels événements sont couverts, et la couverture est-elle proportionnée à leur impact (liste de l'étape 0) ? Piste : un événement régional est-il couvert par la presse régionale d'autres régions (inondations) ? | Barres horizontales avec n (G10) | idée (jamais vraiment mis en œuvre) |
| X4 | Cadrages concurrents | Quel angle domine : pouvoir d'achat, souveraineté, géopolitique, industrie et emploi, santé, « écologie punitive » ? Codage booléen non exclusif, un dictionnaire par cadrage | G9 | idée (Bilan OMÉ S1 2026) |
| X5 | Politisation | Le sujet est-il présenté comme l'affaire d'un camp politique ? Demande une lecture humaine ; un codage par LLM impose les précautions de la charte IA | G10 | idée (Bilan OMÉ S1 2026) |
| X6 | Couplage avec une autre actualité | Quelle part du corpus relie le sujet à un autre événement du moment (élection, crise énergétique...) ? | G7 | idée (Bilan OMÉ S1 2026) |

### 4.3 Croisements

Un croisement met deux variables face à face, en général sous forme de nuage de points (G13). Exemple : un point = un média, x = part des séquences du corpus qui font le lien avec le changement climatique (C1), y = part qui évoque ses causes (C2). Chaque part est calculée séparément sur le corpus (certains contenus relèvent des deux axes, pas tous) : on ne restreint pas une variable à l'autre. Les médias en haut à droite font le lien et l'expliquent.

- **Deux variables**, pas plus, sauf idée qui le justifie clairement : au-delà, le résultat devient illisible.
- **Les meilleurs croisements découpent le nuage en quatre groupes**, pas seulement en bons et mauvais élèves, et donnent quelque chose à dire sur chaque groupe.
- **On ne s'interdit aucun croisement** : on produit les données de tous les axes en amont, puis on essaie les croisements et on garde ceux qui parlent.

| Croisement | Question posée | Graphique | Explorée dans |
|---|---|---|---|
| `C1 x C2`, par média | Faire le lien, est-ce expliquer ? | G13 | Incendies 2026, Chaleur 2026 |
| Vocabulaire climat sans lien nommé | Le vocabulaire climat du dictionnaire est-il présent sans que le lien avec le changement climatique soit nommé ? (lien CC via le dictionnaire et non mention explicite) | G10 | Incendies 2026 |
| Volume x recontextualisation, par média | Ceux qui en parlent le plus recontextualisent-ils le plus ? | G13 | idée |
| Longueur x lien CC | Les articles longs recontextualisent-ils davantage ? | Barres par tranche de longueur | Incendies 2026 |
| Évolution du lien CC dans l'épisode | Le climat arrive-t-il au pic ou après ? | G2 | idée |
| Comparaison entre épisodes | Un aléa est-il mieux recontextualisé qu'un autre ? | G3, G4 | Incendies 2026 vs Chaleur 2026 |
| Spectacle ou explication | La couverture porte-t-elle sur l'opérationnel (images, dégâts, secours) plutôt que sur les causes, les enjeux et les solutions ? | G11 | idée (Acrimed) |

### 4.4 Représentations graphiques

Les codes G sont une référence pour Claude. Dans un échange ou une proposition, un graphique est toujours nommé en clair à côté de son code (« G13, nuage de points, un point par média »).

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
| G13 | Nuage de points, un point par média | Relation entre deux mesures | Petits effectifs : afficher n ou la taille du point | Chaleur 2026 (Metabase) |
| G14 | Courbe des mentions face à un indicateur physique (ITN, hectares brûlés, niveau des crues...), sur un double axe | Si la couverture suit l'intensité de l'aléa (X3) | Double axe : ne se lit qu'en tendance | Surchauffe 2026 |

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
| [Vague de chaleur de mai 2026 : couverture médiatique](https://quotaclimat.org/actualites/vague-chaleur-mai-2026-couverture-mediatique/) | QuotaClimat | non indiquée | Analyse de référence. Voir `2026-05-chaleur/`. |
| [Incendies 2026 : analyse du traitement médiatique](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/) | QuotaClimat | 22/09/2026 | Analyse de référence. Voir `2026-07-incendies/`. |
| Surchauffe des logements, support présenté à l'ONPE (non publié) | QuotaClimat, OMÉ | 21/09/2026 | Analyse de référence. Voir `2026-06-surchauffe-logement/`. Courbe des mentions face à un indicateur physique (ITN), comparaison avec un sujet concurrent (climatisation), axes précarité et personnes vulnérables. |
| [Bilan OMÉ, 1er semestre 2026](https://observatoiremediaecologie.fr/app/uploads/2026/09/BILAN_OME_S1_2026.pdf) | OMÉ | 09/2026 | Axes (chaîne causale, conséquences par secteur, solutions individuelles ou systémiques, cadrages concurrents, politisation, couplage avec une autre actualité, reflux), graphiques G1 à G10 et G12, contrôles provisoires du §3.6. Le dédoublonnage des reprises de presse (titre normalisé, date, empreinte du contenu) y est décrit, non retenu pour l'instant. |
| [20h de France 2 : une télé de préfecture](https://www.acrimed.org/20h-de-France-2-Une-tele-de-prefecture) | Acrimed (hors écologie) | 21/10/2026 | Communiqués syndicaux critiquant un JT, sans protocole de mesure : inspiration seulement. Axes : spectacle ou explication, types d'intervenants, hiérarchie dans le JT. |

## 6. Modèle pour une analyse

À copier dans `<AAAA-MM>-<sujet>/decisions.md`.

```markdown
# <Sujet> <période>

- Responsable : <prénom>
- Statut : en cours | publié (<lien article ou bilan>)
- Entrée de `thematic_jobs_config.yml` : `<thème>`
- Tables : `thematic_extracts.<thème>_articles_<pays>`, `thematic_extracts.<thème>_keywords_<pays>`
- Collection Metabase : `03. Analyses thématiques/NN. <Sujet> <Année>`
- Document de résultats (avant publication) : <lien document Metabase>

## 1. Méthode spécifique

Seulement ce qui s'écarte de la méthode commune. Sinon : « Méthode commune, sans écart. »

- Événements recensés (étape 0) : tableau début, fin, lieu, impact, source datée ; pics de la courbe d'occurrences rattachés ou ajoutés
- Période d'analyse : `<id>` dans le YAML, du ... au ... (justification, source datée)
- Période de comparaison : `<id>` dans le YAML, du ... au ... (justification)
- Contrôles : précision, rappel, sur 30 contenus au plus (résultat qualitatif et date ; aucun chiffre avant publication)

## 2. Mots-clés

- Entrée `<thème>` de `thematic_jobs_config.yml`, créée ou modifiée le ..., correspondance avec le dictionnaire OME vérifiée le ...
- Termes discutés (bruit, exclusions) et décision :

## 3. Axes

Chaque axe des catégories V et C de la banque (méthode commune §4) est soit listé ici, soit noté comme non exploité avec sa raison. X : seulement s'ils sont traités.

| Code | Axe | Origine | Termes | Graphique | Question Metabase (id) | Résultat |
|---|---|---|---|---|---|---|
| C1 | Lien avec le changement climatique | banque | [banque](../decisions.md#lien-avec-le-changement-climatique) | G7 | | testé |
| C3e | <axe spécifique, rangé dans son maillon> | spécifique | ci-dessous | | | |

Résultat : publié, testé (jusqu'à publication), sans résultat (après publication, avec la raison).

**Axes de la banque non exploités** : <axe> (raison), ...

**Termes des axes spécifiques**

- **<axe spécifique>** : `terme`, `terme`

## 4. Résultats

Après publication uniquement (le repo est public) : takeaways publiés avec chiffre, dénominateur et période ; limites ; axes à remonter dans la banque.
```

## 7. Modèles de requêtes

**Frugalité (adoptée).** Une requête part du modèle de ce paragraphe et des regex existantes (§3.3, §4.5) : on remplace les bornes, la période, le dictionnaire et les termes, sans relire les questions Metabase d'une analyse précédente. On ne lit une question Metabase que si elle est citée par son numéro, ou pour mettre à jour un modèle. Après l'étape 1, les requêtes portent sur les tables thématiques, pas sur `keywords` et `factiva_articles`. Les contrôles ont leur propre règle (§3.6). Un modèle amélioré au cours d'une analyse remplace celui d'ici.

Dans les modèles, `<sujet>` (en-tête), `<thematique>` (commentaires, noms de CTE et de colonnes), `<axe>` (code et nom court de l'axe, par exemple `c1_lien_cc`) et `<periode>` (`id` de la période dans le YAML) se remplacent avant exécution (conventions Metabase, §1).

| Modèle | Étape | Média | Unité | Tables |
|---|---|---|---|---|
| [7.1 Courbe journalière](#71-courbe-journalière-audiovisuel) | 0.3 | audiovisuel | occurrence, par heure de grille | `keywords`, `program_metadata` |
| [7.2 Courbe journalière](#72-courbe-journalière-presse) | 0.3 | presse | article, part des articles du jour | `factiva_articles`, `stats_factiva_articles` |
| [7.3 Axe par chaîne](#73-axe-par-chaîne-audiovisuel) | 2 et 3 | audiovisuel | segment thématique, fenêtre de ±2 minutes | `thematic_extracts.<thematique>_keywords_fr` |
| [7.4 Axe par titre](#74-axe-par-titre-presse) | 2 et 3 | presse | article d'au moins 100 mots | `thematic_extracts.<thematique>_articles_fr`, `source_classification` |

### 7.1 Courbe journalière, audiovisuel

Base Baromètre. Compte les mots du dictionnaire noyau détectés dans `keywords_with_timestamp`, comme le job, et non une regex sur le texte. `keywords.start` est en heure de Paris. Le dénominateur est la grille en vigueur chaque jour (`program_metadata`), seule source d'heures disponible avant 2025 (`time_monitored` ne commence qu'en 2025).

```sql
-- AV - V2 - Courbe journalière <sujet>, toutes chaînes confondues
--   Mesure : occurrences des mots du dictionnaire <thematique> par heure de grille
--   (un segment de 2 minutes peut contenir plusieurs occurrences). Maille : jour.
--   Tables : public.keywords (keywords_with_timestamp), public.program_metadata
WITH bornes(t0, t1) AS (
    VALUES (date '2024-01-01', date '2026-10-26')
),
jours AS (
    SELECT d::date AS jour
    FROM bornes b, generate_series(b.t0, b.t1 - 1, interval '1 day') AS d
),
-- entrée <thematique> > keywords > french > audiovisuel de thematic_jobs_config.yml
dictionnaire_<thematique>(mot) AS (
    VALUES ('mot 1'), ('mot 2')
),
occurrences AS (
    SELECT
        k.start::date        AS jour,
        COUNT(*)             AS nb_occurrences,  -- une ligne par mot détecté
        COUNT(DISTINCT k.id) AS nb_segments      -- segments de 2 minutes concernés
    FROM public.keywords k
    CROSS JOIN bornes b
    CROSS JOIN LATERAL json_array_elements(k.keywords_with_timestamp) AS kwt(e)
    WHERE k.country = 'france'
      AND k.start >= b.t0 AND k.start < b.t1
      AND lower(kwt.e ->> 'keyword') IN (SELECT mot FROM dictionnaire_<thematique>)
    GROUP BY 1
),
heures AS (
    -- grille en vigueur le jour donné, toutes chaînes
    SELECT j.jour, SUM(pm.duration_minutes) / 60.0 AS nb_heures_grille
    FROM jours j
    JOIN public.program_metadata pm
      ON pm.country = 'france'
     AND pm.weekday = EXTRACT(ISODOW FROM j.jour)::int
     AND j.jour BETWEEN CAST(pm.program_grid_start AS date) AND CAST(pm.program_grid_end AS date)
    GROUP BY j.jour
)
SELECT
    h.jour,
    COALESCE(o.nb_occurrences, 0) AS nb_occurrences,
    COALESCE(o.nb_segments, 0)    AS nb_segments,
    h.nb_heures_grille,
    COALESCE(o.nb_occurrences, 0) / NULLIF(h.nb_heures_grille, 0) AS occ_par_heure
FROM heures h
LEFT JOIN occurrences o USING (jour)
ORDER BY h.jour;
```

Affichage : courbe, `jour` en x, `occ_par_heure` en y, annotée des événements (G2).

### 7.2 Courbe journalière, presse

Base Print Media DB. À l'étape 0, le job n'a pas encore tourné : la requête normalise elle-même le texte et les mots (§3.5) ; après l'extraction, les tables thématiques fournissent les colonnes `*_normalized` et les requêtes n'ont plus à normaliser. Le dénominateur est `stats_factiva_articles` (nombre d'articles par titre et par jour). Pas de filtre `word_count` ici : la courbe mesure l'agitation des médias sur tous les articles, comme son dénominateur.

```sql
-- PE - V2 - Courbe journalière <sujet>, presse
--   Mesure : articles dont le titre, le chapô ou le corps contient un mot du dictionnaire
--   <thematique>, rapportés au nombre total d'articles du jour. Maille : jour (heure de Paris).
--   Tables : public.factiva_articles (hors DUP), public.stats_factiva_articles
WITH bornes(t0, t1) AS (
    VALUES (date '2025-01-01', date '2026-10-19')  -- t1 une semaine avant la date du jour (§2)
),
-- entrée <thematique> > keywords > french > presse de thematic_jobs_config.yml, copiée telle quelle
dictionnaire_<thematique>(mot) AS (
    VALUES ('mot 1'), ('mots 1')
),
-- même regex que le job : expressions normalisées, mots entiers
motif AS (
    SELECT '\y(' || string_agg(
               regexp_replace(
                   translate(replace(replace(lower(mot), 'œ', 'oe'), 'æ', 'ae'),
                             'àâäéèêëîïôöùûüÿç', 'aaaeeeeiioouuuyc'),
                   '[''’‘‛ʼʹ´`′＇-]', ' ', 'g'),
               '|' ORDER BY length(mot) DESC) || ')\y' AS regex
    FROM dictionnaire_<thematique>
),
<thematique> AS (
    SELECT
        (a.publication_datetime AT TIME ZONE 'Europe/Paris')::date AS jour,
        COUNT(*) AS nb_articles_<thematique>
    FROM public.factiva_articles a
    CROSS JOIN bornes b
    CROSS JOIN motif m
    WHERE a.publication_datetime >= b.t0::timestamp AT TIME ZONE 'Europe/Paris'
      AND a.publication_datetime <  b.t1::timestamp AT TIME ZONE 'Europe/Paris'
      AND a.duplicate_status <> 'DUP'
      AND regexp_replace(
              translate(replace(replace(lower(concat_ws(' ', a.snippet, a.title, a.body)), 'œ', 'oe'), 'æ', 'ae'),
                        'àâäéèêëîïôöùûüÿç', 'aaaeeeeiioouuuyc'),
              '[''’‘‛ʼʹ´`′＇-]', ' ', 'g') ~ m.regex
    GROUP BY 1
),
total AS (
    -- une ligne par titre et par jour
    SELECT
        (s.publication_datetime AT TIME ZONE 'Europe/Paris')::date AS jour,
        SUM(s.count) AS nb_articles_total
    FROM public.stats_factiva_articles s
    CROSS JOIN bornes b
    WHERE s.publication_datetime >= b.t0::timestamp AT TIME ZONE 'Europe/Paris'
      AND s.publication_datetime <  b.t1::timestamp AT TIME ZONE 'Europe/Paris'
    GROUP BY 1
)
SELECT
    t.jour,
    COALESCE(n.nb_articles_<thematique>, 0) AS nb_articles_<thematique>,
    t.nb_articles_total,
    100.0 * COALESCE(n.nb_articles_<thematique>, 0) / NULLIF(t.nb_articles_total, 0) AS part_articles_<thematique>_pct
FROM total t
LEFT JOIN <thematique> n USING (jour)
ORDER BY t.jour;
```


### 7.3 Axe par chaîne, audiovisuel

Base Baromètre. Unité : segment thématique de 2 minutes (`thematic_chunk = true`). Un axe compte s'il apparaît dans le segment ou dans un segment de la même chaîne qui commence moins de 2 min 30 s avant ou après (§3.4) ; c'est l'écart que le job utilise pour retenir les segments adjacents. La colonne `_sans_fenetre` ne lit que le segment lui-même : un résultat publiable tient avec et sans la fenêtre (§3.8). Un axe défini par les thèmes du dictionnaire OME (têtes de maillon C1 à C4) teste la colonne `themes` au lieu d'une regex ; `themes` est vide pour un segment adjacent, la fenêtre ne s'applique donc pas à ces axes. Un axe de plus : une regex et deux colonnes de plus.

```sql
-- AV - <code> - <axe> <sujet>, par chaîne
--   Une ligne = une chaîne. Unité : segment thématique de 2 minutes.
--   L'axe compte s'il est dans le segment ou dans un segment de la même chaîne à ±2 min 30 s.
--   Table : thematic_extracts.<thematique>_keywords_fr
WITH segments AS (
    SELECT this_id, channel_name, channel_title, start, thematic_chunk, plaintext_normalized
    FROM thematic_extracts.<thematique>_keywords_fr
    WHERE period = '<periode>'
),
-- un drapeau par axe : regex sur le texte normalisé, mots entiers (§3.3, §3.5)
regex AS (
    SELECT '\y(terme 1|terme 2)\y' AS <axe>
),
par_segment AS (
    SELECT
        t.channel_title,
        t.this_id,
        bool_or(v.plaintext_normalized ~ r.<axe>)                           AS <axe>,
        bool_or(v.this_id = t.this_id AND v.plaintext_normalized ~ r.<axe>) AS <axe>_sans_fenetre
    FROM segments t
    JOIN segments v
      ON v.channel_name = t.channel_name
     AND v.start BETWEEN t.start - interval '2 minutes 30 seconds'
                     AND t.start + interval '2 minutes 30 seconds'
    CROSS JOIN regex r
    WHERE t.thematic_chunk
    GROUP BY t.channel_title, t.this_id
)
SELECT
    channel_title,
    COUNT(*)                                             AS nb_segments_<thematique>,
    COUNT(*) FILTER (WHERE <axe>)                        AS nb_segments_<axe>,
    100.0 * COUNT(*) FILTER (WHERE <axe>) / COUNT(*)     AS part_<axe>_pct,
    COUNT(*) FILTER (WHERE <axe>_sans_fenetre)           AS nb_segments_<axe>_sans_fenetre,
    100.0 * COUNT(*) FILTER (WHERE <axe>_sans_fenetre) / COUNT(*) AS part_<axe>_sans_fenetre_pct
FROM par_segment
GROUP BY channel_title
ORDER BY part_<axe>_pct DESC;
```

Affichage : classement des chaînes en barres (G7), `part_<axe>_pct` ; une chaîne sous 30 segments ne se lit qu'avec sa famille (§3.7).

### 7.4 Axe par titre, presse

Base Print Media DB. Unité : article d'au moins 100 mots (§2). Un axe compte s'il apparaît dans le titre, le chapô ou le corps (§3.4). Les colonnes `*_normalized` sont déjà normalisées par le job.

```sql
-- PE - <code> - <axe> <sujet>, par titre
--   Une ligne = un titre de presse. Unité : article d'au moins 100 mots.
--   L'axe compte s'il est dans le titre, le chapô ou le corps.
--   Tables : thematic_extracts.<thematique>_articles_fr, public.source_classification
WITH regex AS (
    SELECT '\y(terme 1|terme 2)\y' AS <axe>
),
articles AS (
    SELECT
        a.source_code,
        a.source_name,
        concat_ws(' ', a.title_normalized, a.snippet_normalized, a.body_normalized) ~ r.<axe> AS <axe>
    FROM thematic_extracts.<thematique>_articles_fr a
    CROSS JOIN regex r
    WHERE a.period = '<periode>'
      AND a.word_count >= 100
)
SELECT
    ar.source_name,
    sc.source_type,
    COUNT(*)                                         AS nb_articles_<thematique>,
    COUNT(*) FILTER (WHERE ar.<axe>)                 AS nb_articles_<axe>,
    100.0 * COUNT(*) FILTER (WHERE ar.<axe>) / COUNT(*) AS part_<axe>_pct
FROM articles ar
LEFT JOIN public.source_classification sc USING (source_code)
GROUP BY ar.source_name, sc.source_type
ORDER BY part_<axe>_pct DESC;
```

Affichage : classement des titres en barres colorées par famille (G7) ; un titre sous 30 articles ne se lit qu'avec sa famille (§3.7).
