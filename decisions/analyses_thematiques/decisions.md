# Analyses thématiques : méthode commune

Analyses ponctuelles de l'Observatoire des Médias sur l'Écologie (OMÉ) sur un sujet donné : incendies, vagues de chaleur, inondations, etc. Ce fichier décrit la méthode commune à toutes les analyses, la banque d'axes et les références. Chaque analyse a ensuite son propre `<AAAA-MM>-<sujet>/decisions.md`, qui ne contient que ce qui lui est propre.

Les analyses incendies et vague de chaleur 2026 servent de modèle pour la démarche, les axes et la restitution. Leur corpus noyau a été construit avant la révision de la méthode, directement sur `keywords` et `factiva_articles`. Leurs résultats restent réutilisables et comparables à ceux des analyses suivantes.

Statuts des règles : **adoptée** (s'applique), **provisoire** (s'applique, à valider).

## 1. Démarche

Cinq étapes, de 0 à 4, chacune validée explicitement par l'équipe avant de passer à la suivante. Une exception, pour gagner du temps : le job de l'étape 1 peut tourner longtemps, et l'on prépare les axes (étape 2) pendant qu'il tourne ; les requêtes et les contrôles qui lisent les tables thématiques attendent la fin du job.

**Étape 0, d'abord l'angle (adoptée).** On ne part pas bille en tête : l'étape 0 commence par l'angle. Claude crée le `decisions.md` de l'analyse à partir du [modèle](#6-modèle-pour-une-analyse), puis, avant le dictionnaire noyau, l'équipe écrit ce que l'analyse veut montrer, sous forme de quelques questions, dans la section « Angle » du `decisions.md` de l'analyse. Elles restent le fil de toutes les étapes, pour ne pas les perdre de vue : chaque axe de l'étape 2 se rattache à l'une d'elles, et les takeaways de l'étape 4 y répondent. Exemple, incendies 2026 :

- Parle-t-on du réchauffement climatique ?
- Parle-t-on davantage des solutions curatives que préventives ?
- Ne parle-t-on que des pyromanes, ou d'autres causes ?
- Parle-t-on des causes structurelles ou immédiates ?
- Quels feux de forêt ont le plus ému les médias ? Est-ce lié à la proximité avec Paris, ou à la gravité de la situation ?

L'angle s'écrit avec son contexte, en tête de la section « Angle » :

- **Destination** : rapport OMÉ (semestriel, annuel), article QuotaClimat, présentation à des partenaires (non publique)... avec sa date de parution prévue. Elle fixe ce que couvre l'analyse : un rapport annuel porte sur les principaux épisodes de l'année, un article peut ne porter que sur un épisode.
- **Plan de comparaison** : les périodes d'analyse, et à quoi chacune se compare (épisodes antérieurs en France, épisodes à l'étranger, même saison sans événement...). Une ligne par période du YAML avec son rôle. Ce plan fixe la liste complète des périodes avant la première entrée du YAML : chaque période ajoutée après coup demande une modification du YAML, une PR et un nouveau lancement du job.

| Étape | Ce qu'on produit | Où on le consigne |
|---|---|---|
| 0. Cadrage | Angle (destination, questions auxquelles l'analyse répond, plan de comparaison), mots du dictionnaire OME retenus (liste stricte audiovisuel, regex presse), événements recensés, courbes journalières (V1), périodes candidates | `decisions.md` de l'analyse, Angle, §1 et §2 |
| 1. Extraction | Entrée de `thematic_jobs_config.yml` (mots-clés, périodes), tables thématiques générées par le job | `decisions.md` de l'analyse, §1 et §2 |
| 2. Axes | Brainstorm (posts LinkedIn de QuotaClimat, analyses précédentes, références), axes de la banque appliqués ou non exploités, axes spécifiques, termes | `decisions.md` de l'analyse, §3 |
| 3. Requêtes | Toutes les questions Metabase : liste des périodes, volumétrie, A à D, T, contrôles (CTRL) | Colonne « Question Metabase » du §3 |
| 4. Résultats | Takeaways triés (publiable, à creuser, rien à signaler), encadré méthodologique | Google Sheets séparé avant publication, `decisions.md` de l'analyse §4 après publication |

**Étape 0, cadrage, après l'angle.** Elle précède l'entrée du YAML et se fait donc sur les tables sources (`public.keywords`, `public.factiva_articles`). Une fois l'angle validé, ses trois sous-étapes se valident une par une, comme les étapes : on ne propose pas la suivante (événements, courbe, périodes) tant que la précédente n'est pas validée par l'équipe.

1. **Dictionnaire noyau.** Recenser, dans le dictionnaire OME du repo (`quotaclimat/data_processing/mediatree/keyword/keyword.py`, généré depuis l'Excel par `transform_excel_to_json.py`, une entrée par mot avec sa `language`, son drapeau `high_risk_of_false_positive` et ses thèmes), tous les mots qui touchent au sujet, directs (HRFP) ou indirects. On produit deux listes : la liste stricte pour l'audiovisuel, sous la forme exacte du dictionnaire, et les regex pour la presse, avec les variantes de genre et de nombre. Les mots écartés sont notés avec leur raison. Règles : §3.1.
2. **Recherche des événements.** Lister sur internet les événements marquants de la période envisagée (épisodes d'incendie, d'inondation, de chaleur, décisions politiques, procès...), un événement par ligne :

   | Début | Fin | Lieu (ville, département, région) | Impact | Source datée |
   |---|---|---|---|---|

   L'impact dépend de l'aléa : hectares brûlés, personnes évacuées ou déplacées, victimes, dégâts matériels ou coût assuré, vigilance orange ou rouge, arrêtés de catastrophe naturelle, pic de pollution... Il sert à comparer les épisodes entre eux et à tester si le traitement médiatique est proportionné aux conséquences de l'événement ou relève d'un choix éditorial (axe T7).
3. **Courbe des occurrences par jour (axe V1).** La recherche sur internet laisse passer des événements. On trace donc le nombre d'occurrences par jour des mots du dictionnaire noyau sur une fenêtre large (plusieurs mois à deux ans), en audiovisuel (occurrences par heure de grille, par jour) et en presse (part des articles, par semaine, pour lisser les rythmes de parution et d'ingestion des quotidiens et des hebdomadaires). Chaque pic doit être rattaché à un événement de la liste ; un pic sans événement signale un oubli : on cherche ce qui s'est passé ce jour-là et on complète la liste. Modèles : §7.1 (audiovisuel) et §7.2 (presse).

La recherche donne les événements marquants, susceptibles d'être cités dans la publication ; les pics servent de tamis pour ce qu'elle a manqué. La liste fixe ensuite les périodes d'analyse et de comparaison (§3.2) déclarées dans le YAML.

**Étape 1, extraction.** L'entrée du YAML (mots-clés noyau, périodes) passe en PR, puis le job génère les tables thématiques. Les contrôles, qui lisent ces tables, se font à l'étape 3.

**Étape 2, axes (adoptée).** Les axes se cherchent en brainstorm, à partir de trois sources : la banque (§4) et les analyses précédentes, les références (§5), et ce que QuotaClimat a déjà publié sur le sujet. Pour ce dernier, la table `wp_linkedin_posts` (base Metabase `db_wordpress`) contient nos posts LinkedIn, avec leur texte (`content`) et leur engagement : un simple filtre `content LIKE '%<thematique>%'` donne tous les posts qui en parlent, où l'on pioche des angles. La question se nomme `Brainstorm - LinkedIn <thematique>`, dans la collection de l'analyse (§7.5 ; exemple : question 2433 pour les inondations). Chaque axe retenu se rattache à une question de l'Angle et s'écrit au §3 de l'analyse (règles : §3.3).

**Étape 3, construction des questions (adoptée).** L'étape 3 commence quand le dictionnaire noyau et les périodes sont validés, que le job de l'étape 1 a tourné et que les axes de l'étape 2 sont validés. À ce stade, Claude propose à la personne responsable de l'analyse de construire toutes les questions ci-dessous, en AV et en PE, sans attendre qu'elle les demande une à une. Il présente la liste complète, avec le nom de chaque question, puis demande, une catégorie après l'autre, lesquelles garder : il retire d'office ce qui ne s'applique pas au sujet (C et D hors catastrophe climatique, T sans thème retenu) et fait choisir entre une question par maillon et une question par axe. Il crée ensuite les questions retenues, sans les lancer.

| Catégorie | Questions proposées | Modèles |
|---|---|---|
| Périodes (indispensable, à créer en premier) | `<thematique> period filters`, à la racine de la collection. Toutes les autres questions en tirent la liste déroulante de leur variable `{{period}}` | §7.10 |
| V | `AV - V2 - Volume`, `PE - V2 - Volume` ; `AV - V3 - Part de couverture`, `PE - V3 - Part de couverture`. Les courbes (V1) sont déjà tracées à l'étape 0 | V2 : §7.8, §7.9 ; V3 : questions 2432 (AV) et 2513 (PE), à dupliquer |
| A, B, C, D | Par maillon : `AV - A - Constat`, `AV - B - Causes`, `AV - C - Conséquences`, `AV - D - Solutions` et les mêmes en `PE -` ; ou par axe : `AV - A1 - Lien CC`, `PE - A1 - Lien CC`... | §7.3, §7.4 |
| T | `AV - T - Thèmes`, `PE - T - Thèmes`, ou une question par thème (`AV - T1 - Transport`) ; une question par autre axe T retenu (Unes, cadrages, étranger ou France...) ; les croisements se font dans le Google Sheets (§4.3) | §7.3, §7.4 |
| CTRL | `AV - CTRL - Relecture humaine chunks et voisins par axe`, `PE - CTRL - Relecture humaine articles par axe`, lues dans les visionneuses (§3.6) ; `AV - CTRL - Pareto par mot clé`, `PE - CTRL - Pareto par mot clé` : occurrences de chaque terme du noyau (maillon `noyau`) et des axes (`all` ou un maillon), pour repérer les termes qui portent un axe à eux seuls | §7.6, §7.7, §7.11, §7.12 |

Les questions sont créées dans les collections de l'analyse (conventions Metabase ci-dessous) et notées dans la colonne « Question Metabase » du §3 de l'analyse. Les réglages que le connecteur ne sait pas faire restent à la personne responsable : brancher la liste déroulante de `{{period}}` sur `<thematique> period filters`, saisir la liste statique des codes d'axe des questions CTRL de relecture.

**Le repo est public** : aucun chiffre ni takeaway non publié n'y entre. Avant publication, les résultats des requêtes sont consignés dans un Google Sheets séparé. Le statut d'un axe reste « testé » jusqu'à la publication.

**Conventions Metabase.** Collection `03. Analyses thématiques/NN. <Sujet> <Année>`, avec une sous-collection `AV` et une sous-collection `PE` pour les questions de chaque média. À la racine de la collection : la liste des périodes (`<thematique> period filters`, ci-dessous et §7.10) et les questions qui servent à toute l'analyse : les posts LinkedIn (§7.5), la relecture humaine (§7.6) et le Pareto (§7.11). Préfixe `AV -` pour l'audiovisuel, `PE -` pour la presse. Les questions de contrôle prennent le code `CTRL` à la place de celui d'un axe : `AV - CTRL - Relecture humaine chunks et voisins par axe`, `PE - CTRL - Pareto par mot clé`. Après le préfixe, le code de l'axe ([§4.1](#41-catégories-et-codes)) : `AV - V2 - Volume`, `PE - A1 - Lien CC`. Une question qui regroupe tous les axes d'un maillon prend la lettre du maillon : `AV - A - Constat`, `AV - B - Causes`, `AV - C - Conséquences`, `AV - D - Solutions`. Les questions sur les tables thématiques prennent la période en variable Metabase `{{period}}` (texte, libellé « Period ») : `[[WHERE k.period = {{period}}]]`, sans valeur toutes les périodes. Dans Metabase, la variable s'affiche en liste déroulante alimentée par une question de la collection de l'analyse, à créer à chaque analyse : `<thematique> period filters`, qui fait `SELECT DISTINCT period FROM thematic_extracts.<thematique>_keywords_fr` (exemple : question 2435 pour les inondations). La liste suit ainsi les périodes réellement extraites, sans saisie à la main. Le connecteur ne sait pas brancher cette source : elle se choisit dans les réglages de la variable (liste déroulante, depuis une question, colonne `period`). Versions abandonnées préfixées `OLD-`. On ne modifie jamais une question d'une autre analyse : on la duplique. Dans le SQL, `<sujet>` (en-tête) et `<thematique>` (commentaires, noms de CTE et de colonnes) se remplacent par le thème : `Courbe journalière inondations`, `dictionnaire_inondations`, `nb_articles_inondations`. On n'emploie pas « noyau » dans les requêtes. L'extrait audiovisuel de 2 minutes s'y appelle `chunk`, et non segment (`nb_chunks_inondations`, CTE `chunks`). Une part en pourcentage (de 0 à 100) se nomme `pct_<mesure>` (`pct_a1_lien_cc`, `pct_articles_inondations`), un nombre `nb_<mesure>`. Toutes les questions sont versionnées dans Metabase, et leur SQL commence par un chapeau en commentaire : le média (audiovisuel ou presse écrite), le titre de la question, une description courte de ce qu'elle fait. Les modèles du §7 suivent ce format. Après publication, le SQL des questions utilisées est copié dans `<AAAA-MM>-<sujet>/queries/`.

**Nouvelle analyse.** Au début de l'étape 0, Claude crée `<AAAA-MM>-<sujet>/decisions.md` à partir du [modèle](#6-modèle-pour-une-analyse) ; l'Angle, les §1 et §2 sont remplis avant toute requête.

## 2. Données

Les analyses tournent sur des tables thématiques légères, générées par un job Scaleway lancé à la main (code : `quotaclimat/data_processing/thematic_extracts/`), et non plus directement sur `keywords` et `factiva_articles`.

**Configuration.** `quotaclimat/data_processing/thematic_extracts/thematic_jobs_config.yml`, une entrée par thème :
- `periodes.<pays>` (`france`, `belgiumfr`, `belgiumnl`) : liste des périodes à extraire, chacune avec un `id` (`[a-z0-9_]`), un `start` (inclus) et un `end` (exclu). Les périodes d'un même pays ne se chevauchent pas ;
- `keywords.<langue>.audiovisuel` (`french` pour `france` et `belgiumfr`, `dutch` pour `belgiumnl`) : termes sous leur forme exacte dans `keywords_with_timestamp` (orthographe et accents du dictionnaire OME) ;
- `keywords.<langue>.presse` : toutes les variantes (genre, nombre) des termes OME, détectées par regex après normalisation.

Le job prend en entrée `THEMATIC` (l'entrée du fichier), `MODE` et `COUNTRY` (par défaut `france`). Modes : `new` crée la table, `resume` reprend chaque période à son dernier jour ingéré (c'est ainsi qu'on ajoute une période), `reset` vide la table et repart du début (obligatoire si l'on modifie les dates ou l'`id` d'une période déjà ingérée). `DRY_RUN` n'écrit rien.

**Tables.** Schéma `thematic_extracts`, nommées `<thème>_<articles|keywords>_<pays>`, sans date, où `<pays>` vaut `fr`, `befr` ou `benl` (exemple : `incendies_keywords_fr`). Une table par thème, par type de média et par pays ; elle contient toutes les périodes de l'entrée, quel que soit leur nombre. On sélectionne une période en filtrant sur la colonne `period`, qui contient l'`id` de la période dans le YAML (par exemple `ete_2026`).

| | Presse écrite | Audiovisuel |
|---|---|---|
| Base | Print Media DB (Metabase id 8) | Baromètre (Scaleway Postgres, Metabase id 2) |
| Source | `public.factiva_articles`, hors `duplicate_status = 'DUP'` | `public.keywords` pour les chunks thématiques, S3 Mediatree pour le texte de leurs voisins |
| Une ligne | Un article dont le titre, le chapô ou le corps contient un terme de l'entrée `presse` | Un chunk thématique de 2 minutes (il contient un terme de l'entrée `audiovisuel`), avec le texte de ses voisins : le chunk précédent et le suivant de la même chaîne, lus sur S3 (le plus proche à 2 min 30 près, vide s'il n'y en a pas) |
| Texte (normalisé, §3.5) | `title_normalized`, `snippet_normalized`, `body_normalized` | `plaintext_normalized` ; voisins : `plaintext_normalized_prev`, `plaintext_normalized_next` |
| Colonne de contrôle | `keywords_found` : termes détectés, distincts, sous forme normalisée | `keywords_found` : termes détectés, distincts, sous leur forme dans le dictionnaire OME (non normalisée) ; `themes` : thèmes OME détectés |
| Période | `period` : `id` de la période dans le YAML | `period` : `id` de la période dans le YAML |
| Autres colonnes | `an` (clé), `source_code`, `source_name`, `publication_datetime`, `word_count`, `created_at` | `id` (clé : id de la ligne de `keywords`), `channel_name`, `channel_title`, `start` (heure de Paris), `start_prev`, `start_next` (heure des voisins, pour ne compter qu'une fois un chunk voisin de deux chunks thématiques, §3.4), `country` (contrôle), `created_at` |

Profondeur des données : rien avant le 1er janvier 2023 en audiovisuel (`keywords`), et rien avant le 1er décembre 2023 sur S3, d'où vient le texte des voisins (avant cette date, pas de recontextualisation à ±2 minutes) ; rien avant le 1er janvier 2025 en presse (`factiva_articles`). Les articles de presse n'entrent en base qu'une semaine après leur parution : pas d'analyse presse au jour le jour, la dernière semaine est toujours incomplète. `time_monitored` ne remonte pas avant 2025 : en deçà, les heures se prennent dans la grille (`program_metadata`).

Volume du corpus audiovisuel : une ligne par chunk thématique, sans filtre à poser. Les voisins ne servent qu'à la recontextualisation (§3.4).

Corpus presse : à partir de l'étape 2 (axes), toujours filtrer sur `word_count >= 100` (articles de moins de 100 mots exclus). La courbe journalière de l'étape 0 n'applique pas ce filtre : elle mesure l'agitation des médias sur tous les articles, comme son dénominateur `stats_factiva_articles`. Le job garde les articles courts dans les tables thématiques, pour pouvoir y revenir.

Classification des titres de presse (nationale, régionale...) : table `source_classification` (`media_all`, `source_type`), à joindre sur `source_code`.

## 3. Méthode

### 3.1 Corpus noyau (adoptée)

- Le corpus noyau d'un thème est construit uniquement à partir de mots du dictionnaire OME, directs (HRFP) ou indirects.
- Un terme n'entre dans le dictionnaire noyau que s'il désigne le sujet sans ambiguïté : `crue centennale` oui, `piégé par les eaux` non.
- Le dictionnaire noyau ne contient pas de terme qui relève d'un axe, cause ou contexte (`pluie torrentielle` pour les inondations) : le corpus serait construit sur ce que l'axe veut mesurer.
- On n'écarte jamais un terme parce qu'il est HRFP : il est dans le dictionnaire OME parce que nous voulons qu'il y soit. Ses faux positifs sont contrôlés par la personne responsable de l'analyse (§3.6).
- On garde toujours l'écriture du dictionnaire OME, même si elle paraît fautive ou incomplète (`augmentation du risque inondation`, sans « d' ») : elle peut reprendre un terme administratif. En presse, les variantes de genre et de nombre partent de cette écriture.
- La liste de chaque thème est fixe : un thème récurrent (incendies, inondations...) utilise toujours la même entrée de `thematic_jobs_config.yml`.
- En presse, une expression qui en contient une autre (« gestion des inondations » et « inondations ») n'ajoute aucun article au corpus. On la garde si elle est non-HRFP : `keywords_found` l'affiche alors en priorité et indique par quel terme l'article est entré dans `factiva_articles`.
- La correspondance entre `thematic_jobs_config.yml` et le dictionnaire OME est vérifiée à la main à la création ou à la modification d'une entrée.
- Le corpus noyau ne contient pas de mot absent du dictionnaire OME. Si le besoin se présente, se demander s'il devrait entrer dans le dictionnaire OME.

### 3.2 Périodes (adoptée)

- **Période d'analyse** : une fenêtre d'actualité chaude sur le sujet, justifiée par une source datée (Météo-France, préfecture, presse), renseignée dans `periodes.<pays>`. C'est elle qui limite les faux positifs d'un dictionnaire noyau large.
- **Durée de la fenêtre (repères, à adapter à chaque analyse)** : la fenêtre couvre les jours où le sujet est fortement traité, pour limiter les segments hors sujet. Ses bornes dépendent du type d'événement :
  - élection : à partir de la date officielle de début de campagne ;
  - événement dont la date est connue à l'avance (COP, sommet, vote...) : à partir de 2 à 3 semaines avant, pour voir une éventuelle montée en puissance ;
  - événement météorologique : à l'échelle des prévisions, soit 1 à 2 semaines avant et après l'épisode.
- **Période de comparaison** : un épisode similaire (même aléa, actualité comparable), renseigné comme une autre période de la même entrée. Les requêtes comparent les périodes via la colonne `period`.
- **Rôle de chaque période** : il est écrit dans le plan de comparaison de l'Angle (§1) avant la première entrée du YAML, pour déclarer toutes les périodes en une fois.

### 3.3 Axes (adoptée)

- Chaque axe a son propre dictionnaire de termes explicites. Ces termes n'ont pas à appartenir au dictionnaire OME, puisqu'un axe peut porter sur l'économie, les transports, la santé...
- Les axes qui ne dépendent pas du sujet sont dans la [banque d'axes génériques](#4-banque-daxes-génériques). Chaque analyse liste dans son §3 tous les axes appliqués : pour un axe de la banque, un renvoi et les éventuels ajouts ou retraits de termes ; pour un axe spécifique, ses termes. Les axes de la banque non exploités sont listés avec leur raison.
- Un axe entre dans la banque (catégories V, A, B, C, D et T) s'il remplit deux conditions :
  1. sa question se transpose à au moins deux aléas (incendies, inondations, chaleur...), ou à deux sujets pour un thème (T), sans changer de sens ; les termes peuvent varier, l'analyse note ses ajouts et retraits dans son §3 ;
  2. il a une trace : une analyse qui l'a testé, ou une référence datée (§5) pour une idée.
- Un axe qui ne remplit pas la condition 1 reste dans le `decisions.md` de son analyse ; une idée sans trace reste dans le `decisions.md` de son analyse, au §3, parmi les pistes.
- Le recensement des axes non exploités commence avec les analyses menées après la révision ; les analyses antérieures (incendies et chaleur 2026) ne le contiennent pas.
- Dans le SQL, chaque axe est un drapeau (une regex). Réutiliser les regex existantes avant d'en écrire.

### 3.4 Recontextualisation (adoptée)

- **Audiovisuel** : un axe compte s'il apparaît dans le segment thématique ou dans un segment adjacent de la même chaîne (±2 minutes).
- **Voisins partagés** : un chunk peut être le voisin de deux chunks thématiques. Pour la part d'un axe, chaque chunk thématique se juge avec ses propres voisins, même s'ils sont thématiques : pas de dédoublonnage. Pour un comptage d'occurrences (T1 des inondations), chaque chunk, thématique ou voisin, ne compte qu'une fois, repéré par sa chaîne et son heure (`start_prev`, `start_next`), avec le texte du chunk thématique en priorité.
- **Presse** : l'article entier (titre, chapô, corps).

### 3.5 Normalisation du texte (adoptée)

Le job normalise le texte des tables thématiques et, en presse, les mots-clés recherchés, de la même manière :

1. minuscules ;
2. `œ` et `æ` remplacés par `oe` et `ae`, accents retirés ;
3. apostrophes (toutes variantes typographiques) et tirets remplacés par un espace.

Les regex des axes s'appliquent aux colonnes `*_normalized`, en mots entiers (`\y...\y` en Postgres). Leurs termes sont donc écrits sous la même forme : `secheresse`, `manque d eau`, `pare feu`.

### 3.6 Contrôles (étape 3)

**Frugalité (adoptée).** Claude ne lit pas le texte du corpus (`plaintext_normalized`) et aucun corpus n'est relu par un LLM. Le contrôle se fait avec la question de contrôle de l'analyse (`AV - CTRL - Relecture humaine chunks et voisins par axe`, 2488 pour les inondations) : 25 chunks thématiques détectés pour l'axe choisi, tirés au hasard à chaque exécution, avec le texte de leurs voisins à ±2 minutes. En presse, la question `PE - CTRL - Relecture humaine articles par axe` (2548 pour les inondations) tire de même 25 articles, avec leur titre, leur chapô et leur corps d'origine (§7.7). Pas de contrôle de complétude : les scripts du job vérifient déjà ce qu'ils ingèrent.

**Lecture humaine (adoptée).** Les contrôles qui demandent de lire des contenus sont faits par la personne responsable de l'analyse. Claude ne relit pas les textes du corpus et ne propose pas de le faire.

### 3.7 Unités et effectif minimal (adoptée)

- Audiovisuel : unité = segment de 2 minutes ; temps d'antenne en minutes. Presse : unité = article.
- Ne jamais comparer une part de segments à une part de temps d'antenne.
- Chaque requête sort numérateurs et dénominateurs bruts en colonnes séparées, en plus des pourcentages.
- Un média dont le corpus compte moins de **30** segments ou articles reste dans les résultats, mais on n'en tire aucune conclusion individuelle. L'effectif minimal ne change pas les requêtes (ni filtre ni colonne) : il se garde à l'esprit à la lecture des résultats.

### 3.8 Takeaway publiable (adoptée)

Un résultat est publiable si :

- le dénominateur est affiché et atteint l'effectif minimal ;
- l'écart entre périodes ou entre groupes est net, pas quelques points sur de petits effectifs ;
- les unités sont homogènes ;
- il est formulé avec chiffre, dénominateur et période.

Les pistes testées sans résultat sont notées dans le §3 de l'analyse, jamais effacées.

### 3.9 Encadré méthodologique et validation humaine (adoptée)

- Chaque article publié inclut : corpus (médias, nombre de segments et d'articles), période et comparaison, méthode de recontextualisation, limites (§3.10).
- Tout contenu diffusé est relu par une personne experte du sujet. Les dictionnaires sont versionnés dans le repo, le SQL dans Metabase puis dans `queries/` après publication.

### 3.10 Limites persistantes

- **Presse, mots HRFP** : sans réactiver l'API Factiva, on ne récupère pas tous les articles qui ne contiennent que des mots clés HRFP, car seuls les mots non-HRFP déclenchent leur recensement dans `factiva_articles`. Nous décidons de ne pas la réactiver pour l'instant.
- **Uniformité entre médias** : en audiovisuel, le corpus inclut tous les segments contenant un mot du noyau, direct ou indirect ; en presse, l'accès dépend des mots non-HRFP. À rappeler dans l'encadré méthodologique.
- **Segments adjacents** : récupérés sur S3 sous réserve qu'ils y soient présents.
- **Dictionnaires d'axes** : explicites mais non exhaustifs ; ni la précision ni le rappel ne sont garantis à 100 %.
- **Valence** : détecter un thème ne dit pas si le contenu le défend ou le critique.

## 4. Banque d'axes génériques

Axes qui ne dépendent pas du sujet. Les termes ([§4.4](#44-termes)) sont un point de départ : à contrôler avant réutilisation. Une analyse qui ajoute ou retire des termes l'écrit dans son §3, sans modifier la liste d'ici.

Statuts : **publié** (a donné un résultat publié), **présenté** (figure dans un livrable non public, par exemple pour un partenaire), **testé** (requête faite, résultat non publié ou nul), **idée** (jamais testé).

### 4.1 Catégories et codes

Les axes se rangent en six catégories, dans l'ordre où un article les présente généralement : la volumétrie, les quatre maillons de la chaîne causale, les thèmes. La chaîne causale convient aux catastrophes climatiques (incendies, inondations, chaleur). Pour un sujet qui n'en est pas une (prix des carburants), conséquences et solutions s'appliquent mal : on regarde plutôt de quels thèmes on parle, avec les axes T, seuls ou à côté de A à D.

| Préfixe | Catégorie | Question type | Traitement |
|---|---|---|---|
| V | Volumétrie | Combien de données a-t-on ? Leur timeline (V1, tracée en premier, à l'étape 0), le nombre d'articles, d'occurrences, de segments (V2), la part de couverture (V3). Sert surtout à savoir s'il faudra agréger (effectif minimal) ; la timeline sert aussi à fixer les périodes et à comparer l'agitation médiatique d'une thématique à l'autre | Quelques requêtes à chaque analyse ; l'article commence en général par la timeline |
| A | Constat | L'événement est-il relié au changement climatique, présenté comme une tendance, expliqué par des scientifiques ? | Le cœur de l'analyse, avec B, C et D |
| B | Causes | Quelles causes sont évoquées : climatiques, météorologiques, humaines directes, locales ? | |
| C | Conséquences | Quelles conséquences sont évoquées : humaines, matérielles, par secteur ? Les personnes vulnérables relèvent des conséquences | |
| D | Solutions | Quelles solutions sont présentées : curatives, préventives, atténuation, adaptation ? Les pompiers relèvent des solutions | |
| T | Thèmes | De quels thèmes parle-t-on avec le sujet : transport, économie, agriculture, tourisme, santé... ? Un thème est une mention large du domaine, sans dire s'il est cause, conséquence ou solution. T reçoit aussi les angles qui n'entrent dans aucun maillon (Unes, hiérarchie, cadrages concurrents, politisation...), les croisements de variables (§4.3) et les requêtes annexes : c'est le réservoir d'idées pour sortir des analyses répétitives, à enrichir à chaque analyse | Le cœur de l'analyse quand le sujet n'est pas une catastrophe climatique ; sinon, en complément de A à D |

**Codage.**

- V : `V` + numéro (`V1`, `V2`).
- A, B, C, D : la lettre est le maillon (`A` constat, `B` causes, `C` conséquences, `D` solutions), suivie d'un numéro (`C2` santé).
- T : `T` + numéro (`T1` transport).
- Les questions Metabase reprennent le code : `AV - A1 - Lien CC`.
- Dans une analyse, les axes sont numérotés dans leur maillon (`A1`, `A2`...) ; un axe repris de la banque indique son code de banque dans la colonne « Origine » (`banque A1`). Un axe propre à l'analyse n'entre pas dans la banque.
- Les analyses antérieures (incendies, chaleur et surchauffe 2026) gardent leurs anciens codes (Q1, A1, B1...), qui ne correspondent pas aux codes actuels.

Ventilations habituelles, pour toutes les catégories : chaîne ou titre, public / privé, presse nationale / régionale, TV / radio.

### 4.2 Axes

Statuts : voir plus haut. « Explorée dans » cite les analyses ; « idée » renvoie à la référence qui l'a inspirée (§5).

**V. Volumétrie**

| Code | Ancien code | Axe | Question posée | Explorée dans |
|---|---|---|---|---|
| V1 | Q7 | Timeline (courbe journalière) | Comment le volume évolue-t-il jour par jour ? Occurrences par heure de grille sur une fenêtre large, tracées en premier, à l'étape 0. Sert à fixer les périodes, à mesurer l'agitation des médias sur le sujet et à la comparer d'une thématique à l'autre (incendies face à inondations), pour formuler des hypothèses sur ce qui fait réagir les médias. Attention à la taille des dictionnaires noyau comparés | Incendies 2026 |
| V2 | Q1 (en partie) | Volume | Combien d'articles, d'occurrences, de segments, par média ? Faudra-t-il agréger ? | Incendies 2026, Chaleur 2026 |
| V3 | Q1 (en partie), Q6 | Part de couverture, comparaison et répartition | Quelle part du temps d'antenne ou des articles porte sur les enjeux environnementaux, est-elle exceptionnelle par rapport au niveau habituel ou à un épisode similaire, et quel maillon domine ? Takeaway à en tirer : le reflux (ancien Q7b), la couverture retombe-t-elle sous son niveau habituel après le pic ? Sources, par semaine : `public.core_query_environmental_shares` en audiovisuel (par chaîne), question Metabase 1384 « Modèle crises Presse Ecrite - splitées par crise » en presse (par type de média). Forme de la requête audiovisuelle : une ligne par période du YAML et par chaîne, avec côte à côte la part de la période, celle des 52 semaines précédentes et l'écart en points (exemple : question 2432, inondations 2026) | Incendies 2026, Chaleur 2026 ; répartition : idée (Bilan OMÉ S1 2026) |

**A à D. Chaîne causale**

Les axes sont des listes de termes explicites (§4.4), comme la mention explicite du changement climatique. Les thèmes du dictionnaire OME (colonne `themes` en audiovisuel, `predict_*` de `factiva_articles` en presse) ne définissent pas d'axe : ils sont trop larges.

| Code | Maillon | Axe | Question posée | Explorée dans |
|---|---|---|---|---|
| A1 | Constat | [Lien avec le changement climatique](#lien-avec-le-changement-climatique) | Les médias relient-ils l'événement au changement climatique ? | Incendies 2026, Chaleur 2026 |
| A2 | Constat | [Mise en série](#mise-en-série) | L'événement est-il présenté comme une tendance (« de plus en plus fréquent ») ? | Incendies 2026 |
| A3 | Constat | Intervenants : qui on appelle pour en parler ([voix scientifiques](#voix-scientifiques) et autres) | Qui est mentionné pour expliquer : scientifiques, élus, secours, sinistrés, associations, monde agricole ? Limite : on détecte la mention d'un type d'intervenant, pas qui parle | Incendies 2026 (voix scientifiques) ; autres intervenants : idée (Acrimed) |
| B1 | Causes | [Causes du réchauffement climatique](#causes-climatiques) | Les causes du réchauffement climatique (émissions, énergies fossiles, déforestation...) sont-elles évoquées ? | Incendies 2026, Chaleur 2026 |
| B2 | Causes | [Co-aléa chaleur](#co-aléa-chaleur) | L'événement est-il relié à la chaleur ? | Incendies 2026 |
| B3 | Causes | [Co-aléa sécheresse](#co-aléa-sécheresse) | L'événement est-il relié à la sécheresse ? | Incendies 2026 |
| B4 | Causes | [Co-aléa vent et météo](#co-aléa-vent-et-météo) | L'événement est-il relié aux conditions météo ? | Incendies 2026 |
| B5 | Causes | [Aléas composés](#aléas-composés) | L'événement est-il présenté comme la combinaison de plusieurs aléas ? | Incendies 2026 |
| B6 | Causes | [Forces de l'ordre et justice](#forces-de-lordre-et-justice) | Les causes humaines directes (origine criminelle, enquête) dominent-elles, le sujet est-il traité comme un fait divers ? | Incendies 2026 |
| C1 | Conséquences | [Par secteur](#conséquences-par-secteur) | De quelles conséquences de l'événement parle-t-on le plus : transports, économie, assurance, écoles, travail, agriculture, alimentation... (secteurs à choisir selon le sujet) ? Piste : ne compter un article dans un secteur qu'à partir de 2 ou 3 mentions de ses termes, pour écarter les mentions en passant | Chaleur 2026 (juin, presse) ; idée (Bilan OMÉ S1 2026) |
| C2 | Conséquences | [Santé](#santé) | Les effets sanitaires sont-ils évoqués ? | Incendies 2026, Surchauffe 2026, Chaleur 2026 (juin, presse) |
| C3 | Conséquences | Précarité du ménage | Les ménages précaires ou modestes sont-ils évoqués ? Termes larges (`populaire`, `fragile`) : à contrôler | Surchauffe 2026 |
| C4 | Conséquences | Personnes vulnérables ([personnes âgées](#personnes-âgées), enfants) | Les personnes âgées, les enfants, les nourrissons sont-ils évoqués ? Termes larges (`enfant`) : à contrôler | Surchauffe 2026, Chaleur 2026 (juin, presse) |
| D1 | Solutions | [Solutions](#solutions) | Quelles solutions sont présentées : atténuation, adaptation ? | Incendies 2026, Chaleur 2026 |
| D2 | Solutions | Individuelles ou systémiques | Les solutions relèvent-elles du geste individuel et du court terme, ou de l'action collective et structurelle ? Généralise l'axe chaleur « adaptation palliative ou structurelle » | Chaleur 2026 (adaptation) ; idée en général (Bilan OMÉ S1 2026) |
| D3 | Solutions | Curatif ou préventif | La couverture porte-t-elle sur la lutte et les secours (pompiers, moyens aériens) ou sur la prévention ? | Incendies 2026 |

La désinformation n'entre pas dans ces analyses : elle fait l'objet d'analyses annexes, avec leur propre méthode.

**T. Thèmes**

T1 à T4 : mentions larges du domaine, termes génériques réutilisables d'un sujet à l'autre. Les inondations 2026 les ont testés comme conséquences par secteur (C4 à C7) : leurs regex servent de point de départ. T5 à T11 : angles qui n'entrent dans aucun maillon (anciens X1 à X7). Les croisements de variables sont au §4.3.

| Code | Axe | Question posée | Explorée dans |
|---|---|---|---|
| T1 | Transport | Le sujet est-il relié aux transports (routes, trains, circulation) ? | Inondations 2026 (C5, bruité) |
| T2 | Économie | Le sujet est-il relié à l'économie (entreprises, commerces, emploi) ? | Inondations 2026 (C6, bruité) |
| T3 | [Agriculture](#agriculture) | Le sujet est-il relié à l'agriculture et à l'élevage ? | Inondations 2026 (C4), Chaleur 2026 (juin, presse) |
| T4 | Tourisme | Le sujet est-il relié au tourisme ? | Inondations 2026 (C7) |
| T5 | Unes | Le sujet fait-il la Une ? Présence en Une, par titre | Chaleur 2026 |
| T6 | Hiérarchie dans le JT | Le sujet ouvre-t-il le journal, à quelle position arrive-t-il ? Piste : `keywords` porte `channel_program` et `program_metadata` l'heure de début de l'émission dans la grille, ce qui situe un segment en minutes depuis le début prévu de l'émission. Limite : heure de la grille, pas de la diffusion réelle (retards, éditions spéciales) | idée (Acrimed) |
| T7 | Par événement et proportionnalité (ancien Q8) | Quels événements sont couverts, et la couverture est-elle proportionnée à leur impact (liste de l'étape 0) ? Pistes : un événement régional est-il couvert par la presse régionale d'autres régions (inondations) ? La couverture suit-elle un indicateur physique de l'aléa (ITN, hectares brûlés, niveau des crues), jour par jour ? | Surchauffe 2026 (mentions face à l'ITN) ; par événement : idée (jamais vraiment mis en œuvre) |
| T8 | Cadrages concurrents | Quel angle domine : pouvoir d'achat, souveraineté, géopolitique, industrie et emploi, santé, « écologie punitive » ? Codage booléen non exclusif, un dictionnaire par cadrage | idée (Bilan OMÉ S1 2026) |
| T9 | Politisation | Le sujet est-il présenté comme l'affaire d'un camp politique ? Demande une lecture humaine ; un codage par LLM impose les précautions de la charte IA | idée (Bilan OMÉ S1 2026) |
| T10 | Couplage avec une autre actualité | Quelle part du corpus relie le sujet à un autre événement du moment (élection, crise énergétique...) ? | idée (Bilan OMÉ S1 2026) |
| T11 | Étranger ou France | Recontextualise-t-on davantage un événement qui se passe à l'étranger qu'en France (lien avec le climat, causes, solutions) ? Croiser les axes avec une regex de lieu (exemple : colonne `espagne` de la question 2427, analyse inondations) | idée (posts LinkedIn de QuotaClimat, question 2433) |

### 4.3 Croisements

Les croisements relèvent de la catégorie T. Un croisement met deux variables face à face. Il n'a pas de requête propre : il se fait dans le Google Sheets des résultats, à partir des sorties des requêtes d'axes. Exemple, par média : part des séquences du corpus qui font le lien avec le changement climatique (A1), et part qui évoque ses causes (B1). Chaque part est calculée séparément sur le corpus (certains contenus relèvent des deux axes, pas tous) : on ne restreint pas une variable à l'autre. Les médias hauts sur les deux parts font le lien et l'expliquent.

- **Deux variables**, pas plus, sauf idée qui le justifie clairement : au-delà, le résultat devient illisible.
- **Les meilleurs croisements découpent les médias en quatre groupes**, pas seulement en bons et mauvais élèves, et donnent quelque chose à dire sur chaque groupe.
- **On ne s'interdit aucun croisement** : on produit les données de tous les axes en amont, puis on essaie les croisements dans le Google Sheets et on garde ceux qui parlent.

| Croisement | Question posée | Explorée dans |
|---|---|---|
| Lien CC (A1) et causes (B1), par média | Faire le lien, est-ce expliquer ? | Incendies 2026, Chaleur 2026 |
| Volume et recontextualisation, par média | Ceux qui en parlent le plus recontextualisent-ils le plus ? | idée |
| Longueur et lien CC | Les articles longs recontextualisent-ils davantage ? | Incendies 2026 |
| Évolution du lien CC dans l'épisode | Le climat arrive-t-il au pic ou après ? | idée |
| Comparaison entre épisodes | Un aléa est-il mieux recontextualisé qu'un autre ? | Incendies 2026 vs Chaleur 2026 |
| Spectacle ou explication | La couverture porte-t-elle sur l'opérationnel (images, dégâts, secours) plutôt que sur les causes, les enjeux et les solutions ? | idée (Acrimed) |

### 4.4 Termes

#### Lien avec le changement climatique

Mention explicite : `changement climatique`, `changements climatiques`, `dereglement climatique`, `rechauffement climatique`, `rechauffement de la planete`, `rechauffement planetaire`, `crise climatique`, `urgence climatique`, `climat se rechauffe`, `le climat change`

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

Mots français du dictionnaire OME de thème `changement_climatique_causes` (direct et indirect), hors HRFP, lus dans `public.dictionary` et normalisés dans la requête (exemple : question 2426 de l'analyse inondations). La même construction sert à toutes les analyses, pour que B1 reste comparable d'une analyse à l'autre. Limites : correspondance exacte sur le texte normalisé, sans lemmatisation (un pluriel absent du dictionnaire n'est pas trouvé) ; quelques mots hors sujet selon l'aléa (`a69`, `mercosur`).

#### Solutions

- **Atténuation** : `reduire les emissions`, `reduction des emissions`, `baisse des emissions`, `decarbonation`, `decarboner`, `neutralite carbone`, `zero emission nette`, `sortie des energies fossiles`, `sortie du charbon`, `energies fossiles`, `puits de carbone`, `budget carbone`, `objectifs climatiques`, `accord de paris`, `green deal`, `plan climat`, `pniec`, `snbc`

#### Conséquences par secteur

Axe C1 ; termes de la chaleur de juin 2026 (question 2061, presse), à choisir selon le sujet. L'agriculture est en T3 ([Agriculture](#agriculture)).

- **Enfance et écoles** : `ecoles?`, `scolaires?`, `eleves`, `creches?`, `cantine`, `colleges?`, `lycees?`, `brevet`, `baccalaureat`, `enfants?`, `nourrissons?`, `cour de recreation`, `bati scolaire`
- **Alimentation** : `prix alimentaires?`, `fruits et legumes`, `rayon frais`, `chaine du froid`, `supermarches?`, `consommateurs?`, `restauration`, `penuries? alimentaires?`, `securite alimentaire`, `gaspillage alimentaire`
- **Travail** : `chantiers?`, `btp`, `travailleurs?`, `salaries?`, `droit de retrait`, `conditions de travail`, `horaires decales`, `travail en exterieur`, `inspection du travail`

Termes larges : `enfants?`, `brevet`, `restauration`, `consommateurs?`, `salaries?`.

#### Santé

- **Qualité de l'air et consignes** : `qualite de l air`, `pollution de l air`, `particules fines`, `pm10`, `pm2`, `microparticules`, `masque`, `personnes vulnerables`, `personnes fragiles`, `problemes respiratoires`, `asthme`, `rester chez soi`, `rester a l interieur`, `consigne sanitaire`, `consignes sanitaires`, `recommandation sanitaire`, `fermer les fenetres`, `atmo`, `prevair`
- **Soins et mortalité** (chaleur de juin 2026, question 2061) : `urgences`, `hopital`, `hopitaux`, `mortalite`, `deces`, `surmortalite`, `deshydratation`, `coup de chaleur`, `hyperthermie`, `sante publique france`, `samu`. Termes larges : `urgences`, `deces`.

#### Personnes âgées

Axe C4 ; termes de la chaleur de juin 2026 (question 2061). Les enfants sont dans [Conséquences par secteur](#conséquences-par-secteur) (enfance et écoles). `personnes? agees?`, `ehpad`, `maisons? de retraite`, `residence senior`, `geriatri\w+`, `isolement des aines`, `registre canicule`, `plan bleu`, `octogenaire`, `nonagenaire`

#### Forces de l'ordre et justice

`parquet`, `enquete judiciaire`, `ouvre une enquete`, `juge d instruction`, `police`, `auteur`, `poursuites`, `plainte`, `risque jusqu a`, `ans de prison`, `inculpe`

#### Agriculture

Axe T3 ; termes de C4 des inondations 2026 et de la chaleur de juin 2026 (question 2061) : `agricult\w+`, `agricoles?`, `paysans?`, `eleveurs?`, `elevages?`, `betail`, `maraich\w+`, `viticulteurs?`, `vignerons?`, `vendanges?`, `arboriculteurs?`, `cerealiers?`, `recoltes?`, `moissons?`, `irrigation`

## 5. Références

Notre sujet est l'écologie. Une référence qui traite d'un autre sujet ne sert qu'à s'inspirer pour les axes ; on n'en reprend ni la méthode de constitution du corpus, ni les chiffres à des fins de comparaison. Les axes repérés sont reportés dans la banque (§4).

| Référence | Organisation | Date | Ce qu'on peut en reprendre |
|---|---|---|---|
| [Vague de chaleur de mai 2026 : couverture médiatique](https://quotaclimat.org/actualites/vague-chaleur-mai-2026-couverture-mediatique/) | QuotaClimat | non indiquée | Analyse de référence. Voir `2026-05-chaleur/`. |
| [Incendies 2026 : analyse du traitement médiatique](https://quotaclimat.org/actualites/incendies-2026-analyse-traitement-mediatique/) | QuotaClimat | 22/09/2026 | Analyse de référence. Voir `2026-07-incendies/`. |
| Surchauffe des logements, support présenté à l'ONPE (non publié) | QuotaClimat, OMÉ | 21/09/2026 | Analyse de référence. Voir `2026-06-surchauffe-logement/`. Courbe des mentions face à un indicateur physique (ITN), comparaison avec un sujet concurrent (climatisation), axes précarité et personnes vulnérables. |
| [Bilan OMÉ, 1er semestre 2026](https://observatoiremediaecologie.fr/app/uploads/2026/09/BILAN_OME_S1_2026.pdf) | OMÉ | 09/2026 | Axes (chaîne causale, conséquences par secteur, solutions individuelles ou systémiques, cadrages concurrents, politisation, couplage avec une autre actualité, reflux). Le dédoublonnage des reprises de presse (titre normalisé, date, empreinte du contenu) y est décrit, non retenu pour l'instant. |
| [Inondations : est-ce la faute du changement climatique ?](https://bonpote.com/inondations-est-ce-la-faute-du-changement-climatique/) | Bon Pote (Thomas Wagner) | 14/09/2021 | Vulgarisation sourcée (GIEC AR6 WG1, World Weather Attribution, Haut Conseil pour le Climat). Termes des axes causes et mécanismes de l'analyse inondations : Clausius-Clapeyron, état des sols et ruissellement, exposition et vulnérabilité, aléas combinés, moyens de prévision. Chiffres datés de 2021 : ne pas les reprendre sans vérifier la source originale. |
| [Inondation : à quoi s'attendre et comment s'adapter ?](https://www.adaptation-changement-climatique.gouv.fr/dossiers-thematiques/impacts/inondation) | Centre de ressources pour l'adaptation au changement climatique (Cerema, DGEC, ADEME, Météo-France, PNACC 3) | 29/07/2025 | Termes des axes causes et solutions de l'analyse inondations : sols et bocages, artificialisation des cours d'eau, politiques publiques (PAPI, Gemapi, PGRI, fonds Barnier), solutions fondées sur la nature, ville éponge, culture du risque. Chiffres à vérifier dans leurs sources primaires (GIEC AR6, France Assureurs). |
| [Inondation](https://fr.wikipedia.org/w/index.php?title=Inondation&oldid=239846357) | Wikipédia | version 239846357, consultée le 28/10/2026 | Termes des axes causes, conséquences et solutions de l'analyse inondations (sols et ruissellement, santé, ouvrages et réglementation comme PPRI et zones d'expansion des crues). Plusieurs chiffres y sont marqués « référence nécessaire » : ne rien reprendre sans la source primaire. |
| [20h de France 2 : une télé de préfecture](https://www.acrimed.org/20h-de-France-2-Une-tele-de-prefecture) | Acrimed (hors écologie) | 21/10/2026 | Communiqués syndicaux critiquant un JT, sans protocole de mesure : inspiration seulement. Axes : spectacle ou explication, types d'intervenants, hiérarchie dans le JT. |
| [« Racailles », « banlieues », « ultragauche » : après le 17 octobre, les chaînes d'info tiennent leurs coupables](https://www.arretsurimages.net/articles/racailles-banlieues-lfi-apres-le-17-octobre-les-chaines-dinfo-tiennent-leurs-coupables) | Arrêt sur images (Matteo Trabelsi, hors écologie) | non indiquée (article réservé aux abonnés) | Présentation : nombre de mentions en diagramme en barres horizontales. Termes, chaînes, période et méthode non relus (article réservé aux abonnés). |

## 6. Modèle pour une analyse

À copier dans `<AAAA-MM>-<sujet>/decisions.md`.

```markdown
# <Sujet> <période>

- Responsable : <prénom>
- Statut : en cours | publié (<lien article ou bilan>)
- Entrée de `thematic_jobs_config.yml` : `<thème>`
- Tables : `thematic_extracts.<thème>_articles_<pays>`, `thematic_extracts.<thème>_keywords_<pays>`
- Collection Metabase : `03. Analyses thématiques/NN. <Sujet> <Année>`

## Angle

Ce que l'analyse veut montrer, en quelques questions écrites avant le dictionnaire noyau (§1 de la méthode commune). Chaque axe du §3 se rattache à l'une d'elles.

- Destination : <rapport OMÉ, article QuotaClimat, présentation à des partenaires (non publique)...>, parution prévue <mois année>
- Ce que couvre l'analyse : <épisode unique, principaux épisodes de l'année...>

Questions :

- <question 1>
- <question 2>

Plan de comparaison (écrit avant la première entrée du YAML) :

| `id` | Épisode | Rôle |
|---|---|---|
| `<id>` | <lieu, date> | analyse \| comparaison (antérieur, étranger...) |

## 1. Méthode spécifique

Seulement ce qui s'écarte de la méthode commune. Sinon : « Méthode commune, sans écart. »

- Événements recensés (étape 0) : tableau début, fin, lieu, impact, source datée ; pics de la courbe d'occurrences rattachés ou ajoutés
- Période d'analyse : `<id>` dans le YAML, du ... au ... (justification, source datée)
- Période de comparaison : `<id>` dans le YAML, du ... au ... (justification)
- Contrôles : relecture humaine par la personne responsable, sur 25 chunks ou 25 articles par axe (questions de relecture humaine, §7.6 et §7.7) ; résultat qualitatif et date, aucun chiffre avant publication

## 2. Mots-clés

- Entrée `<thème>` de `thematic_jobs_config.yml`, créée ou modifiée le ..., correspondance avec le dictionnaire OME vérifiée le ...
- Termes discutés (bruit, exclusions) et décision :

## 3. Axes

Chaque axe des catégories V, A, B, C, D et T de la banque (méthode commune §4) est soit listé ici, soit noté comme non exploité avec sa raison ; un sujet qui n'est pas une catastrophe climatique peut écarter C et D en bloc (§4.1). Les angles hors maillon de T (T5 à T11) ne sont listés que s'ils sont traités.

| Code | Axe | Origine | Termes | Question Metabase (id) | Résultat |
|---|---|---|---|---|---|
| A1 | Lien avec le changement climatique | banque A1 | [banque](../decisions.md#lien-avec-le-changement-climatique) | | testé |
| C5 | <axe spécifique, numéroté dans son maillon> | spécifique | ci-dessous | | |

Résultat : publié, testé (jusqu'à publication), sans résultat (après publication, avec la raison).

**Axes de la banque non exploités** : <axe> (raison), ...

**Termes des axes spécifiques**

- **<axe spécifique>** : `terme`, `terme`

## 4. Résultats

Après publication uniquement (le repo est public) : takeaways publiés avec chiffre, dénominateur et période ; limites ; axes à remonter dans la banque.
```

## 7. Modèles de requêtes

**Frugalité (adoptée).** Une requête part du modèle de ce paragraphe et des regex existantes (§3.3, §4.4) : on remplace les bornes, la période, le dictionnaire et les termes, sans relire les questions Metabase d'une analyse précédente. On ne lit une question Metabase que si elle est citée par son numéro, ou pour mettre à jour un modèle. Après l'étape 1, les requêtes portent sur les tables thématiques, pas sur `keywords` et `factiva_articles`. Les contrôles ont leur propre règle (§3.6). Un modèle amélioré au cours d'une analyse remplace celui d'ici.

Dans les modèles, `<sujet>` (en-tête), `<thematique>` (commentaires, noms de CTE et de colonnes), `<axe>` (code et nom court de l'axe, par exemple `a1_lien_cc`) se remplacent avant exécution ; la période est la variable Metabase `{{period}}` (conventions Metabase, §1).

| Modèle | Étape | Média | Unité | Tables |
|---|---|---|---|---|
| [7.1 Courbe journalière](#71-courbe-journalière-audiovisuel) | 0.3 | audiovisuel | occurrence, par heure de grille | `keywords`, `program_metadata` |
| [7.2 Courbe hebdomadaire](#72-courbe-hebdomadaire-presse) | 0.3 | presse | article, part des articles de la semaine | `factiva_articles`, `stats_factiva_articles` |
| [7.3 Axe par chaîne](#73-axe-par-chaîne-audiovisuel) | 3 | audiovisuel | chunk thématique, avec ses voisins (`_prev`, `_next`) | `thematic_extracts.<thematique>_keywords_fr`, `curated.chaine` |
| [7.4 Axe par titre](#74-axe-par-titre-presse) | 3 | presse | article d'au moins 100 mots | `thematic_extracts.<thematique>_articles_fr`, `source_classification` |
| [7.5 Posts LinkedIn](#75-posts-linkedin-de-quotaclimat) | 2 (axes) | posts LinkedIn de QuotaClimat | post | `wp_linkedin_posts` (base db_wordpress) |
| [7.6 Relecture humaine](#76-relecture-humaine-des-chunks-et-voisins-audiovisuel) | contrôle (§3.6) | audiovisuel | chunk thématique, 25 tirés au hasard | `thematic_extracts.<thematique>_keywords_fr` |
| [7.7 Relecture humaine](#77-relecture-humaine-des-articles-presse) | contrôle (§3.6) | presse | article d'au moins 100 mots, 25 tirés au hasard | `thematic_extracts.<thematique>_articles_fr`, `factiva_articles`, `source_classification` |
| [7.8 Volume par chaîne](#78-volume-par-chaîne-audiovisuel) | 3 | audiovisuel | chunk thématique et minutes | `thematic_extracts.<thematique>_keywords_fr`, `curated.chaine` |
| [7.9 Volume par média](#79-volume-par-média-presse) | 3 | presse | article, et part des articles du média | `thematic_extracts.<thematique>_articles_fr`, `stats_factiva_articles`, `source_classification` |
| [7.10 Liste des périodes](#710-liste-des-périodes) | 1 à 3 | audiovisuel et presse | période extraite | `thematic_extracts.<thematique>_keywords_fr` |
| [7.11 Pareto des termes](#711-pareto-des-termes-audiovisuel) | contrôle (§3.6) | audiovisuel | occurrence d'un terme, chaque chunk ou voisin une fois | `thematic_extracts.<thematique>_keywords_fr` |
| [7.12 Pareto des termes](#712-pareto-des-termes-presse) | contrôle (§3.6) | presse | occurrence d'un terme, articles d'au moins 100 mots | `thematic_extracts.<thematique>_articles_fr` |

Les modèles 7.5, 7.6, 7.10 et 7.11 servent à toute l'analyse : leurs questions sont à la racine de la collection de l'analyse, les autres dans ses sous-collections `AV` et `PE` (conventions Metabase, §1). La relecture presse (7.7) et le Pareto presse (7.12) sont dans `PE`.

### 7.1 Courbe journalière, audiovisuel

Base Baromètre. Compte les mots du dictionnaire noyau détectés dans `keywords_with_timestamp`, comme le job, et non une regex sur le texte. `keywords.start` est en heure de Paris. Le dénominateur est la grille en vigueur chaque jour (`program_metadata`), seule source d'heures disponible avant 2025 (`time_monitored` ne commence qu'en 2025).

```sql
-- Média : audiovisuel
-- Question : AV - V1 - Courbe journalière <sujet>
-- Description : occurrences des mots du dictionnaire <thematique> par heure de grille, par jour,
--   toutes chaînes confondues (un chunk de 2 minutes peut contenir plusieurs occurrences).
-- Tables : public.keywords (keywords_with_timestamp), public.program_metadata
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
        COUNT(DISTINCT k.id) AS nb_chunks      -- chunks de 2 minutes concernés
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
    COALESCE(o.nb_chunks, 0)    AS nb_chunks,
    h.nb_heures_grille,
    COALESCE(o.nb_occurrences, 0) / NULLIF(h.nb_heures_grille, 0) AS occ_par_heure
FROM heures h
LEFT JOIN occurrences o USING (jour)
ORDER BY h.jour;
```

Affichage : courbe, `jour` en x, `occ_par_heure` en y, annotée des événements.

### 7.2 Courbe hebdomadaire, presse

Base Print Media DB. La courbe est hebdomadaire, pas journalière : quotidiens et hebdomadaires paraissent et sont ingérés à des rythmes différents, et la semaine les met sur le même pas. Les semaines vont du lundi au dimanche ; la dernière semaine retenue se termine au moins une semaine avant la date du jour (délai d'ingestion, §2), calculée dans la requête. À l'étape 0, le job n'a pas encore tourné : la requête normalise elle-même le texte et les mots (§3.5) ; après l'extraction, les tables thématiques fournissent les colonnes `*_normalized` et les requêtes n'ont plus à normaliser. Le dénominateur est `stats_factiva_articles` (nombre d'articles par titre et par jour). Pas de filtre `word_count` ici : la courbe mesure l'agitation des médias sur tous les articles, comme son dénominateur.

```sql
-- Média : presse écrite
-- Question : PE - V1 - Courbe hebdomadaire <sujet>
-- Description : articles dont le titre, le chapô ou le corps contient un mot du dictionnaire
--   <thematique>, rapportés au nombre total d'articles de la semaine, par semaine (lundi, heure de Paris).
-- Tables : public.factiva_articles (hors DUP), public.stats_factiva_articles
WITH bornes(t0, t1) AS (
    VALUES (
        date '2025-01-06',                                -- premier lundi après le début des données
        date_trunc('week', current_date - 7)::date        -- lundi de la dernière semaine complète ingérée (§2)
    )
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
        date_trunc('week', a.publication_datetime AT TIME ZONE 'Europe/Paris')::date AS semaine,
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
    -- une ligne par titre et par jour, sommées par semaine
    SELECT
        date_trunc('week', s.publication_datetime AT TIME ZONE 'Europe/Paris')::date AS semaine,
        SUM(s.count) AS nb_articles_total
    FROM public.stats_factiva_articles s
    CROSS JOIN bornes b
    WHERE s.publication_datetime >= b.t0::timestamp AT TIME ZONE 'Europe/Paris'
      AND s.publication_datetime <  b.t1::timestamp AT TIME ZONE 'Europe/Paris'
    GROUP BY 1
)
SELECT
    t.semaine,
    COALESCE(n.nb_articles_<thematique>, 0) AS nb_articles_<thematique>,
    t.nb_articles_total,
    100.0 * COALESCE(n.nb_articles_<thematique>, 0) / NULLIF(t.nb_articles_total, 0) AS pct_articles_<thematique>
FROM total t
LEFT JOIN <thematique> n USING (semaine)
ORDER BY t.semaine;
```

Affichage : courbe, `semaine` en x, `pct_articles_<thematique>` en y, annotée des événements.


### 7.3 Axe par chaîne, audiovisuel

Base Baromètre. Une question par maillon, qui regroupe tous ses axes : `AV - A - Constat`, `AV - B - Causes`, `AV - C - Conséquences`, `AV - D - Solutions` (exemples : questions 2429, 2426, 2436 et 2434 des inondations, construites sur ce modèle). Unité : chunk thématique de 2 minutes ; la table n'a qu'une ligne par chunk thématique. Un axe compte s'il apparaît dans le chunk ou dans ses voisins, les chunks précédent et suivant de la même chaîne (`plaintext_normalized_prev`, `plaintext_normalized_next`), même s'ils sont eux-mêmes thématiques : pas de dédoublonnage des voisins (§3.4). Les trois textes sont lus ensemble, séparés par ` /// ` pour qu'un terme ne soit pas lu à cheval sur deux chunks ; un voisin absent est ignoré (`concat_ws`). Ces requêtes ne calculent pas l'axe sur le chunk seul, pour rester rapides. Une ligne par période et par chaîne. La variable `{{period}}` (plusieurs valeurs possibles) reste toujours dans la requête : sans valeur, toutes les périodes ; quand la requête est trop longue, on la lance période par période (§7.10). Chaque chaîne porte son secteur et son type, lus dans `curated.chaine` (jointure `chaine_id = channel_name`), pour les ventilations public / privé et par type de média (§4.1) : `media_sector` vaut `Public` ou `Privé`, `media_type` vaut `Télévision généraliste`, `Info en continu` ou `Radio généraliste` (une radio d'information en continu, comme France Info, compte en information en continu). Un axe de plus : une regex, un drapeau et deux colonnes de plus. Un axe lu dans le dictionnaire OME (A5 des inondations) se construit dans une CTE sur `public.dictionary`, comme dans la question 2429.

```sql
-- Média : audiovisuel
-- Question : AV - <lettre> - <maillon>
-- Description : part des chunks thématiques <sujet> où chaque axe du maillon apparaît, dans le chunk ou ses voisins
--   (chunks précédent et suivant, même thématiques). Une ligne par période et par chaîne.
-- Tables : thematic_extracts.<thematique>_keywords_fr, curated.chaine
WITH regex AS (
    -- un drapeau par axe : regex sur le texte normalisé, mots entiers (§3.3, §3.5)
    SELECT
        '\y(terme 1|terme 2)\y' AS <axe_1>,
        '\y(terme 3|terme 4)\y' AS <axe_2>
),
-- drapeaux de chaque chunk thématique, avec ses voisins
par_chunk AS (
    SELECT
        k.period, k.channel_name, k.channel_title,
        f.fenetre ~ r.<axe_1> AS <axe_1>,
        f.fenetre ~ r.<axe_2> AS <axe_2>
    FROM thematic_extracts.<thematique>_keywords_fr k
    CROSS JOIN regex r
    -- chunk et voisins séparés par ' /// ' : un terme n'est pas lu à cheval sur deux chunks
    CROSS JOIN LATERAL (SELECT concat_ws(' /// ', k.plaintext_normalized_prev, k.plaintext_normalized,
                                         k.plaintext_normalized_next) AS fenetre) f
    [[WHERE k.period IN ({{period}})]]  -- variable Period, liste déroulante des id du YAML
)
SELECT
    p.period,
    p.channel_title,
    CASE WHEN c.est_service_public THEN 'Public' ELSE 'Privé' END AS media_sector,
    CASE WHEN c.est_info_continue THEN 'Info en continu'
         WHEN c.est_radio THEN 'Radio généraliste'
         ELSE 'Télévision généraliste' END AS media_type,
    COUNT(*)                                             AS nb_chunks_<thematique>,
    COUNT(*) FILTER (WHERE <axe_1>)                      AS nb_chunks_<axe_1>,
    100.0 * COUNT(*) FILTER (WHERE <axe_1>) / COUNT(*)   AS pct_<axe_1>,
    COUNT(*) FILTER (WHERE <axe_2>)                      AS nb_chunks_<axe_2>,
    100.0 * COUNT(*) FILTER (WHERE <axe_2>) / COUNT(*)   AS pct_<axe_2>
FROM par_chunk p
LEFT JOIN curated.chaine c ON c.chaine_id = p.channel_name
GROUP BY p.period, p.channel_title, media_sector, media_type
ORDER BY p.period, p.channel_title;
```

Affichage : classement des chaînes en barres, `pct_<axe>` ; une chaîne sous 30 chunks ne se lit qu'avec sa famille (§3.7).

### 7.4 Axe par titre, presse

Base Print Media DB. Unité : article d'au moins 100 mots (§2). Un axe compte s'il apparaît dans le titre, le chapô ou le corps (§3.4). Les colonnes `*_normalized` sont déjà normalisées par le job.

```sql
-- Média : presse écrite
-- Question : PE - <code> - <axe> <sujet>
-- Description : part des articles d'au moins 100 mots où l'axe apparaît dans le titre, le chapô
--   ou le corps. Une ligne = un titre de presse.
-- Tables : thematic_extracts.<thematique>_articles_fr, public.source_classification
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
    WHERE a.word_count >= 100
      [[AND a.period = {{period}}]]  -- variable Period, liste déroulante des id du YAML
)
SELECT
    ar.source_name,
    sc.source_type,
    COUNT(*)                                         AS nb_articles_<thematique>,
    COUNT(*) FILTER (WHERE ar.<axe>)                 AS nb_articles_<axe>,
    100.0 * COUNT(*) FILTER (WHERE ar.<axe>) / COUNT(*) AS pct_<axe>
FROM articles ar
LEFT JOIN public.source_classification sc USING (source_code)
GROUP BY ar.source_name, sc.source_type
ORDER BY pct_<axe> DESC;
```

Affichage : classement des titres en barres colorées par famille ; un titre sous 30 articles ne se lit qu'avec sa famille (§3.7).

### 7.5 Posts LinkedIn de QuotaClimat

Base db_wordpress (MySQL, Metabase id 5). Étape 2, brainstorm des axes : ce que QuotaClimat a déjà publié sur le sujet, avec l'engagement de chaque post, pour y piocher des angles. `LIKE` ignore la casse et les accents (collation MySQL) : `'%inondation%'` trouve aussi « Inondations ». Exemple : question 2433 (inondations).

```sql
-- Source : posts LinkedIn de QuotaClimat
-- Question : Brainstorm - LinkedIn <sujet>
-- Description : posts LinkedIn publiés par QuotaClimat dont le texte contient le mot du sujet, du plus récent au plus
--   ancien, avec leur engagement. Sert au cadrage : voir ce qu'on a déjà dit sur le sujet et y piocher des angles.
--   Une ligne = un post.
-- Tables : db_wordpress.wp_linkedin_posts
SELECT
    published_at,
    content,
    url,
    likes_count,
    comments_count,
    shares_count,
    impression_count,
    engagement_rate
FROM wp_linkedin_posts
WHERE content LIKE '%<mot>%'  -- un LIKE par mot : OR content LIKE '%<mot 2>%'
ORDER BY published_at DESC;
```

Affichage : tableau.

### 7.6 Relecture humaine des chunks et voisins, audiovisuel

Base Baromètre. Contrôle de précision (§3.6), lu par la personne responsable de l'analyse : 25 chunks thématiques où l'axe choisi est détecté, dans le chunk ou ses voisins, tirés au hasard à chaque exécution, avec le texte des chunks précédent et suivant et le premier terme trouvé dans chacun. `trouve_dans` dit si le terme est dans le chunk ou seulement chez un voisin ; `keywords_found` dit pourquoi le chunk est dans le corpus. Les regex sont celles des questions d'axes, recopiées : à resynchroniser quand leurs termes changent. La variable `axe` (texte, obligatoire) s'affiche en liste déroulante statique des codes d'axe, à saisir dans les réglages de la variable : le connecteur ne sait pas la remplir. Exemple : question 2488 (inondations).

```sql
-- Média : audiovisuel
-- Question : AV - CTRL - Relecture humaine chunks et voisins par axe
-- Description : 25 chunks thématiques <sujet> détectés pour l'axe choisi (dans le chunk ou ses voisins), tirés au hasard
--   à chaque exécution, avec le texte des chunks précédent et suivant et le premier terme trouvé dans chacun.
--   Regex copiées des questions d'axes : à resynchroniser si leurs termes changent.
-- Tables : thematic_extracts.<thematique>_keywords_fr
WITH regex(axe, motif) AS (
    -- une ligne par axe : code (valeur de la liste déroulante) et regex de la question d'axe
    VALUES
        ('<axe 1>', '\y(terme 1|terme 2)\y'),
        ('<axe 2>', '\y(terme 3|terme 4)\y')
),
motif AS (
    SELECT axe, motif FROM regex WHERE axe = {{axe}}
),
-- drapeaux de chaque chunk thématique pour l'axe choisi : chunk seul, et avec ses voisins (comme §7.3)
drapeaux AS (
    SELECT
        k.period, k.channel_title, k.start, k.keywords_found,
        k.plaintext_normalized, k.plaintext_normalized_prev, k.plaintext_normalized_next,
        k.plaintext_normalized ~ m.motif AS trouve,
        concat_ws(' /// ', k.plaintext_normalized_prev, k.plaintext_normalized, k.plaintext_normalized_next) ~ m.motif
            AS trouve_fenetre
    FROM thematic_extracts.<thematique>_keywords_fr k
    CROSS JOIN motif m
),
-- 25 chunks détectés, tirés au hasard : nouvel échantillon à chaque exécution
echantillon AS (
    SELECT d.*
    FROM drapeaux d
    WHERE d.trouve_fenetre
    ORDER BY random()
    LIMIT 25
)
SELECT
    m.axe,
    e.period,
    e.channel_title,
    e.start,
    CASE WHEN e.trouve THEN 'chunk' ELSE 'voisin seulement' END AS trouve_dans,
    e.keywords_found,
    substring(e.plaintext_normalized      FROM m.motif) AS terme_chunk,
    substring(e.plaintext_normalized_prev FROM m.motif) AS terme_avant,
    substring(e.plaintext_normalized_next FROM m.motif) AS terme_apres,
    e.plaintext_normalized      AS texte_chunk,
    e.plaintext_normalized_prev AS texte_avant,
    e.plaintext_normalized_next AS texte_apres
FROM echantillon e
CROSS JOIN motif m
ORDER BY e.period, e.channel_title, e.start;
```

Affichage : tableau, ou export CSV ou JSON lu dans la visionneuse ci-dessous.

**Visionneuse.** [`visionneuse_axes_thematiques.html`](visionneuse_axes_thematiques.html), à côté de ce fichier : page HTML autonome, à ouvrir dans un navigateur, sans serveur. On y glisse l'export de la question, ou on le colle. Chaque ligne devient une carte : le chunk et ses voisins en blocs dépliables, les termes surlignés (jaune : termes de l'axe ; violet : mots qui ont rendu le chunk thématique ; vert : liste libre ; bleu : recherche). Un verdict par carte (validé, faux positif, incertain ; touches `v`, `f`, `i`, et `j` / `k` pour passer d'une carte à l'autre) donne la précision de l'axe avec son intervalle de confiance à 95 % (Wilson), ventilée par `trouve_dans` ; les incertains sont hors calcul. Rien n'est envoyé : les verdicts restent dans le navigateur (clé de la ligne et verdict, jamais le texte) et s'exportent en CSV, à reporter dans le Google Sheets des résultats.

### 7.7 Relecture humaine des articles, presse

Base Print Media DB. Contrôle de précision (§3.6), lu par la personne responsable de l'analyse : 25 articles d'au moins 100 mots où l'axe choisi apparaît dans le titre, le chapô ou le corps, tirés au hasard à chaque exécution, avec le texte d'origine (`factiva_articles`, jointure sur `An`) et le premier terme trouvé dans chaque partie. `trouve_dans` dit dans quelle partie l'axe apparaît en premier (titre, chapô, corps) ; « à cheval » quand le terme n'est lu que sur la jonction de deux parties, comme le permet le texte concaténé des requêtes d'axes (§7.4). `keywords_found` dit pourquoi l'article est dans le corpus. Les regex sont celles des questions d'axes presse, recopiées : à resynchroniser quand leurs termes changent ; un axe lu dans le dictionnaire OME (A5 des inondations) se construit dans une CTE sur `public.dictionary`, comme dans la question 2463. La variable `axe` (texte, obligatoire) s'affiche en liste déroulante statique des codes d'axe, à saisir dans les réglages de la variable ; `media` (facultative) filtre le média avec `ILIKE` (`%figaro%`). L'export se lit dans la visionneuse d'articles, [`visionneuse_articles.html`](visionneuse_articles.html), à côté de ce fichier : mêmes verdicts et même compteur de précision que la visionneuse de l'audiovisuel (§7.6), termes surlignés sur le texte d'origine, touche `n` pour passer au terme suivant d'un article. Exemple : question 2548 (inondations).

```sql
-- Média : presse écrite
-- Question : PE - CTRL - Relecture humaine articles par axe
-- Description : 25 articles <sujet> d'au moins 100 mots où l'axe choisi apparaît (titre, chapô ou corps), tirés au
--   hasard à chaque exécution, avec le texte d'origine et le premier terme trouvé dans chaque partie.
--   Regex copiées des questions d'axes presse : à resynchroniser si leurs termes changent.
-- Tables : thematic_extracts.<thematique>_articles_fr, public.factiva_articles, public.source_classification
WITH regex(axe, motif) AS (
    -- une ligne par axe : code (valeur de la liste déroulante) et regex de la question d'axe
    VALUES
        ('<axe 1>', '\y(terme 1|terme 2)\y'),
        ('<axe 2>', '\y(terme 3|terme 4)\y')
),
motif AS (
    SELECT axe, motif FROM regex WHERE axe = {{axe}}
),
-- articles d'au moins 100 mots où l'axe apparaît dans le titre, le chapô ou le corps (comme §7.4)
detectes AS (
    SELECT
        a.an, a.period, a.publication_datetime, a.source_name, a.word_count, a.keywords_found,
        a.title_normalized, a.snippet_normalized, a.body_normalized,
        sc.source_type,
        COALESCE(sc.media_all, a.source_name) AS media_all
    FROM thematic_extracts.<thematique>_articles_fr a
    LEFT JOIN public.source_classification sc ON sc.source_code = a.source_code
    CROSS JOIN motif m
    WHERE a.word_count >= 100
      AND concat_ws(' ', a.title_normalized, a.snippet_normalized, a.body_normalized) ~ m.motif
      [[AND COALESCE(sc.media_all, a.source_name) ILIKE {{media}}]]
      [[AND a.period = {{period}}]]
),
-- 25 articles détectés, tirés au hasard : nouvel échantillon à chaque exécution
echantillon AS (
    SELECT d.*
    FROM detectes d
    ORDER BY random()
    LIMIT 25
)
SELECT
    m.axe,
    e.period,
    COALESCE(e.source_type, 'Non classé') AS source_type,
    e.media_all,
    e.source_name,
    e.publication_datetime,
    e.an,
    e.word_count,
    -- première partie où l'axe apparaît ; à cheval : terme lu sur la jonction de deux parties
    CASE WHEN e.title_normalized   ~ m.motif THEN 'titre'
         WHEN e.snippet_normalized ~ m.motif THEN 'chapô'
         WHEN e.body_normalized    ~ m.motif THEN 'corps'
         ELSE 'à cheval' END AS trouve_dans,
    e.keywords_found,
    substring(e.title_normalized   FROM m.motif) AS terme_titre,
    substring(e.snippet_normalized FROM m.motif) AS terme_chapo,
    substring(e.body_normalized    FROM m.motif) AS terme_corps,
    -- texte d'origine (factiva_articles), texte normalisé s'il manque
    COALESCE(f.title,   e.title_normalized)   AS titre,
    COALESCE(f.snippet, e.snippet_normalized) AS chapo,
    COALESCE(f.body,    e.body_normalized)    AS corps
FROM echantillon e
CROSS JOIN motif m
LEFT JOIN LATERAL (
    SELECT fa.title, fa.snippet, fa.body
    FROM public.factiva_articles fa
    WHERE fa."An" = e.an
    LIMIT 1
) f ON true
ORDER BY e.period, e.source_type, e.media_all, e.publication_datetime;
```

Affichage : export CSV ou JSON lu dans la visionneuse d'articles.

### 7.8 Volume par chaîne, audiovisuel

Base Baromètre. Axe V2 : nombre de chunks thématiques et minutes correspondantes (2 minutes par chunk), une ligne par période et par chaîne. La table n'a qu'une ligne par chunk thématique : pas de filtre à poser. Secteur et type de la chaîne lus dans `curated.chaine`, comme au §7.3. Exemple : question 2427 (inondations).

```sql
-- Média : audiovisuel
-- Question : AV - V2 - Volume
-- Description : nombre de chunks thématiques <sujet> et minutes correspondantes (2 min par chunk), par chaîne.
--   Une ligne par période et par chaîne.
-- Tables : thematic_extracts.<thematique>_keywords_fr, curated.chaine
SELECT
    k.period,
    k.channel_title,
    CASE WHEN c.est_service_public THEN 'Public' ELSE 'Privé' END AS media_sector,
    CASE WHEN c.est_info_continue THEN 'Info en continu'
         WHEN c.est_radio THEN 'Radio généraliste'
         ELSE 'Télévision généraliste' END AS media_type,
    COUNT(*)     AS nb_chunks_<thematique>,
    2 * COUNT(*) AS nb_minutes_<thematique>
FROM thematic_extracts.<thematique>_keywords_fr k
LEFT JOIN curated.chaine c ON c.chaine_id = k.channel_name
[[WHERE k.period IN ({{period}})]]  -- variable Period, liste déroulante des id du YAML
GROUP BY k.period, k.channel_title, media_sector, media_type
ORDER BY k.period, k.channel_title;
```

Affichage : tableau, ou classement des chaînes en barres.

### 7.9 Volume par média, presse

Base Print Media DB. Axe V2 : nombre d'articles par média et part dans le total des articles du média sur la période. Un média (`media_all`) additionne ses éditions papier et web ; une source absente de `source_classification` garde son `source_name`. Deux comptes : `nb_articles_<thematique>` (articles d'au moins 100 mots, le corpus des axes, §2) et `nb_articles_<thematique>_tous` (tous les articles). La part se calcule sur tous les articles, comme son dénominateur `stats_factiva_articles` (§7.2), sur les jours de Paris. Les bornes des périodes sont recopiées du YAML : à resynchroniser quand l'entrée change. Exemple : question 2480 (inondations).

```sql
-- Média : presse écrite
-- Question : PE - V2 - Volume
-- Description : articles <sujet> par média, en nombre et en part du total des articles du média sur la période
--   (stats_factiva_articles, tous les articles). Une ligne par période et par média (media_all : éditions papier et
--   web additionnées ; source_name pour une source non classée). Périodes recopiées du YAML.
-- Tables : thematic_extracts.<thematique>_articles_fr, public.stats_factiva_articles, public.source_classification
WITH periodes(period, debut, fin) AS (
    -- entrée <thematique> de thematic_jobs_config.yml : id, start (inclus), end (exclu)
    VALUES
        ('<id 1>', date '<AAAA-MM-JJ>', date '<AAAA-MM-JJ>'),
        ('<id 2>', date '<AAAA-MM-JJ>', date '<AAAA-MM-JJ>')
),
-- correspondance source / média : toutes les éditions classées, plus les sources non classées lues dans les articles
medias AS (
    SELECT sc.source_code, sc.media_all FROM public.source_classification sc
    UNION
    SELECT DISTINCT a.source_code, a.source_name
    FROM thematic_extracts.<thematique>_articles_fr a
    WHERE NOT EXISTS (SELECT 1 FROM public.source_classification sc WHERE sc.source_code = a.source_code)
),
-- total des articles de chaque média sur chaque période (jours de Paris), toutes éditions additionnées
total AS (
    SELECT pe.period, m.media_all, SUM(s.count) AS nb_articles_total
    FROM periodes pe
    JOIN public.stats_factiva_articles s
      ON (s.publication_datetime AT TIME ZONE 'Europe/Paris')::date >= pe.debut
     AND (s.publication_datetime AT TIME ZONE 'Europe/Paris')::date <  pe.fin
    JOIN medias m ON m.source_code = s.source_code
    GROUP BY pe.period, m.media_all
),
-- articles du sujet : d'au moins 100 mots, et tous (le total de stats_factiva_articles n'a pas de filtre de longueur)
agregat AS (
    SELECT
        a.period,
        COALESCE(sc.source_type, 'Non classé')     AS source_type,
        COALESCE(sc.media_all, a.source_name)       AS media_all,
        sc.source_region,
        COUNT(*) FILTER (WHERE a.word_count >= 100) AS nb_articles_<thematique>,
        COUNT(*)                                    AS nb_articles_<thematique>_tous
    FROM thematic_extracts.<thematique>_articles_fr a
    LEFT JOIN public.source_classification sc ON sc.source_code = a.source_code
    [[WHERE a.period IN ({{period}})]]  -- variable Period, liste déroulante des id du YAML
    GROUP BY a.period, COALESCE(sc.source_type, 'Non classé'), COALESCE(sc.media_all, a.source_name), sc.source_region
)
SELECT
    ag.period,
    ag.source_type,
    ag.media_all,
    ag.source_region,
    ag.nb_articles_<thematique>,
    ag.nb_articles_<thematique>_tous,
    tt.nb_articles_total,
    100.0 * ag.nb_articles_<thematique>_tous / NULLIF(tt.nb_articles_total, 0) AS pct_articles_<thematique>
FROM agregat ag
LEFT JOIN total tt ON tt.media_all = ag.media_all AND tt.period = ag.period
ORDER BY ag.period, ag.source_type, ag.media_all;
```

Affichage : tableau, classement des médias en barres colorées par famille, ou carte par `source_region` pour la presse régionale.

### 7.10 Liste des périodes

Base Baromètre. Les `id` des périodes réellement extraites, lus dans la table thématique audiovisuelle. Deux usages :

- **Liste déroulante de la variable `{{period}}`** des autres questions, audiovisuel et presse (conventions Metabase, §1) : elle se choisit dans les réglages de la variable (liste déroulante, depuis une question, colonne `period`), le connecteur ne sait pas la brancher. En presse, une période sans article (avant 2025) ne renvoie rien.
- **Diviser pour mieux régner** : une requête trop longue sur toutes les périodes se lance période par période, en choisissant chaque valeur de la liste dans sa variable `{{period}}`.

La question se nomme `<thematique> period filters`, à la racine de la collection de l'analyse. Exemple : question 2435 (inondations).

```sql
-- Média : audiovisuel
-- Question : <thematique> period filters
-- Description : id des périodes extraites dans la table thématique, pour la liste déroulante de la variable Period
--   des autres questions. Une ligne = une période.
-- Tables : thematic_extracts.<thematique>_keywords_fr
SELECT DISTINCT period
FROM thematic_extracts.<thematique>_keywords_fr
ORDER BY 1;
```

Affichage : tableau.

### 7.11 Pareto des termes, audiovisuel

Base Baromètre. Contrôle : nombre d'occurrences de chaque terme, du plus dit au moins dit, avec la part cumulée, pour repérer les termes qui portent le corpus ou un axe à eux seuls et ceux qui ne sont jamais dits. La variable `{{maillon}}` choisit les termes :

- `noyau` : les termes du dictionnaire noyau ;
- `all` (tous les axes) ou un maillon (`constat`, `causes`, `consequences`, `solutions`, `themes`) : les termes des axes.

Les occurrences se comptent dans le texte (`regexp_count`), pas dans `keywords_found`. Chaque chunk, thématique ou voisin, compte une fois, repéré par sa chaîne et son heure, avec le texte du chunk thématique en priorité (§3.4). `all` laisse de côté le noyau, qui écraserait le reste, et les axes qui reprennent les termes d'un autre (B1 des inondations répète A1). La part cumulée se calcule sur les termes affichés. Les termes sont recopiés des questions d'axes au moment de la création ; on ne les resynchronise pas d'office quand un terme change. Un axe lu dans `public.dictionary` s'ajoute par un `UNION ALL` dans la CTE `termes`, avec ses mots normalisés et échappés (CTE `mots_a5` de la question 2430). Requête lourde : la lancer période par période (§7.10). Question à la racine de la collection, `AV - CTRL - Pareto par mot clé`. Exemple : question 2430 (inondations).

```sql
-- Média : audiovisuel
-- Question : AV - CTRL - Pareto par mot clé
-- Description : occurrences de chaque terme du dictionnaire noyau <sujet> et des axes dans les chunks thématiques
--   et leurs voisins (chaque chunk une fois), en Pareto. Variable maillon : all (tous les axes, sans le noyau ni les
--   axes qui en répètent un autre), noyau, constat, causes, consequences, solutions ou themes ; la part cumulée se
--   calcule sur les termes affichés. Une ligne = un terme.
-- Tables : thematic_extracts.<thematique>_keywords_fr
WITH filtre AS (
    SELECT lower(COALESCE(NULLIF(trim({{maillon}}), ''), 'all')) AS m
),
-- un terme par ligne, sous forme normalisée (§3.5) : maillon, code de l'axe, terme
termes_ecrits(maillon, axe, terme) AS (
    VALUES
        -- dictionnaire noyau (entrée <thematique> > keywords > french > audiovisuel du YAML, avec pluriels)
        ('noyau', 'noyau', '<terme>'), ('noyau', 'noyau', '<terme>'),
        -- A1 <nom de l'axe>
        ('constat', 'A1', '<terme>'), ('constat', 'A1', '<terme>'),
        -- B1 <nom de l'axe>
        ('causes', 'B1', '<terme>'),
        -- C1 <nom de l'axe>
        ('consequences', 'C1', '<terme>'),
        -- D1 <nom de l'axe>
        ('solutions', 'D1', '<terme>'),
        -- T1 <nom du thème>
        ('themes', 'T1', '<terme>')
),
termes AS (
    SELECT t.*
    FROM termes_ecrits t
    CROSS JOIN filtre f
    -- all : tous les axes, sans le noyau ni les axes qui en répètent un autre
    WHERE (f.m = 'all' AND t.axe NOT IN ('noyau' /*, '<axe répété>' */)) OR t.maillon = f.m
),
-- chaque chunk une fois, thématique ou voisin, le texte du chunk thématique en priorité
chunks AS (
    SELECT DISTINCT ON (k.period, k.channel_name, t.start) t.texte
    FROM thematic_extracts.<thematique>_keywords_fr k
    CROSS JOIN LATERAL (VALUES
        (k.start,      k.plaintext_normalized,      true),
        (k.start_prev, k.plaintext_normalized_prev, false),
        (k.start_next, k.plaintext_normalized_next, false)
    ) AS t(start, texte, est_thematique)
    WHERE t.start IS NOT NULL  -- côté sans voisin
      [[AND k.period IN ({{period}})]]  -- variable Period, liste déroulante des id du YAML
    ORDER BY k.period, k.channel_name, t.start, t.est_thematique DESC
),
-- occurrences de chaque terme : chaque paire terme × chunk n'est testée qu'une fois
occurrences AS (
    SELECT
        t.maillon,
        t.axe,
        t.terme,
        COALESCE(SUM(n.nb), 0)           AS nb_occurrences,
        COUNT(*) FILTER (WHERE n.nb > 0) AS nb_chunks
    FROM termes t
    CROSS JOIN chunks c
    CROSS JOIN LATERAL (SELECT regexp_count(c.texte, '\y(' || t.terme || ')\y') AS nb) n
    GROUP BY t.maillon, t.axe, t.terme
)
SELECT
    ROW_NUMBER() OVER (ORDER BY nb_occurrences DESC, axe, terme) AS rang,
    axe,
    terme,
    nb_occurrences,
    nb_chunks,
    100.0 * SUM(nb_occurrences) OVER (ORDER BY nb_occurrences DESC, axe, terme ROWS UNBOUNDED PRECEDING)
          / NULLIF(SUM(nb_occurrences) OVER (), 0) AS pct_cumule,
    maillon
FROM occurrences
ORDER BY rang;
```

Variables : `maillon` (texte, requise, `all` par défaut), `period` (texte, liste déroulante du §7.10). Affichage : combo, barres pour `nb_occurrences` et courbe pour `pct_cumule` (axe de droite), abscisse `terme` en échelle ordinale.

### 7.12 Pareto des termes, presse

Base Print Media DB. Même requête que le §7.11, sur les articles : le texte compté est `concat_ws(' ', title_normalized, snippet_normalized, body_normalized)`, des articles d'au moins 100 mots (le corpus des axes, §2), chaque article une fois ; `nb_chunks` devient `nb_articles`. Les termes des axes sont ceux de l'audiovisuel (les questions d'axes presse reprennent les mêmes regex) ; ceux du noyau sont les regex presse de l'entrée du YAML (`keywords > french > presse`), écrites sous forme normalisée avec leurs variantes (`crues?`, `coulees? de boue`). Requête lourde, les corps d'articles étant longs : la lancer période par période. Question dans la sous-collection `PE`, `PE - CTRL - Pareto par mot clé`. Exemple : question 2549 (inondations).

```sql
-- Média : presse écrite
-- Question : PE - CTRL - Pareto par mot clé
-- Description : occurrences de chaque terme du dictionnaire noyau <sujet> (regex presse du YAML) et des axes dans les
--   articles <sujet> d'au moins 100 mots (titre, chapô et corps, chaque article une fois), en Pareto. Variable maillon :
--   all (tous les axes, sans le noyau ni les axes qui en répètent un autre), noyau, constat, causes, consequences,
--   solutions ou themes ; la part cumulée se calcule sur les termes affichés. Une ligne = un terme.
-- Tables : thematic_extracts.<thematique>_articles_fr
WITH filtre AS (
    SELECT lower(COALESCE(NULLIF(trim({{maillon}}), ''), 'all')) AS m
),
-- un terme par ligne, sous forme normalisée (§3.5) : maillon, code de l'axe, terme
termes_ecrits(maillon, axe, terme) AS (
    VALUES
        -- dictionnaire noyau (entrée <thematique> > keywords > french > presse du YAML)
        ('noyau', 'noyau', '<terme>'), ('noyau', 'noyau', '<terme>'),
        -- A1 <nom de l'axe> (mêmes termes que le §7.11)
        ('constat', 'A1', '<terme>'),
        -- T1 <nom du thème>
        ('themes', 'T1', '<terme>')
),
termes AS (
    SELECT t.*
    FROM termes_ecrits t
    CROSS JOIN filtre f
    -- all : tous les axes, sans le noyau ni les axes qui en répètent un autre
    WHERE (f.m = 'all' AND t.axe NOT IN ('noyau' /*, '<axe répété>' */)) OR t.maillon = f.m
),
-- chaque article une fois : titre, chapô et corps, articles d'au moins 100 mots (corpus des axes)
articles AS (
    SELECT concat_ws(' ', a.title_normalized, a.snippet_normalized, a.body_normalized) AS texte
    FROM thematic_extracts.<thematique>_articles_fr a
    WHERE a.word_count >= 100
      [[AND a.period = {{period}}]]  -- variable Period, liste déroulante des id du YAML
),
-- occurrences de chaque terme : chaque paire terme × article n'est testée qu'une fois
occurrences AS (
    SELECT
        t.maillon,
        t.axe,
        t.terme,
        COALESCE(SUM(n.nb), 0)           AS nb_occurrences,
        COUNT(*) FILTER (WHERE n.nb > 0) AS nb_articles
    FROM termes t
    CROSS JOIN articles ar
    CROSS JOIN LATERAL (SELECT regexp_count(ar.texte, '\y(' || t.terme || ')\y') AS nb) n
    GROUP BY t.maillon, t.axe, t.terme
)
SELECT
    ROW_NUMBER() OVER (ORDER BY nb_occurrences DESC, axe, terme) AS rang,
    axe,
    terme,
    nb_occurrences,
    nb_articles,
    100.0 * SUM(nb_occurrences) OVER (ORDER BY nb_occurrences DESC, axe, terme ROWS UNBOUNDED PRECEDING)
          / NULLIF(SUM(nb_occurrences) OVER (), 0) AS pct_cumule,
    maillon
FROM occurrences
ORDER BY rang;
```

Variables et affichage : comme au §7.11.
