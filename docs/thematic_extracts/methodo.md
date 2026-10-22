# Méthodologie des analyses thématiques

Règles communes à toutes les analyses sur les tables `thematic_extracts`. Une règle propre à un sujet va dans le `decisions.md` du sujet ; quand elle sert à plusieurs sujets, on la remonte ici.

Les analyses incendies et vague de chaleur 2026 servent de modèle pour la démarche, les axes contextuels et la restitution. Leur corpus noyau a été construit avant la révision, directement sur `keywords` et `factiva_articles`. Leurs résultats restent réutilisables et comparables à ceux des analyses suivantes.

Statuts : **adoptée** (s'applique), **provisoire** (s'applique, à valider).

## 1. Corpus noyau (adoptée)

- Le corpus noyau d'un thème est construit uniquement à partir de mots du dictionnaire OME, directs (HRFP) ou indirects.
- La liste de chaque thème est fixe et centralisée dans `quotaclimat/data_processing/thematic_extracts/thematic_dict.yml`, sous la forme `[thème][langue][audiovisuel|presse]`. Un thème récurrent (incendies, inondations...) utilise toujours la même liste.
  - Audiovisuel : termes sous leur forme exacte dans le dictionnaire OME (détection faite en amont dans `keywords`).
  - Presse : toutes les variantes (genre, nombre) des termes OME, détectées par regex. Une expression qui en contient une autre (« gestion des inondations » et « inondations ») n'ajoute aucun article au corpus ; on la garde si elle est non-HRFP, car `keywords_found` l'affiche alors en priorité et indique par quel terme l'article est entré dans `factiva_articles`.
- La correspondance entre `thematic_dict.yml` et le dictionnaire OME est vérifiée à la main à la création ou à la modification d'une entrée.
- Le corpus noyau ne contient pas de mot absent du dictionnaire OME. Si le besoin se présente, se demander s'il devrait entrer dans le dictionnaire OME.
- On ne filtre pas le corpus sur les contenus validés « environnement » par l'OME, pour éviter la circularité : mesurer la recontextualisation environnementale sur des contenus déjà sélectionnés pour leur caractère environnemental.

## 2. Période (adoptée)

- **Période d'analyse** : une fenêtre d'actualité chaude sur le sujet, justifiée par une source datée (Météo-France, préfecture, presse). C'est elle qui limite les faux positifs d'un dictionnaire noyau large.
- **Période de comparaison** : un épisode similaire (même aléa, actualité comparable), de durée équivalente ou normalisée.

## 3. Axes d'analyse contextuelle (adoptée)

- Chaque axe a son propre dictionnaire de termes explicites. Ces termes n'ont pas à appartenir au dictionnaire OME, puisqu'un axe peut porter sur l'économie, les transports, la santé...
- Les axes qui ne dépendent pas du sujet et leurs termes sont dans `axes_generiques.md`. Chaque analyse liste dans son propre `axes.md` tous les axes appliqués : pour un axe générique, un renvoi vers la banque et les éventuels ajouts ou retraits de termes ; pour un axe spécifique, ses termes. Les axes génériques écartés y sont listés avec leur raison.
- Un axe spécifique remonte dans `axes_generiques.md` quand il ne dépend pas du sujet, ou dès qu'une deuxième analyse le réutilise.
- Les termes sont écrits sous forme normalisée (§5).
- Dans le SQL, chaque axe est un drapeau (une regex). Réutiliser les regex existantes avant d'en écrire.

## 4. Recontextualisation (adoptée)

- **Audiovisuel** : un axe compte s'il apparaît dans le segment thématique ou dans un segment adjacent de la même chaîne (±2 minutes). Les segments adjacents sont dans la table thématique (`thematic_chunk = false`) et ne comptent jamais dans le volume du corpus.
- **Presse** : l'article entier (titre, chapô, corps).

## 5. Normalisation du texte (adoptée)

Les tables thématiques contiennent le texte d'origine (`title`, `snippet`, `body` en presse, `plaintext` en audiovisuel). La normalisation appliquée par le job est à reproduire dans chaque requête, avant toute regex :

1. minuscules ;
2. accents retirés ;
3. apostrophes (`'`, `’`, `‘`, `ʼ`) remplacées par un espace, jamais supprimées ;
4. recherche en mots entiers (`\y...\y` en Postgres).

Les termes des axes contextuels sont écrits directement sous forme normalisée : `secheresse`, `manque d eau`, `rechauffement climatique`.

Les traits d'union ne sont pas normalisés. Un terme qui peut s'écrire avec ou sans trait d'union est listé sous ses deux formes : `pare feu` et `pare-feu`.

🔴 [préciser l'équivalent Postgres de `strip_accents` (DuckDB) utilisé dans Metabase, et vérifier qu'il traite œ, æ, ç comme le job]

## 6. Contrôles (étape 1)

- **Complétude (adoptée)** : comparer la table thématique avec la table source sur la même période et les mêmes termes. Tout écart doit être expliqué.
- **Précision (provisoire)** : sur un échantillon de contenus par terme fréquent, relire et compter les faux positifs à l'aide de la colonne de contrôle des termes détectés. Un terme trop bruité est discuté (exclusion ou contrainte) et le choix noté dans `decisions.md` §1.
- **Rappel (adoptée)** : vérifier que quelques contenus connus de l'épisode (JT du jour du pic, Unes) sont dans le corpus.
- **Périmètre cadré (provisoire)** : pour distinguer le sujet principal de la mention incidente, calculer aussi les résultats sur les contenus où un terme du noyau est dans le titre ou apparaît au moins N fois. Le seuil N retenu est noté dans `decisions.md` §1.
- **Discrimination d'un axe (provisoire)** : un axe présent dans plus de 70 % du corpus ne discrimine plus ; le redéfinir en une version large et une version stricte.
- **Robustesse (provisoire)** : recalculer les principaux résultats sur un sous-corpus plus strict (plusieurs mentions du noyau, articles longs) et vérifier qu'ils tiennent.
- **Lecture du bruit (provisoire)** : un bruit réparti uniformément dans le temps fausse les niveaux absolus mais pas les comparaisons entre périodes. Le vérifier avant de publier un niveau absolu.

Source de ces contrôles provisoires : annexes méthodologiques du Bilan OMÉ S1 2026 (voir `references.md`).

## 7. Unités et mesures (adoptée)

- Audiovisuel : unité = segment de 2 minutes ; temps d'antenne en minutes. Presse : unité = article.
- Ne jamais comparer une part de segments à une part de temps d'antenne.
- Chaque requête sort numérateurs et dénominateurs bruts en colonnes séparées, en plus des pourcentages.

## 8. Effectif minimal (adoptée)

Un média dont le corpus compte moins de **30** segments ou articles reste dans les résultats, mais on n'en tire aucune conclusion individuelle (classement, comparaison, takeaway). Il est exploité à l'intérieur de sa famille : presse nationale, presse régionale, audiovisuel public, audiovisuel privé.

## 9. Takeaway publiable (adoptée)

Un résultat est publiable si :

- le dénominateur est affiché et atteint l'effectif minimal du §8 ;
- l'écart entre périodes ou entre groupes est net, pas quelques points sur de petits effectifs ;
- la conclusion tient avec et sans la fenêtre de ±2 minutes (audiovisuel) ;
- les unités sont homogènes (§7) ;
- il est formulé avec chiffre, dénominateur et période.

Les pistes testées sans résultat sont notées dans `decisions.md` §4, jamais effacées.

## 10. Limites persistantes

- **Presse, mots HRFP** : sans réactiver l'API Factiva, on ne récupère pas tous les articles qui ne contiennent que des mots clés HRFP, car seuls les mots non-HRFP déclenchent leur recensement dans `factiva_articles`. Nous décidons de ne pas la réactiver pour l'instant.
- **Uniformité entre médias** : en audiovisuel, le corpus inclut tous les segments contenant un mot du noyau, direct ou indirect ; en presse, l'accès dépend des mots non-HRFP. À rappeler dans l'encadré méthodologique.
- **Dictionnaires d'axes** : explicites mais non exhaustifs ; ni la précision ni le rappel ne sont garantis à 100 %.
- **Valence** : détecter un thème ne dit pas si le contenu le défend ou le critique.

## 11. Encadré méthodologique (adoptée)

Chaque article publié inclut : corpus (médias, nombre de segments et d'articles), période et comparaison, lien vers l'entrée de `thematic_dict.yml` et vers le `axes.md` de l'analyse, méthode de recontextualisation, limites (§10).

## 12. Validation humaine (adoptée)

Tout contenu diffusé est relu par une personne experte du sujet. Les dictionnaires sont versionnés dans le repo, le SQL dans Metabase puis dans `<AAAA-MM>-<sujet>/queries/` après publication.
