# Consignes pour Claude

## Analyses Metabase : où mettre les requêtes SQL

Une requête SQL native n'est versionnée dans ce dépôt que si l'utilisateur le demande. Sinon, selon sa demande, on la colle dans le chat, on l'exécute avec le connecteur Metabase (`execute_sql`) ou on crée une question avec ce connecteur. Le fonctionnement des requêtes versionnées est dans `metabase/README.md`.

Pour une requête à versionner :

- **Emplacement** : `metabase/<sujet>/metabase_<nom>.sql`. Les sujets actuels sont `advertising/` (pub autour des émissions) et `mesinformation/` (mésinformation par émission). Un nouveau sujet a son propre sous-dossier et son propre `questions.yml`.
- **En-tête du fichier, en français** : ce que fait la requête et pour quel graphique, chaque variable (type Metabase, facultative ou non, valeur par défaut), chaque colonne de sortie, les tables lues. Suivre le modèle des fichiers existants.
- **Manifeste `questions.yml` du dossier** : y déclarer le fichier, chaque `{{variable}}` avec son type, et au moins un cas de test (`params`, plus `min_rows` quand les seeds de test donnent des lignes). Les données de test sont en 2025 : prendre `debut: 2025-01-01`, `fin: 2025-12-31`.
- Une requête d'exploration versionnée se supprime quand l'exploration est finie.

## Garder les identifiants des questions Metabase

La synchronisation (flow Kestra `quotaclimat/metabase-sync`) ne met à jour que les questions listées dans `questions:` des manifestes. Pour une requête versionnée, ne pas en perdre une :

- Dès qu'une question qui l'exécute est citée (numéro, URL `.../question/2358-...`, capture), créée par l'utilisateur ou par le connecteur Metabase, ajouter son numéro au `questions:` du fichier qu'elle exécute, avec un commentaire sur son rôle (`- 2364 # nuage de points, secteurs`).
- Plusieurs questions peuvent exécuter le même fichier (tableau, camembert, courbes).
- Ne jamais retirer un numéro sans l'accord de l'utilisateur.
- Avant de modifier une requête, comparer si possible le SQL de ses questions (connecteur Metabase, `get_content` avec `definition`) avec le fichier. Si l'utilisateur a modifié le SQL dans Metabase, ou colle une version différente, reporter ses changements dans le fichier avant d'ajouter les siens, et le lui signaler. Sinon la synchronisation refusera la question ou écrasera son travail.
- Quand le connecteur ne permet pas d'écrire (`question_write` demande la permission `agent:content:write`), donner le SQL à coller et demander le numéro de la question créée.

## Règles d'écriture des requêtes Metabase

- **Variables texte** : Metabase les envoie en `varchar`. Les convertir avec `CAST({{x}} AS numeric)`, `AS boolean` ou `AS date`.
- **Variables facultatives** : `COALESCE([[CAST({{x}} AS numeric),]] 0.5)`.
- **Filtres de champ** (`dimension`) : Metabase les remplace par `"schéma"."table"."colonne" IN (...)`, ou par `1 = 1` sans valeur. La table doit donc être lue sous son nom complet, sans alias (`analytics.publicites`, pas `p`).
- **Noms de colonnes** : ne pas renommer une colonne existante. Les graphiques des questions en dépendent. Ajouter une colonne ou un paramètre plutôt que changer l'existant.
- **Axe x des courbes** : un rang unique par ligne (`ROW_NUMBER`, cumuls en `ROWS`), car Metabase additionne les lignes de même abscisse.
- **Regroupement en « Autres »** : le laisser au camembert de Metabase plutôt que de l'écrire dans la requête.
- **Filtres qui choisissent des lignes** : par défaut, ils ne changent pas les valeurs (parts, rangs). Le dire dans l'en-tête quand c'est le cas.
- **Logique lourde ou réutilisée** : la mettre dans un modèle dbt plutôt que dans la requête.

## Tables dbt lues par les requêtes

- `analytics` : `program`, `cas_de_mesinformation`, `cas_theme`, `publicites`, `task_global_completion`, `dbt_objects`.
- `public` : `emission_jour`, `environmental_shares_with_desinfo_counts`, `mesinfo_theme_mots_cles` (seed).
- `advertising` : `pub_annonceur_emission_jour`, `ad_*`, seeds `ref_*`.
- `curate` : `chaine`, `publicite`, `cas_mesinformation`, `emission_jour` (voir « Schéma curate »).

Un nouveau modèle (hors `curate`) demande trois choses :
- sa documentation dans `my_dbt_project/models/schema.yml` ;
- un test dans `my_dbt_project/pytest_tests/test_dbt_model_analytics.py` ;
- sa place dans les `dbt run` de `entrypoints/dbt.sh` et `entrypoints/mediatree_import.sh`, quand il dépend des modèles advertising.

Un modèle qui change de schéma laisse l'ancienne table en place : le signaler, avec le `DROP` à faire à la main.

Les mots-clés des thèmes de cas (`my_dbt_project/seeds/themes/mesinfo_theme_mots_cles.csv`, des expressions régulières) sont versionnés et modifiés dans le code, pas dans un Google Sheet.

## Schéma curate

Le schéma `curate` (`my_dbt_project/models/curate/`) ne contient que des tables propres, pour les équipes non techniques et leur Claude, qui les lit par le MCP Metabase sans accès au dépôt. Tout ce qu'il faut pour les utiliser sans erreur doit donc être dans les tables et leur documentation.

### Ce qui entre dans curate

- Des objets réels (une publicité diffusée, un cas de mésinformation, une chaîne) ou des agrégats de haut niveau avec leurs mesures additives (durées, nombres) à côté des parts. Pas de table intermédiaire, ni de table qui se recalcule simplement à partir des autres.
- Des données déjà filtrées : pas de colonne qu'il faut filtrer pour obtenir le bon chiffre (ex. `mesinfo_choice = 'Correct'`).
- Pas de code interne du pipeline (statuts, codes de prédiction, scores non calibrés) : on le traduit (`classement_par`) ou on le laisse dans `analytics`.
- Pas d'identifiant qui pointe hors de `curate`.
- Rien dans dbt ne lit une table `curate` (test `test_curate_is_the_last_layer`).

### Nommage

- Tables et colonnes en français, en snake_case sans accent. Un modèle dont le nom existe déjà dans dbt (ex. `public.emission_jour`) s'appelle `curate_<table>` avec `alias='<table>'`.
- Une table porte le nom de son grain, au singulier : une ligne de `publicite` est une publicité diffusée. Un agrégat : `<mesure>_<dimension>_<période>` (ex. `part_environnement_chaine_jour`).
- L'unité en suffixe : `_sec`, `_min`, `_pct` (toujours de 0 à 100) ; un nombre commence par `nb_`.
- Booléens en `est_` ; heures en heure de Paris, nommées `..._paris` ; `jour` pour la date de Paris.
- Un identifiant de ligne composé (ex. émission et jour) devient une seule colonne `<table>_id`, clé des jointures entre tables (`emission_jour_id`).
- Une colonne présente dans plusieurs tables y a le même nom et la même description (`chaine_id`, `chaine_nom`, blocs `{% docs %}` de `_curate_docs.md`). Les tables reprennent `chaine_nom` et les caractéristiques de la chaîne, pour éviter une jointure.
- Un identifiant se termine par `_id` ; une colonne lisible (nom, libellé) l'accompagne.

### Documentation (`models/curate/_curate.yml`)

- **Description du modèle** : commence par `Une ligne = ...`, puis les sections `Usage :`, `Pièges :`, `Source :` (et `Période :` quand elle est limitée). Les pièges disent comment compter juste (ex. lignes ou valeurs distinctes, ne pas moyenner des parts).
- **Description de chaque colonne** : ce qu'elle contient, quand elle est vide, un exemple de valeur ou de filtre utile.
- **`config.meta`** : `libelle` (nom affiché dans Metabase) pour le modèle et chaque colonne ; pour une colonne numérique, `unite` et `agregation` (`somme`, `moyenne` ou `aucune`) ; `type_metabase` (type sémantique : `type/PK`, `type/FK`, `type/Category`...) ; `cle_etrangere: table.colonne` pour une jointure vers une autre table de `curate`.
- **`data_type`** de chaque colonne : le contrat dbt (`contract: enforced`) fait échouer le build si une colonne change de nom ou de type. Caster dans le SQL (`::text`, `::double precision`).
- **Tests** : une clé `type/PK` avec `unique` et `not_null` ; `relationships` pour chaque `cle_etrangere` ; `not_null` et `accepted_values` dès que possible.
- Une liste (thèmes, locuteurs) s'accompagne d'un booléen par valeur (`est_theme_automobile`, `est_dit_par_invite`) pour compter sans découper de texte.
- Pas de chiffre qui change avec les données (« 9 % des diffusions ») dans une description.
- `my_dbt_project/pytest_tests/test_curate_conventions.py` vérifie ces règles sur le manifeste dbt.
- Une description rédigée par Claude porte des choix de méthode : la faire relire par un expert du sujet, et le dire dans la PR.

### Ne pas casser les usages

Les questions Metabase, les tableaux de bord et les analyses des équipes lisent ces tables. Ne jamais renommer ni retirer une colonne ou une table de `curate` sans l'accord de l'utilisateur : on ajoute une colonne, on ne modifie pas l'existante. Une table retirée ou renommée laisse l'ancienne en place : le signaler, avec le `DROP` à faire à la main.

### Construction et Metabase

- `entrypoints/curate.sh` construit `curate` (tout le dossier, après `entrypoints/dbt.sh` qui l'exclut), le teste, puis pousse sa documentation dans Metabase. Il tourne dans la tâche Kestra `curate_and_metabase_docs` du flow `barometre`, qui peut échouer sans arrêter le pipeline. Un nouveau modèle de `curate` n'a rien à ajouter aux entrypoints.
- `quotaclimat/metabase/sync_curate.py` écrit dans Metabase, pour chaque table et colonne : le libellé, la description (avec l'unité et l'agrégation), le type sémantique et la clé étrangère. dbt fait référence : une modification faite dans Metabase est écrasée. Les mêmes descriptions sont écrites dans Postgres (`persist_docs`).
- Les bases Metabase mises à jour sont dans `METABASE_CURATE_DATABASES` (tâche Kestra). En local : `poetry run python -m quotaclimat.metabase.sync_curate --database 2` (aperçu, après `dbt parse`), `--apply` pour écrire.
- Un nouveau modèle demande sa documentation dans `_curate.yml` et un test de ses données dans `my_dbt_project/pytest_tests/test_dbt_model_analytics.py`.

## Tests

```bash
poetry run pytest test/metabase                                    # synchronisation, sans base
POSTGRES_USER=user POSTGRES_DB=dbt_test POSTGRES_PASSWORD=postgres POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
DBT_PROJECT_DIR=my_dbt_project DBT_PROFILES_DIR=my_dbt_project/dbt \
poetry run pytest my_dbt_project/pytest_tests -k metabase           # requêtes Metabase sur les tables dbt de test
poetry run pytest my_dbt_project/pytest_tests/test_curate_conventions.py  # conventions du schéma curate (dbt parse, sans base)
```

Les tests dbt demandent un Postgres (job `dbt` de la CI). Une requête ajoutée ou modifiée doit passer `test_metabase_query` avant la PR. Les tests vérifient que la requête s'exécute, pas ses chiffres : un résultat se vérifie dans Metabase sur les vraies données.

## Langue

- **Commits** : en anglais.
- **SQL Metabase, manifestes et `metabase/README.md`** : commentaires en français.
- **Schéma `curate`** : noms de tables et de colonnes, descriptions et `meta` en français ; commentaires du SQL dbt en anglais, comme les autres modèles.
- **PR** : description en français.
