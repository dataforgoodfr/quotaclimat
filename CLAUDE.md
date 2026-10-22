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
- Avant de modifier une requête, comparer si possible le SQL de ses questions (connecteur Metabase, `get_content` avec `definition`) avec le fichier. Si l'utilisateur a modifié le SQL dans Metabase, ou colle une version différente, reporter ses changements dans le fichier avant d'ajouter les siens, et le lui signaler. La synchronisation de `main-pipeline` tourne avec `force` : elle écrase deux fois par jour le SQL modifié dans Metabase (l'écart reste dans les logs du flow `metabase-sync`).
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

Un nouveau modèle demande trois choses :
- sa documentation dans `my_dbt_project/models/schema.yml` ;
- un test dans `my_dbt_project/pytest_tests/test_dbt_model_analytics.py` ;
- sa place dans les `dbt run` de `entrypoints/dbt.sh` et `entrypoints/mediatree_import.sh`, quand il dépend des modèles advertising.

Un modèle qui change de schéma laisse l'ancienne table en place : le signaler, avec le `DROP` à faire à la main.

Les mots-clés des thèmes de cas (`my_dbt_project/seeds/themes/mesinfo_theme_mots_cles.csv`, des expressions régulières) sont versionnés et modifiés dans le code, pas dans un Google Sheet.

## Tests

```bash
poetry run pytest test/metabase                                    # synchronisation, sans base
POSTGRES_USER=user POSTGRES_DB=dbt_test POSTGRES_PASSWORD=postgres POSTGRES_HOST=localhost POSTGRES_PORT=5432 \
DBT_PROJECT_DIR=my_dbt_project DBT_PROFILES_DIR=my_dbt_project/dbt \
poetry run pytest my_dbt_project/pytest_tests -k metabase           # requêtes Metabase sur les tables dbt de test
```

Les tests dbt demandent un Postgres (job `dbt` de la CI). Une requête ajoutée ou modifiée doit passer `test_metabase_query` avant la PR. Les tests vérifient que la requête s'exécute, pas ses chiffres : un résultat se vérifie dans Metabase sur les vraies données.

## Langue

- **Commits** : en anglais.
- **SQL Metabase, manifestes et `metabase/README.md`** : commentaires en français.
- **PR** : description en français.
