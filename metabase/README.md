# Requêtes Metabase versionnées

Ce dossier contient le SQL des questions natives de Metabase qui lisent les tables dbt (`analytics.*`), rangé par sujet :
- `advertising/` : la publicité autour des émissions ;
- `mesinformation/` : la mésinformation par émission.

Chaque fichier commence par sa documentation : variables, colonnes, sources.
Le fichier versionné fait référence : une question n'est qu'une copie de son SQL.

## Les manifestes `questions.yml`

Chaque sous-dossier a son manifeste `questions.yml`. Pour chaque fichier du dossier, il donne trois choses.

- **Les questions Metabase qui l'exécutent.** Ce sont les numéros dans l'URL des questions.
- **Le type de chaque `{{variable}}`.** Les types possibles sont `text`, `number`, `date`, `boolean` et `dimension`. Un filtre de champ (`dimension`) indique aussi la colonne `schema.table.colonne` et le widget.
- **Les valeurs de test.**

Les tests vérifient que chaque requête figure dans le manifeste de son dossier, et que ce manifeste déclare toutes ses variables.

## Tests

`my_dbt_project/pytest_tests/test_dbt_model_analytics.py::test_metabase_query` exécute chaque requête avec les valeurs de test. Il tourne sur les tables construites par dbt à partir des seeds de test, dans le job dbt de la CI.

Les variables sont remplacées comme le fait Metabase :
- une clause `[[ ... ]]` n'est gardée que si toutes ses variables ont une valeur ;
- une variable texte est envoyée en `varchar` (d'où les `CAST` dans les requêtes) ;
- un filtre de champ devient `"analytics"."table"."colonne" IN (...)`, ou `1 = 1` sans valeur. La requête doit donc lire la table sous son nom complet, sans alias.

Une requête qui ne tourne plus après une modification du modèle dbt fait échouer la CI de la PR.

Les tests vérifient que la requête s'exécute, et pour certains cas qu'elle renvoie au moins une ligne. Ils ne contrôlent pas les chiffres : un résultat se vérifie toujours dans Metabase sur les vraies données.

```bash
poetry run pytest test/metabase                                      # synchronisation, sans base
poetry run pytest my_dbt_project/pytest_tests -k metabase_query      # requêtes, base dbt de test
```

## Synchronisation avec Metabase

Le flow Kestra `quotaclimat/metabase-sync` (`infrastructure/kestra/flows/main_quotaclimat_metabasesync.yaml`) met à jour les questions du manifeste. Il utilise l'image `quotaclimat_base` construite sur `main`, donc le SQL de `main`.

Pour chaque question, il fait quatre choses :
- il remplace son SQL par celui du fichier ;
- il garde le réglage des variables qui existent déjà : type, widget, valeur par défaut, liste de valeurs. Les réglages faits dans Metabase restent ;
- il crée les nouvelles variables à partir du manifeste et retire celles que la requête n'utilise plus ;
- il ajoute ou met à jour, dans la description, une ligne « Requête versionnée : metabase/… (version …) ». Cette version est le hash du SQL synchronisé.

Si le SQL d'une question a été modifié dans Metabase depuis sa dernière synchronisation, le flow ne la touche pas. C'est aussi le cas d'une question jamais synchronisée. Le flow échoue alors et affiche l'écart dans ses logs. Deux façons d'en sortir :
- reporter la modification dans le fichier, par une PR ;
- relancer avec `force` pour écraser la version de Metabase.

En local :

```bash
export METABASE_URL=https://... METABASE_API_KEY=...
poetry run python -m quotaclimat.metabase.sync_questions            # aperçu, ne modifie rien
poetry run python -m quotaclimat.metabase.sync_questions --apply    # met à jour
```

La clé d'API se crée dans Metabase : Admin > Authentification > Clés d'API. Elle appartient à un groupe qui peut modifier les collections des questions. Elle est rangée avec les secrets Kestra (`METABASE_URL`, `METABASE_API_KEY`, voir `infrastructure/.env.secrets.dist`).

## Ajouter ou modifier une requête

1. Modifier ou ajouter le fichier `.sql`, avec sa documentation en tête.
2. Le déclarer dans le `questions.yml` de son dossier : variables, valeurs de test, puis les questions une fois créées dans Metabase. Un nouveau sujet a son propre sous-dossier et son manifeste.
3. Lancer les tests, ouvrir une PR.
4. Après la fusion et la construction de l'image, lancer le flow `metabase-sync`.

Une nouvelle question se crée à la main dans Metabase, en collant le SQL. On ajoute ensuite son numéro au manifeste. La synchronisation reprend alors son SQL, sa description et ses variables.
