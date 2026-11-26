# Exporter les données d'un rapport vers Google Sheets

Le flow `export_report_data` (Windmill `f/quotaclimat/export_report_data`, Kestra `quotaclimat/export-report-data`, lancé à la main) exécute les requêtes SQL d'un rapport sur la base Baromètre et écrit chaque résultat dans un onglet d'un Google Sheet. Il sert aux graphiques faits hors de Metabase (par exemple un graphique à bulles avec le nom de chaque point). Code : [`quotaclimat/reports/export_report_data.py`](../../quotaclimat/reports/export_report_data.py) ; flow : [`docs/kestra_flows.md`](../../docs/kestra_flows.md#export-report-data).

**Où sont les Sheets** : dans le dossier Drive `Rapports` du dossier partagé des données de référence (celui des Google Sheets importés par dbt, dont l'identifiant est le secret `EXTERNAL_SOURCES_DRIVE_FOLDER`). Le flow trouve ce dossier par son nom, puis chaque Sheet par son nom : aucun identifiant de plus à gérer.

## Fichier `export_data.yml`

Il se trouve dans le dossier du rapport, à côté de `queries/` :

```yaml
# Exportation des données du rapport (flow export_report_data)
outputs:
  - type: sheet                                   # seul type pour l'instant
    spreadsheet: 2027-02-pub-desinformation       # nom exact du Google Sheet dans le dossier Rapports
    tabs:
      - tab: part_temps_de_pub_prox_mesinfo_par_annonceurs   # onglet écrit à chaque export
        query: bulles_annonceurs_secteur_sheets.sql   # fichier de queries/
        columns: A:H                              # facultatif : seules ces colonnes sont remplacées
      - tab: part_temps_de_pub_prox_mesinfo_par_categ
        query: bulles_categories_secteur_sheets.sql
        columns: A:H
```

- `outputs` : une sortie par Google Sheet. `type` est obligatoire (`sheet` aujourd'hui ; une autre sortie, un fichier par exemple, aura son propre type).
- `spreadsheet` : nom exact du Google Sheet dans le dossier `Rapports`. **Jamais d'identifiant ni d'adresse de Sheet** : le dépôt est public, seul l'identifiant du dossier partagé est un secret.
- `tabs` : un onglet par requête. Une même requête peut alimenter plusieurs onglets. Les noms d'onglet commençant par `_` sont réservés (onglets pour les humains et `_export`).
- `query` : nom d'un fichier `.sql` de `queries/`. SQL brut, **une seule requête `SELECT`** (ou `WITH ... SELECT`) par fichier, sans variable Metabase : ses paramètres sont des constantes dans la requête.
- `columns` (facultatif) : les colonnes écrites par l'export, par exemple `A:H`. Seules ces colonnes sont effacées (sur toutes les lignes) puis réécrites à partir de la ligne 1 ; le reste de l'onglet (graphique, notes) ne bouge pas. Une requête qui renvoie plus de colonnes que la plage fait échouer l'export. Sans `columns`, toutes les valeurs de l'onglet sont remplacées.

Une clé inconnue, un fichier absent ou deux sorties vers le même Sheet font échouer le flow avant toute écriture.

## Ce que fait le flow

1. Il cherche le dossier `Rapports`, puis le Sheet par son nom dans ce dossier, et refuse un Sheet lisible sans identifiant (« Tous les utilisateurs disposant du lien », publié sur le web).
2. Il exécute les requêtes du Sheet, chacune dans une transaction en lecture seule, avec un utilisateur Postgres en lecture seule.
3. Il remplace, en une seule requête à Google (tout ou rien), le contenu de chaque onglet, en-têtes compris : les valeurs (de tout l'onglet, ou des colonnes de `columns`) sont effacées puis réécrites depuis la ligne 1. Les formats, les graphiques et les autres onglets ne bougent pas. Un onglet absent est créé.
4. Il écrit dans l'onglet `_export` la date et l'heure de l'export (heure de Paris), le rapport, la version du pipeline et le nombre de lignes de chaque onglet. La date est dans un onglet à part pour ne pas décaler les plages de données des graphiques.

Les nombres restent des nombres dans le Sheet ; un texte n'est jamais lu comme une formule ; une valeur vide (NULL) donne une cellule vide ; les dates sont écrites en texte `AAAA-MM-JJ`.

Les journaux ne contiennent que les noms (rapport, Sheet, onglets, requêtes) et les nombres de lignes, jamais une valeur : les résultats sont des données de suivi non publiées.

## Mettre en place l'export d'un rapport

1. Créer le Google Sheet **à la main** dans le dossier `Rapports` du dossier partagé (le flow n'en crée pas), avec le nom écrit dans `export_data.yml`. Partage restreint à l'équipe, sans lien public.
2. Une seule fois : partager le dossier `Rapports` en **éditeur** avec le compte de service d'écriture (adresse `...@....iam.gserviceaccount.com`, la même que pour `Inventaire_des_marques`). Le compte doit voir le dossier pour le trouver, et les Sheets créés dedans héritent du partage. Ne jamais partager tout le dossier partagé avec ce compte.
3. Ajouter `export_data.yml` et les requêtes de `queries/` dans le dossier du rapport, et les fusionner dans `main` : le flow tourne sur l'image construite depuis `main`.
4. Lancer le flow `f/quotaclimat/export_report_data` dans Windmill avec `report` = nom du dossier du rapport (par exemple `2027-02-pub-desinformation`). Pour vérifier sans écrire : `apply` à `false` (les requêtes tournent et le Sheet est contrôlé, rien n'est écrit).
5. Faire les graphiques dans un onglet à part, ou dans l'onglet des données à condition de donner `columns` et de placer le graphique hors de ces colonnes. Ne rien écrire à la main dans les colonnes exportées : tout y est remplacé au prochain export.
6. **Plage du graphique** : le flow ne modifie pas les graphiques (l'API ne permet pas de les réécrire sans perdre les réglages faits à la main). Pour que le graphique suive le nombre de lignes, lui donner une plage ouverte, sans numéro de ligne de fin, par exemple `A1:E` au lieu de `A1:E73`. Les lignes vides sous les données sont effacées à chaque export. À vérifier une fois sur le graphique : il ne doit pas afficher de point vide.

En local : `poetry run python -m quotaclimat.reports.export_report_data --report <dossier> [--check]`, avec les variables `POSTGRES_*`, `EXTERNAL_SOURCES_DRIVE_FOLDER` et `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON` (et `REPORTS_DRIVE_FOLDER_NAME` si le dossier des Sheets ne s'appelle pas `Rapports`).

## Rafraîchir les données d'un rapport

Les requêtes de `queries/` font foi (voir [`decisions.md`](decisions.md)) : on ne colle pas à la main dans le Sheet un résultat obtenu dans Metabase.

1. Si une requête a changé, la fusionner dans `main` et attendre la construction de l'image (le flow lit les requêtes de l'image).
2. Lancer le flow `f/quotaclimat/export_report_data` (Windmill) avec `report` = nom du dossier du rapport.
3. Dans le Sheet, vérifier l'onglet `_export` (date de l'export, nombre de lignes de chaque onglet), puis le contenu attendu de chaque graphique, décrit dans la carte des graphiques du `decisions.md` du rapport.
4. Dans le Google Doc du rapport, cliquer sur « Mettre à jour » au-dessus de chaque graphique collé depuis le Sheet. Un graphique Sheets collé dans un Doc reste lié mais ne se met pas à jour seul : la version du Doc ne change que quand on le décide.
5. Si la plage de dates analysée bouge encore, noter la date du rafraîchissement dans le sous-titre du graphique.

## Construire un graphique à bulles (une fois)

Metabase n'écrit pas de nom à côté des points d'un nuage de points : un graphique qui doit nommer ses bulles se fait dans Google Sheets (Insertion, Graphique, type « Graphique à bulles »). Il attend les colonnes dans cet ordre, que la requête doit produire :

| Colonne | Rôle dans le graphique |
|---|---|
| 1 (par exemple `etiquette`) | ID, écrit à côté de la bulle ; vide pour un point sans nom (une moyenne) |
| 2 (`x`) | axe horizontal, numérique (Sheets n'accepte pas de catégorie en abscisse d'un graphique à bulles) |
| 3 | axe vertical |
| 4 (`serie`) | série, donc couleur et légende |
| 5 (`taille`) | taille de la bulle ; une racine carrée de la mesure rend la surface proportionnelle à la mesure |

Les colonnes suivantes ne servent qu'à vérifier.

- Plage de données : les cinq premières colonnes de l'onglet, en-têtes en première ligne, en plage ouverte (`A1:E`, voir ci-dessus).
- Les couleurs des séries et les axes se règlent dans l'éditeur de graphique (l'API ne les règle pas pour un graphique à bulles) ; les couleurs retenues sont dans le `decisions.md` du rapport.
- Ne jamais déplacer un point à la main : il reviendrait à l'export suivant. Si des étiquettes se chevauchent, agrandir le graphique.
- Copier le graphique et le coller dans le Google Doc en gardant le lien avec la feuille de calcul.
