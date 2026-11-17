# Exporter les données d'un rapport vers Google Sheets

Le flow Kestra `quotaclimat/report-export-data` (lancé à la main) exécute les requêtes SQL d'un rapport sur la base Baromètre et écrit chaque résultat dans un onglet d'un Google Sheet. Il sert aux graphiques faits hors de Metabase (par exemple un graphique à bulles avec le nom de chaque point). Code : [`quotaclimat/reports/export_data.py`](../../quotaclimat/reports/export_data.py) ; flow : [`docs/kestra_flows.md`](../../docs/kestra_flows.md#report-export-data).

## Fichier `export_data.yml`

Il se trouve dans le dossier du rapport, à côté de `queries/` :

```yaml
# Exportation des données du rapport (flow Kestra quotaclimat/report-export-data)
outputs:
  - type: sheet                                   # seul type pour l'instant
    spreadsheet: Rapport 2027-02 pub désinformation - données   # nom exact du Google Sheet dans le dossier d'export
    tabs:
      - tab: donnees                              # onglet remplacé à chaque export
        query: bulles_annonceurs_secteur_sheets.sql   # fichier de queries/
```

- `outputs` : une sortie par Google Sheet. `type` est obligatoire (`sheet` aujourd'hui ; une autre sortie, un fichier par exemple, aura son propre type).
- `spreadsheet` : nom exact du Google Sheet dans le dossier Drive d'export. **Jamais d'identifiant ni d'adresse de Sheet** : le dépôt est public, seul l'identifiant du dossier est un secret.
- `tabs` : un onglet par requête. Une même requête peut alimenter plusieurs onglets. Les noms d'onglet commençant par `_` sont réservés (onglets pour les humains et `_export`).
- `query` : nom d'un fichier `.sql` de `queries/`. SQL brut, **une seule requête `SELECT`** (ou `WITH ... SELECT`) par fichier, sans variable Metabase : ses paramètres sont des constantes dans la requête.

Une clé inconnue, un fichier absent ou deux sorties vers le même Sheet font échouer le flow avant toute écriture.

## Ce que fait le flow

1. Il cherche le Sheet par son nom dans le dossier d'export et refuse un Sheet lisible sans identifiant (« Tous les utilisateurs disposant du lien », publié sur le web).
2. Il exécute les requêtes du Sheet, chacune dans une transaction en lecture seule, avec un utilisateur Postgres en lecture seule.
3. Il remplace, en une seule requête à Google (tout ou rien), le contenu de chaque onglet, en-têtes compris : les valeurs sont effacées puis réécrites depuis A1. Les formats, les graphiques et les autres onglets ne bougent pas. Un onglet absent est créé.
4. Il écrit dans l'onglet `_export` la date et l'heure de l'export (heure de Paris), le rapport, la version du pipeline et le nombre de lignes de chaque onglet. La date est dans un onglet à part pour ne pas décaler les plages de données des graphiques.

Les nombres restent des nombres dans le Sheet ; un texte n'est jamais lu comme une formule ; une valeur vide (NULL) donne une cellule vide ; les dates sont écrites en texte `AAAA-MM-JJ`.

Les journaux ne contiennent que les noms (rapport, Sheet, onglets, requêtes) et les nombres de lignes, jamais une valeur : les résultats sont des données de suivi non publiées.

## Mettre en place l'export d'un rapport

1. Créer le Google Sheet **à la main** dans le dossier Drive d'export (le flow n'en crée pas), avec le nom écrit dans `export_data.yml`. Partage restreint à l'équipe, sans lien public.
2. Le partager en **éditeur** avec le compte de service d'écriture (adresse `...@....iam.gserviceaccount.com`, la même que pour `Inventaire_des_marques`). Ne partager que ce Sheet, jamais tout le dossier.
3. Ajouter `export_data.yml` et les requêtes de `queries/` dans le dossier du rapport, et les fusionner dans `main` : le flow tourne sur l'image construite depuis `main`.
4. Lancer le flow `quotaclimat/report-export-data` dans Kestra avec `report` = nom du dossier du rapport (par exemple `2027-02-pub-desinformation`). Pour vérifier sans écrire : `apply` à `false` (les requêtes tournent et le Sheet est contrôlé, rien n'est écrit).
5. Faire les graphiques dans d'autres onglets que ceux de l'export, et ne rien écrire à la main dans les onglets exportés : tout y est remplacé au prochain export.

En local : `poetry run python -m quotaclimat.reports.export_data --report <dossier> [--check]`, avec les variables `POSTGRES_*`, `REPORT_EXPORT_DRIVE_FOLDER` et `GOOGLE_SHEETS_EDITOR_SERVICE_ACCOUNT_JSON`.
