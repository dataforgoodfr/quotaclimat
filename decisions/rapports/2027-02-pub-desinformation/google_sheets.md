# Graphique à bulles de la partie 3 dans Google Sheets

Pourquoi ce chemin : Metabase n'écrit pas le nom des entreprises à côté des bulles d'un nuage de points ([`decisions.md`](decisions.md), § 3). Le graphique du rapport est donc fait dans Google Sheets, avec les données de [`queries/bulles_annonceurs_secteur_sheets.sql`](queries/bulles_annonceurs_secteur_sheets.sql), puis collé dans le Google Doc du rapport. La question Metabase 2500 reste la référence pour explorer (variables, survol).

- Google Sheet : 🔴 [nom ou emplacement du Sheet dans le Drive]
- Onglet des données : `donnees` (remplacé à chaque rafraîchissement, ne rien y écrire à la main) ; le graphique est dans un autre onglet.
- Partage : restreint à l'équipe. Les données sont des résultats de suivi non publiés (charte IA) ; pas de lien public.

## Rafraîchir les données

1. Exécuter la requête sur la base Baromètre : dans Metabase (nouvelle question SQL sur la base Scaleway Postgres, sans l'enregistrer), avec le connecteur Metabase d'une session Claude (`execute_sql`, base 2), ou en `psql`. La requête n'a pas de variable Metabase : ses paramètres sont les constantes du CTE `parametres`.
2. Remplacer tout l'onglet `donnees` par le résultat, en-têtes compris (colonnes `etiquette`, `x`, `part_proche_pct`, `serie`, `taille`, puis `secteur`, `rang_secteur`, `pub_min` pour vérifier).
3. Vérifier : 9 secteurs, au plus 6 entreprises par secteur, une ligne « Moyenne du secteur » et une ligne « Moyenne des chaînes d'info » par secteur ; la valeur de « Moyenne des chaînes d'info » égale à celle de la question Metabase 2517.
4. Dans le Google Doc, cliquer sur « Mettre à jour » au-dessus du graphique. Un graphique Sheets collé dans un Doc reste lié mais ne se met pas à jour seul : la version du Doc ne change que quand on le décide.
5. Noter la date du rafraîchissement dans la légende du graphique (la plage analysée bouge jusqu'au 31/12/2026, voir R1).

Avec Claude : « exécute `decisions/rapports/2027-02-pub-desinformation/queries/bulles_annonceurs_secteur_sheets.sql` avec le connecteur Metabase et remplace l'onglet `donnees` du Sheet <lien> par le résultat ». Le Doc reste à mettre à jour à la main (étape 4).

## Construire le graphique (une fois)

Graphique à bulles de Google Sheets (Insertion, Graphique, type « Graphique à bulles »). Il attend les colonnes dans cet ordre, ce que fait la requête :

| Colonne | Rôle dans le graphique | Contenu |
|---|---|---|
| `etiquette` | ID, écrit à côté de la bulle | nom de l'entreprise ; vide pour les moyennes |
| `x` | axe horizontal | numéro du secteur (1 = premier), décalé de -0,3 à +0,3 selon la part de l'entreprise, pour écarter les étiquettes |
| `part_proche_pct` | axe vertical | part du temps de pub rattaché à une émission à fort taux, de 0 à 100 |
| `serie` | série, donc couleur et légende | nom du secteur, « Moyenne du secteur », « Moyenne des chaînes d'info » |
| `taille` | taille de la bulle | racine carrée des minutes de pub ; 1 pour les moyennes |

Réglages :

- Plage de données : colonnes A à E de l'onglet `donnees`, en-têtes en première ligne.
- Couleurs des séries (comme dans Metabase, `decisions.md` § 3) : Automobile et transport `#509EE3`, Alimentation `#88BF4D`, Établissements financiers et assurance `#F2A86F`, Moyenne du secteur `#2E353B`, Moyenne des chaînes d'info `#949AAB` ; les autres secteurs en couleurs automatiques.
- Axe horizontal : l'axe est numérique (Sheets n'accepte pas de catégorie en abscisse d'un graphique à bulles). Masquer ses étiquettes ou les laisser en numéros ; c'est la légende des couleurs qui dit quel secteur est où. Minimum 0,5, maximum 9,5.
- Axe vertical : titre « Part du temps de pub autour d'émissions à 1 cas ou plus par heure d'environnement (%) », minimum 0.
- Bulles : opacité autour de 50 % pour voir les étiquettes qui se chevauchent.
- Les étiquettes peuvent encore se chevaucher quand deux entreprises d'un même secteur ont des parts proches : élargir le graphique, ou augmenter `largeur_secteur` dans la requête (0,6 par défaut, au plus 0,8 pour ne pas empiéter sur le secteur voisin). Ne pas déplacer les points à la main : ils reviendraient au rafraîchissement suivant.

Puis copier le graphique et le coller dans le Google Doc en gardant le lien avec la feuille de calcul.

## Ce qui n'a pas été retenu

- **Flux Kestra** qui écrirait le Sheet : faisable (le pipeline écrit déjà dans des Google Sheets avec un compte de service, voir `quotaclimat/data_ingestion/advertising/s05_enrichment/e01_brand_groups/sheet.py`), mais disproportionné pour quelques rafraîchissements avant une publication dont la plage se fige. À reconsidérer si plusieurs graphiques du rapport passent par Sheets.
- **Lien public Metabase lu par `IMPORTDATA`** : rafraîchissement automatique, mais publie sur internet des données non publiées.
