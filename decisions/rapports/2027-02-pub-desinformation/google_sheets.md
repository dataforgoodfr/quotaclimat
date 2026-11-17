# Graphiques à bulles dans Google Sheets

Pourquoi ce chemin : Metabase n'écrit pas de nom à côté des bulles d'un nuage de points ([`decisions.md`](decisions.md), § 3). Les graphiques à bulles du rapport sont donc faits dans Google Sheets, avec les données des requêtes de [`queries/`](queries/), puis collés dans le Google Doc du rapport. Les questions Metabase restent la référence pour explorer (variables, survol).

- Google Sheet : `2027-02-pub-desinformation` (le nom du dossier du rapport).
- Un onglet par graphique : les données en colonnes A à H (remplacées à chaque rafraîchissement, ne rien y écrire à la main), le graphique à côté, dans le même onglet.
- Partage : restreint à l'équipe. Les données sont des résultats de suivi non publiés (charte IA) ; pas de lien public.

| Onglet | Requête | Graphique | Contenu attendu |
|---|---|---|---|
| `donnees` | [`queries/bulles_annonceurs_secteur_sheets.sql`](queries/bulles_annonceurs_secteur_sheets.sql) | Part exposée par annonceur (partie 3, question Metabase 2500) | 9 secteurs, au plus 6 entreprises par secteur, une ligne « Moyenne du secteur » par secteur |
| `categories` | [`queries/bulles_categories_secteur_sheets.sql`](queries/bulles_categories_secteur_sheets.sql) | Part exposée par catégorie de produit (partie 4.1, R13) | 3 secteurs, au plus 8 catégories par secteur, une ligne « Moyenne du secteur » par secteur |

## Rafraîchir les données

1. Exécuter la requête sur la base Baromètre : dans Metabase (nouvelle question SQL sur la base Scaleway Postgres, sans l'enregistrer), avec le connecteur Metabase d'une session Claude (`execute_sql`, base 2), ou en `psql`. Les requêtes n'ont pas de variable Metabase : leurs paramètres sont les constantes du CTE `parametres`.
2. Remplacer les colonnes A à H de l'onglet par le résultat, en-têtes compris (colonnes `etiquette`, `x`, `part_proche_pct`, `serie`, `taille`, puis `secteur`, `rang_secteur`, `pub_min` pour vérifier). Si le nombre de lignes change, vérifier que la plage du graphique couvre toutes les lignes.
3. Vérifier le contenu attendu (tableau ci-dessus).
4. Dans le Google Doc, cliquer sur « Mettre à jour » au-dessus de chaque graphique. Un graphique Sheets collé dans un Doc reste lié mais ne se met pas à jour seul : la version du Doc ne change que quand on le décide.
5. Noter la date du rafraîchissement dans le sous-titre du graphique (la plage analysée bouge jusqu'au 31/12/2026, voir R1).

Avec Claude : « exécute `decisions/rapports/2027-02-pub-desinformation/queries/<requête>.sql` avec le connecteur Metabase et remplace les colonnes A à H de l'onglet `<onglet>` du Sheet <lien> par le résultat ». Le Doc reste à mettre à jour à la main (étape 4).

## Construire un graphique (une fois)

Graphique à bulles de Google Sheets (Insertion, Graphique, type « Graphique à bulles »). Il attend les colonnes dans cet ordre, ce que font les requêtes :

| Colonne | Rôle dans le graphique | Contenu |
|---|---|---|
| `etiquette` | ID, écrit à côté de la bulle | nom de l'entreprise ou de la catégorie ; vide pour les moyennes |
| `x` | axe horizontal | numéro du secteur (1 = premier) : les bulles d'un secteur sont alignées verticalement |
| `part_proche_pct` | axe vertical | part du temps de pub rattaché à une émission à fort taux, de 0 à 100 |
| `serie` | série, donc couleur et légende | nom du secteur, ou « Moyenne du secteur » |
| `taille` | taille de la bulle | racine carrée des minutes de pub ; 1 pour les moyennes |

Réglages (l'API ne règle ni les couleurs par série ni les axes d'un graphique à bulles : à faire dans l'éditeur) :

- Plage de données : colonnes A à E de l'onglet, en-têtes en première ligne.
- Couleurs des séries (comme dans Metabase, `decisions.md` § 3) : Automobile et transport `#509EE3`, Alimentation `#88BF4D`, Établissements financiers et assurance `#F2A86F`, Moyenne du secteur en noir `#000000` ; les autres secteurs en couleurs automatiques.
- Axe horizontal : l'axe est numérique (Sheets n'accepte pas de catégorie en abscisse d'un graphique à bulles). Masquer ses étiquettes ou les laisser en numéros ; c'est la légende des couleurs qui dit quel secteur est où. Minimum 0,5, maximum le nombre de secteurs + 0,5.
- Axe vertical : titre « Part du temps de pub autour d'émissions à 1 cas ou plus par heure d'environnement (%) », minimum 0.
- Bulles : opacité autour de 80 %, police des étiquettes 16.
- Les étiquettes se chevauchent quand deux bulles d'un même secteur ont des parts proches : agrandir le graphique en hauteur. Ne pas déplacer les points à la main : ils reviendraient au rafraîchissement suivant.

Puis copier le graphique et le coller dans le Google Doc en gardant le lien avec la feuille de calcul.

## Ce qui n'a pas été retenu

- **Lien public Metabase lu par `IMPORTDATA`** : rafraîchissement automatique, mais publie sur internet des données non publiées.
