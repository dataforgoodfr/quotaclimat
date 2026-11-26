# Rapport pub février 2027 : qui finance la désinformation climatique à la télévision ?

- Statut : en cours, chiffres préliminaires (à faire vérifier par l'équipe pub et l'équipe mésinformation avant toute publication)
- Publication prévue : février 2027
- Requêtes de référence : [`queries/`](queries/). L'exploration préliminaire s'est faite dans Metabase (tableau de bord 228) ; un graphique marqué « Pour l'instant dans Metabase » n'a pas encore sa requête versionnée.
- Conventions communes aux rapports : [`../decisions.md`](../decisions.md) (dépôt public : aucun chiffre ici)

## 1. Objet et architecture

Le rapport décrit quels annonceurs financent, par leur publicité, les émissions de télévision où l'OMÉ relève de la mésinformation climatique.

Chaîne de données :

1. **Publicités** : détection, classification et consolidation (`quotaclimat/data_ingestion/advertising/`, voir les README des étapes s03 à s05), puis dbt : `analytics.publicites` (une diffusion), `advertising.pub_annonceur_emission_jour` (secondes de pub de chaque marque autour de chaque émission, par jour ; [`my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql`](../../../my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql)). Les colonnes `secteur`, `categorie`, `entreprise`, `agence_media`, `groupe_agence`, `fiabilite_agence` viennent de là.
2. **Mésinformation et climat par émission** : `public.emission_jour` (une ligne par émission et par jour : `climat_heures`, `nb_cas` validés ; [`my_dbt_project/models/analytics/emission_jour.sql`](../../../my_dbt_project/models/analytics/emission_jour.sql)). Jointure avec la pub sur `emission_cle`.
3. **Chaînes** : `curated.chaine` (`est_info_continue` sépare chaînes d'information en continu et généralistes). **Plage de dates** : `curated.publicite` (dernier jour de pub extrait).
4. **Google Sheets puis Google Doc** : les résultats des requêtes de `queries/` vont dans le Google Sheet `2027-02-pub-desinformation` (dossier Drive `Rapports`, un onglet `<requête>_sheet` par requête, données en colonnes A à H et graphique à droite dans le même onglet), dont les graphiques sont collés dans le Google Doc du rapport. L'export se fait avec le flow `export_report_data` (Windmill), lancé à la main avec `report=2027-02-pub-desinformation`. Procédure de rafraîchissement (Sheet puis Google Doc) : [`../export_report_data.md`](../export_report_data.md).

## 2. Règles de calcul

- **Plage analysée** : un an glissant jusqu'au dernier jour de pub extrait (`MAX(jour)` de `curated.publicite`), jamais avant le 29/09/2025 (données pub jugées non fiables avant), jamais après le 31/12/2026. À partir de 2027, la plage se fige sur l'année 2026. Même CTE `plage` dans chaque requête.
- **Temps d'antenne, pas budget** : nous n'avons ni tarifs d'écran ni recettes. Partout où le plan parle de budget, de recettes ou de dépenses, on montre des minutes de pub ou des parts de minutes.
- **Annonceur** : l'entreprise (`entreprise` de `pub_annonceur_emission_jour`, qui regroupe ses marques), sauf quand le titre du graphique dit autre chose. Pubs sans marque détectée écartées.
- **Pub autour d'une émission** : règle du modèle dbt `pub_annonceur_emission_jour` (la même que [`metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql`](../../../metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql)). Un écran publicitaire va à l'émission qui le contient ; sinon aux émissions qui finissent moins de 15 min avant son début ou commencent moins de 15 min après sa fin (moitié de sa durée à chacune, toute sa durée s'il n'y en a qu'une) ; toutes les pubs de l'écran suivent. 15 min : variable dbt `ad_emission_window_minutes`.
- **Taux de mésinformation** : cas validés par heure consacrée au climat (`climat_heures` : fenêtres de 20 s avec au moins un mot-clé climat ; biodiversité et ressources exclues), `SUM(nb_cas) / SUM(climat_heures)` de `public.emission_jour` sur la plage.
- **Émission à fort taux de mésinformation** (en discussion : seuil à confirmer par l'équipe mésinformation) : sur la plage, au moins 1 cas validé par heure de climat **et** au moins 0,5 h de climat au total (en dessous, le taux repose sur trop peu d'heures). Paramètres `seuil` et `climat_heures_min` des requêtes de `queries/` (`env_heures_min` dans les questions Metabase, nom gardé pour les filtres existants). Ce seuil reproduit « à proximité d'un cas avéré » du plan sans dépendre du jour exact du cas.
- **Part exposée** : part du temps de pub rattaché à une émission à fort taux. Une moyenne (secteur, type de chaîne) est pondérée par le temps de pub : on somme les secondes, on ne moyenne pas des parts. Elle porte sur toutes les pubs du groupe, pas seulement les points affichés.
- **Chaînes d'info seules** dans les parties 3 et 4.1 : aucune émission généraliste n'atteint le seuil de fort taux, leur part exposée est nulle. À revoir si le seuil change.
- **Secteurs affichés** : les 9 premiers secteurs par temps de pub **toutes chaînes confondues** (généralistes et info), quel que soit le type de chaîne du graphique, pour garder la même liste de secteurs d'un graphique à l'autre. Dans chaque secteur : les 6 premières entreprises par temps de pub.
- **Catégories de produit** (partie 4.1) : les 3 premiers secteurs de cette même liste ; dans chacun, les 8 premières catégories de produit par temps de pub ; une catégorie inconnue prend le nom du secteur.
- **Exposition des principaux annonceurs** : minutes de pub de l'annonceur rattachées à une émission à fort taux, découpées par émission (les 8 premières nommées, les autres en « Autres émissions à fort taux »). Deux classements : par minutes exposées, et par minutes totales (avec, en gris, la pub hors de ces émissions).
- **Agence média** : agence de la marque, sinon de son entreprise (Google Sheet `Inventaire_des_annonceurs`, via `ad_brand_agencies`) ; pour une entreprise, l'agence qui achète le plus de ses secondes. Affichée en une colonne : « Agence (Groupe) » quand elle est confirmée (pas de mention « confirmée », c'est le cas par défaut), « probable : Agence (Groupe) », « indice : Agence (Groupe) », « Agence inconnue » ; le groupe n'est pas répété quand il porte le nom de l'agence.

## 3. Présentation

- **Couleurs** : une couleur par secteur dans les graphiques à bulles (la série est le nom du secteur), moyenne du secteur en noir ; codes dans [`metabase/couleurs.yml`](../../../metabase/couleurs.yml), groupes `secteurs_publicite` et `exposition_annonceurs`.
- **Taille des bulles** : racine carrée des minutes de pub (la surface est proportionnelle aux minutes) ; taille fixe 4 pour les moyennes de secteur.
- **Bulles alignées** : toutes les bulles d'un secteur ont la même abscisse (le rang du secteur).
- **Noms sur les bulles** : Metabase ne sait pas les écrire (nom au survol seulement), d'où les graphiques à bulles dans Google Sheets.

## 4. Remarques

- On ne répartit pas les cas d'une émission entre ses annonceurs au prorata de leur pub (une entreprise qui a 10 % de la pub d'une émission à 20 cas se verrait attribuer 2 cas) : une émission avec très peu de pub (France Info TV, surtout du parrainage) concentre alors tous ses cas sur deux ou trois annonceurs, qui remontent en tête sans être exposés en volume. On mesure plutôt les minutes de pub exposées.
- On ne montre pas tous les secteurs dans les graphiques par entreprise : illisible.
- On ne trace pas la moyenne des chaînes d'info dans les graphiques à bulles : un point par secteur à la même hauteur, peu lisible ; seules les moyennes de secteur restent.
- On ne fait pas de bulles par catégorie dans le graphique de la partie 3, qui porte sur les annonceurs : les catégories ont leur propre graphique (partie 4.1).
- Pas de lien public Metabase lu par `IMPORTDATA` pour alimenter le Sheet : il publierait sur internet des données de suivi non publiées. Le Sheet est rempli par le flow `export_report_data`, lancé à la main (voir § 1).
- Les pubs ne sont cherchées qu'à ±15 min des programmes suivis : les généralistes ne sont vues qu'autour de leurs JT et matinales, ne jamais les comparer aux chaînes d'info en niveau. France Télévisions, Arte et France Médias Monde : surtout des parrainages.
- Une pub « autour » d'une émission ne dit pas qu'elle a été diffusée pendant le passage mésinformant, ni le même jour que lui (le fort taux porte sur toute la plage).

## 5. Carte des graphiques

Dans l'ordre du plan du rapport.

| Partie | Graphique | Requête |
|---|---|---|
| 1.1 | Acteurs du marché publicitaire | aucune (schéma à dessiner à partir de recherches documentaires) |
| 1.2 | Poids de la pub dans les revenus des chaînes | aucune (il faut le chiffre d'affaires des chaînes : rapports annuels, Arcom) |
| 1.3 | Intensité publicitaire par heure d'information | Pour l'instant dans Metabase |
| 1.3 | Concentration : part des 10 premiers annonceurs | Pour l'instant dans Metabase |
| 1.4 | Répartition sectorielle du temps de pub par type de chaîne | Pour l'instant dans Metabase |
| 1.4 | Zooms par catégorie de produit : Automobile et transport, Alimentation, Établissements financiers et assurances | Pour l'instant dans Metabase |
| 2.1 | Fréquence de mésinformation par chaîne | Pour l'instant dans Metabase |
| 2.2 | Fréquence de mésinformation par émission (au moins 2 h de climat) | Pour l'instant dans Metabase |
| 3 | Part exposée par annonceur, chaînes d'info (graphique à bulles) | [`queries/bulles_annonceurs_secteur.sql`](queries/bulles_annonceurs_secteur.sql), onglet `bulles_annonceurs_secteur_sheet` du Sheet |
| 3 | Pub des 15 principaux annonceurs autour d'émissions à fort taux, par émission, tri par pub exposée puis par pub totale | Pour l'instant dans Metabase |
| 3 | Principaux annonceurs exposés et leurs agences média | Pour l'instant dans Metabase |
| 3 | Part de CNews dans la pub achetée par chaque groupe d'agences, parmi BFM TV, CNews et LCI | Pour l'instant dans Metabase |
| 4.1 | Part exposée par catégorie de produit, trois premiers secteurs, chaînes d'info (graphique à bulles) | [`queries/bulles_categories_secteur.sql`](queries/bulles_categories_secteur.sql), onglet `bulles_categories_secteur_sheet` du Sheet |
| 4.2 | Pubs automobiles à proximité d'une mésinformation sur l'automobile | Pour l'instant dans Metabase |

## 6. Où vivent les requêtes

- **Les requêtes de `queries/` font foi** : ce sont elles qui produisent les chiffres du rapport. SQL brut, sans variable Metabase : leurs paramètres sont les constantes du CTE `parametres`.
- **Metabase sert à l'exploration préliminaire** (tableau de bord 228) : ses questions ne sont ni dans `metabase/` ni synchronisées, et peuvent diverger des règles de ce fichier. Avant de publier un graphique « Pour l'instant dans Metabase », rapatrier son SQL dans `queries/` en appliquant les règles ci-dessus, et mettre à jour la carte des graphiques.
- Une règle modifiée se reporte dans toutes les requêtes de `queries/` qui l'appliquent.
- Toute requête de `queries/` est exportée dans l'onglet `<requête>_sheet` du Sheet : la carte des graphiques ci-dessus donne cet onglet.
