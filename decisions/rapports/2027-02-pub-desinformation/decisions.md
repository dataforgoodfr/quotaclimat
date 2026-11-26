# Rapport pub février 2027 : qui finance la désinformation climatique à la télévision ?

- Statut : en cours, chiffres préliminaires (à faire vérifier par l'équipe pub et l'équipe mésinformation avant toute publication)
- Publication prévue : février 2027
- Requêtes de référence : [`queries/`](queries/). Graphiques : Google Sheet `2027-02-pub-desinformation`, un onglet par graphique. L'exploration préliminaire s'est faite dans Metabase (tableau de bord 228).
- Conventions communes aux rapports : [`../decisions.md`](../decisions.md) (dépôt public : aucun chiffre ici)

## 1. Objet et architecture

Le rapport décrit quels annonceurs financent, par leur publicité, les émissions de télévision où l'OMÉ relève de la mésinformation climatique.

Chaîne de données :

1. **Publicités** : détection, classification et consolidation (`quotaclimat/data_ingestion/advertising/`, voir les README des étapes s03 à s05), puis dbt : `analytics.publicites` (une diffusion), `advertising.pub_annonceur_emission_jour` (secondes de pub de chaque marque autour de chaque émission, par jour ; [`my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql`](../../../my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql)). Les colonnes `secteur`, `categorie`, `entreprise`, `agence_media`, `groupe_agence`, `fiabilite_agence` viennent de là.
2. **Mésinformation et climat par émission** : `public.emission_jour` (une ligne par émission et par jour : `climat_heures`, `nb_cas` validés ; [`my_dbt_project/models/analytics/emission_jour.sql`](../../../my_dbt_project/models/analytics/emission_jour.sql)). Jointure avec la pub sur `emission_cle`.
3. **Cas et catégories de pub visées** (partie 4.2) : `analytics.cas_categorie_visee` (catégories de pub visées par chaque cas, d'après les cibles de ses affirmations, rattachées à la main dans le Google Sheet `Cas_de_mesinfo`) et la grille cibles et sens × catégories `advertising.ref_cas_grille_binaire` ; voir le README de `my_dbt_project/`.
4. **Chaînes** : `curated.chaine` (`est_info_continue` sépare chaînes d'information en continu et généralistes). **Plage de dates** : `curated.publicite` (dernier jour de pub extrait).
5. **Google Sheets puis Google Doc** : tous les graphiques du rapport sont faits dans le Google Sheet `2027-02-pub-desinformation` : un onglet par graphique, avec le résultat de sa requête de `queries/` à partir de A1 et le graphique branché sur ces cellules (le nom de l'onglet est dans la carte des graphiques). Les graphiques sont collés dans le Google Doc du rapport. Pour mettre à jour un graphique : relancer sa requête et remplacer les données de l'onglet (même nombre de colonnes ; si le nombre de lignes change, étendre la plage du graphique), puis changer la date d'extraction dans le sous-titre.

## 2. Règles de calcul

- **Plage analysée** : l'année 2026 (du 01/01/2026 au 31/12/2026), CTE `plage` de chaque requête. Les données de 2026 ne sont pas encore toutes extraites ni classifiées : les chiffres bougeront jusqu'à la fin de l'extraction, d'où la date d'extraction dans le sous-titre de chaque graphique. Exception : un ratio qui rapporte du temps de pub à du temps de programme (intensité publicitaire, partie 1.3) s'arrête au dernier jour de pub extrait (`MAX(jour)` de `curated.publicite`), sinon les jours sans pub extraite gonflent le dénominateur.
- **Temps d'antenne, pas budget** : nous n'avons ni tarifs d'écran ni recettes. Partout où le plan parle de budget, de recettes ou de dépenses, on montre des minutes de pub ou des parts de minutes.
- **Annonceur** : l'entreprise (`entreprise` de `pub_annonceur_emission_jour`, qui regroupe ses marques), sauf quand le titre du graphique dit autre chose. Pubs sans marque détectée écartées.
- **Pub autour d'une émission** : règle du modèle dbt `pub_annonceur_emission_jour` (la même que [`metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql`](../../../metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql)). Un écran publicitaire va à l'émission qui le contient ; sinon aux émissions qui finissent moins de 15 min avant son début ou commencent moins de 15 min après sa fin (moitié de sa durée à chacune, toute sa durée s'il n'y en a qu'une) ; toutes les pubs de l'écran suivent. 15 min : variable dbt `ad_emission_window_minutes`.
- **Taux de mésinformation** : cas validés par heure consacrée au climat (`climat_heures` : fenêtres de 20 s avec au moins un mot-clé climat ; biodiversité et ressources exclues), `SUM(nb_cas) / SUM(climat_heures)` de `public.emission_jour` sur la plage.
- **Émission à fort taux de mésinformation** (en discussion : seuil à confirmer par l'équipe mésinformation) : sur la plage, au moins 1 cas validé par heure de climat **et** au moins 0,5 h de climat au total (en dessous, le taux repose sur trop peu d'heures). Paramètres `seuil` et `climat_heures_min` des requêtes de `queries/` (`env_heures_min` dans les questions Metabase, nom gardé pour les filtres existants). Ce seuil reproduit « à proximité d'un cas avéré » du plan sans dépendre du jour exact du cas.
- **Catégorie visée par un cas** (partie 4.2) : un cas vise une catégorie de produit quand au moins une de ses affirmations porte sur une cible (véhicule électrique, renouvelables...), dans le sens d'une ligne de la grille cibles et sens × catégories qui indique « Produit visé » = 1 (lien direct : l'argument de vente du produit est contredit ; liens indirects et produits confortés exclus). Les catégories visées par exactement les mêmes couples cible et sens forment un **groupe**, attaqué par les mêmes cas.
- **Émission à fort taux contre une catégorie** (partie 4.2) : on ne connaît pas le temps d'antenne passé sur chaque catégorie, donc pas de taux par catégorie. L'émission doit passer le seuil de fort taux ci-dessus, tous cas confondus (au moins 1 cas validé par heure de climat et au moins 0,5 h de climat sur la période), **et** compter au moins 2 cas contre le groupe de la catégorie (un cas isolé ne dit pas que l'émission s'en prend à ce produit). Paramètres `seuil`, `climat_heures_min` et `nb_cas_groupe_min` de la requête. Le minimum de 2 cas est à valider par l'équipe mésinformation.
- **Période de la partie 4.2** : la plage analysée (les cas ne sont rattachés aux cibles qu'à partir de 2026). Le seuil de fort taux de l'émission est calculé sur cette même période.
- **Part exposée** : part du temps de pub rattaché à une émission à fort taux. Une moyenne (secteur, type de chaîne) est pondérée par le temps de pub : on somme les secondes, on ne moyenne pas des parts. Elle porte sur toutes les pubs du groupe, pas seulement les points affichés.
- **Chaînes d'info seules** dans les parties 3, 4.1 et 4.2 : aucune émission généraliste n'atteint le seuil de fort taux, leur part exposée est nulle. À revoir si le seuil change.
- **Secteurs affichés** : les 9 premiers secteurs par temps de pub **toutes chaînes confondues** (généralistes et info), quel que soit le type de chaîne du graphique, pour garder la même liste de secteurs d'un graphique à l'autre. Dans chaque secteur : les 6 premières entreprises par temps de pub.
- **Catégories de produit** (partie 4.1) : les 3 premiers secteurs de cette même liste ; dans chacun, les 8 premières catégories de produit par temps de pub ; une catégorie inconnue prend le nom du secteur.
- **Exposition des principaux annonceurs** : minutes de pub de l'annonceur rattachées à une émission à fort taux, découpées par émission (les 8 premières nommées, les autres en « Autres émissions à fort taux »). Deux classements : par minutes exposées, et par minutes totales (avec, en gris, la pub hors de ces émissions).
- **Agence média** : agence de la marque, sinon de son entreprise (Google Sheet `Inventaire_des_annonceurs`, via `ad_brand_agencies`) ; pour une entreprise, l'agence qui achète le plus de ses secondes. Affichée en une colonne : « Agence (Groupe) » quand elle est confirmée (pas de mention « confirmée », c'est le cas par défaut), « probable : Agence (Groupe) », « indice : Agence (Groupe) », « Agence inconnue » ; le groupe n'est pas répété quand il porte le nom de l'agence.

## 3. Présentation

- **Couleurs** : une couleur par secteur dans les graphiques à bulles (la série est le nom du secteur), moyenne du secteur en noir ; codes dans [`metabase/couleurs.yml`](../../../metabase/couleurs.yml), groupes `secteurs_publicite` et `exposition_annonceurs`.
- **Taille des bulles** : racine carrée des minutes de pub (la surface est proportionnelle aux minutes) ; taille fixe 4 pour les moyennes de secteur.
- **Bulles alignées** : toutes les bulles d'un secteur ont la même abscisse (le rang du secteur).
- **Noms sur les bulles** : Metabase ne sait pas les écrire (nom au survol seulement), une des raisons de faire les graphiques dans Google Sheets.
- **Couleur par type de chaîne** (parties 1.3, 1.4 et 2) : les généralistes en bleu (#509EE3), les chaînes d'information en rouge clair (#EF8C8C). Pour une barre par chaîne, la requête met la valeur dans la colonne « Généralistes » ou « Chaînes d'information » selon le type (barres empilées, l'autre colonne vide).
- **Pub des 15 principaux annonceurs par émission** (partie 3) : la requête donne une ligne par annonceur et émission ; un tableau croisé en formules (`SOMME.SI.ENS`) à côté des données met une colonne par émission, et le graphique en barres empilées est construit dessus. « Autres émissions à fort taux » en gris (#949AAB, `metabase/couleurs.yml`). Seul le classement par pub exposée est gardé (pas de version par pub totale).
- **Principaux annonceurs et agences média** (partie 3) : tableau, pas de graphique ; indice de surexposition coloré du vert (0) au rouge (2), blanc à 1.

## 4. Remarques

- On ne répartit pas les cas d'une émission entre ses annonceurs au prorata de leur pub (une entreprise qui a 10 % de la pub d'une émission à 20 cas se verrait attribuer 2 cas) : une émission avec très peu de pub (France Info TV, surtout du parrainage) concentre alors tous ses cas sur deux ou trois annonceurs, qui remontent en tête sans être exposés en volume. On mesure plutôt les minutes de pub exposées.
- On ne montre pas tous les secteurs dans les graphiques par entreprise : illisible.
- On ne trace pas la moyenne des chaînes d'info dans les graphiques à bulles : un point par secteur à la même hauteur, peu lisible ; seules les moyennes de secteur restent.
- On ne fait pas de bulles par catégorie dans le graphique de la partie 3, qui porte sur les annonceurs : les catégories ont leur propre graphique (partie 4.1).
- Pas de lien public Metabase lu par `IMPORTDATA` pour alimenter le Sheet : il publierait sur internet des données de suivi non publiées. Un flow Kestra d'export des requêtes de `queries/`, lancé à la main, est en cours de développement.
- Les pubs ne sont cherchées qu'à ±15 min des programmes suivis : les généralistes ne sont vues qu'autour de leurs JT et matinales, ne jamais les comparer aux chaînes d'info en niveau. France Télévisions, Arte et France Médias Monde : surtout des parrainages.
- Une pub « autour » d'une émission ne dit pas qu'elle a été diffusée pendant le passage mésinformant, ni le même jour que lui (le fort taux porte sur toute la plage).
- Partie 4.2 : on ne met pas de seuil sur le taux des seuls cas contre une catégorie (par heure de climat ou par heure d'antenne). Le temps de climat surestime de beaucoup le temps passé sur une catégorie, et avec un ou deux cas ce taux n'est que du bruit ; le seuil de l'émission dit si elle mésinforme beaucoup, le minimum de cas dit si elle vise le produit.
- Partie 4.2 : on abandonne l'ancienne version (pub automobile le jour d'un cas du thème automobile, repéré par mots-clés) : un seul thème, et un mot-clé ne dit pas que le produit est attaqué. Les cibles des affirmations couvrent tous les produits de la grille.

## 5. Carte des graphiques

Dans l'ordre du plan du rapport.

| Partie | Graphique | Requête |
|---|---|---|
| 1.1 | Acteurs du marché publicitaire | aucune (schéma à dessiner à partir de recherches documentaires) |
| 1.2 | Poids de la pub dans les revenus des chaînes | aucune (il faut le chiffre d'affaires des chaînes : rapports annuels, Arcom) |
| 1.3 | Intensité publicitaire par heure de programme suivi | [`queries/intensite_pub_par_chaine_sheets.sql`](queries/intensite_pub_par_chaine_sheets.sql), onglet `intensite_pub_par_chaine` |
| 1.3 | Concentration : part des 10 premiers annonceurs | [`queries/concentration_top10_par_chaine_sheets.sql`](queries/concentration_top10_par_chaine_sheets.sql), onglet `concentration_top10_par_chaine` |
| 1.4 | Répartition sectorielle du temps de pub par type de chaîne | [`queries/repartition_secteurs_par_type_chaine_sheets.sql`](queries/repartition_secteurs_par_type_chaine_sheets.sql), onglet `repartition_secteurs_par_type_chaine` |
| 1.4 | Zooms par catégorie de produit : Automobile et transport, Alimentation, Établissements financiers et assurance | [`queries/zoom_categories_secteurs_sheets.sql`](queries/zoom_categories_secteurs_sheets.sql) (les trois secteurs), onglets `zoom_categ_automobile_transport`, `zoom_categ_alimentation` et `zoom_categ_finance_assurance` (lignes du secteur) |
| 2.1 | Fréquence de mésinformation par chaîne | [`queries/frequence_mesinfo_par_chaine_sheets.sql`](queries/frequence_mesinfo_par_chaine_sheets.sql), onglet `frequence_mesinfo_par_chaine` |
| 2.2 | Fréquence de mésinformation par émission (au moins 2 h de climat) | [`queries/frequence_mesinfo_par_emission_sheets.sql`](queries/frequence_mesinfo_par_emission_sheets.sql), onglet `frequence_mesinfo_par_emission` |
| 3 | Part exposée par annonceur, chaînes d'info (graphique à bulles) | [`queries/bulles_annonceurs_secteur_sheets.sql`](queries/bulles_annonceurs_secteur_sheets.sql), onglet `part_temps_de_pub_prox_mesinfo_par_annonceurs` |
| 3 | Pub des 15 principaux annonceurs autour d'émissions à fort taux, par émission, tri par pub exposée | [`queries/pub_annonceurs_emissions_fort_taux_sheets.sql`](queries/pub_annonceurs_emissions_fort_taux_sheets.sql), onglet `pub_annonceurs_emissions_fort_taux` |
| 3 | Principaux annonceurs exposés et leurs agences média (tableau) | [`queries/annonceurs_agences_media_sheets.sql`](queries/annonceurs_agences_media_sheets.sql), onglet `annonceurs_agences_media` |
| 3 | Part de CNews dans la pub achetée par chaque groupe d'agences, parmi BFM TV, CNews et LCI | [`queries/part_cnews_par_groupe_agence_sheets.sql`](queries/part_cnews_par_groupe_agence_sheets.sql), onglet `part_cnews_par_groupe_agence` |
| 4.1 | Part exposée par catégorie de produit, trois premiers secteurs, chaînes d'info (graphique à bulles) | [`queries/bulles_categories_secteur_sheets.sql`](queries/bulles_categories_secteur_sheets.sql), onglet `part_temps_de_pub_prox_mesinfo_par_categ` |
| 4.2 | Pub de chaque catégorie de produit autour d'émissions à fort taux de mésinformation contre cette catégorie, chaînes d'info (barres) | [`queries/pub_categories_visees_fort_taux.sql`](queries/pub_categories_visees_fort_taux.sql), onglet `pub_categories_visees_fort_taux` |

## 6. Où vivent les requêtes

- **Les requêtes de `queries/` font foi** : ce sont elles qui produisent les chiffres du rapport. SQL brut, sans variable Metabase : leurs paramètres sont les constantes du CTE `parametres`.
- **Metabase sert à l'exploration préliminaire** (tableau de bord 228) : ses questions ne sont ni dans `metabase/` ni synchronisées, et peuvent diverger des règles de ce fichier (notamment la plage, restée sur un an glissant). Un nouveau graphique passe par une requête de `queries/` et un onglet du Sheet, et entre dans la carte des graphiques.
- Une règle modifiée se reporte dans toutes les requêtes de `queries/` qui l'appliquent.
