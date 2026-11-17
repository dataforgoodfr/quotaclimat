# Rapport pub février 2027 : qui finance la désinformation climatique à la télévision ?

- Statut : en cours, chiffres préliminaires (à faire vérifier par l'équipe pub et l'équipe mésinformation avant toute publication)
- Publication prévue : février 2027
- Requêtes de référence : [`queries/`](queries/). L'exploration préliminaire s'est faite dans Metabase (tableau de bord 228) ; un graphique marqué « Pour l'instant dans Metabase » n'a pas encore sa requête versionnée (§ 5).
- Conventions communes aux rapports : [`../decisions.md`](../decisions.md) (dépôt public : aucun chiffre ici)

## 1. Objet et architecture

Le rapport décrit quels annonceurs financent, par leur publicité, les émissions de télévision où l'OMÉ relève de la mésinformation climatique.

Chaîne de données :

1. **Publicités** : détection, classification et consolidation (`quotaclimat/data_ingestion/advertising/`, voir les README des étapes s03 à s05), puis dbt : `analytics.publicites` (une diffusion), `advertising.pub_annonceur_emission_jour` (secondes de pub de chaque marque autour de chaque émission, par jour ; [`my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql`](../../../my_dbt_project/models/analytics/pub_annonceur_emission_jour.sql)). Les colonnes `secteur`, `categorie`, `entreprise`, `agence_media`, `groupe_agence`, `fiabilite_agence` viennent de là.
2. **Mésinformation et environnement par émission** : `public.emission_jour` (une ligne par émission et par jour : `env_heures`, `nb_cas` validés ; [`my_dbt_project/models/analytics/emission_jour.sql`](../../../my_dbt_project/models/analytics/emission_jour.sql)). Jointure avec la pub sur `emission_cle`.
3. **Chaînes** : `curated.chaine` (`est_info_continue` sépare chaînes d'information en continu et généralistes). **Plage de dates** : `curated.publicite` (dernier jour de pub extrait).
4. **Google Sheets puis Google Doc** : les résultats des requêtes de `queries/` vont dans le Google Sheet `2027-02-pub-desinformation` (un onglet par graphique, données et graphique dans le même onglet), dont les graphiques sont collés dans le Google Doc du rapport.

## 2. Règles de calcul

| Règle | Contenu | Statut |
|---|---|---|
| R1 Plage analysée | Un an glissant jusqu'au dernier jour de pub extrait (`MAX(jour)` de `curated.publicite`), jamais avant le 29/09/2025 (données pub jugées non fiables avant), jamais après le 31/12/2026. À partir de 2027, la plage se fige sur l'année 2026. Calculée dans chaque requête par le même CTE `plage`. | adoptée |
| R2 Temps d'antenne, pas budget | Nous n'avons ni tarifs d'écran ni recettes. Partout où le plan parle de budget, de recettes ou de dépenses, on montre des minutes de pub ou des parts de minutes. Les montants en euros sont déclarés impossibles. | adoptée |
| R3 Niveau d'annonceur | L'entreprise (`entreprise` de `pub_annonceur_emission_jour`, qui regroupe ses marques), sauf quand le titre du graphique dit autre chose. Pubs sans marque détectée écartées. | adoptée |
| R4 Pub autour d'une émission | Règle appliquée par le modèle dbt `pub_annonceur_emission_jour` (la même que [`metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql`](../../../metabase/advertising/metabase_pub_annonceurs_ponderes_cas.sql)) : un écran publicitaire va à l'émission qui le contient ; sinon aux émissions qui finissent moins de 15 min avant son début ou commencent moins de 15 min après sa fin (moitié de sa durée à chacune, toute sa durée s'il n'y en a qu'une) ; toutes les pubs de l'écran suivent. 15 min : variable dbt `ad_emission_window_minutes`. | adoptée |
| R5 Taux de mésinformation | Cas validés par heure consacrée à l'environnement (climat, biodiversité, ressources) : `SUM(nb_cas) / SUM(env_heures)` de `public.emission_jour` sur la plage. | adoptée |
| R6 Émission à fort taux de mésinformation | Sur la plage, au moins 1 cas validé par heure d'environnement (R5) **et** au moins 0,5 h d'environnement au total (en dessous, le taux repose sur trop peu d'heures). Paramètres `seuil` et `env_heures_min` des requêtes. Seuil choisi pour reproduire « à proximité d'un cas avéré » du plan sans dépendre du jour exact du cas. | provisoire : seuil à confirmer par l'équipe mésinformation |
| R7 Part exposée | Part du temps de pub (R4) rattaché à une émission à fort taux (R6). Une moyenne (secteur, type de chaîne) est pondérée par le temps de pub : on somme les secondes, on ne moyenne pas des parts. Elle porte sur toutes les pubs du groupe, pas seulement les points affichés : les paramètres `nb_points` et `nb_secteurs` ne changent pas les moyennes. | adoptée |
| R8 Périmètre et comparaisons | Les pubs ne sont cherchées qu'à ±15 min des programmes suivis : les généralistes ne sont vues qu'autour de leurs JT et matinales. Ne jamais comparer généralistes et chaînes d'info en niveau ; les graphiques les séparent ou les filtrent. France Télévisions, Arte et France Médias Monde : surtout des parrainages. | adoptée |
| R9 Partie 3 sur les chaînes d'info seules | Aucune émission généraliste n'atteint le seuil R6 sur la plage : leur part exposée est nulle et leur nuage de points n'apporte rien. À revoir si le seuil change. | adoptée |
| R10 Secteurs affichés | Les 9 premiers secteurs par temps de pub **toutes chaînes confondues** (généralistes et info), quel que soit le type de chaîne du graphique. On ne prend pas le classement propre aux chaînes d'info, pour garder la même liste de secteurs d'un graphique à l'autre. Dans chaque secteur : les 6 premières entreprises par temps de pub. | adoptée |
| R11 Exposition des principaux annonceurs | Minutes de pub de l'annonceur rattachées à une émission à fort taux (R6), découpées par émission (les 8 premières nommées, les autres en « Autres émissions à fort taux »). Deux classements : par minutes exposées (sans la pub hors émissions à fort taux) et par minutes totales (avec, en gris, la pub hors de ces émissions). Chaînes d'info seules. | adoptée |
| R12 Agence média | Agence de la marque, sinon de son entreprise (Google Sheet `Inventaire_des_annonceurs`, via `ad_brand_agencies`) ; pour une entreprise, l'agence qui achète le plus de ses secondes. Affichée en une colonne : « Agence (Groupe) » quand elle est confirmée (pas de mention « confirmée », c'est le cas par défaut), « probable : Agence (Groupe) », « indice : Agence (Groupe) », « Agence inconnue » ; le groupe n'est pas répété quand il porte le nom de l'agence. | adoptée |
| R13 Catégories de produit des trois premiers secteurs | Graphique à bulles de la partie 4.1 : les 3 premiers secteurs de R10 (même classement, toutes chaînes confondues), sur les chaînes d'info (R9) ; dans chaque secteur, les 8 premières catégories de produit par temps de pub ; une catégorie inconnue prend le nom du secteur ; une bulle noire par secteur pour sa moyenne (R7). | adoptée |

### Abandonnées

| Règle | Raison | Date |
|---|---|---|
| Attribution des cas aux annonceurs au prorata de leur pub (une entreprise qui a 10 % de la pub d'une émission à 20 cas se voit attribuer 2 cas), pour classer et représenter les annonceurs exposés | Une émission qui a très peu de pub (France Info TV, surtout du parrainage) concentre tous ses cas sur deux ou trois annonceurs, qui remontent en tête sans être exposés en volume. Remplacée par R11. Le tableau des agences garde encore ce classement : voir § 6. | 16/11/2026 |
| Tous les secteurs dans le nuage de points de la partie 3 | Illisible ; on se limite aux secteurs de R10 dès qu'on entre dans le détail par entreprise. | 16/11/2026 |
| Moyenne du type de chaîne (chaînes d'info) dans les nuages de points | Un point par secteur à la même hauteur, peu lisible ; seules les moyennes des secteurs restent. | 17/11/2026 |
| Une bulle par catégorie de produit dans le nuage de la partie 3 | Essayée puis annulée : la partie 3 porte sur les annonceurs ; les catégories ont leur propre graphique (R13). | 17/11/2026 |

## 3. Choix de présentation

- **Une couleur par secteur** dans les graphiques à bulles (la série est le nom du secteur). Couleurs fixées : Automobile et transport `#509EE3`, Alimentation `#88BF4D`, Établissements financiers et assurance `#F2A86F`, moyenne du secteur en noir ; les autres secteurs en couleurs automatiques. Barres de R11 : gris clair `#DCDFE0` pour la pub hors émissions à fort taux, gris `#949AAB` pour « Autres émissions à fort taux ».
- **Taille des bulles** : racine carrée des minutes de pub. Le rayon est proportionnel à la valeur, la surface l'est donc aux minutes ; avec les minutes brutes, les plus gros annonceurs écrasaient les autres. Les moyennes de secteur ont une taille fixe (4).
- **Bulles alignées** : toutes les bulles d'un secteur ont la même abscisse (le rang du secteur).
- **Noms des entreprises ou des catégories sur les bulles** : impossible dans Metabase (les étiquettes d'un nuage de points ne montrent que la valeur de l'axe vertical ; le nom n'est qu'au survol). Les graphiques à bulles sont donc faits dans Google Sheets (colonne d'étiquette), puis collés dans le Google Doc.
- **Rafraîchissement du Sheet** : un flow Kestra d'export des requêtes de `queries/` vers les Sheets, lancé à la main, est en cours de développement. Pas de lien public Metabase lu par `IMPORTDATA` : il publierait sur internet des données de suivi non publiées.

## 4. Carte des graphiques

Dans l'ordre du plan du rapport.

| Partie | Graphique | Requête | Règles | Remarque |
|---|---|---|---|---|
| 1.1 | Acteurs du marché publicitaire | aucune | | 🚫 schéma à dessiner à partir de recherches documentaires |
| 1.2 | Poids de la pub dans les revenus des chaînes | aucune | R2 | 🚫 il faut le chiffre d'affaires des chaînes (rapports annuels, Arcom) |
| 1.3 | Intensité publicitaire par heure d'information | Pour l'instant dans Metabase | R2, R8 | |
| 1.3 | Concentration : part des 10 premiers annonceurs | Pour l'instant dans Metabase | R2, R3 | |
| 1.4 | Répartition sectorielle du temps de pub par type de chaîne | Pour l'instant dans Metabase | R8 | sa liste de secteurs est celle de R10 |
| 1.4 | Zooms par catégorie de produit : Automobile et transport, Alimentation, Établissements financiers et assurances | Pour l'instant dans Metabase | R8 | |
| 2.1 | Fréquence de mésinformation par chaîne | Pour l'instant dans Metabase | R5 | |
| 2.2 | Fréquence de mésinformation par émission | Pour l'instant dans Metabase | R5 | émissions avec au moins 2 h d'environnement |
| 3 | Part exposée par annonceur, chaînes d'info (graphique à bulles) | [`queries/bulles_annonceurs_secteur_sheets.sql`](queries/bulles_annonceurs_secteur_sheets.sql) | R1, R3 à R10 | onglet `part_temps_de_pub_prox_mesinfo_par_annonceurs` du Sheet |
| 3 | Pub des 15 principaux annonceurs autour d'émissions à fort taux, par émission, tri par pub exposée puis par pub totale | Pour l'instant dans Metabase | R11 | |
| 3 | Principaux annonceurs exposés et leurs agences média | Pour l'instant dans Metabase | R12 | classement à revoir, voir § 6 |
| 3 | Part de CNews dans la pub achetée par chaque groupe d'agences | Pour l'instant dans Metabase | R12 | parmi BFM TV, CNews et LCI |
| 4.1 | Part exposée par catégorie de produit, trois premiers secteurs, chaînes d'info (graphique à bulles) | [`queries/bulles_categories_secteur_sheets.sql`](queries/bulles_categories_secteur_sheets.sql) | R1, R4 à R9, R13 | onglet `part_temps_de_pub_prox_mesinfo_par_categ` du Sheet |
| 4.2 | Pubs automobiles à proximité d'une mésinformation sur l'automobile | Pour l'instant dans Metabase | R4 | seul thème de cas disponible |

## 5. Où vivent les requêtes

- **Les requêtes de `queries/` font foi.** Ce sont elles qui produisent les chiffres du rapport. SQL brut, sans variable Metabase : leurs paramètres sont les constantes du CTE `parametres`.
- **Metabase sert à l'exploration préliminaire** (tableau de bord 228). Ses questions ne sont ni dans `metabase/` ni synchronisées, et peuvent diverger des règles de ce fichier. Un graphique marqué « Pour l'instant dans Metabase » n'a pas encore sa requête versionnée : avant de le publier, rapatrier son SQL dans `queries/` en appliquant les règles ci-dessus, et mettre à jour la carte des graphiques.
- Une règle modifiée se reporte dans toutes les requêtes de `queries/` qui l'appliquent (colonne « Règles » de la carte).

## 6. Limites et points à valider

- Seuil R6 (1 cas par heure d'environnement, 0,5 h minimum) : 🔴 [validation de l'équipe mésinformation]
- Tableau des agences (pour l'instant dans Metabase) : il classe encore les annonceurs par cas attribués (règle abandonnée), avec le même biais vers les émissions qui ont peu de pub. 🔴 [décider : garder ce classement ou reprendre celui de R11]
- Qualité des données pub, à signaler à l'équipe pub : certaines entreprises détectées sont des agences ou des intermédiaires plutôt que des annonceurs, une entreprise est nommée « null », certains rattachements de secteur sont douteux (une même entreprise dans deux secteurs, un assureur classé en boissons). Le classement des secteurs et des marques est automatique (LLM), corrigé à la main dans le Google Sheet `Corrections_publicites`.
- Le nombre de cas dépend de l'effort d'annotation : une chaîne ou une émission plus annotée a mécaniquement plus de cas.
- Proximité : une pub « autour » d'une émission (R4) ne dit pas que la pub a été diffusée pendant le passage mésinformant, ni le même jour que lui (R6 porte sur toute la plage).
- Non reproductible en l'état : parties 1.1 et 1.2, tout montant en euros, partie 4.2 hors automobile (les cas n'ont qu'un thème, automobile, via `my_dbt_project/seeds/themes/mesinfo_theme_mots_cles.csv`).

## 7. Historique

- 16/11/2026 : exploration dans Metabase de la partie 3. Nuage des généralistes retiré (R9) ; une couleur par secteur, bulles en racine des minutes ; secteurs limités à R10 ; attribution des cas au prorata abandonnée au profit de R11 ; colonne d'agence unique (R12).
- 17/11/2026 : création de ce dossier ; requêtes des graphiques à bulles par annonceur (partie 3) et par catégorie de produit (partie 4.1, R13) ; bulles alignées par secteur, moyenne des chaînes d'info retirée, moyennes de secteur en taille 4. Les requêtes de `queries/` font foi, Metabase sert à l'exploration.
