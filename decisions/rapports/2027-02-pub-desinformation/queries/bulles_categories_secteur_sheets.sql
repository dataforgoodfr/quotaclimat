-- Rapport pub février 2027, partie 3 : données du graphique à bulles « Part du temps de pub autour d'émissions à fort
-- taux de mésinformation, par catégorie de produit », pour Google Sheets (graphique à bulles avec le nom de chaque
-- catégorie).
-- Même calcul que la question Metabase 2500 (chaînes d'information en continu), mis en forme pour le graphique à bulles
-- de Google Sheets : une colonne d'étiquette, une abscisse numérique, une ordonnée, une série (couleur), une taille.
-- Règles et choix : ../decisions.md (R1 à R10, R13). Procédure Sheets et Docs : ../google_sheets.md.
--
-- Base : Baromètre (Scaleway Postgres, base Metabase 2). SQL brut, sans variable Metabase : les paramètres sont les
-- constantes du CTE parametres, à modifier ici (et dans decisions.md si la règle change).
--
-- Émission à fort taux de mésinformation : sur la période, au moins seuil cas validés par heure consacrée à
-- l'environnement (public.emission_jour : nb_cas / env_heures), et au moins env_heures_min heures d'environnement.
-- Rattachement des pubs aux émissions : advertising.pub_annonceur_emission_jour (règle de la question 2358). Bulle :
-- une catégorie de produit, le nom du secteur quand elle est inconnue (comme la question 2502). Pubs sans marque
-- détectée écartées. Temps d'antenne, pas budget.
-- Secteurs gardés : les nb_secteurs premiers par temps de pub toutes chaînes confondues (généralistes et chaînes
-- d'info), comme le camembert de la partie 1.4 ; points : les nb_points premières catégories de chaque secteur par
-- temps de pub sur les chaînes d'info. Moyennes pondérées par le temps de pub, calculées sur toutes les pubs du
-- secteur (ou du type de chaîne), pas seulement les points affichés.
--
-- Colonnes, dans l'ordre attendu par le graphique à bulles de Google Sheets :
--   etiquette : nom de la catégorie ; vide pour les moyennes (pas d'étiquette) ;
--   x : numéro du secteur (rang_secteur, 1 = premier secteur) décalé de -0,3 à +0,3 selon la part de la catégorie,
--     pour écarter les étiquettes d'un même secteur ; les moyennes sont au centre (x = rang_secteur) ;
--   part_proche_pct : part du temps de pub rattaché à une émission à fort taux (de 0 à 100) ;
--   serie : nom du secteur pour les catégories (une couleur par secteur), « Moyenne du secteur », « Moyenne des
--     chaînes d'info » (même valeur dans chaque secteur : remplace la ligne d'objectif de Metabase) ;
--   taille : racine carrée des minutes de pub (le rayon de la bulle est proportionnel à la valeur, la surface l'est
--     donc aux minutes) ; 1 pour les moyennes.
-- Colonnes suivantes (hors graphique, pour vérifier) : secteur, rang_secteur, pub_min.
--
-- Plage analysée, la même pour tout le rapport : un an glissant jusqu'au dernier jour de pub extrait
-- (curated.publicite), jamais avant le 29 septembre 2025 (début de l'analyse des publicités) ni après le
-- 31 décembre 2026 (à partir de 2027 : toute l'année 2026).
WITH plage AS (
    SELECT GREATEST(DATE '2025-09-29', (m.fin - INTERVAL '1 year')::date + 1) AS debut, m.fin
    FROM (SELECT LEAST(MAX(jour), DATE '2026-12-31') AS fin FROM curated.publicite) m
),
parametres AS (
    SELECT
        1::numeric AS seuil,
        0.5::numeric AS env_heures_min,
        9 AS nb_secteurs,
        6 AS nb_points,
        0.6::numeric AS largeur_secteur
),
-- émissions à fort taux sur la période
emissions_fort_taux AS (
    SELECT ej.emission_cle
    FROM public.emission_jour ej
    CROSS JOIN plage pl
    WHERE ej.jour BETWEEN pl.debut AND pl.fin
    GROUP BY ej.emission_cle
    HAVING SUM(ej.env_heures) >= (SELECT env_heures_min FROM parametres)
       AND SUM(ej.nb_cas) / SUM(ej.env_heures) >= (SELECT seuil FROM parametres)
),
-- pubs des chaînes d'information en continu
pub AS (
    SELECT
        COALESCE(p.secteur, 'Secteur inconnu') AS secteur,
        COALESCE(p.categorie, p.secteur) AS categorie,
        p.pub_secondes,
        ef.emission_cle IS NOT NULL AS est_proche
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    LEFT JOIN emissions_fort_taux ef ON ef.emission_cle = p.emission_cle
    WHERE p.jour BETWEEN pl.debut AND pl.fin
      AND c.est_info_continue
),
-- rang des secteurs par temps de pub, toutes chaînes confondues
rang_secteurs AS (
    SELECT
        COALESCE(p.secteur, 'Secteur inconnu') AS secteur,
        ROW_NUMBER() OVER (ORDER BY SUM(p.pub_secondes) DESC, COALESCE(p.secteur, 'Secteur inconnu')) AS rang_secteur
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN plage pl
    JOIN curated.chaine c ON c.chaine_id = p.channel_name
    WHERE p.jour BETWEEN pl.debut AND pl.fin
    GROUP BY 1
),
moyenne_type AS (
    SELECT 100 * SUM(pub_secondes) FILTER (WHERE est_proche) / SUM(pub_secondes) AS part_pct FROM pub
),
secteurs AS (
    SELECT
        pu.secteur,
        SUM(pu.pub_secondes) / 60 AS pub_min,
        100 * COALESCE(SUM(pu.pub_secondes) FILTER (WHERE pu.est_proche), 0) / SUM(pu.pub_secondes) AS part_pct,
        MAX(r.rang_secteur) AS rang_secteur
    FROM pub pu
    JOIN rang_secteurs r ON r.secteur = pu.secteur
    GROUP BY pu.secteur
),
categories AS (
    SELECT
        secteur,
        categorie,
        SUM(pub_secondes) / 60 AS pub_min,
        100 * COALESCE(SUM(pub_secondes) FILTER (WHERE est_proche), 0) / SUM(pub_secondes) AS part_pct,
        ROW_NUMBER() OVER (PARTITION BY secteur ORDER BY SUM(pub_secondes) DESC, categorie) AS rang
    FROM pub
    WHERE categorie IS NOT NULL
    GROUP BY secteur, categorie
),
points AS (
    SELECT
        a.*,
        s.rang_secteur,
        -- position dans le secteur, de la plus faible part à la plus forte : les étiquettes s'étagent en diagonale
        ROW_NUMBER() OVER (PARTITION BY a.secteur ORDER BY a.part_pct, a.categorie) AS position,
        COUNT(*) OVER (PARTITION BY a.secteur) AS nb
    FROM categories a
    JOIN secteurs s ON s.secteur = a.secteur
    CROSS JOIN parametres pa
    WHERE s.rang_secteur <= pa.nb_secteurs
      AND a.rang <= pa.nb_points
)
SELECT
    p.categorie AS etiquette,
    ROUND((p.rang_secteur
        + CASE WHEN p.nb > 1 THEN pa.largeur_secteur * ((p.position - 1)::numeric / (p.nb - 1) - 0.5) ELSE 0 END
    )::numeric, 3) AS x,
    ROUND(p.part_pct::numeric, 3) AS part_proche_pct,
    p.secteur AS serie,
    ROUND(SQRT(p.pub_min)::numeric, 2) AS taille,
    p.secteur,
    p.rang_secteur,
    ROUND(p.pub_min::numeric, 1) AS pub_min
FROM points p
CROSS JOIN parametres pa
UNION ALL
SELECT '', s.rang_secteur, ROUND(s.part_pct::numeric, 3), 'Moyenne du secteur', 1, s.secteur, s.rang_secteur,
    ROUND(s.pub_min::numeric, 1)
FROM secteurs s
CROSS JOIN parametres pa
WHERE s.rang_secteur <= pa.nb_secteurs
UNION ALL
SELECT '', s.rang_secteur, ROUND(m.part_pct::numeric, 3), 'Moyenne des chaînes d''info', 1, s.secteur, s.rang_secteur,
    NULL
FROM secteurs s
CROSS JOIN parametres pa
CROSS JOIN moyenne_type m
WHERE s.rang_secteur <= pa.nb_secteurs
ORDER BY 7, 4, 2
