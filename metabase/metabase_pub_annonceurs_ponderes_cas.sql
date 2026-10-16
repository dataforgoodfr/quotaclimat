-- Part des annonceurs pondérée par la mésinformation des émissions autour desquelles ils font de la pub, toutes
-- émissions et tous annonceurs confondus (chaînes françaises) : camembert (dimension annonceur, métrique
-- cas_attribues) et courbes cumulées (abscisse rang_cas ou rang_pub).
-- Variables Metabase :
--   debut   type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin     type Date, dernier jour inclus, ex. 2026-09-30
--   niveau  type Texte, échelle de l'annonceur : marque, entreprise ou groupe (société mère ultime) ; à régler en
--           liste déroulante de ces trois valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien)
--   ponderation_par_temps_denv type Texte, facultatif (false si vide), true ou false (liste déroulante) : poids
--           d'une seconde de pub autour d'une émission
--     - false : les cas de chaque émission sur la période sont répartis entre les annonceurs au prorata de leurs
--       secondes de pub autour de l'émission, puis additionnés par annonceur (cas_attribues). Un annonceur qui a
--       10 % de la pub d'une émission à 20 cas se voit attribuer 2 cas. Favorise les émissions longues (plus
--       d'heures, plus de cas) ;
--     - true : chaque seconde de pub pèse le score de son émission, ses cas par heure consacrée à l'environnement
--       sur la période (intensité de la mésinformation climatique, indépendante de la durée des émissions) : une
--       seconde autour d'une émission à 2 cas par heure d'environnement pèse deux fois plus qu'autour d'une
--       émission à 1 cas par heure. cas_attribues est alors la somme des secondes x score (ce n'est plus un
--       nombre de cas, seules les parts, rangs et cumuls se lisent) ; les colonnes _cas se lisent « pondéré ».
--   env_heures_min type Nombre, facultatif (0.5 si vide), avec ponderation_par_temps_denv seulement : émissions
--           gardées, au moins ce temps consacré à l'environnement sur la période ; en dessous le score repose sur
--           trop peu d'heures (1 cas en 6 min d'environnement donne 10 cas par heure) et l'émission est écartée de
--           tout le calcul, pub comprise
--   nb_points type Nombre, facultatif (2000 si vide) : annonceurs gardés, ceux classés dans les nb_points premiers
--           des deux classements (rang_pub et rang_cas) ; les parts et les cumuls restent calculés sur tous les
--           annonceurs (le dernier point n'atteint donc pas 100 %)
-- Une ligne par annonceur : le regroupement des petites parts en « Autres » est laissé au camembert de Metabase
-- (réglage « Pourcentage minimum de part »).
-- Colonnes :
--   cas_attribues, part_cas_pct : poids de l'annonceur (voir ponderation_par_temps_denv) et sa part dans le
--     total ;
--   pub_secondes, part_pub_pct : sa pub rattachée à une émission (gardée) et sa part, pour comparer ;
--   indice_surexposition : part_cas_pct / part_pub_pct ; 1 : l'annonceur est autour de la mésinformation autant
--     que sa pub le laisse attendre, 2 : deux fois plus (avec ponderation_par_temps_denv : le score moyen de ses
--     émissions est le double de celui de toute la pub) ;
--   rang_pub, rang_cas : rang de l'annonceur dans le classement par pub et dans le classement par cas_attribues
--     (1 : le premier ; un rang par annonceur, ex aequo départagés par ordre alphabétique, pour que chaque point
--     des courbes ait sa propre abscisse : Metabase additionne les lignes de même abscisse) ; ecart_rang :
--     rang_pub - rang_cas, positif quand l'annonceur remonte une fois sa pub pondérée ;
--   part_pub_cumulee_pct, part_cas_cumulee_pct : part cumulée des annonceurs classés jusqu'à ce rang (au rang 1
--     sa propre part, au rang 2 la somme des deux premiers…), dans chacun des deux classements ;
--   part_pub_cumulee_rang_cas_pct : part de pub cumulée, mais dans le classement par cas_attribues : à comparer
--     à part_cas_cumulee_pct (ex. les 10 premiers par cas font 30 % des cas pour 12 % de la pub) ;
--   part_cas_cumulee_rang_pub_pct : l'inverse, part des cas cumulée dans le classement par pub : à comparer à
--     part_pub_cumulee_pct (ex. les 10 premiers annonceurs par pub font 25 % de la pub et 20 % des cas) ;
--   ecart_cumule_rang_cas_pct, ecart_cumule_rang_pub_pct : écart entre les deux courbes cumulées de chaque
--     classement, part des cas cumulée - part de pub cumulée. Il grandit tant que les annonceurs ajoutés ont plus
--     de part de cas que de part de pub (indice_surexposition > 1), diminue ensuite ; son maximum (son minimum
--     s'il est négatif) marque le rang où la deuxième courbe commence à rattraper la première ; un plateau : des
--     annonceurs neutres (indice proche de 1) ou très petits. Il revient à 0 quand tous les annonceurs sont
--     comptés ;
--   cas_par_heure_moyen, cas_par_heure_env_moyen : cas par heure de programme et cas par heure d'environnement des
--     émissions de l'annonceur, moyennes pondérées par ses secondes de pub (la seconde sur les seules émissions
--     d'au moins env_heures_min heures d'environnement), quelle que soit la pondération.
-- Sources (dbt) : analytics.pub_annonceur_emission_jour (secondes de pub de chaque marque autour de chaque
-- émission, par jour de la pub) et analytics.emission_jour (cas, heures de programme et d'environnement par
-- émission, par jour de programme : écart possible d'une journée aux bords de la période pour les émissions qui
-- passent minuit).
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
parametres AS (
    SELECT
        COALESCE([[CAST({{ponderation_par_temps_denv}} AS boolean),]] FALSE) AS par_temps_env,
        COALESCE([[CAST({{env_heures_min}} AS numeric),]] 0.5) AS env_heures_min,
        COALESCE([[CAST({{nb_points}} AS integer),]] 2000) AS nb_points
),
-- secondes de pub par annonceur (au niveau choisi) et par émission
pub AS MATERIALIZED (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN p.marque
            WHEN 'entreprise' THEN p.entreprise
            WHEN 'groupe' THEN p.groupe
        END AS annonceur,
        p.emission_cle,
        SUM(p.pub_secondes) AS pub_secondes
    FROM analytics.pub_annonceur_emission_jour p
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe')
    GROUP BY 1, 2
),
-- cas, heures de programme et d'environnement de chaque émission sur la période
metriques AS (
    SELECT
        ej.emission_cle,
        SUM(ej.nb_cas) AS nb_cas,
        SUM(ej.volume_heures) AS volume_heures,
        SUM(ej.env_heures) AS env_heures
    FROM analytics.emission_jour ej
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
),
-- émissions gardées (pondération par le temps d'environnement : assez de temps d'environnement) et cas_attribues d'une seconde de pub autour
-- d'elles ; MATERIALIZED : calculé une seule fois
emissions AS MATERIALIZED (
    SELECT
        pe.emission_cle,
        m.nb_cas,
        m.volume_heures,
        -- score, défini seulement avec assez de temps d'environnement
        CASE WHEN m.env_heures >= pa.env_heures_min THEN m.nb_cas / m.env_heures END AS cas_par_heure_env,
        CASE WHEN pa.par_temps_env
            -- score de l'émission
            THEN m.nb_cas / m.env_heures
            -- cas de l'émission par seconde de sa pub : x secondes de l'annonceur = ses cas attribués
            ELSE COALESCE(m.nb_cas, 0) / pe.pub_secondes_emission
        END AS poids_par_seconde
    FROM (SELECT emission_cle, SUM(pub_secondes) AS pub_secondes_emission FROM pub GROUP BY emission_cle) pe
    LEFT JOIN metriques m ON m.emission_cle = pe.emission_cle
    CROSS JOIN parametres pa
    WHERE NOT pa.par_temps_env
       OR m.env_heures >= pa.env_heures_min
),
par_annonceur AS (
    SELECT
        pu.annonceur,
        SUM(pu.pub_secondes) AS pub_secondes,
        SUM(pu.pub_secondes * e.poids_par_seconde) AS cas_attribues,
        -- pour les moyennes pondérées des cas par heure (émissions sans heures sur la période écartées)
        SUM(pu.pub_secondes * e.nb_cas / NULLIF(e.volume_heures, 0)) AS pub_x_cas_par_heure,
        SUM(pu.pub_secondes) FILTER (WHERE e.volume_heures > 0) AS pub_secondes_avec_taux,
        SUM(pu.pub_secondes * e.cas_par_heure_env) AS pub_x_cas_par_heure_env,
        SUM(pu.pub_secondes) FILTER (WHERE e.cas_par_heure_env IS NOT NULL) AS pub_secondes_avec_score
    FROM pub pu
    JOIN emissions e ON e.emission_cle = pu.emission_cle
    GROUP BY pu.annonceur
),
-- parts, rangs et cumuls sur tous les annonceurs, avant la limite
resultat AS (
    SELECT
        annonceur,
        ROUND(cas_attribues::numeric, 2) AS cas_attribues,
        ROUND((100 * cas_attribues / NULLIF(SUM(cas_attribues) OVER (), 0))::numeric, 2) AS part_cas_pct,
        ROUND(pub_secondes::numeric, 0) AS pub_secondes,
        ROUND((100 * pub_secondes / SUM(pub_secondes) OVER ())::numeric, 2) AS part_pub_pct,
        ROUND(((cas_attribues / NULLIF(SUM(cas_attribues) OVER (), 0)) / (pub_secondes / SUM(pub_secondes) OVER ()))::numeric, 2)
            AS indice_surexposition,
        ROW_NUMBER() OVER classement_pub AS rang_pub,
        ROW_NUMBER() OVER classement_cas AS rang_cas,
        ROW_NUMBER() OVER classement_pub - ROW_NUMBER() OVER classement_cas AS ecart_rang,
        ROUND((100 * SUM(pub_secondes) OVER classement_pub / SUM(pub_secondes) OVER ())::numeric, 2)
            AS part_pub_cumulee_pct,
        ROUND((100 * SUM(cas_attribues) OVER classement_cas / NULLIF(SUM(cas_attribues) OVER (), 0))::numeric, 2)
            AS part_cas_cumulee_pct,
        ROUND((100 * SUM(pub_secondes) OVER classement_cas / SUM(pub_secondes) OVER ())::numeric, 2)
            AS part_pub_cumulee_rang_cas_pct,
        ROUND((100 * SUM(cas_attribues) OVER classement_pub / NULLIF(SUM(cas_attribues) OVER (), 0))::numeric, 2)
            AS part_cas_cumulee_rang_pub_pct,
        ROUND((100 * SUM(cas_attribues) OVER classement_cas / NULLIF(SUM(cas_attribues) OVER (), 0)
            - 100 * SUM(pub_secondes) OVER classement_cas / SUM(pub_secondes) OVER ())::numeric, 2)
            AS ecart_cumule_rang_cas_pct,
        ROUND((100 * SUM(cas_attribues) OVER classement_pub / NULLIF(SUM(cas_attribues) OVER (), 0)
            - 100 * SUM(pub_secondes) OVER classement_pub / SUM(pub_secondes) OVER ())::numeric, 2)
            AS ecart_cumule_rang_pub_pct,
        ROUND((pub_x_cas_par_heure / NULLIF(pub_secondes_avec_taux, 0))::numeric, 3) AS cas_par_heure_moyen,
        ROUND((pub_x_cas_par_heure_env / NULLIF(pub_secondes_avec_score, 0))::numeric, 3) AS cas_par_heure_env_moyen
    FROM par_annonceur
    -- un rang par annonceur et des cumuls ligne à ligne (ROWS) : ex aequo départagés par le nom
    WINDOW
        classement_pub AS (ORDER BY pub_secondes DESC, annonceur ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        classement_cas AS (ORDER BY cas_attribues DESC, annonceur ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    p.par_temps_env AS ponderation_par_temps_denv,
    r.*
FROM resultat r
CROSS JOIN parametres p
WHERE r.rang_pub <= p.nb_points
  AND r.rang_cas <= p.nb_points
ORDER BY r.rang_cas, r.rang_pub, r.annonceur
