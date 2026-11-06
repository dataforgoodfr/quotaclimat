-- Carte des émissions (chaînes françaises) : note de mésinformation et publicité rattachée, pour voir si la pub va
-- plutôt aux émissions qui font le plus de mésinformation. Une ligne par émission.
-- Pour un nuage de points : abscisse cas_par_heure_env (la note), ordonnée pub_minutes (ou pub_min_par_heure),
-- taille volume_heures, couleur chaine.
-- Variables Metabase :
--   debut  type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin    type Date, dernier jour inclus, ex. 2026-09-30
--   env_heures_min type Nombre, facultatif (0.5 si vide) : émissions gardées, au moins ce temps consacré à
--          l'environnement sur la période (heures) ; en dessous la note repose sur trop peu d'heures (1 cas en
--          6 min d'environnement donne 10 cas par heure). Même seuil par défaut que les questions de l'onglet
--          Annonceurs x programmes
--   inclure_info_continue type Texte, facultatif (true si vide), true ou false (liste déroulante) : false écarte
--          les chaînes d'information en continu (public.emission_jour.infocontinue), qui ont bien plus de pub et
--          écrasent l'échelle des autres
--   inclure_service_public type Texte, facultatif (true si vide), true ou false (liste déroulante) : false écarte
--          les chaînes publiques (public.emission_jour.public), qui n'ont que du parrainage ou peu de pub (France
--          Télévisions, France 24, Arte...) et peu de cas : elles tirent vers le bas la note moyenne des
--          programmes plus que celle de la pub, d'où un écart entre les deux qui tient à la composition par
--          chaîne et pas au choix des émissions par les annonceurs
-- Émissions gardées : celles des chaînes qui ont au moins une pub rattachée à une émission sur la période (les
-- chaînes sans détection de pub n'apparaissent pas), diffusées pendant la période, avec assez de temps
-- d'environnement. Une émission de ces chaînes sans pub rattachée est gardée, avec 0 minute de pub. Les parts et
-- les moyennes sont calculées sur les émissions gardées.
-- Colonnes :
--   emission, chaine, channel_name, infocontinue, service_public, rediffusion : l'émission (nom de
--     public.emission_jour) ;
--   point : chaine / emission, suivi de (rediffusion) pour une rediffusion (même nom que l'émission d'origine) ;
--   volume_heures, env_heures, nb_cas : heures de programme, heures consacrées à l'environnement et cas de
--     mésinformation de l'émission sur la période ;
--   cas_par_heure_env : la note, cas par heure consacrée à l'environnement (comme le score de
--     metabase_pub_annonceurs_ponderes_cas.sql avec ponderation_par_temps_denv) ;
--   pub_secondes, pub_minutes : pub rattachée à l'émission sur la période, toutes marques confondues ;
--   pub_min_par_heure : minutes de pub rattachée par heure de programme, pour comparer des émissions de durée
--     différente (une émission longue a mécaniquement plus de pub) ;
--   part_pub_pct, part_cas_pct, part_volume_pct : part de l'émission dans la pub, les cas et les heures de
--     programme de toutes les émissions gardées ;
--   note_moyenne_pub : note moyenne d'une seconde de pub (notes des émissions pondérées par leur pub), la même
--     sur toutes les lignes ;
--   note_moyenne_programme : note moyenne d'une heure de programme (pondérée par volume_heures), la même sur
--     toutes les lignes. note_moyenne_pub au-dessus : la pub se concentre sur les émissions à forte note ;
--   note_globale : tous les cas des émissions gardées / toutes leurs heures d'environnement ;
--   correlation_note_pub_par_heure : corrélation (Pearson, une émission = un point) entre la note et
--     pub_min_par_heure, la même sur toutes les lignes ; proche de 0 : pas de lien entre la note d'une émission et
--     la densité de sa pub. Sensible aux émissions extrêmes, à lire avec le nuage.
-- Sources (dbt) : advertising.pub_annonceur_emission_jour (secondes de pub de chaque marque autour de chaque
-- émission, par jour de la pub : 100 % à l'émission qui contient tout l'écran de pub, sinon 50 % à l'émission
-- avant et 50 % à l'émission après, à moins de 15 min, 100 % à la seule des deux assez proche) et
-- public.emission_jour (cas, heures de programme et d'environnement par émission, par jour de programme : écart
-- possible d'une journée aux bords de la période pour les émissions qui passent minuit).
WITH bornes AS NOT MATERIALIZED (
    SELECT
        CAST({{debut}} AS date) AS debut,
        CAST({{fin}} AS date) AS fin
),
parametres AS (
    SELECT
        COALESCE([[CAST({{env_heures_min}} AS numeric),]] 0.5) AS env_heures_min,
        COALESCE([[CAST({{inclure_info_continue}} AS boolean),]] TRUE) AS info_continue,
        COALESCE([[CAST({{inclure_service_public}} AS boolean),]] TRUE) AS service_public
),
-- pub rattachée à chaque émission sur la période, toutes marques confondues
pub AS MATERIALIZED (
    SELECT p.emission_cle, SUM(p.pub_secondes) AS pub_secondes
    FROM advertising.pub_annonceur_emission_jour p
    CROSS JOIN bornes b
    WHERE p.jour BETWEEN b.debut AND b.fin
    GROUP BY p.emission_cle
),
-- métriques de chaque émission sur la période
emissions AS MATERIALIZED (
    SELECT
        ej.emission_cle,
        MAX(ej.channel_name) AS channel_name,
        MAX(ej.channel_title) AS chaine,
        BOOL_OR(ej.infocontinue) AS infocontinue,
        BOOL_OR(ej.public) AS service_public,
        MAX(ej.nom) AS nom,
        BOOL_OR(ej.rediffusion) AS rediffusion,
        SUM(ej.nb_diffusions) AS nb_diffusions,
        SUM(ej.volume_heures) AS volume_heures,
        SUM(ej.env_heures) AS env_heures,
        SUM(ej.nb_cas) AS nb_cas
    FROM public.emission_jour ej
    CROSS JOIN bornes b
    WHERE ej.jour BETWEEN b.debut AND b.fin
    GROUP BY ej.emission_cle
),
-- chaînes où la pub est détectée sur la période
chaines_avec_pub AS (
    SELECT DISTINCT e.channel_name
    FROM pub
    JOIN emissions e ON e.emission_cle = pub.emission_cle
),
gardees AS MATERIALIZED (
    SELECT
        e.*,
        COALESCE(pub.pub_secondes, 0) AS pub_secondes,
        e.nb_cas / e.env_heures AS note
    FROM emissions e
    JOIN chaines_avec_pub c ON c.channel_name = e.channel_name
    LEFT JOIN pub ON pub.emission_cle = e.emission_cle
    CROSS JOIN parametres pa
    WHERE e.nb_diffusions > 0
      AND e.volume_heures > 0
      AND e.env_heures >= pa.env_heures_min
      AND e.env_heures > 0
      AND (pa.info_continue OR NOT COALESCE(e.infocontinue, FALSE))
      AND (pa.service_public OR NOT COALESCE(e.service_public, FALSE))
)
SELECT
    g.nom AS emission,
    g.chaine,
    g.channel_name,
    g.infocontinue,
    g.service_public,
    g.rediffusion,
    g.chaine || ' / ' || g.nom || CASE WHEN g.rediffusion THEN ' (rediffusion)' ELSE '' END AS point,
    ROUND(g.volume_heures::numeric, 1) AS volume_heures,
    ROUND(g.env_heures::numeric, 2) AS env_heures,
    g.nb_cas,
    ROUND(g.note::numeric, 2) AS cas_par_heure_env,
    ROUND(g.pub_secondes::numeric, 0) AS pub_secondes,
    ROUND((g.pub_secondes / 60)::numeric, 1) AS pub_minutes,
    ROUND((g.pub_secondes / 60 / g.volume_heures)::numeric, 2) AS pub_min_par_heure,
    ROUND((100 * g.pub_secondes / NULLIF(SUM(g.pub_secondes) OVER (), 0))::numeric, 2) AS part_pub_pct,
    ROUND((100 * g.nb_cas / NULLIF(SUM(g.nb_cas) OVER (), 0))::numeric, 2) AS part_cas_pct,
    ROUND((100 * g.volume_heures / SUM(g.volume_heures) OVER ())::numeric, 2) AS part_volume_pct,
    ROUND((SUM(g.pub_secondes * g.note) OVER () / NULLIF(SUM(g.pub_secondes) OVER (), 0))::numeric, 2)
        AS note_moyenne_pub,
    ROUND((SUM(g.volume_heures * g.note) OVER () / SUM(g.volume_heures) OVER ())::numeric, 2)
        AS note_moyenne_programme,
    ROUND((SUM(g.nb_cas) OVER () / SUM(g.env_heures) OVER ())::numeric, 2) AS note_globale,
    ROUND((CORR(g.note, g.pub_secondes / 60 / g.volume_heures) OVER ())::numeric, 2) AS correlation_note_pub_par_heure
FROM gardees g
ORDER BY g.note DESC, g.pub_secondes DESC, g.chaine, g.nom
