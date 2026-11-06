-- Part des annonceurs pondérée par la mésinformation des émissions autour desquelles ils font de la pub, toutes
-- émissions et tous annonceurs confondus (chaînes françaises) : camembert (dimension annonceur, métrique
-- cas_attribues) et courbes cumulées (abscisse rang_cas ou rang_pub).
-- Variables Metabase :
--   debut   type Date, premier jour inclus (heure de Paris), ex. 2026-09-01
--   fin     type Date, dernier jour inclus, ex. 2026-09-30
--   niveau  type Texte, échelle de l'annonceur : marque, entreprise, groupe (société mère ultime) ou secteur (secteur
--           prédit des pubs, « Secteur inconnu » sans prédiction) ; à régler en liste déroulante de ces quatre
--           valeurs, valeur par défaut entreprise (une autre valeur ne renvoie rien). Avec secteur, chaque ligne est
--           un secteur et la colonne annonceur porte son nom
--   inclure_info_continue type Texte, facultatif (true si vide), true ou false (liste déroulante) : false écarte
--           les chaînes d'information en continu (analytics.program.infocontinue), leur pub comme leurs cas
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
--   theme_cas type Texte, facultatif (vide : tous les cas) : ne compter que les cas de ce thème
--          (analytics.cas_theme, ex. automobile), rattachés à leur émission par analytics.cas_de_mesinformation
--   pertinence_min type Texte, facultatif (faible si vide), faible, moyenne ou forte (liste déroulante) : avec
--          theme_cas, pertinence minimale des cas du thème (faible : tous, moyenne : moyenne et forte, forte :
--          forte seulement ; une autre valeur ne renvoie rien)
--   secteur, categorie type Filtre de champ sur Advertising > Pub Annonceur Emission Jour > Secteur / Categorie,
--          sélection multiple, facultatifs (vides : pas de filtre) : n'afficher que les annonceurs qui ont de la pub
--          dans ces secteurs / catégories (ex. Automobile, Voiture électrique). Le filtre ne choisit que les lignes
--          affichées : parts, rangs, cumuls et cas_attribues restent ceux du calcul sur toute la pub (ex. avec le
--          seul secteur Automobile, si ses annonceurs ont 10 % de la pub, leurs part_pub_pct totalisent 10 %, pas
--          100 %) ; un annonceur gardé l'est avec toute sa pub, y compris dans d'autres secteurs. Les courbes
--          cumulées sautent donc les rangs des annonceurs écartés
--   agence, groupe_agence type Filtre de champ sur Advertising > Pub Annonceur Emission Jour > Agence Media /
--          Groupe Agence, sélection multiple, facultatifs (vides : pas de filtre) : n'afficher que les annonceurs qui
--          ont de la pub achetée par ces agences / groupes d'agences (agence média de la marque, sinon de son
--          entreprise, d'après le Google Sheet Inventaire_des_annonceurs). Comme secteur et categorie, le filtre ne
--          choisit que les lignes affichées : rangs, parts et cumuls restent calculés sur tous les annonceurs, avec
--          ou sans agence connue ; pub_secondes_selection, pub_dans_selection_pct et part_selection_pct se lisent
--          alors pour la pub de ces agences
--   nb_points type Nombre, facultatif (2000 si vide) : annonceurs gardés, ceux classés dans les nb_points premiers
--           des deux classements (rang_pub et rang_cas) ; les parts et les cumuls restent calculés sur tous les
--           annonceurs (le dernier point n'atteint donc pas 100 %)
-- Une ligne par annonceur : le regroupement des petites parts en « Autres » est laissé au camembert de Metabase
-- (réglage « Pourcentage minimum de part »).
-- Colonnes :
--   cas_attribues, part_cas_pct : poids de l'annonceur (voir ponderation_par_temps_denv) et sa part dans le
--     total ;
--   nb_entreprises : entreprises distinctes qui ont de la pub dans la ligne (1 au niveau marque ou entreprise,
--     les entreprises du groupe ou du secteur sinon) ;
--   pub_secondes, part_pub_pct : sa pub rattachée à une émission (gardée) et sa part, pour comparer ;
--   pub_secondes_selection : sa pub dans les secteurs, catégories et agences choisis (toute sa pub sans filtre) ;
--   pub_dans_selection_pct : part de sa pub qui est dans ces secteurs / catégories (ex. 60 % de la pub d'un
--     groupe est dans l'automobile) ; 100 % sans filtre ;
--   part_selection_pct : sa part dans la pub de ces secteurs / catégories, tous annonceurs confondus (ex. le
--     groupe fait 15 % de la pub automobile) ; égale à part_pub_pct sans filtre ;
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
--     d'au moins env_heures_min heures d'environnement), quelle que soit la pondération ;
--   agence_media, groupe_agence, fiabilite_agence : agence média de l'annonceur qui a le plus de ses secondes de
--     pub dans la sélection (avec un filtre d'agence : l'agence choisie), son groupe et la fiabilité de
--     l'information (confirmee, probable, indice, inconnue) ; NULL quand l'annonceur n'est pas dans le Google
--     Sheet. À un niveau qui regroupe plusieurs entreprises (groupe, secteur), l'agence principale seulement ;
--   autres_agences_media : les autres agences connues de l'annonceur, par secondes de pub décroissantes (NULL
--     sans autre agence), utile aux niveaux groupe et secteur ;
--   part_pub_agence_pct : part des secondes de pub de l'annonceur achetées par agence_media (100 : toute sa pub ;
--     NULL quand l'agence n'est pas connue). Calculée sur toute sa pub de la période, émissions écartées par
--     env_heures_min comprises ;
--   perimetre_media_agence : périmètre média de l'agence d'après le Google Sheet (partiel : elle n'achète qu'une
--     partie des médias de l'annonceur, ex. programmatique et réseaux sociaux seulement ; tout ; NULL si non dit) ;
--   statut_agence : pour colorer les lignes du tableau (couleurs de metabase/couleurs.yml, groupe statut_agence) :
--     partiel quand agence_media n'achète qu'une partie de la pub de la ligne (part_pub_agence_pct < 100, ex. une
--     marque d'une entreprise chez une autre agence, ou un groupe dont les entreprises ont plusieurs agences) ou
--     quand son perimetre_media_agence est partiel ; sinon fiabilite_agence (confirmee, probable, indice, inconnue) ;
--     NULL quand l'annonceur n'est pas dans le Google Sheet.
-- Sources (dbt) : advertising.pub_annonceur_emission_jour (secondes de pub de chaque marque autour de chaque
-- émission, par jour de la pub, avec son agence média) et public.emission_jour (cas, heures de programme et d'environnement par
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
        COALESCE([[CAST({{inclure_info_continue}} AS boolean),]] TRUE) AS info_continue,
        COALESCE([[CAST({{env_heures_min}} AS numeric),]] 0.5) AS env_heures_min,
        COALESCE([[CAST({{nb_points}} AS integer),]] 2000) AS nb_points,
        NULLIF(COALESCE([[CAST({{theme_cas}} AS text),]] ''), '') AS theme,
        COALESCE([[CAST({{pertinence_min}} AS text),]] 'faible') AS pertinence_min
),
-- chaînes d'information en continu
chaines AS (
    SELECT channel_name, BOOL_OR(infocontinue) AS infocontinue
    FROM analytics.program
    WHERE country = 'france'
    GROUP BY channel_name
),
-- secondes de pub par annonceur (au niveau choisi), entreprise, émission et agence, et si cette pub est dans les
-- secteurs, catégories et agences choisis ; table sans alias : les filtres de champ de Metabase (secteur, categorie,
-- agence, groupe_agence) la nomment en entier (sans valeur, Metabase les remplace par 1 = 1)
pub AS MATERIALIZED (
    SELECT
        CASE CAST({{niveau}} AS text)
            WHEN 'marque' THEN advertising.pub_annonceur_emission_jour.marque
            WHEN 'entreprise' THEN advertising.pub_annonceur_emission_jour.entreprise
            WHEN 'groupe' THEN advertising.pub_annonceur_emission_jour.groupe
            WHEN 'secteur' THEN COALESCE(advertising.pub_annonceur_emission_jour.secteur, 'Secteur inconnu')
        END AS annonceur,
        advertising.pub_annonceur_emission_jour.entreprise,
        advertising.pub_annonceur_emission_jour.emission_cle,
        advertising.pub_annonceur_emission_jour.agence_media,
        advertising.pub_annonceur_emission_jour.groupe_agence,
        advertising.pub_annonceur_emission_jour.fiabilite_agence,
        advertising.pub_annonceur_emission_jour.perimetre_media_agence,
        SUM(advertising.pub_annonceur_emission_jour.pub_secondes) AS pub_secondes,
        -- NULL sans pub dans les secteurs, catégories et agences choisis
        SUM(advertising.pub_annonceur_emission_jour.pub_secondes)
            FILTER (WHERE {{secteur}} AND {{categorie}} AND {{agence}} AND {{groupe_agence}}) AS pub_secondes_selection
    FROM advertising.pub_annonceur_emission_jour
    CROSS JOIN bornes b
    CROSS JOIN parametres pa
    LEFT JOIN chaines c ON c.channel_name = advertising.pub_annonceur_emission_jour.channel_name
    WHERE advertising.pub_annonceur_emission_jour.jour BETWEEN b.debut AND b.fin
      AND CAST({{niveau}} AS text) IN ('marque', 'entreprise', 'groupe', 'secteur')
      AND pa.pertinence_min IN ('faible', 'moyenne', 'forte')
      AND (pa.info_continue OR NOT COALESCE(c.infocontinue, FALSE))
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
-- avec theme_cas : cas du thème par émission sur la période (jour de programme, comme emission_jour), à partir de
-- la pertinence minimale
cas_du_theme AS (
    SELECT c.emission_cle, COUNT(*) AS nb_cas
    FROM analytics.cas_de_mesinformation c
    JOIN analytics.cas_theme t ON t.task_aggregate_id = c.task_aggregate_id
    CROSS JOIN bornes b
    CROSS JOIN parametres pa
    WHERE pa.theme IS NOT NULL
      AND t.theme = pa.theme
      AND array_position(ARRAY['faible', 'moyenne', 'forte'], t.pertinence)
          >= array_position(ARRAY['faible', 'moyenne', 'forte'], pa.pertinence_min)
      AND c.emission_cle IS NOT NULL
      AND c.jour_programme BETWEEN b.debut AND b.fin
    GROUP BY c.emission_cle
),
-- cas (tous, ou ceux du thème), heures de programme et d'environnement de chaque émission sur la période
metriques AS (
    SELECT
        t.emission_cle,
        CASE WHEN pa.theme IS NULL THEN t.nb_cas ELSE COALESCE(ct.nb_cas, 0) END AS nb_cas,
        t.volume_heures,
        t.env_heures
    FROM (
        SELECT
            ej.emission_cle,
            SUM(ej.nb_cas) AS nb_cas,
            SUM(ej.volume_heures) AS volume_heures,
            SUM(ej.env_heures) AS env_heures
        FROM public.emission_jour ej
        CROSS JOIN bornes b
        WHERE ej.jour BETWEEN b.debut AND b.fin
        GROUP BY ej.emission_cle
    ) t
    CROSS JOIN parametres pa
    LEFT JOIN cas_du_theme ct ON ct.emission_cle = t.emission_cle
),
-- émissions gardées (pondération par le temps d'environnement : assez de temps d'environnement) et cas_attribues d'une seconde de pub autour
-- d'elles ; MATERIALIZED : calculé une seule fois
emissions AS MATERIALIZED (
    SELECT
        pe.emission_cle,
        m.nb_cas,
        m.volume_heures,
        -- score, défini seulement avec assez de temps d'environnement
        CASE WHEN m.env_heures >= pa.env_heures_min THEN m.nb_cas / NULLIF(m.env_heures, 0) END AS cas_par_heure_env,
        CASE WHEN pa.par_temps_env
            -- score de l'émission (NULL sans environnement, avec env_heures_min à 0)
            THEN m.nb_cas / NULLIF(m.env_heures, 0)
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
        COUNT(DISTINCT pu.entreprise) AS nb_entreprises,
        -- annonceur affiché avec les filtres secteur et categorie quand non NULL
        SUM(pu.pub_secondes_selection) AS pub_secondes_selection,
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
-- agences de chaque annonceur, par secondes de pub dans la sélection puis au total
agences AS (
    SELECT
        annonceur,
        agence_media,
        groupe_agence,
        fiabilite_agence,
        perimetre_media_agence,
        SUM(pub_secondes_selection) AS pub_secondes_selection,
        SUM(pub_secondes) AS pub_secondes
    FROM pub
    GROUP BY 1, 2, 3, 4, 5
),
-- agence principale : la plus présente dans la sélection (l'agence choisie avec un filtre d'agence), puis au total
agence_principale AS (
    SELECT DISTINCT ON (annonceur) annonceur, agence_media, groupe_agence, fiabilite_agence, perimetre_media_agence
    FROM agences
    ORDER BY annonceur, pub_secondes_selection DESC NULLS LAST, pub_secondes DESC, agence_media
),
autres_agences AS (
    SELECT
        a.annonceur,
        STRING_AGG(a.agence_media, ', ' ORDER BY a.pub_secondes DESC, a.agence_media) AS autres_agences_media
    FROM (
        SELECT annonceur, agence_media, SUM(pub_secondes) AS pub_secondes
        FROM agences
        WHERE agence_media IS NOT NULL
        GROUP BY 1, 2
    ) a
    JOIN agence_principale ap ON ap.annonceur = a.annonceur
    WHERE a.agence_media IS DISTINCT FROM ap.agence_media
    GROUP BY a.annonceur
),
-- part de la pub de l'annonceur achetée par son agence principale
part_agence AS (
    SELECT
        ag.annonceur,
        ROUND((100 * SUM(ag.pub_secondes) FILTER (WHERE ag.agence_media = ap.agence_media) / SUM(ag.pub_secondes))::numeric, 2)
            AS part_pub_agence_pct
    FROM agences ag
    JOIN agence_principale ap ON ap.annonceur = ag.annonceur
    WHERE ap.agence_media IS NOT NULL
    GROUP BY ag.annonceur
),
-- parts, rangs et cumuls sur tous les annonceurs, avant la limite
resultat AS (
    SELECT
        annonceur,
        ROUND(cas_attribues::numeric, 2) AS cas_attribues,
        ROUND((100 * cas_attribues / NULLIF(SUM(cas_attribues) OVER (), 0))::numeric, 2) AS part_cas_pct,
        nb_entreprises,
        ROUND(pub_secondes::numeric, 0) AS pub_secondes,
        ROUND((100 * pub_secondes / SUM(pub_secondes) OVER ())::numeric, 2) AS part_pub_pct,
        ROUND(pub_secondes_selection::numeric, 0) AS pub_secondes_selection,
        ROUND((100 * pub_secondes_selection / pub_secondes)::numeric, 2) AS pub_dans_selection_pct,
        ROUND((100 * pub_secondes_selection / SUM(pub_secondes_selection) OVER ())::numeric, 2)
            AS part_selection_pct,
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
    p.theme AS theme_cas,
    r.*,
    ap.agence_media,
    ap.groupe_agence,
    ap.fiabilite_agence,
    aa.autres_agences_media,
    pg.part_pub_agence_pct,
    ap.perimetre_media_agence,
    CASE
        WHEN ap.agence_media IS NOT NULL
         AND (pg.part_pub_agence_pct < 100 OR ap.perimetre_media_agence = 'partiel') THEN 'partiel'
        ELSE ap.fiabilite_agence
    END AS statut_agence
FROM resultat r
CROSS JOIN parametres p
LEFT JOIN agence_principale ap ON ap.annonceur = r.annonceur
LEFT JOIN autres_agences aa ON aa.annonceur = r.annonceur
LEFT JOIN part_agence pg ON pg.annonceur = r.annonceur
WHERE r.rang_pub <= p.nb_points
  AND r.rang_cas <= p.nb_points
  -- filtres secteur, categorie, agence et groupe_agence : lignes affichées seulement, valeurs calculées sur tous
  -- les annonceurs
  AND r.pub_secondes_selection IS NOT NULL
ORDER BY r.rang_cas, r.rang_pub, r.annonceur
