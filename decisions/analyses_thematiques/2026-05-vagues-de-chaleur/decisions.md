# Vague de chaleur, mai 2026

Fichier reconstitué à partir des questions Metabase et de l'article publié.

- Statut : publié, [article sur la vague de chaleur de mai 2026](https://quotaclimat.org/actualites/vague-chaleur-mai-2026-couverture-mediatique/) (date de publication non indiquée sur la page)
- Tables : `public.keywords` (audiovisuel) et `public.factiva_articles` (presse)
- Collection Metabase : `42. QuotaClimat - data/Analyse Vague de chaleur` (id 263), tableau de bord « Vagues de chaleurs » (id 192)

## 1. Méthode spécifique

Analyse menée avant la révision de la méthode, directement sur `public.keywords` et `public.factiva_articles`. Le corpus n'est pas défini par une liste de mots-clés sur la canicule mais par le thème « conséquences du changement climatique » du dictionnaire OME :

- **audiovisuel** : segments de `public.keywords` (France) avec au moins un mot du thème conséquences (`number_of_changement_climatique_consequences > 0`) ;
- **presse** : articles de `public.factiva_articles` prédits « conséquences » (`predict_climat_consequence = TRUE`), hors `DUP`.

Détection des axes :

- **audiovisuel** : mots détectés dans `keywords_with_timestamp` du segment et des segments situés exactement 2 minutes avant et après, sur la même chaîne ;
- **presse** : recherche `LIKE '%terme%'` sur titre, chapô et corps en minuscules, sans retrait des accents ni limite de mot. Un terme peut donc être trouvé à l'intérieur d'un autre mot.

Périodes :

- analyse : du 19 mai au 31 mai 2026 inclus en audiovisuel ; à partir du 19 mai en presse, au plus tard jusqu'au 30 juin 2026 (la question de base 1993 n'a pas de date de fin) ;
- comparaison : moyenne annuelle du temps d'antenne environnemental, canicules de juillet et août 2025, semaine précédente pour la presse (selon l'article). Périodes exactes et questions Metabase inconnues.

## 2. Mots-clés

Pas de liste propre à la canicule : le corpus repose sur le thème « conséquences du changement climatique » du dictionnaire OME (audiovisuel) et sur la prédiction `predict_climat_consequence` (presse). Voir §1.

## 3. Axes

Questions Metabase : « Base Presse écrite » (1993), « Presse écrite - liens causaux » (1994), « Presse écrite - adaptation systémique vs. individuelle » (1995), « Requete mot-clé Presse ecrite Vague de chaleur » (1996), « Analyse par mots-clés - Vague de chaleur » (1992), « Vague de Chaleur - Adaptation - Requête propre pour graphiques Flourish » (2009).

| Axe | Origine | Termes | Question Metabase | Résultat |
|---|---|---|---|---|
| Lien avec le changement climatique (maillon « constat ») | banque, termes propres | ci-dessous | 1992, 1993, 1994 | publié |
| Causes climatiques (maillon « causes ») | banque, termes propres | ci-dessous | 1992, 1993, 1994 | publié |
| Adaptation individuelle ou systémique | spécifique | ci-dessous | 1992, 1993, 1995, 2009 | publié |
| Désinformation | banque | Cas détectés par climateguard, saisines Arcom | hors Metabase | publié |
| Unes | banque | Présence en Une, par titre | hors Metabase | publié |
| Croisement Lien CC x causes, par média (presse) | banque | | 1994 | testé |
| Croisement Comparaison entre épisodes (chaleur vs incendies) | banque | | | publié |

La collection contient aussi des questions paramétrées sur le second épisode de 2026 (19 au 30 juin) : liens causaux (2023), mots-clés (2024), mots-clés en presse (2070), conséquences sectorielles en presse (2061). Elles ont servi au Bilan OMÉ S1 2026, pas à cet article : voir §5.

**Axes de la banque non exploités** : non recensés, l'analyse est antérieure au recensement des axes.

**Termes des axes** (forme du dictionnaire OME en audiovisuel, sous-chaînes en presse)

- **Constat** : `climatique`, `ademe`, `écologique`, `green deal`, `industrie`, `transition écologique`, `réchauffement climatique`, `changement climatique`, `transition énergétique`, `agriculture`, `batterie`, `accord de paris`, `empreinte carbone`, `climatologue`, `effet de serre`, `bilan carbone`, `carbone`, `glacier`, `planification écologique`, `oms` ; en presse, en plus : `réchauffement`, `climat`
- **Causes** : `co deux`, `gaz à effet de serre`, `polluant`, `co2`, `déforestation`, `énergie fossile`, `greenwashing`, `voiture thermique`, `méthane`, `combustibles fossiles`, `productivisme`, `émission de carbone`, `agriculture intensive`, `inaction climatique`, `pollueur`, `productiviste`, `élevage intensif`, `agriculture conventionnelle`, `exploitation pétrolière`, `agriculture industrielle`
- **Adaptation individuelle** : `se rafraîchir`, `hydratation`, `boire`, `hydrater`, `piscine`, `ventilateur`, `climatiseur`, `climatisation`, `stores`, `volets`, `sieste`, `horaires décalés`, `télétravail`, `protection solaire`
- **Adaptation systémique** : `ilot`, `fraicheur`, `renaturation`, `végétalisation`, `trame verte`, `arbre planté`, `fontaine`, `brumisateur`, `ombre`, `espace vert`, `parc urbain`, `toiture végétalisée`, `permis de végétaliser`, `rénovation thermique`, `isolation`, `passif`, `pompe à chaleur`, `surélévation`, `construction durable`, `agroforesterie`, `cultures résistantes`, `semences`, `raisonnée`, `conservation`, `maraîchage`, `résilience`, `anticipation`, `territoire résilient`, `concertation`, `sobriété`, `transition`
- **Regroupements pour les graphiques d'adaptation (2009)** : climatisation et climatiseur ; isolation et rénovation thermique ; ventilation et ventilateur. `protection solaire` exclu.

## 4. Résultats

**Takeaways publiés** (vérifiés sur l'article)

- Temps d'antenne consacré aux enjeux environnementaux : 10,7 % pendant l'épisode, contre 4,2 % en moyenne annuelle (12,8 % et 11,79 % lors des canicules de juillet et août 2025).
- Presse : 8,9 % des articles portent sur les crises environnementales la semaine du 25 au 31 mai, +67 % par rapport à la semaine précédente.
- Lien avec le changement climatique : 54 % des séquences sur France Info TV, 43 % sur M6, environ 20 % sur CNews, Europe 1 et BFMTV ; plus de 60 % des articles de presse.
- Causes : 20 % des articles de presse ; plus de 20 % pour l'AFP, Le Monde, Le Nouvel Obs, Mediapart et L'Humanité, qui font le lien dans plus de 80 % de leurs articles.
- Adaptation : environ 50 % des séquences sur M6, TF1, France Info TV et RTL. Sur France Info TV, climatisation 26 %, rénovation 3,5 %, végétalisation 3,8 % ; sur CNews, climatisation 16 %, rénovation et végétalisation 0 %.
- Désinformation : trois narratifs identifiés (climatisation sans impact, vague de chaleur non exceptionnelle, origine humaine incertaine) ; 5 saisines de l'Arcom.

**Limites** : pas d'encadré méthodologique dans l'article (voir méthode commune §3.9).

**Axes remontés dans la banque** : adaptation individuelle ou systémique (généralisée en « solutions individuelles ou systémiques »).

## 5. Épisode de juin 2026 (Bilan OMÉ S1 2026)

Section reconstituée le 24/11/2026 à partir des questions Metabase 2023, 2024, 2070 et 2061. Selon le §3, elles ont servi au Bilan OMÉ S1 2026 (à vérifier sur le bilan). Aucun résultat ici.

### Méthode

**Audiovisuel, maillons (2023, 2024).** Même corpus qu'en mai (thème conséquences du dictionnaire OME, France). Différence : un axe est compté quand un de ses mots figure dans le segment ou dans un segment de la même chaîne qui commence dans les 2 minutes avant ou après (fenêtre, et non plus segments situés exactement à ±2 minutes). Ce voisin doit lui aussi appartenir au corpus. 2024 classe les chaînes en info en continu (BFM TV, LCI, France Info TV, CNews, France 24, FranceinfoRadio), radio généraliste et TV généraliste, et garde les dix mots les plus fréquents par chaîne et maillon.

**Presse, maillons (2070).** Version juin de 1996 : même corpus et même recherche `LIKE` qu'en mai, avec les variantes regroupées en libellés (ci-dessous) et les dix premiers libellés par média et maillon.

**Presse, conséquences par secteur (2061).**

- Corpus propre, par regex sur le titre et le corps (chapô non lu), insensible à la casse, avec limites de mot : `canicules?`, `vagues? de chaleur`, `fortes chaleurs`, `chaleurs extrêmes`, `dôme de chaleur`. Hors articles supprimés et `DUP`, 100 mots et plus.
- Un article relève d'un secteur quand les termes du secteur y apparaissent au moins 2 ou 3 fois au total (seuil par secteur, ci-dessous), pour écarter les mentions en passant.
- Médias regroupés par `source_classification.media_all` ; seuls les médias d'au moins 20 articles du corpus sont gardés.

**Périodes.** Valeurs par défaut des questions : du 19 au 30 juin 2026 pour 2023, 2024 et 2061 ; du 17 au 29 juin pour 2070. Écart non expliqué ; la période retenue pour le bilan n'est pas vérifiée.

### Axes

| Axe | Origine | Question Metabase | Seuil par article | Résultat |
|---|---|---|---|---|
| Constat | banque A1, termes propres | 2023, 2024 (AV), 2070 (PE) | 1 | testé |
| Causes | banque B1, termes propres | 2023, 2024 (AV), 2070 (PE) | 1 | testé |
| Adaptation (individuelle et systémique réunies) | banque D2 | 2023, 2024 (AV), 2070 (PE) | 1 | testé |
| Enfance et écoles | banque C1 (secteur) et C4 | 2061 (PE) | 3 | testé |
| Vieillesse | banque C4 | 2061 (PE) | 2 | testé |
| Alimentation | banque C1 (secteur) | 2061 (PE) | 2 | testé |
| Agriculture | banque T3 | 2061 (PE) | 3 | testé |
| Santé | banque C2 | 2061 (PE) | 3 | testé |
| Travail | banque C1 (secteur) | 2061 (PE) | 2 | testé |

Résultat : « testé » faute d'avoir vérifié ce que le bilan a repris.

### Termes

**Maillons** : ceux du §3, avec ces écarts.

- 2023 cherche `s hydrater`, 2024 et 2070 `hydrater` : un seul des deux correspond au mot du dictionnaire.
- 2070 écrit `îlot` et `fraîcheur` avec accents (`ilot`, `fraicheur` en audiovisuel), retire `réchauffement climatique` et `changement climatique` (trouvés par `climatique` avec `LIKE`) et `co deux`.
- Libellés regroupés dans 2070 : carbone (`carbone`, `bilan carbone`, `empreinte carbone`) ; énergie fossile (`énergie fossile`, `combustibles fossiles`) ; productivisme (`productivisme`, `productiviste`) ; agriculture intensive (`agriculture intensive`, `agriculture conventionnelle`, `agriculture industrielle`) ; hydratation (`hydratation`, `hydrater`, `boire`) ; climatisation (`climatiseur`, `climatisation`) ; îlot de fraîcheur (`îlot`, `fraîcheur`) ; végétalisation (`végétalisation`, `arbre planté`, `permis de végétaliser`) ; rénovation thermique (`rénovation thermique`, `isolation`).

**Secteurs (2061)**, regex sur le texte brut :

- **Enfance et écoles** : `école`, `écoles`, `scolaire`, `scolaires`, `élèves`, `crèche`, `crèches`, `cantine`, `collège`, `collèges`, `lycée`, `lycées`, `brevet`, `baccalauréat`, `enfant`, `enfants`, `nourrisson`, `nourrissons`, `cour de récréation`, `bâti scolaire`
- **Vieillesse** : `personnes âgées`, `personne âgée`, `EHPAD`, `maison de retraite`, `maisons de retraite`, `résidence senior`, `gériatri\w+`, `isolement des aînés`, `registre canicule`, `plan bleu`, `octogénaire`, `nonagénaire`
- **Alimentation** : `prix alimentaires?`, `fruits et légumes`, `rayon frais`, `chaîne du froid`, `supermarchés?`, `consommateurs?`, `restauration`, `pénuries? alimentaires?`, `sécurité alimentaire`, `gaspillage alimentaire`
- **Agriculture** : `agricult\w+`, `récoltes?`, `moissons?`, `élevages?`, `éleveurs?`, `bétail`, `irrigation`, `rendements? agricoles?`, `vendanges?`, `maraîch\w+`
- **Santé** : `urgences`, `hôpital`, `hôpitaux`, `mortalité`, `décès`, `surmortalité`, `déshydratation`, `coup de chaleur`, `hyperthermie`, `Santé publique France`, `SAMU`
- **Travail** : `chantiers?`, `BTP`, `travailleurs?`, `salariés?`, `droit de retrait`, `conditions de travail`, `horaires décalés`, `travail en extérieur`, `inspection du travail`

Termes larges, qui peuvent compter des articles hors sujet malgré le seuil : `enfant`, `brevet`, `restauration`, `consommateurs`, `urgences`, `décès`, `salariés`.

**Axes remontés dans la banque** (24/11/2026) : conséquences par secteur (C1 : enfance et écoles, alimentation, travail), santé (C2, effets de la chaleur), personnes âgées (C4), agriculture (T3).
