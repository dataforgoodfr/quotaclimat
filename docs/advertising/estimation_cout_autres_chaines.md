# Estimer le coût des spots : France 2, France 3, France 24, Arte, M6

Statut : proposition de méthode, non validée par l'équipe. Sources consultées le 2 octobre 2026.
Complète [`estimation_cout_tf1.md`](estimation_cout_tf1.md), dont la logique générale (tunnel = écran, position,
indice de format, brut et net) s'applique aussi ici. Aucun montant ne doit être diffusé sans relecture experte.

## En bref

| `channel_name` | Régie | Grilles écran par écran | Historique depuis le 29/09/2025 | Net |
|---|---|---|---|---|
| `france2` | FranceTV Publicité | **Publiques** (PDF par période, secteurs tarifaires 1, 2, 3) | **Oui**, médiathèque du site | Cascade entièrement publiée, estimable |
| `fr3-idf` | FranceTV Publicité | **Publiques** : grille France 3 nationale + tarifs France 3 Régions | **Oui** | Idem |
| `france24` | FranceTV Publicité (signal France) ; FranceTV Publicité International (hors de France) | **Publique** : 120 € HT les 20 s pour tous les écrans (sept.-déc. 2025 et sept.-déc. 2026) | **Oui** | Idem pour le signal France ; grille internationale non trouvée |
| `arte` | Aucune | Pas de publicité à l'antenne | Sans objet | Sans objet |
| `m6` | M6 Unlimited | Sur My6, avec connexion ; ajustées chaque semaine | **Non public** | Cascade publiée : remise volume de 23 à 40 % |

France 2, France 3 et France 24 sont calculables dès maintenant avec des données publiques. M6 est dans la
même situation que TF1 : formule connue, grilles derrière une connexion.

## 1. France 2 et France 3 (FranceTV Publicité)

### 1.1 Formule

Décomposition publiée [FTV 2027 p.50 ; terminologie FTV 2025 p.62-63] :

```
Tarif initial          = grille publiée, format 20 s, secteur tarifaire 1, 2 ou 3 du produit
Tarif initial corrigé  = Tarif initial x indice de format (et incidents, solutions, blocs)
+ majorations          (calculées sur le Tarif initial corrigé, additionnées)
- gracieux et modulations tarifaires   -> Tarif de référence
- minorations          (calculées sur le Tarif de référence)  -> Tarif net avant remise
- Taux CGV             (somme de dégressifs)                   -> Tarif net
```

- **Secteur tarifaire** : pour France 2, France 3 national et France 5, le prix dépend du code secteur principal
  du produit (Tarif secteur 1, 2 ou 3) [FTV 2026 p.51]. La nomenclature publique donne le secteur tarifaire de
  chaque code à 8 chiffres : alimentation et boissons en 1, automobile et **énergie (famille 13) en 2**,
  quelques codes hygiène-beauté, pharmacie, voyage et services en 3 [Nomenclature FTV 2026].
- **Indices de format** (base 20 s = 100) : 30 s = 110 en 2025, 2026 et 2027 ; écarts entre 2025 et 2026 sur
  les formats courts (10 s = 65 puis 68). Table complète : [`francetvpub_indices_format.csv`](francetvpub_indices_format.csv)
  [FTV 2025 p.73 ; FTV 2026 p.60 ; FTV 2027 p.62]. Au-delà de 60 s : +5 points par seconde.
- **Majorations** [FTV 2025 p.64-65 ; FTV 2026 p.51 ; FTV 2027 p.53] :
  - emplacement préférentiel A, B, C, X, Y, Z : +7, +6, +5, +5, +6, +7 % en 2025 ; +7, +7, +5, +5, +7, +7 %
    en 2026 et 2027 (hypothèse : A, B, C = trois premières positions, X, Y, Z = trois dernières, à confirmer) ;
  - multi-SECODIP +15 %, co-branding +15 %, exclusivité dans un écran +30 %, priorité planning +20 %,
    Écrin +25 %, habillage d'écran +25 % ;
  - depuis 2026, EP offerts aux voitures de classe A, B, C (méthode Carbone 4) dans le « Pack Visibilité ».
- **Minorations** 2026 [FTV 2026 p.53] : nouvel annonceur -7 %, petite entreprise -15 %, publicité collective
  -5 %, collective « charte alimentaire » -7 %, intérêt général (associations, administrations, SIG) -10 %.
  Cinéma et édition : -52 à -82 % selon budget et délai [FTV 2025 p.67].
- **Taux CGV** : dégressif volume de 0 à -18 % selon le CA net annuel chez la régie, plus dégressifs périodes
  creuses, France 3 Régions, chaînes thématiques et numérique [FTV 2025 p.69-71 ; FTV 2026 p.56 ; FTV 2027 p.58].

Différence majeure avec TF1 : la décomposition publiée ne comporte **pas d'abattement négocié**. Le net se
calcule à partir de conditions publiques, à condition d'estimer le CA annuel de l'annonceur chez la régie
(approximable par nos propres détections, sans le numérique). Pratiques hors CGV non documentées : à vérifier.
Les achats en coût GRP net garanti (Garanty) suivent une autre logique et ne reçoivent pas ces remises.

### 1.2 Grilles et historique

- Pages publiques « Tarifs chaînes nationales / régionales / thématiques » de francetvpub.fr : un PDF par chaîne,
  par période d'ouverture de planning et par secteur, plus les listes « écrans fermés » et « écrans créés ou
  modifiés ». Cinq périodes par an, tarifs publiés environ un mois avant l'ouverture [FTV 2025 p.72 ; FTV 2026 p.59].
- Format vérifié (France 2, 2025 et 2026) : PDF texte, tableau code d'écran x jour de la semaine, en € HT,
  par sous-période. Extraction déterministe possible (pdftotext), sans LLM.
- **Historique** : la médiathèque WordPress du site expose encore les fichiers depuis 2017, dont France 2 et
  France 3 « août-novembre 2025 » et « novembre-décembre 2025 » (téléchargés et lus pour vérification).
- Les tarifs peuvent changer jusqu'à la veille de la diffusion via le « flash programme » [CGV FTV 2026 art. 28,
  p.10]. Ces flashs n'ont pas été trouvés en accès public : écart possible avec la grille initiale.
- Codes d'écran au format HHMM nominal (ex. 1944, 1951) ; « Les intitulés d'écrans n'impliquent pas des horaires
  de diffusion » (mention des grilles).

### 1.3 Spécificités

- **Après 20 h** : pas de publicité sur les programmes nationaux de France Télévisions entre 20 h et 6 h, sauf
  publicité générique, et à l'exception des programmes régionaux et locaux (loi n° 86-1067, art. 53 VI). Les
  grilles France 2 s'arrêtent à l'écran 1951. Une grille dédiée existe pour la publicité d'intérêt général et
  collective.
- **France 3 Paris Île-de-France** (`fr3-idf`) : le flux mélange écrans nationaux (grille France 3) et écrans
  régionaux (tarifs France 3 Régions, annuels, par région). Paris Île-de-France 2025 hors été, pour 20 s :
  écran 1130 (sam.-dim.) 65 €, 1235 (lun.-sam.) 400 €, 1910 (lun.-dim.) 1 210 €, 2012 (lun.-ven.) 745 €,
  2015 (lun.-ven.) 1 210 €, 2015 (sam.-dim.) 650 € [Tarifs France 3 Régions 2025]. Il faut distinguer les deux
  types d'écrans lors du rattachement.

### 1.4 Exemple vérifié

France 2, écran 1944, mardi, période du 1er septembre au 19 octobre 2025, spot de 30 s d'un annonceur du secteur
tarifaire 2 (par exemple énergie) : tarif initial 20 s = 32 700 € [grille France 2 août-novembre 2025] ;
tarif initial corrigé = 32 700 x 110 / 100 = 35 970 € ; en position A (+7 %) : environ 38 488 € avant
minorations et taux CGV. Pour le même écran, secteur 1 = 34 700 € et secteur 3 = 37 100 €.

## 2. France 24

- Le signal France de France 24 est commercialisé par FranceTV Publicité comme chaîne thématique
  [FTV 2027 p.51]. Grille publique : **120 € HT les 20 s, quel que soit l'écran**, du 1er septembre au
  31 décembre 2025 et du 1er septembre au 31 décembre 2026 (grilles intermédiaires 2026 à vérifier).
  PDF en image : transcription manuelle simple vu le tarif unique.
- Hors de France, la vente passe par FranceTV Publicité International, sur la base d'un format 30 s
  [CGV FTPI 2025 p.5] ; grille non trouvée en accès public.
- **À vérifier dans nos données** : quel signal Mediatree enregistre sous `france24`. Des annonceurs surtout
  internationaux (offices de tourisme, pays) indiqueraient le signal international.

## 3. Arte

« L'absence de publicité sur son antenne lui garantit [...] une indépendance précieuse à l'égard des annonceurs »
[COM Arte France 2017-2021 p.13]. Rien à valoriser. Ce que nous détectons sur `arte` devrait être de
l'autopromotion ou du parrainage : à contrôler dans les données (types de contenus classés).

## 4. M6 (M6 Unlimited, ex-M6 Publicité)

### 4.1 Formule

Cascade publiée [M6 2026 p.90 ; M6 2027 p.95] :

```
Brut tarif (grille base 20 s) x indice format     -> Brut tarif format
- abattements catégoriels (cinéma, édition, collective, SIG, campagnes « transition écologique »...)
+ majorations (podium, construction personnalisée, multiproduit / co-branding, accès prioritaire)
                                                  -> Brut payant
- remise volume (23 à 40 %), bonus digital         -> Net HT
```

- **Indices de format** identiques en 2025, 2026 et 2027 (30 s = 110, 45 s = 211, 60 s = 300) :
  [`m6unlimited_indices_format.csv`](m6unlimited_indices_format.csv) [M6 2025 p.47 ; M6 2026 p.51 ; M6 2027 p.55].
- **Podiums** (1re et dernière, 2e et avant-dernière, 3e et antépénultième positions) : +12, +9, +6 % en 2025 ;
  +14, +11, +8 % en 2026 et 2027 [M6 2025 p.48 ; M6 2026 p.52 ; M6 2027 p.56].
- **Remise volume sur M6** : au 1er euro sur le brut payant annuel, de 23 % (moins de 200 k€) à 40 %
  (plus de 40 M€), même barème en 2025, 2026 et 2027 [M6 2025 p.50 ; M6 2026 p.55 ; M6 2027 p.59]. Comme pour
  France TV, on peut l'estimer à partir du brut annuel calculé sur nos propres détections.
- **Offres liées au climat** : campagnes d'information des administrations et associations pour des pratiques
  responsables (contrats climat) -40 % ; produits écoresponsables d'annonceurs nouveaux sur le groupe -55 %
  [M6 2026 p.59].
- Coût GRP net garanti : coût négocié par annonceur, non public.

### 4.2 Grilles et historique

- « Les grilles de tarifs des écrans publicitaires des différentes chaînes peuvent être consultées sur My6 »,
  ajustées chaque semaine, 3 semaines avant diffusion [M6 2026 p.51]. My6 demande un compte [M6 2026 p.25].
- Modification de tarif notifiée sur My6 au moins 4 jours avant, moins pour un événement exceptionnel
  [M6 2026 p.89].
- Les « récap tarifs » publics du site m6unlimited.fr concernent la radio (tranches horaires, tarifs Blanc /
  Orange), pas la télévision : ne pas les utiliser.
- Historique : non public. Mêmes options que pour TF1 (demande à la régie, Kantar).

## 5. Données à préparer côté QuotaClimat

1. Correspondance taxonomie OME vers codes secteurs à 8 chiffres (nomenclatures FTV et ADMTV, publiques) :
   elle sert au secteur tarifaire FTV, au Tarif 1 / 2 de TF1 et aux codes M6.
2. Téléchargement ponctuel des grilles FTV de la période (une trentaine de PDF) et extraction en table
   `(chaîne, date début, date fin, code écran, jour, secteur, tarif 20 s)`. Documents publics, mais le droit du
   producteur de base de données s'applique (CPI art. L342-1) : à faire valider avant collecte.
3. Rattachement tunnel / code d'écran et position dans l'écran, comme pour TF1.

## 6. Vérifications

Faites : conditions commerciales FTV 2025, 2026, 2027 et M6 2025, 2026, 2027 lues ; tables d'indices
recoupées automatiquement avec les PDF (la page FTV 2026 est une image : transcription identique au tableau
2027) ; grilles France 2 et France 3 2025 et 2026, France 3 Régions 2025, France 24 2025 et 2026 téléchargées
et lues.

Non faites : aucun montant calculé sur nos occurrences (pas d'accès à la base) ; flashs FTV non trouvés ;
signal France 24 capté par Mediatree non identifié ; CGU de francetvpub.fr non lues (page chargée en JavaScript).

## Sources

- [FTV 2025] FranceTV Publicité, Conditions commerciales 2025 :
  https://www.admtv.org/wp-content/uploads/2022/10/conditions-commerciales-2025-publicite-parrainage-et-numerique-2.pdf
- [FTV 2026] FranceTV Publicité, Conditions commerciales 2026 :
  https://www.admtv.org/wp-content/uploads/2022/10/Conditions-commerciales-2026-Publicite-Parrainage-et-Numerique.pdf
- [FTV 2027] FranceTV Publicité, Conditions commerciales 2027 :
  https://www.admtv.org/wp-content/uploads/2022/10/Conditions-commerciales-2027-Publicite-Parrainage-et-Numerique-1.pdf
- [CGV FTV 2026] Conditions générales de vente 2026 :
  https://www.admtv.org/wp-content/uploads/2022/10/Conditions-generales-de-vente-de-la-Publicite-du-Parrainage-et-du-Numerique-2026.pdf
- Pages tarifs FranceTV Publicité : https://www.francetvpub.fr/tarifs-chaines-nationales/ ,
  https://www.francetvpub.fr/tarifs-chaines-regionales/ , https://www.francetvpub.fr/tarifs-chaines-thematiques/
- Grille France 2 août-novembre 2025 :
  https://www.francetvpub.fr/wp-content/uploads/2025/05/france-2-tarifs-secteurs-123-aou%CC%82t-novembre-2025.pdf
- Tarifs France 3 Régions 2025 : https://www.francetvpub.fr/wp-content/uploads/2024/10/tarifs-2025-france-3-re%CC%81gions.pdf
- France 24, sept.-déc. 2025 : https://www.francetvpub.fr/wp-content/uploads/2025/07/tarifs-france-24-septembre-de%CC%81cembre-2025.pdf
- [Nomenclature FTV 2026] https://www.francetvpub.fr/wp-content/uploads/2025/09/nomenclature-des-codes-secteurs-2026.pdf
- [CGV FTPI 2025] https://www.francetvpub.fr/wp-content/uploads/2024/11/terms-and-conditions-for-the-commercial-sale-of-advertising-space-and-sponsors-on-france-t%C3%A9l%C3%A9visions-publicit%C3%A9-international-2025.pdf
- Loi n° 86-1067, art. 53 : https://www.legifrance.gouv.fr/loda/article_lc/LEGIARTI000042727380/
- [COM Arte France 2017-2021] https://www2.assemblee-nationale.fr/static/14/comaffcult/COM%202017-2021%20ARTE%20France%20FINAL.pdf
- [M6 2025] M6 Publicité, 2025 Standard Terms and Conditions of Sale TV/Video (version anglaise) :
  https://m6unlimited.fr/app/uploads/sites/2/2024/12/m6-pub-cgv-2025-tv-en.pdf
- [M6 2026] M6 Unlimited, CGV TV-Vidéo 2026 : https://m6unlimited.fr/app/uploads/sites/2/2025/09/m6-unlimited-cgv-tv.video-2026.pdf
- [M6 2027] M6 Unlimited, CGV TV-Vidéo 2027 : https://www.admtv.org/wp-content/uploads/2022/10/M6UNLIMITED-CGV-2027-TVVideo-VDEF-2.pdf
- CPI art. L342-1 : https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000006279247
