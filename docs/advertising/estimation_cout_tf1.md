# Estimer le coût des spots publicitaires : méthode TF1

Statut : proposition de méthode, non validée par l'équipe. Sources consultées le 2 octobre 2026.
Aucun montant issu de cette méthode ne doit être diffusé sans relecture par une personne experte.

## En bref

- TF1 PUB calcule le prix d'un spot à partir du **tarif publié de l'écran** (brut HT, format 20 secondes
  depuis 2025), multiplié par un **indice de format** (durée du spot), puis corrigé par des **majorations**
  (position dans l'écran, plusieurs annonceurs, etc.). C'est la logique des « investissements bruts » de
  Kantar (plaquettes des régies, hors remises), sans garantie d'égalité au spot près : le glossaire des CGV
  TF1 distingue « Tarif de base » et « Brut Kantar Média » [CGV 2023 p.31].
- Le prix réellement payé (net) dépend d'un **abattement conventionnel négocié et confidentiel**. On ne peut
  l'estimer qu'en fourchette, sauf pour quelques offres à abattement fixe (campagnes gouvernementales, PME...).
- **Le point bloquant est la donnée.** En 2026, les tarifs écran par écran sont dans l'espace acheteur
  TF1 AdManager (connexion requise) et les CGU du site interdisent la reproduction. Il n'existe pas d'archive
  publique couvrant notre historique (occurrences analysées depuis le 29/09/2025).

## 1. Formule de calcul (Espace classique)

Notation : `P` = tarif publié de l'écran au format 20 s, `I(d)` = indice de format pour une durée `d`.

```
Tarif de base   = arrondi_euro( P x I(d) / 100 )
Brut corrigé    = Tarif de base x (1 + majorations applicables)
Net             = Brut corrigé, diminué des abattements et remises de la cascade (section 1.5)
```

### 1.1 Tarif publié

- Grilles « Brut HT publiées au format 20'' », par écran, « selon secteur (Tarif 1 ou 2) et
  emplacement/priorité (Standard/Premium 1&99/ 2&98/3&97) » [2026 p.41 ; 2027 p.44]. En 2025 s'ajoute une
  grille PRIO pour TF1 [2025 p.24 et p.58].
- La mention Tarif 1 / Tarif 2 figure dans les cascades TF1 2025 et TF1 PRIME 2026-2027, pas dans les
  cascades TNT 2025 et REACH 2026-2027 [2025 p.58 et p.60 ; 2026 p.41 et p.43].
- Tarif applicable : celui « en vigueur à la date de diffusion » [CGV 2023 p.11]. TF1 PUB peut modifier ses
  tarifs à tout moment, et à moins de 4 jours pour un événement exceptionnel [CGV 2023 p.12]. Les tarifs
  évoluent lors des « flashs » marché [2026 p.29]. Il faut donc la dernière version publiée avant la
  diffusion. CGV 2026 non consultées : point à revérifier.
- Le format de référence est passé de 30 s à 20 s en 2025 [2025 p.55].

### 1.2 Indices de format

Table complète : [`tf1pub_indices_format.csv`](tf1pub_indices_format.csv) (3 à 60 s, base 100 = 20 s,
recoupée avec les PDF). Extrait :

| Durée | 2025 | 2026 | 2027 |
|---|---|---|---|
| 10 s | 62 | 62 | 62 |
| 15 s | 81 | 81 | 81 |
| 20 s | 100 | 100 | 100 |
| 25 s | 105 | 107 | 106 |
| 30 s | 112 | 115 | 110 |
| 45 s | 226 | 217 | 217 |
| 60 s | 300 | 300 | 300 |

Sources : [2025 p.55 ; 2026 p.38 ; 2027 p.41]. Arrondi à l'euro après application de l'indice (inférieur
jusqu'à 0,49 €, supérieur à partir de 0,50 €). Indices au-delà de 60 s « fournis à la demande » ; les durées
41 à 44, 46 à 49, etc. ne figurent pas dans la table (interpolation à signaler comme estimation).

### 1.3 Majorations

| Majoration | Taux | Champ |
|---|---|---|
| Emplacement préférentiel (EP) : 2 premières et 2 dernières positions | +15 % | TNT 2025 ; REACH 2026-2027 |
| EP : 3e position et antépénultième | +10 % | idem |
| Multi-annonceurs, présence ou citation < 3 s | +2 % par annonceur supplémentaire | tout l'Espace classique |
| Multi-annonceurs, présence ou citation >= 3 s | +5 % par annonceur supplémentaire | idem |
| Co-branding (durée partagée, ou 2e annonceur > 10 s) | +12 % | idem |
| Multi-secteurs (au-delà de 2 codes secteur) | +3 % par code | idem |
| Opérations spéciales | +20 % | idem |

Sources : [2025 p.56 ; 2026 p.16 et p.39 ; 2027 p.18 et p.42]. Sur TF1 en 2025 et sur TF1 PRIME en
2026-2027, la cascade ne comporte pas de ligne de majoration EP : les positions 1/99, 2/98, 3/97 sont
vendues via les grilles Premium [2025 p.58 ; 2026 p.13 et p.41 ; 2027 p.44].
En 2027, les publicités comparatives n'ont plus accès aux EP [2027 p.18].

### 1.4 Codes d'écran et segments

- Depuis 2026, **TF1 PRIME** = écrans de TF1 codés 2000 à 2199 et écrans EVENT (terminaison 7 ou 8) quelle
  que soit l'heure. Tous les autres écrans de TF1 forment **TF1 REACH** [2026 p.10 ; 2027 p.12]. Les écrans
  VIP (terminaison 2) sont supprimés en 2026 [2026 p.3].
- Terminaisons (dernier chiffre du code) [2026 p.44 ; 2027 p.47] :
  - colonne TF1 PRIME : 0 standard, 1 et 2 inédit, 3 télé-réalité, 4 divertissement / talk-show,
    6 exceptionnel, 7 EVENT, 8 EVENT sport, 9 sport ;
  - colonne REACH, toutes chaînes hors LCI : 0 standard, 2 films, 3 télé-réalité, 4 divertissement /
    talk-show, 5 jeunesse (utilisé aussi sur TF1 pour l'offre enfants [2026 p.26]), 6 exceptionnel,
    8 Quotidien, 9 sport.
  - En 2025, terminaison 2 = écran VIP sur TF1 [2025 p.24].
- Les day-parts sont définis par plages de codes : Day 0300 à 1799, Access 1800 à 1999, Peak 2000 à 2199,
  Night 2200 à 2899 (Extra-night 2400 à 2899) [2026 p.22 et p.25].
- **Hypothèse à vérifier sur une vraie grille** : le code se lit HH + dizaine de minutes + terminaison
  (écran « 750 » vers 7h50, « 2047 » vers 20h4x de type EVENT ; les codes 24xx à 28xx sont après minuit).
  Indices : exemple de la matinale avec écrans 750, 820, 850 placés entre 7h30 et 9h [2025 p.35]. TF1 PUB
  précise que « les libellés des écrans n'impliquent pas des horaires de diffusion » [2026 p.41].

### 1.5 Du brut au net (cascade)

Lecture des cascades [2025 p.58 et p.60 ; 2026 p.41 et p.43 ; 2027 p.44 et p.46] :

| Cas | Étapes après le brut corrigé | Public ? |
|---|---|---|
| TF1 2025, TF1 PRIME 2026-2027, spot à spot | messages gracieux, abattement conventionnel, dégressif de volume (2025 : 21 à 34 % ; 2026-2027 : 21 à 30 %, acompte de 18 % sur facture) | dégressif oui, abattement conventionnel non |
| REACH 2026-2027 (dont TF1 hors prime), spot à spot ou MPI | messages gracieux, abattement conventionnel, remise de référence 30 % | remise oui, abattement non |
| TNT 2025 | idem, remise de référence 16,5 % | idem |
| Gouvernement et intérêt général | abattement 40 % sur TF1, 50 % pour les campagnes climat et sobriété énergétique des administrations et associations (Contrat Climat) ; en 2025 sur TNT : 30 % ou 40 %, cumulable avec la remise de référence | oui, hors messages gracieux |
| PME-PMI et nouvel annonceur | abattement 45 à 75 % selon segment et période, non cumulable | oui |
| Cinéma, Entertainment, Édition | abattements 45 à 85 % selon catégorie et délai, non cumulables | oui |

Sources des taux : 2025 [dégressif p.57, remise de référence p.59, gouvernement p.31] ; 2026 [offres
catégorielles p.32 à 36, dégressif p.40, remise de référence p.42] ; 2027 [gouvernement p.38, dégressif
p.43, remise de référence p.45]. Les fourchettes PME, cinéma, entertainment et édition sont celles de 2026.

Conséquences :

- Pour un achat classique, la part publique de la cascade donne seulement un **plafond** du net : environ
  0,70 x brut en REACH 2026-2027, 0,70 à 0,79 x brut sur TF1 PRIME 2026-2027, 0,66 à 0,79 x brut sur TF1
  en 2025, 0,835 x brut sur la TNT en 2025. L'abattement conventionnel vient en plus et n'est pas publié.
- Ordre de grandeur marché : l'Arcom, sur données Kantar, estime l'écart brut/net à « environ -70 % pour la
  télévision » en 2018, soit un net proche de 30 % du brut [Arcom 2022 p.4]. Chiffre ancien, toutes chaînes
  confondues, à ne pas appliquer spot par spot.
- Calibration possible plus tard : comparer la somme des bruts estimés sur un an pour toutes les chaînes du
  groupe au chiffre d'affaires publicitaire publié (1 574 M€ en 2025, dont 198 M€ pour TF1+, selon le
  communiqué du 12/02/2026). Ventilation linéaire / autres à vérifier dans le rapport annuel.

## 2. Données nécessaires et disponibilité

### 2.1 Grilles écran par écran

- 2023 : grilles « disponibles sur le Site La Box » [CGV 2023 p.6]. La Box demandait de créer un compte
  pour acheter [2025 p.25].
- 2026 : le site public tf1pub.fr ne publie plus de grille (pages et code du site parcourus le 2 octobre
  2026). Les prix par écran apparaissent dans le parcours d'achat de TF1 AdManager (admanager.tf1pub.fr) ;
  dans le code JavaScript public de l'application, les routes `offers` et `purchases` sont soumises à des
  contrôles d'accès (`canActivate`). Connexion probablement obligatoire, à confirmer avec un compte.
- CGU TF1 PUB : « Toute reproduction totale ou partielle de tout ou partie des éléments présents sur les
  pages des Services non autorisée est strictement interdite » (version du 14/10/2019). Une extraction
  systématique relève aussi du droit du producteur de base de données (CPI art. L342-1). **Question à
  soumettre à un juriste avant toute collecte automatisée.**
- Indice faible d'une consultation publique passée : Purepeople (04/08/2024) cite des prix d'écrans Star
  Academy publiés « sur le site TF1Pub ».

### 2.2 Historique depuis le 29/09/2025

Aucune source publique trouvée. Options, de la plus précise à la moins précise :

1. **Kantar Media (pige TV valorisée)** : valorisation brute de chaque action publicitaire, toutes chaînes,
   sur tout l'historique (granularité, prix et licence de publication à confirmer avec Kantar). Permettrait
   aussi d'auditer notre détection. Attention : le « Brut Kantar » est distinct du « Tarif de base » TF1
   dans le glossaire des CGV [CGV 2023 p.31].
2. **Demande directe à TF1 PUB** : grilles brutes depuis le 29/09/2025 (Standard, Premium, T1/T2),
   segmentation sectorielle T1/T2 2025-2027, CGV juridiques 2025-2026. Gratuit si accepté, issue incertaine.
3. **Popcorn Media** : produit les grilles tarifaires de régies TV (outil PopPricing, TF1 parmi ses
   clients). Peu probable qu'il les cède sans accord de la régie.
4. **Modèle d'estimation** (par day-part, jour, saison) calé sur les grilles obtenues plus tard : à réserver
   aux trous de données, précision faible.

### 2.3 Secteurs Tarif 1 / Tarif 2

- La liste T1/T2 2025-2027 est publiée sur l'espace TF1 AdManager [2026 p.40], non accessible publiquement.
- Exemple ancien (2015) : alimentation, boissons hors sodas, entretien, hygiène-beauté en T1 ;
  automobile, énergie, distribution, banque-assurance, télécoms, services en T2 [CC 2015 p.21-22].
- Le rattachement passe par la Nomenclature TV des produits de l'ADMTV, publique (xlsx), à croiser avec
  notre taxonomie OME. En attendant la liste, calculer les deux valeurs.

## 3. Application à nos données

1. **Périmètre** : occurrences de `ad_occurrences_classified` (contenu AD uniquement). Les autopromotions,
   jingles et parrainages ne sont pas valorisés par les grilles d'écran (le parrainage a sa propre
   tarification).
2. **Écran** : un tunnel (`ad_tunnels`) correspond en principe à un écran. Passer en heure de Paris, puis
   rattacher chaque tunnel d'un jour à un code d'écran de la grille par alignement monotone sur l'heure
   nominale du code (tolérance à calibrer, sans croiser l'ordre des écrans). Fusionner les tunnels
   rattachés au même écran (un trou de plus de 5 s coupe un tunnel). Contrôles : nombre d'écrans de la
   grille et de tunnels par jour, taux de rattachement.
3. **Position** : rang des spots AD dans le tunnel, compté depuis le début (1, 2, 3) et depuis la fin
   (99, 98, 97). Un spot non détecté en tête ou en fin d'écran décale les positions : à contrôler.
4. **Durée** : `duration_sec` arrondie à la seconde, puis indice de l'année de diffusion. Vérifier sur
   l'histogramme des durées que l'arrondi tombe sur les formats commerciaux (10, 15, 20, 30 s...).
5. **Segment et prix** : PRIME ou REACH selon le code (2026-2027), Tarif 1 ou 2 selon le secteur, grille
   Premium pour les positions 1 à 3 et 97 à 99 en PRIME (et sur TF1 en 2025), majoration EP en REACH.
6. **Sorties proposées** : tarif de base, brut corrigé estimé, plafond net public, indicateurs de qualité
   (rattachement écran, position, durée hors table, T1/T2 incertain).

Aucun composant LLM n'est nécessaire : jointures et règles suffisent.

## 4. Vérifications

Faites :

- Formule, indices et cascades relus sur les pages des conditions commerciales 2025, 2026 et 2027 ; les
  126 valeurs d'indices de la table CSV ont été recoupées automatiquement avec le texte des PDF.
- Logique de calcul testée sur un cas fictif (arrondi, positions EP, segment PRIME/REACH).

Non faites, à prévoir :

- Aucun montant réel calculé : pas de grille disponible.
- Rattachement tunnel / écran non testé : pas d'accès à la base depuis l'environnement de travail.
- CGV juridiques 2025 et 2026 et liste T1/T2 actuelle non consultées.
- Validation externe : comparer un échantillon de spots à la valorisation Kantar si un extrait est obtenu.

## 5. Limites connues

- Achats en MPI (coût GRP net garanti) : le net dépend de l'audience livrée, pas du tarif de l'écran.
- Parcours « All Buy Myself » (prix nets en temps réel), Golden Auction Spots (enchères), messages gracieux :
  invisibles depuis nos données, valorisés au brut comme les autres.
- Majorations multi-annonceurs, co-branding et opérations spéciales : détectables seulement en partie
  (marques multiples dans la vidéo).
- Une grille capturée trop tôt peut différer du tarif en vigueur le jour de diffusion.

## Note technique

Le serveur tf1pub.fr n'envoie pas le certificat intermédiaire « GlobalSign GCC R46 OV TLS CA 2025 » : curl et
Python refusent la connexion. Corriger en ajoutant ce certificat (URL fournie dans le champ AIA du
certificat du site) au bundle de confiance, sans désactiver la vérification TLS.

## Sources

- [2025] TF1 PUB, Conditions commerciales 2025 (TV-Streaming, Audio digital, Opérations spéciales) :
  https://www.admtv.org/wp-content/uploads/2022/10/240924-TF1-PUB-Conditions-Commerciales-2025.pdf
- [2026] TF1 PUB, Conditions commerciales saison 2026 :
  https://www.admtv.org/wp-content/uploads/2022/10/Conditions-Commerciales-Le-Book-2026-hors-TVS-streaming-et-audio.pdf
- [2027] TF1 PUB, Conditions commerciales saison 2027 (publiées en septembre 2026, applicables au 01/01/2027) :
  https://www.admtv.org/wp-content/uploads/2022/10/LE-BOOK-saison-2027.pdf
- [CGV 2023] TF1 Publicité, Conditions générales de vente 2023, Espace classique (version du 18/10/2022) :
  https://www.mediacompact.fr/wp-content/uploads/2022/10/CGV-classique.pdf
- [CC 2015] TF1 Publicité, Conditions commerciales 2015 :
  https://www.sri-france.org/wp-content/uploads/2014/12/TF1_conditions-commerciales-2015.pdf
- [Arcom 2022] Arcom, Les investissements publicitaires audiovisuels des annonceurs (mars 2022), p.4 :
  https://www.arcom.fr/nos-ressources/etudes-et-donnees/mediatheque/les-investissements-publicitaires-audiovisuels-des-annonceurs
- Kantar Media, IREP, France Pub, BUMP 2024, méthodologie (valorisation « sur la base des plaquettes
  tarifaires des régies (hors remises, dégressifs et négociations) ») :
  https://kantarmedia.fr/sites/default/files/2025-03/Communiqu%C3%A9%20BUMP%202024%20VDEF11032025.pdf
- ADMTV, Nomenclature TV des produits : https://www.admtv.org/nomenclature/
- TF1 PUB, Conditions générales d'utilisation (14/10/2019) : https://cms.tf1admanager.fr/media/afeicssc/cgu_tf1pub.pdf
- Code de la propriété intellectuelle, art. L342-1 : https://www.legifrance.gouv.fr/codes/article_lc/LEGIARTI000006279247
- Groupe TF1, résultats annuels 2025 (communiqué du 12/02/2026, cité via Zonebourse, original non consulté) :
  https://www.zonebourse.com/actualite-bourse/tf1-resultats-annuels-2025-du-groupe-tf1-ce7e5ad3da8dfe22
- Purepeople, 04/08/2024 (source secondaire) :
  https://www.purepeople.com/article/la-date-du-retour-de-la-star-academy-a-t-elle-fuite-les-tarifs-publicitaires-deja-choisis-seraient-astronomiques_a525920/1
- Popcorn Media : https://www.popcorn-media.fr/
