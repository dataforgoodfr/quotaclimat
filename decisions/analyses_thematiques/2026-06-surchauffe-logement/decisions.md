# Surchauffe des logements (précarité énergétique d'été), été 2026

Fichier reconstitué à partir de la fiche d'analyse, des questions Metabase et du support présenté à l'ONPE.

**Méthode propre à cette analyse, non reconduite.** Le corpus a été construit ad hoc, par regex sur des termes fournis par l'ONPE, hors du dictionnaire OME, et la normalisation différait de celle du job. Les chiffres restent exploitables et comparables ; les regex et requêtes ci-dessous documentent ce qui a été fait, elles ne sont pas un modèle à réutiliser.

- Statut : livré à l'ONPE (Observatoire national de la précarité énergétique), présentation du 21/09/2026, non publié. Aucun résultat dans ce fichier : ils sont dans la collection Metabase. Suite envisagée : bilan OMÉ S2 2026, avec un angle précarité énergétique d'hiver.
- Tables : `public.keywords` (audiovisuel) et `public.factiva_articles` (presse)
- Collection Metabase : `03. Analyses thématiques/18. ONPE - Précarité énergétique d'été` (id 361)

## 1. Méthode spécifique

**Corpus « surchauffe du logement ».** Regex sur le texte, avec des termes fournis par l'ONPE (§2) :

- audiovisuel : `plaintext` de chaque segment de `public.keywords` (France) ;
- presse : titre, chapô et corps de `public.factiva_articles`, hors `DUP`, articles de 100 mots et plus, médias regroupés par `source_classification`.

**Normalisation.** Minuscules, accents retirés, apostrophes supprimées (`d'été` donne `dete`, contre `d ete` dans le job). `œ` est remplacé par `o`, et non `oe`.

**Axes.** Un axe est compté quand ses termes apparaissent dans le même segment ou le même article qu'un terme du corpus. Pas de recontextualisation à ±2 minutes : les bornes sont élargies de 2 minutes, mais chaque segment est lu seul.

**Périodes.**

- analyse : du 1er juin au 31 août 2026 inclus ;
- comparaison : évolution 2025-2026 en presse, depuis 2023 en audiovisuel, mentions par été (selon la fiche et le support ; périodes exactes non retrouvées dans les questions Metabase) ;
- mise en regard avec un indicateur physique : l'ITN (indicateur thermique national) et le nombre de jours de canicule.

## 2. Mots-clés

**Corpus (termes ONPE)** : `bouilloire thermique`, `passoire thermique`, `bouilloire energetique`, `passoire energetique`, `habitabilite`, `precarite energetique`, `temperature ambiante du logement` (singulier et pluriel).

**Termes du dictionnaire OME** relevés dans la fiche (non utilisés dans le corpus des requêtes Q1) : morts et décès liés à la chaleur ou à la canicule ; `ma prime renov`, rénovation, rénovation énergétique ou thermique des bâtiments, taux de rénovation, amélioration du confort thermique, isolation thermique ; passoire thermique ou énergétique ; chaleur, chaleur extrême, chaleur accablante, forte chaleur, coup de chaleur ; climatisation ; logement mal isolé.

## 3. Axes

Questions Metabase : volumétrie (AV 2275, PE 2269), mentions journalières (AV 2271, PE 2274), bâtiment (AV 2290, PE 2276), solutions (AV 2280, PE 2277), précarité et santé (AV 2281, PE 2278), climatisation recontextualisée avec la précarité (AV 2279).

| Ancien code | Axe | Catégorie | Termes (forme normalisée de l'époque) | Question Metabase | Résultat |
|---|---|---|---|---|---|
| Q1 | Volumétrie par été (depuis 2023 en audiovisuel, 2025-2026 en presse) | V1, V2 | corpus | 2275, 2269 | présenté |
| Q2 | Mentions journalières 2026, face à la climatisation et à l'ITN | V2, G14 | corpus, climatisation | 2271, 2274 | présenté |
| | Comparaison avec un sujet concurrent (surchauffe face à climatisation) | X | corpus, climatisation | 2271, 2274 | présenté |
| | Mots les plus employés du corpus | V3 | corpus | | présenté |
| A1 | Performance du logement | C2, spécifique | `dpe`, `diagnostic de performance energetique`, `confort dete` | 2290, 2276 | présenté |
| A2 | Caractéristiques du logement | C2, spécifique | `traversant`, `dernier etage`, `rez de chaussee`, `orientation sud`, `facade sud`, `orientation ouest`, `facade ouest`, `toit`, `sous les toits` | 2290, 2276 | présenté |
| A3 | Localisation du logement dans le quartier | C2, spécifique | `icu`, `ilot de chaleur urbain`, `bruit`, `lieu refuge` | 2290, 2276 | présenté |
| B1 | Protections solaires | C4 | `protection solaire`, `volet`, `rideau thermique`, `occultant`, `persienne`, `ombre` | 2280, 2277 | testé |
| B2 | Isolation thermique | C4 | `isolation`, `isolant`, `enveloppe thermique`, `renovation` (et `renovation energetique`, `renovation thermique` en audiovisuel) | 2280, 2277 | testé |
| B3 | Solutions passives | C4 | `vegetalisation`, `evapotranspiration`, `evaporation` | 2280, 2277 | testé |
| B4 | Ventilation | C4 | `brasseur dair`, `puits canadien`, `puits provencal`, `ventilateur`, `ventilation` (et `courant dair` en audiovisuel) | 2280, 2277 | testé |
| B5 | Dépenses énergétiques d'été, climatisation | C4 | `rafraichissement`, `refroidissement`, `climatisation`, `consommation energetique`, `pompe a chaleur reversible`, `climatiseur` | 2280, 2277 | testé |
| C | Précarité du ménage | C3c | `les plus precaires`, `modestes`, `pauvres`, `fragiles`, `demunis`, `faible revenu`, `populaire` | 2281, 2278 | présenté |
| D1 | Conditions de santé | C3b | `maladie chronique`, `cardiovasculaire`, `respiratoire`, `antipsychotiques`, `anxiolytiques`, `antidepresseurs`, `malaise`, `coup de chaleur`, `surmortalite` | 2281, 2278 | présenté |
| D2 | Personnes vulnérables | C3d | `personne vieillissante`, `personne agee`, `aine`, `senior`, `enfant`, `bebe`, `nourrisson`, `bas age`, `vulnerable` | 2281, 2278 | présenté |
| | Climatisation x précarité | croisement | B5 et C | 2279 | testé, non présenté |
| | Ratio articles passoire thermique / climatisation face au nombre de jours de canicule | V2 | | | testé |

Résultat : « présenté » = l'axe figure dans le support remis à l'ONPE (vérifié sur le texte du support) ; « testé » = produit, sans certitude qu'il figure dans le support ; « testé, non présenté » = produit, absent du support.

Les termes larges (`toit`, `bruit`, `ombre`, `populaire`, `fragile`, `enfant`, `renovation`) comptent des segments hors sujet ; la co-occurrence avec le corpus limite ce bruit sans l'éliminer.

**Axes de la banque non exploités** : non recensés, l'analyse est antérieure au recensement des axes.

## 4. Résultats

Non publiés : résultats dans la collection Metabase et le support présenté à l'ONPE (non référencé ici).

**Axes remontés dans la banque** : précarité du ménage (C3c), personnes vulnérables (C3d), courbe des mentions face à un indicateur physique (G14). Piste non remontée : comparaison avec un sujet concurrent (surchauffe face à climatisation).
