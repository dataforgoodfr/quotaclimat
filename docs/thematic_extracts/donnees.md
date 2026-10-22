# Données des analyses thématiques

Les analyses tournent sur des tables thématiques légères, générées par un job Scaleway lancé à la main, et non plus directement sur `keywords` et `factiva_articles`. Le code du job est dans `quotaclimat/data_processing/thematic_extracts/`.

## Dictionnaire thématique

`quotaclimat/data_processing/thematic_extracts/thematic_dict.yml`, une entrée par thème sous la forme `[thème][langue][audiovisuel|presse]`. Règles de construction : `methodo.md` §1.

## Nommage des tables

`thematic_extracts.<thème>_<articles|keywords>_<pays>_<AAAAMMJJ début>_<AAAAMMJJ fin>`

Exemple : `incendies_articles_fr_20260701_20260708`. Le pays n'existe que dans le nom de la table, pas en colonne.

## Presse écrite

- Base Metabase : **Print Media DB** (id 8), schéma `thematic_extracts`.
- Source : `public.factiva_articles`, hors `duplicate_status = 'DUP'`.
- Une ligne = un article dont le titre, le chapô ou le corps (normalisés, voir `methodo.md` §5) contient au moins un terme de l'entrée `presse` du thème.

| Colonne | Contenu |
|---|---|
| `an` | Identifiant Factiva (clé) |
| `source_code` | Code source Factiva |
| `source_name` | Nom du média |
| `publication_datetime` | Date et heure de publication |
| `title`, `snippet`, `body` | Titre, chapô, corps (texte d'origine, non normalisé) |
| `word_count` | Nombre de mots |
| `keywords_found` | Termes du dictionnaire détectés, distincts, sous forme normalisée. Colonne de contrôle |
| `created_at` | Date de création de la ligne |

Classification des titres (presse nationale, régionale...) : table `source_classification` (`media_all`, `source_type`), à joindre sur `source_code`.

## Audiovisuel

- Base Metabase : **Scaleway Postgres** (id 2), schéma `thematic_extracts`.
- Sources : `public.keywords` pour les segments thématiques, S3 Mediatree pour les segments adjacents absents de `keywords`.
- Une ligne = un segment de 2 minutes d'une chaîne, soit thématique (il contient un terme de l'entrée `audiovisuel` du thème dans `keywords_with_timestamp`), soit adjacent (précédent ou suivant) d'un segment thématique de la même chaîne. Un segment à la fois thématique et adjacent est compté comme thématique.

| Colonne | Contenu |
|---|---|
| `this_id` | Clé propre à la table : `channel_name` + `start` en UTC |
| `channel_name` | Identifiant technique de la chaîne |
| `channel_title` | Nom de la chaîne |
| `start` | Début du segment (heure de Paris) |
| `thematic_chunk` | `true` si le segment contient un terme du thème, `false` s'il est seulement adjacent |
| `keywords` | Termes détectés, distincts. Colonne de contrôle |
| `themes` | Thèmes du dictionnaire OME détectés, distincts |
| `plaintext` | Transcription (texte d'origine, non normalisé) |
| `source_keywords_id` | Identifiant dans `public.keywords`, pour la traçabilité |
| `created_at` | Date de création de la ligne |

Volume du corpus : toujours filtrer sur `thematic_chunk = true`. Les segments adjacents ne servent qu'à la recontextualisation (`methodo.md` §4).
