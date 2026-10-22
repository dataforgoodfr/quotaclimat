# Analyses thématiques (`thematic_extracts`)

Ce dossier documente les analyses ponctuelles de l'Observatoire des Médias sur l'Écologie (OMÉ) sur un sujet donné : incendies, vagues de chaleur, inondations, etc. On y trouve la méthode commune, la description des données, la banque d'axes d'analyse et les décisions prises pour chaque analyse, afin que chaque nouvelle analyse parte de l'acquis.

## À lire dans cet ordre

1. `methodo.md` : règles communes. Elles s'appliquent à toutes les analyses et priment sur les choix locaux.
2. `donnees.md` : tables thématiques presse et audiovisuel, colonnes, dictionnaire thématique.
3. `axes_generiques.md` : banque d'axes qui ne dépendent pas du sujet, avec leurs termes.
4. `<AAAA-MM>-<sujet>/decisions.md` et `<AAAA-MM>-<sujet>/axes.md` des analyses proches du sujet traité.
5. `references.md` : travaux publiés (les nôtres et ceux d'autres organisations) dont s'inspirer pour les axes et les représentations graphiques.

## Démarche en 4 étapes

Chaque étape se termine par une validation explicite de l'équipe avant de passer à la suivante.

| Étape | Ce qu'on produit | Où on le consigne |
|---|---|---|
| 1. Extraction | Entrée de `thematic_dict.yml`, période d'analyse, période de comparaison, tables thématiques générées, contrôles | `decisions.md` §1 |
| 2. Axes | Axes génériques appliqués ou écartés, axes spécifiques et leurs termes | `axes.md` de l'analyse, puis `axes_generiques.md` en fin d'analyse pour les axes à remonter |
| 3. Requêtes | Questions Metabase sur les tables thématiques | `decisions.md` §3 |
| 4. Résultats | Takeaways triés (publiable, à creuser, rien à signaler), encadré méthodologique | Document Metabase avant publication, résumé dans `decisions.md` §4 après publication |

## Où vivent les choses

- **Méthode, axes, décisions, liste des requêtes** : ce dossier.
- **Dictionnaire thématique** : `quotaclimat/data_processing/thematic_extracts/thematic_dict.yml`.
- **Résultats avant publication** : un document Metabase dans la collection de l'analyse. Ce repo est public : aucun chiffre ni takeaway non publié ne doit y être poussé.
- **Résultats après publication** : résumé dans `decisions.md` §4, avec le lien vers l'article.
- **SQL** : dans Metabase. Après publication, copier le SQL des questions utilisées dans `<AAAA-MM>-<sujet>/queries/` pour le figer.

## Conventions Metabase

- Collection : `03. Analyses thématiques/NN. <Sujet> <Année>`.
- Préfixe des questions : `AV -` pour l'audiovisuel, `PE -` pour la presse écrite.
- Numérotation : `Qn - <axe>` (Q1 volumétrie, Q2 recontextualisation, Q3 causes, Q5 solutions, Q6 comparaison, Q7 courbe journalière, Q8 par événement).
- Versions abandonnées : préfixe `OLD-`. On ne modifie jamais une question d'une autre analyse : on la duplique.
- Résultats : un document Metabase `Résultats - <Sujet> <Année>` dans la collection.

## Nouvelle analyse

Copier `_template/decisions.md` et `_template/axes.md` dans `<AAAA-MM>-<sujet>/`, puis remplir la section 1 de `decisions.md` avant toute requête.
