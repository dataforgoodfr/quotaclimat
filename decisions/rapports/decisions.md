# Rapports : conventions communes

Un rapport de l'Observatoire des Médias sur l'Écologie (OMÉ) qui s'appuie sur des données du pipeline a son dossier `<AAAA-MM>-<sujet>/`, daté du mois de publication prévu. On y versionne ce qu'il faut pour comprendre, refaire et vérifier ses chiffres :

- `decisions.md` : le contexte, les règles de calcul (une liste simple, une règle encore discutée est marquée « en discussion »), les choix de présentation, des remarques (dont ce qu'on a décidé de ne pas faire, et pourquoi) et la carte des graphiques (la requête de chaque graphique). Il est écrit pour l'équipe et pour un LLM qui reprend le travail sans l'historique des conversations : chaque règle doit pouvoir s'appliquer sans deviner ;
- d'autres fichiers markdown quand un sujet demande une procédure (par exemple la fabrication d'un graphique hors Metabase) ;
- `queries/` : les requêtes SQL du rapport. Elles font foi : ce sont elles qui produisent les chiffres publiés.

Les analyses thématiques ont leurs propres conventions : [`../analyses_thematiques/decisions.md`](../analyses_thematiques/decisions.md).

## Règles

- **Le dépôt est public.** Aucun chiffre ni résultat non publié n'y entre : ni dans les markdown, ni dans les commentaires SQL, ni dans les messages de commit. Les résultats vivent dans Metabase et dans les documents de travail (Google Docs, Sheets) jusqu'à la publication. On n'y écrit pas non plus les adresses des outils internes (Metabase, Sheets, Docs) ni les identifiants des Sheets : on cite leur nom.
- **Les requêtes du rapport partagent leurs définitions** (plage de dates, seuils, rattachement des pubs) : une règle changée se reporte dans toutes les requêtes de `queries/` qui l'appliquent.
- **Les requêtes de `queries/` font foi ; Metabase sert à l'exploration préliminaire.** Les questions Metabase d'un rapport ne sont ni dans `metabase/` ni synchronisées, et peuvent diverger des règles. Dans la carte des graphiques, un graphique dont la requête n'est pas encore versionnée est marqué « Pour l'instant dans Metabase » ; on rapatrie son SQL dans `queries/` (SQL brut, sans variable Metabase, règles appliquées) avant de le publier. On évite de citer les numéros des questions Metabase, qui ne font pas référence.
- Toute figure publiée est relue par un expert du sujet (équipe pub, équipe mésinformation) : `decisions.md` liste ce qui reste à valider.
