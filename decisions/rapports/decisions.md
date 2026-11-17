# Rapports : conventions communes

Un rapport de l'Observatoire des Médias sur l'Écologie (OMÉ) qui s'appuie sur des données du pipeline a son dossier `<AAAA-MM>-<sujet>/`, daté du mois de publication prévu. On y versionne ce qu'il faut pour comprendre, refaire et vérifier ses chiffres :

- `decisions.md` : le contexte, les règles de calcul et les choix de présentation, avec leur raison, la carte des graphiques (où vit chaque requête), les limites et les points à valider. Il est écrit pour l'équipe et pour un LLM qui reprend le travail sans l'historique des conversations : chaque règle doit pouvoir s'appliquer sans deviner ;
- d'autres fichiers markdown quand un sujet demande une procédure (par exemple la fabrication d'un graphique hors Metabase) ;
- `queries/` : les requêtes SQL qui tournent hors de Metabase (extraction vers Google Sheets...) et, après publication, une copie du SQL des questions Metabase utilisées.

Les analyses thématiques ont leurs propres conventions : [`../analyses_thematiques/decisions.md`](../analyses_thematiques/decisions.md).

## Règles

- **Le dépôt est public.** Aucun chiffre ni résultat non publié n'y entre : ni dans les markdown, ni dans les commentaires SQL, ni dans les messages de commit. Les résultats vivent dans Metabase et dans les documents de travail (Google Docs, Sheets) jusqu'à la publication. On n'y écrit pas non plus les adresses des outils internes (Metabase, Sheets, Docs) : on cite les identifiants (question 2500, tableau de bord 228).
- **Statuts des règles** : **adoptée** (s'applique), **provisoire** (s'applique, à valider par l'équipe ou un expert du sujet), **abandonnée** (gardée avec sa raison, pour ne pas la reproposer).
- **Une règle, un numéro** (`R1`, `R2`...) : les requêtes et les textes du tableau de bord y renvoient. Une règle modifiée garde son numéro ; l'ancienne version passe dans l'historique avec sa date.
- **Les requêtes du rapport partagent leurs définitions** (plage de dates, seuils, rattachement des pubs) : une règle changée se reporte dans toutes les requêtes qui l'appliquent, et la carte des graphiques dit lesquelles.
- **Les questions Metabase du rapport ne sont pas dans `metabase/`** : elles ne sont pas synchronisées par le flow `metabase-sync`, leur SQL de référence est celui de Metabase. On le copie dans `queries/` à la publication, pour figer la version publiée.
- Toute figure publiée est relue par un expert du sujet (équipe pub, équipe mésinformation) : `decisions.md` liste ce qui reste à valider.
