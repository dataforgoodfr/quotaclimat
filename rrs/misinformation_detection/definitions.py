"""Subject-specific misinformation definitions used to build LLM system prompts."""

SUBJECT_DEFINITIONS: dict[str, str] = {
    "insecurity": """
La désinformation est définie ici comme tout extrait dont le thème central est l'immigration, \
les personnes étrangères, immigrées, réfugiées, demandeuses d'asile, sans-papiers, exilées ou \
d'origine étrangère — désignées explicitement ou par substitution manifeste (« certaines \
populations », « quartiers sensibles », etc.) — ET dont le propos fait de cette dimension \
migratoire un élément moteur de l'argumentation, non une simple mention incidente.

Relève de ce périmètre : les données chiffrées sur les migrations, flux ou demandes d'asile ; \
les droits, statuts, dispositifs ou politiques migratoires (accueil, asile, éloignement, \
régularisation, OQTF, aides sociales, circulation intra-européenne liée au statut migratoire) ; \
les récits présentant l'immigration ou une politique migratoire comme cause ou facteur \
aggravant de l'insécurité, de la délinquance ou d'un fait divers précis (y compris si le fait \
rapporté est avéré) ; les statistiques sur la part d'étrangers parmi les personnes détenues ou \
condamnées ; et les propos essentialisants, généralisants ou déshumanisants visant les personnes \
étrangères ou migrantes (métaphores de flux, d'invasion, de submersion ou de remplacement).

N'est PAS retenu, même s'il porte sur la sécurité, la délinquance ou la justice, l'extrait dont \
l'immigration n'est pas l'élément moteur du propos, notamment : la délinquance, le laxisme \
judiciaire, les violences urbaines ou les cambriolages sans lien migratoire thématisé ; \
l'antisémitisme, le racisme ou l'antiracisme sans lien migratoire explicite ; l'islam ou la \
pratique religieuse sans lien explicite à l'immigration ; la responsabilité parentale ou \
éducative sans lien migratoire ; et la baisse de la natalité française, sauf lien explicite à \
l'immigration (ex. argument de remplacement démographique).

N'est pas non plus retenu l'extrait relevant manifestement d'un autre champ (sport, migration \
animale ou de données, commerce, météo, publicité), la fiction/satire identifiée, le \
fact-checking citant une narrative pour la réfuter, ou la couverture d'un événement étranger sans \
donnée ni jugement sur des personnes migrantes ou étrangères.
""".strip(),

    "climate": """
La désinformation est définie ici comme tout contenu qui contredit le consensus scientifique \
établi ou propage des narratifs trompeurs sur le changement climatique, en couvrant trois \
dimensions : la science climatique, l'action climatique, et l'ensemble des solutions \
d'atténuation et d'adaptation telles que décrites dans les rapports du GIEC.

Science climatique : sont visées les affirmations niant ou minimisant les causes humaines du \
réchauffement climatique, contestant l'existence ou la gravité de la crise climatique, \
déformant les projections scientifiques du GIEC, ou présentant le consensus scientifique comme \
incertain ou fabriqué.

Action climatique : sont visés les narratifs discréditant les politiques climatiques nationales \
ou internationales (Accord de Paris, taxonomie verte, lois climat), les affirmations présentant \
l'inaction comme légitime ou la transition comme inutile, ainsi que les contenus instrumentalisant \
de fausses données économiques ou sociales pour bloquer toute régulation climatique.

Solutions d'atténuation et d'adaptation : sont visées les affirmations trompant sur l'efficacité, \
le coût ou la faisabilité des solutions reconnues par le GIEC — énergies renouvelables, efficacité \
énergétique, reforestation, agriculture bas-carbone, capture de carbone, adaptation des \
infrastructures — ainsi que les comparaisons déloyales avec les énergies fossiles ou le nucléaire \
visant à disqualifier ces solutions sans base factuelle sérieuse.

Sont également concernés les chiffres falsifiés ou sortis de contexte, les corrélations abusives, \
les théories du complot sur les motivations des scientifiques ou des acteurs de la transition, et \
tout amalgame visant à associer l'action climatique à des agendas idéologiques sans rapport avec \
les faits
""".strip(),

    "environmental_health": """
La santé environnementale est ici définie comme l'ensemble des atteintes à la santé 
humaine, animale et des écosystèmes liées à l'exposition à des substances chimiques 
polluantes, en particulier les polluants persistants dans l'environnement (PFAS, 
pesticides, métaux lourds tels que le cadmium ou le plomb, biocides), ainsi que les 
controverses, politiques et pratiques de prévention qui leur sont associées.\n
Sont notamment incluses les voies d'exposition par l'alimentation (résidus dans les 
aliments), l'eau, l'air, les poussières et les sols, qu'elles résultent d'une proximité 
directe (riverains de zones traitées, expositions professionnelles) ou d'une 
contamination diffuse et persistante de l'environnement (sols agricoles, nappes 
phréatiques, chaînes alimentaires). Les effets sur la vie du sol, la faune aquatique et 
les insectes pollinisateurs sont également couverts, dans la mesure où ils affectent 
indirectement la santé humaine (fertilité des sols, qualité de l'eau, pollinisation).\n
Sont couverts aussi bien les effets sanitaires établis (cancers, troubles de la 
reproduction, atteintes neurologiques ou immunitaires) que les incertitudes 
scientifiques qui subsistent, notamment sur les expositions cumulées à plusieurs 
substances (« effet cocktail ») et les difficultés méthodologiques à démontrer un lien 
de causalité. Cette thématique est fréquemment articulée à celles portant sur 
l'agriculture, l'industrie, l'alimentation et la régulation sanitaire.
""".strip(),

    "purchase_power": """
Le thème couvre les débats publics portant sur l'argent des ménages français
et sur les finances publiques considérées sous l'angle de ce qu'elles coûtent
ou rapportent aux Français.

Il comprend quatre registres :

CE QUE LES MÉNAGES REÇOIVENT
salaires, Smic, primes, pensions de retraite, allocations chômage,
prestations sociales et minima sociaux.
CE QUE LES MÉNAGES DÉPENSENT
prix et inflation, alimentation, énergie et carburants, logement et loyers,
santé et mutuelles, transports, assurances, crédits et frais bancaires.
CE QUI EST PRÉLEVÉ
impôts, taxes, TVA, cotisations sociales, fiscalité du patrimoine et des
successions, fiscalité locale.
CE QUI EST FAIT DE CET ARGENT
dépense publique, dette et déficit, fonction publique et bureaucratie,
fraude sociale et fiscale, compétitivité et commerce extérieur, et les
modèles économiques invoqués dans le débat.
Le thème inclut aussi bien les affirmations chiffrées que les récits et
accusations qui les accompagnent (« on nous vole », « le travail ne paie
plus », « l'État gaspille »), ainsi que la contestation des chiffres officiels
et des institutions qui les produisent.

Il inclut les contre-récits au même titre que les récits dominants.

NE RELÈVENT PAS DU THÈME :
– les emplois figurés du vocabulaire économique (inflation des normes,
  gaspillage alimentaire, capitalisation boursière) ;
– l'économie d'autres pays sans lien établi avec la France ;
– la vie financière des entreprises sans effet énoncé sur les ménages ;
– les termes du dictionnaire employés dans un tout autre sens
""".strip(),
}


def get_definition(subject: str) -> str:
    """Return the misinformation definition for a subject, or raise if unknown."""
    if subject not in SUBJECT_DEFINITIONS:
        raise ValueError(
            f"No misinformation definition for subject '{subject}'. "
            f"Available: {list(SUBJECT_DEFINITIONS.keys())}"
        )
    return SUBJECT_DEFINITIONS[subject]


def build_system_prompt(subject: str) -> str:
    """Prompt for TV/radio transcript classification."""
    definition = get_definition(subject)
    return f"""Tu es un expert en détection de désinformation médiatique en France. \
Ta tâche est d'analyser des extraits de programmes télévisés ou radiophoniques \
et de déterminer s'ils contiennent de la désinformation selon la définition suivante :

{definition}

IMPORTANT : certains extraits peuvent être des publicités ou des annonces commerciales. \
Dans ce cas, indique-le et considère qu'il n'y a pas de désinformation.

IMPORTANT : ne classifier pas en tant de mésinformation des segments qui ne concernent pas \
la France, la politique française ou des acteurs français.

Avant de conclure, réponds successivement aux questions suivantes dans le champ "analysis". \
Reprends chaque question telle quelle, suivie de sa réponse strictement par "oui" ou "non" \
(une question par ligne, au format "<question> <oui/non>") :
1. L'extrait comporte-t-il une affirmation précise, une opinion, un jugement de valeur ou une promesse ?
2. L'affirmation est-elle suffisamment précise ? (Date, lieu, institution etc.)
3. Porte-t-elle sur un événement observable ?

La justification doit être courte.

- "oui" : l'extrait contient de la désinformation telle que définie
- "non" : l'extrait ne contient pas de désinformation (y compris les publicités)
- "incertain" : l'extrait est ambigu ou insuffisant pour conclure
"""


