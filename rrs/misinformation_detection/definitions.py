"""Subject-specific misinformation definitions used to build LLM system prompts."""

SUBJECT_DEFINITIONS: dict[str, str] = {
    "insecurity": """
La désinformation est définie ici comme l'ensemble des faits de délinquance et de criminalité \
survenant en France ou concernant directement la société française, du sentiment d'insécurité \
qui leur est associé, ainsi que de tout narratif ou fait susceptible de nourrir un sentiment de \
peur et une sensation d'insécurité pouvant être instrumentalisés dans le débat public français. \
Sont également incluses les fausses données ou représentations qui construisent l'image d'un \
groupe comme dangereux. Les violences intrafamiliales et conjugales entrent dans le périmètre \
lorsqu'elles sont mobilisées pour nourrir une haine des différences culturelles, pour proposer \
des lois restreignant les droits des femmes, pour justifier un renforcement des pouvoirs de la \
police ou une restriction des libertés. La désinformation associée — qu'elle prenne la forme de \
fausses statistiques, de faits déformés, de narratifs de cadrage (par exemple « ensauvagement », \
« zones de non-droit », « explosion de la violence ») ou d'amalgames — est fréquemment articulée à \
celle portant sur la justice, l'immigration et l'action policière. Les faits survenus à l'étranger \
ne sont retenus que lorsqu'ils sont explicitement mobilisés dans le débat public français à des \
fins de comparaison, d'analogie ou d'instrumentalisation politique
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

La justification doit être courte.

- "oui" : l'extrait contient de la désinformation telle que définie
- "non" : l'extrait ne contient pas de désinformation (y compris les publicités)
- "incertain" : l'extrait est ambigu ou insuffisant pour conclure
"""


