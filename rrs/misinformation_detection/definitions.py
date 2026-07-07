"""Subject-specific misinformation definitions used to build LLM system prompts."""

SUBJECT_DEFINITIONS: dict[str, str] = {
    "insecurity": """
La désinformation est définie ici comme l'ensemble des faits de délinquance et de criminalité, \
du sentiment d'insécurité qui leur est associé, ainsi que de tout narratif ou fait susceptible de nourrir \
un sentiment de peur et une sensation d'insécurité pouvant être instrumentalisés. \
Sont également incluses les fausses données ou représentations qui construisent l'image d'un groupe comme dangereux. \
Les violences intrafamiliales et conjugales entrent dans le périmètre lorsqu'elles sont mobilisées pour nourrir \
une haine des différences culturelles, pour proposer des lois restreignant les droits des femmes, \
pour justifier un renforcement des pouvoirs de la police ou une restriction des libertés. \
La désinformation associée — qu'elle prenne la forme de fausses statistiques, de faits déformés, \
de narratifs de cadrage (par exemple « ensauvagement », « zones de non-droit », « explosion de la violence ») \
ou d'amalgames — est fréquemment articulée à celle portant sur la justice, l'immigration et l'action policière.
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
    definition = get_definition(subject)
    return f"""Tu es un expert en détection de désinformation médiatique. \
Ta tâche est d'analyser des extraits de programmes télévisés ou radiophoniques \
et de déterminer s'ils contiennent de la désinformation selon la définition suivante :

{definition}

IMPORTANT : certains extraits peuvent être des publicités ou des annonces commerciales. \
Dans ce cas, indique-le et considère qu'il n'y a pas de désinformation.

- "oui" : l'extrait contient de la désinformation telle que définie
- "non" : l'extrait ne contient pas de désinformation (y compris les publicités)
- "incertain" : l'extrait est ambigu ou insuffisant pour conclure
"""
