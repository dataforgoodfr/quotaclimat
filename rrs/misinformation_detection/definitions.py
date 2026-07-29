"""Subject-specific misinformation definitions used to build LLM system prompts."""

SUBJECT_DEFINITIONS: dict[str, str] = {
    "insecurity": """
La désinformation est définie ici comme tout extrait qui porte sur les personnes étrangères, immigrées, réfugiées, demandeuses d'asile, \ 
sans-papiers, exilées ou d'origine étrangère, qu'elles soient désignées explicitement — par leur statut administratif, \
leur nationalité, leur religion, leur origine réelle ou supposée — ou par substitution, au moyen de formules telles que \
« certaines populations », « quartiers sensibles », « personnes issues de », « nouveaux arrivants », « communautés », \
de prénoms, ou de toute autre allusion tenue pour révélatrice d'une origine. Relève de ce périmètre tout extrait qui avance, \
commente, compare ou conteste des données chiffrées sur les migrations et sur la présence d'étrangers, en France comme dans \
n'importe quel autre pays, que ces chiffres soient exacts, approximatifs, anciens, non sourcés, déformés, sortis de leur périmètre \
ou entièrement inventés ; tout extrait qui traite de leurs droits et des dispositifs qui les concernent, qu'il les décrive, \
en réclame l'extension, en demande la restriction ou la suppression, les présente comme excessifs, indus ou détournés, \
ou qu'il en dénonce au contraire la violation ; tout extrait qui traite de leur culture, de leur religion, de leur langue, \
de leurs mœurs, de leur famille, de leur mode de vie, de leur intégration ou de leur prétendu refus de s'intégrer ; \
tout extrait qui les associe, directement ou par allusion, à des faits de violence, de délinquance ou de désordre, \
y compris lorsque l'association ne passe que par la mention du statut, de la nationalité, de l'origine ou de la religion \
dans le récit d'un fait divers, y compris lorsque le fait rapporté est avéré et judiciairement établi, et y compris lorsque \
cette mention est présentée comme une simple information de contexte ; et tout extrait qui, par la généralisation d'un cas au groupe, \
l'essentialisation par l'origine ou la religion, l'opposition entre « eux » et « nous », la hiérarchisation entre nationaux et étrangers, \
l'imputation d'un projet collectif de conquête ou de remplacement, les métaphores de la masse, du flux, de l'invasion, de la maladie ou de l'animalité, \
le récit sériel de faits divers, l'appel à l'exclusion, à des mesures d'exception ou à la violence, ou l'emploi à contresens de formules positives, \
alimente la haine, le stéréotype ou la déshumanisation à leur égard. Le fait qu'un extrait soit exact, sourcé, mesuré, \
courtois, ou qu'il émane d'une autorité publique, d'un chercheur, d'un magistrat ou d'un journaliste, ne le fait pas \
sortir du périmètre : celui-ci est thématique et volontairement large, et seule la qualification en aval distingue la \
couverture légitime de l'instrumentalisation. N'est pas retenu l'extrait qui mentionne une personne étrangère ou d'origine \
étrangère sans que cette qualité n'y soit thématisée ni ne joue aucun rôle dans le propos ; l'extrait où les mots du filtre \
relèvent manifestement d'un autre champ, notamment le sport, la migration animale, la migration de données, le commerce, \
la météorologie ou la publicité ; la fiction et la satire clairement identifiées ; le fact-checking et le \
contre-discours qui citent une narrative pour la réfuter, enregistrés séparément ; et la couverture d'un événement \
étranger qui ne comporte ni donnée, ni jugement, ni propos sur les personnes migrantes ou étrangères
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


