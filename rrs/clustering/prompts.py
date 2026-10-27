_SYSTEM_PROMPTS: dict[str, str] = {
    "climate": "You are an assistant helping editors to aggregate claims on climate change discussions.",
    "insecurity": (
        "You are an assistant helping editors to aggregate claims about immigration and its "
        "presentation as a security issue in French media (immigration policy, migration flows, "
        "asylum, deportation/OQTF, foreign nationals and crime/prison statistics, and related "
        "public-safety narratives that explicitly involve immigration or foreign nationals). "
        "Claims about crime, justice, or public safety that do NOT involve immigration or foreign "
        "nationals are out of scope."
    ),
    "environmental_health": (
        "You are an assistant helping editors to aggregate claims about environmental health in "
        "French media, defined as follows:\n"
        "La santé environnementale est ici définie comme l'ensemble des atteintes à la santé "
        "humaine, animale et des écosystèmes liées à l'exposition à des substances chimiques "
        "polluantes, en particulier les polluants persistants dans l'environnement (PFAS, "
        "pesticides, métaux lourds tels que le cadmium ou le plomb, biocides), ainsi que les "
        "controverses, politiques et pratiques de prévention qui leur sont associées.\n"
        "Sont notamment incluses les voies d'exposition par l'alimentation (résidus dans les "
        "aliments), l'eau, l'air, les poussières et les sols, qu'elles résultent d'une proximité "
        "directe (riverains de zones traitées, expositions professionnelles) ou d'une "
        "contamination diffuse et persistante de l'environnement (sols agricoles, nappes "
        "phréatiques, chaînes alimentaires). Les effets sur la vie du sol, la faune aquatique et "
        "les insectes pollinisateurs sont également couverts, dans la mesure où ils affectent "
        "indirectement la santé humaine (fertilité des sols, qualité de l'eau, pollinisation).\n"
        "Sont couverts aussi bien les effets sanitaires établis (cancers, troubles de la "
        "reproduction, atteintes neurologiques ou immunitaires) que les incertitudes "
        "scientifiques qui subsistent, notamment sur les expositions cumulées à plusieurs "
        "substances (« effet cocktail ») et les difficultés méthodologiques à démontrer un lien "
        "de causalité. Cette thématique est fréquemment articulée à celles portant sur "
        "l'agriculture, l'industrie, l'alimentation et la régulation sanitaire."
    ),
    "purchase_power": (
        "You are an assistant helping editors to aggregate claims about purchasing power and "
        "household finances in French media, defined as follows:\n"
        "Le thème couvre les débats publics portant sur l'argent des ménages français et sur les "
        "finances publiques considérées sous l'angle de ce qu'elles coûtent ou rapportent aux "
        "Français.\n"
        "Il comprend quatre registres : ce que les ménages reçoivent (salaires, Smic, primes, "
        "pensions de retraite, allocations chômage, prestations sociales et minima sociaux) ; ce "
        "que les ménages dépensent (prix et inflation, alimentation, énergie et carburants, "
        "logement et loyers, santé et mutuelles, transports, assurances, crédits et frais "
        "bancaires) ; ce qui est prélevé (impôts, taxes, TVA, cotisations sociales, fiscalité du "
        "patrimoine et des successions, fiscalité locale) ; ce qui est fait de cet argent "
        "(dépense publique, dette et déficit, fonction publique et bureaucratie, fraude sociale et "
        "fiscale, compétitivité et commerce extérieur, et les modèles économiques invoqués dans "
        "le débat).\n"
        "Le thème inclut aussi bien les affirmations chiffrées que les récits et accusations qui "
        "les accompagnent (« on nous vole », « le travail ne paie plus », « l'État gaspille »), "
        "ainsi que la contestation des chiffres officiels et des institutions qui les produisent. "
        "Il inclut les contre-récits au même titre que les récits dominants.\n"
        "Ne relèvent pas du thème : les emplois figurés du vocabulaire économique (inflation des "
        "normes, gaspillage alimentaire, capitalisation boursière) ; l'économie d'autres pays sans "
        "lien établi avec la France ; la vie financière des entreprises sans effet énoncé sur les "
        "ménages ; les termes du dictionnaire employés dans un tout autre sens."
    ),
}

_DOMAIN_LABELS: dict[str, str] = {
    "climate": "climate change",
    "insecurity": "immigration et insécurité liée à l'immigration",
    "environmental_health": "santé environnementale",
    "purchase_power": "pouvoir d'achat et finances publiques",
}

_DOMAIN_EXAMPLES: dict[str, str] = {
    "climate": (
        "'Le nucléaire est essentiel pour la décarbonation', "
        "'Le nucléaire est la seule énergie propre', 'Le nucléaire est préférable aux renouvelables' "
        "→ should all merge into a single label like "
        "'Le nucléaire est une énergie décarbonée supérieure aux renouvelables'."
    ),
    "insecurity": (
        "'L\\'immigration est liée à la hausse de la criminalité' and 'Les étrangers sont responsables "
        "de l\\'insécurité' are paraphrases of the same causal claim → merge into "
        "'L\\'immigration est présentée comme une cause de l\\'insécurité'.\n"
        "  But keep ALL of the following separate, even though they are all about immigration and "
        "security, because each expresses a different claim:\n"
        "  - 'L\\'immigration est présentée comme une cause de l\\'insécurité' (causal claim)\n"
        "  - 'Le nombre de personnes détenues de nationalité étrangère est disproportionné' "
        "(statistical-composition claim)\n"
        "  - 'Les OQTF ne sont pas exécutées, ce qui prouve l\\'incapacité de la France à expulser les "
        "étrangers en situation irrégulière' (policy-failure claim)\n"
        "  - 'Les personnes régularisées en Espagne peuvent ensuite s\\'installer librement en France' "
        "(geographic/routing claim)\n"
        "  - 'La France connaît un niveau d\'immigration record, submergeant le pays' "
        "(quantitative/volume claim, distinct from the causal-crime claim above)."
    ),
    "environmental_health": (
        "'Les pesticides sont sans danger aux doses autorisées', 'Il n\'existe aucune preuve que les "
        "produits phytosanitaires nuisent à la santé' and 'Les seuils réglementaires garantissent une "
        "exposition totalement sûre aux pesticides' are paraphrases of the same claim minimising a "
        "proven risk → should all merge into a single label like "
        "'Les risques des pesticides pour la santé sont minimisés ou niés malgré les preuves "
        "scientifiques'.\n"
        "  Similarly, 'Le problème des PFAS est réglé depuis l\'interdiction du PFOA et du PFOS', "
        "'On n\'a pas montré d\'effet pour la plupart des PFAS, donc ils sont sûrs' and 'Les PFAS, "
        "c\'est juste une histoire de poêles antiadhésives' are paraphrases of the same claim "
        "minimising a proven risk → should all merge into a single label like 'Les risques "
        "sanitaires des PFAS sont minimisés malgré une exposition large et des effets avérés pour "
        "plusieurs molécules'."
    ),
    "purchase_power": (
        "'Les prélèvements obligatoires sont du vol', 'On nous prend la moitié de notre salaire' and "
        "'Les Français travaillent la moitié de l\'année pour payer l\'État' are paraphrases of the "
        "same framing of taxation as confiscation → should all merge into a single label like "
        "'Les prélèvements sur les salaires sont présentés comme un vol de la moitié du revenu'.\n"
        "  Similarly, 'L\'Insee truque les chiffres de l\'inflation', 'La vraie inflation est bien "
        "supérieure à celle annoncée' and 'Les chiffres officiels des prix ne reflètent pas la "
        "réalité des courses' are paraphrases of the same claim contesting official statistics → "
        "should all merge into a single label like 'Les chiffres officiels de l\'inflation sont "
        "présentés comme faussés ou sous-évalués'."
    ),
}


def get_system_prompt(subject: str) -> str:
    return _SYSTEM_PROMPTS.get(
        subject,
        f"You are an assistant helping editors to aggregate claims related to {subject} discussions.",
    )


_RELEVANCE_CRITERIA: dict[str, str] = {
    "insecurity": (
        "l'immigration, les personnes étrangères, migrantes, réfugiées ou d'origine étrangère "
        "(ou une politique migratoire) constituent l'élément moteur du propos — et pas "
        "seulement une mention incidente (statut, origine, religion mentionnés en passant dans "
        "un fait divers, une discussion sur la délinquance, l'islam, l'antisémitisme ou la "
        "démographie sans lien migratoire explicite, etc.)"
    ),
    "climate": (
        "le changement climatique en constitue l'élément moteur — science climatique, "
        "politiques/accords climatiques, ou solutions d'atténuation et d'adaptation (énergies "
        "renouvelables, nucléaire, efficacité énergétique, etc.) — et pas seulement une mention "
        "incidente (météo, environnement ou énergie sans lien avec le climat, etc.)"
    ),
    "environmental_health": (
        "une atteinte à la santé humaine, animale ou des écosystèmes liée à l'exposition à des "
        "substances chimiques polluantes persistantes (pesticides, PFAS, métaux lourds, biocides) "
        "— contamination de l'eau, de l'air, des sols ou des chaînes alimentaires — en constitue "
        "l'élément moteur, ou les politiques, controverses et pratiques de prévention associées — "
        "et pas seulement une mention incidente de l'environnement, de la pollution ou de la "
        "santé sans lien explicite avec une exposition à une substance chimique polluante"
    ),
    "purchase_power": (
        "l'argent des ménages français (revenus, prix, prélèvements) ou l'usage des finances "
        "publiques sous l'angle de ce qu'elles coûtent ou rapportent aux Français en constitue "
        "l'élément moteur, y compris la contestation des chiffres officiels — et pas seulement "
        "un emploi figuré du vocabulaire économique, l'économie d'un autre pays sans lien avec la "
        "France, ou la vie financière d'une entreprise sans effet énoncé sur les ménages"
    ),
}


def _relevance_prompt(text: str, subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    criterion = _RELEVANCE_CRITERIA.get(
        subject,
        f"« {domain} » en constitue l'élément moteur, et non une simple mention incidente",
    )
    return (
        f"Le texte suivant a été pré-sélectionné comme concernant potentiellement le thème "
        f"« {domain} ». Réponds uniquement par 'oui' si {criterion}.\n"
        "Réponds 'non' sinon.\n"
        "Réponds par un seul mot : oui ou non.\n\n"
        f"Texte : {text}"
    )


_STEP1_LABEL_CAP: dict[str, str] = {
    "climate": "1 or 2",
    "insecurity": "1 to 3",
}
_DEFAULT_STEP1_LABEL_CAP = "1 to 3"

_STEP1_EXTRA_RULES: dict[str, str] = {
    "climate": "",
    "environmental_health": "",
    "purchase_power": "",
    "insecurity": (
        "- Do NOT return labels that merely comment on how the topic is discussed or "
        "politicized (media-coverage criticism, political positioning/strategy, electoral "
        "exploitation, or general public/political disagreement) — only label an actual claim "
        "being made about the subject itself, not a claim about the debate around it.\n"
        "- Do NOT return vague accusatory framings with no concrete specifics — a label must name "
        "a specific actor, mechanism, statistic, policy, or event; reject labels like 'X ignores Y' "
        "or 'X is overwhelmed by Y' when neither X nor Y nor the mechanism linking them is named.\n"
    ),
}
_DEFAULT_STEP1_EXTRA_RULES = _STEP1_EXTRA_RULES["insecurity"]


def _step1_prompt(sentences: list[str], subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    label_cap = _STEP1_LABEL_CAP.get(subject, _DEFAULT_STEP1_LABEL_CAP)
    extra_rules = _STEP1_EXTRA_RULES.get(subject, _DEFAULT_STEP1_EXTRA_RULES)
    return (
        "Given these sentences from a news transcript, identify "
        "narrative(s)/concepts they express. Generate a concise, meaningful label for each distinct "
        f"narrative present. Limit yourself to {label_cap} labels.\n"
        "Rules:\n"
        '- Return ONLY a JSON list of label strings with double quotes, e.g. ["label 1", "label 2"]. No code fences.\n'
        "- Labels must describe specific claims, not generic categories.\n"
        "- Each label must state the misinformation narrative itself — the false, exaggerated, "
        "minimised, or misleadingly framed claim actually being made — not a neutral factual "
        "statement and not a generic mention of the topic. If the underlying claim is accurate and "
        "not misleading, it does not belong in a label (see the exclusion rule below).\n"
        "- Do NOT return meaningless names such as 'new_label_1' or 'unknown_topic'.\n"
        + extra_rules
        + f"- If no {domain} misinformation is present, return only []. Nothing else.\n"
        "- The labels must be in french.\n"
        f"Sentences: {sentences}"
    )


_STEP2_MERGE_INSTRUCTIONS: dict[str, str] = {
    "climate": (
        "Group labels that share the same core subject and overall message, even if the wording differs.\n"
        "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
    ),
    "environmental_health": (
        "Group labels that share the same core subject and overall message, even if the wording differs.\n"
        "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
    ),
    "purchase_power": (
        "Group labels that share the same core subject and overall message, even if the wording differs.\n"
        "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
    ),
    "insecurity": (
        "Merge two labels ONLY if they express the SAME underlying claim in different words — same "
        "subject, same causal/evaluative mechanism, and same conclusion. Do NOT merge labels that are "
        "merely topically adjacent.\n"
        "In particular, treat the following as distinct dimensions of a claim, and do NOT collapse "
        "labels that differ along any of them, even if they discuss the same broad topic:\n"
        "- Causal claims (X causes/increases Y) vs. quantitative/volume claims (there is more of X, "
        "record levels of X) vs. policy-failure claims (a specific policy or procedure, e.g. "
        "deportation orders (OQTF), asylum processing, is ineffective or too lenient) vs. "
        "geographic/routing claims (people move via a specific country or route, e.g. regularised in "
        "Spain then settling in France, or entering via Ceuta then circulating within Schengen) vs. "
        "statistical-composition claims (proportion of a specific group among a specific population, "
        "e.g. share of foreign nationals in prison).\n"
        "- Keep labels naming a specific mechanism, country, procedure, or statistic separate from "
        "more general labels on the same topic, unless they are truly paraphrases of each other.\n"
    ),
}
_DEFAULT_STEP2_MERGE_INSTRUCTIONS = _STEP2_MERGE_INSTRUCTIONS["insecurity"]

_STEP2_MERGE_RULES: dict[str, str] = {
    "climate": (
        "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
        "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
        "- Prefer fewer, broader labels over many narrow ones.\n"
        "- Do NOT keep two labels if they could reasonably be covered by one.\n"
    ),
    "environmental_health": (
        "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
        "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
        "- Prefer fewer, broader labels over many narrow ones.\n"
        "- Do NOT keep two labels if they could reasonably be covered by one.\n"
    ),
    "purchase_power": (
        "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
        "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
        "- Prefer fewer, broader labels over many narrow ones.\n"
        "- Do NOT keep two labels if they could reasonably be covered by one.\n"
    ),
    "insecurity": (
        "- Merge two labels only if a French reader would consider them the same statement, not just "
        "the same subject area.\n"
        "- Write each merged (or kept) label as a short, conversational French sentence starting with "
        "its subject.\n"
        "- When in doubt, keep labels SEPARATE rather than merge them — under-merging is preferable to "
        "over-merging.\n"
    ),
}
_DEFAULT_STEP2_MERGE_RULES = _STEP2_MERGE_RULES["insecurity"]


def _step2_prompt(label_list: list[str], subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    example = _DOMAIN_EXAMPLES.get(subject, "")
    instructions = _STEP2_MERGE_INSTRUCTIONS.get(subject, _DEFAULT_STEP2_MERGE_INSTRUCTIONS)
    rules = _STEP2_MERGE_RULES.get(subject, _DEFAULT_STEP2_MERGE_RULES)
    return (
        f"You are merging a list of French {domain}-discussion labels into a shorter, cleaner list.\n"
        "Each label states a misinformation narrative — the false, exaggerated, minimised, or "
        "misleadingly framed claim itself. Keep that framing intact when merging: a merged label "
        "must still name the disinformation narrative it covers, never a neutral factual statement "
        "or a generic topic mention.\n"
        + instructions
        + (f"Example: {example}\n" if example else "")
        + "Rules:\n"
        + rules
        + f"Here is the list of labels:\n{label_list}.\n"
        "Produce the final merged list as a JSON array in French, using double quotes. No code fences."
    )


def _step3_prompt(sentences: list[str], label_list: list[str]) -> str:
    return (
        "Given the label list and the sentences, select all labels that describe "
        "the concepts expressed in the sentences.\n"
        f"Label list: {label_list}\n"
        f"Sentences: {sentences}\n"
        "Return ONLY a JSON array of matching label strings using double quotes starting with "
        "'[' and anding with ']'. No code fences."
        "If no labels match the sentences return '[]'."
    )


def _zone3_prompt(candidate: str, close_existing: list[str], subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    labels_block = "\n".join(f"  - {lb}" for lb in close_existing)
    return (
        f"You are an expert in French {domain} discourse analysis.\n"
        "You are given a candidate label and a list of the most semantically close existing labels "
        f"from a taxonomy of French {domain} claims.\n\n"
        f'Candidate label:\n  "{candidate}"\n\n'
        f"Closest existing labels:\n{labels_block}\n\n"
        "Decide whether the candidate expresses essentially the SAME claim as at least one of the "
        "existing labels.\n"
        "Reply with a single word, no punctuation:\n"
        "  YES — the candidate is already covered by an existing label\n"
        "  NO  — the candidate expresses a distinct claim\n"
    )
