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
}

_DOMAIN_LABELS: dict[str, str] = {
    "climate": "climate change",
    "insecurity": "immigration et insécurité liée à l'immigration",
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
        "  - 'La France connaît un niveau d\\'immigration record, submergeant le pays' "
        "(quantitative/volume claim, distinct from the causal-crime claim above)."
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


def _step1_prompt(sentences: list[str], subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    if subject == "climate":
        # Kept byte-for-byte identical to the pre-existing behavior: climate's clustering
        # quality was not part of the "insecurity" precision fix and must not change.
        return (
            "Given these sentences from a news transcript, identify "
            "narrative(s)/concepts they express. Generate a concise, meaningful label for each distinct "
            "narrative present. Limit yourself to 1 or 2 labels.\n"
            "Rules:\n"
            '- Return ONLY a JSON list of label strings with double quotes, e.g. ["label 1", "label 2"]. No code fences.\n'
            "- Labels must describe specific claims, not generic categories.\n"
            "- Do NOT return meaningless names such as 'new_label_1' or 'unknown_topic'.\n"
            f"- If no {domain} misinformation is present, return only []. Nothing else.\n"
            "- The labels must be in french.\n"
            f"Sentences: {sentences}"
        )
    return (
        "Given these sentences from a news transcript, identify "
        "narrative(s)/concepts they express. Generate a concise, meaningful label for each distinct "
        "narrative present. Limit yourself to 1 to 3 labels.\n"
        "Rules:\n"
        '- Return ONLY a JSON list of label strings with double quotes, e.g. ["label 1", "label 2"]. No code fences.\n'
        "- Labels must describe specific claims, not generic categories.\n"
        "- Do NOT return meaningless names such as 'new_label_1' or 'unknown_topic'.\n"
        "- Do NOT return labels that merely comment on how the topic is discussed or "
        "politicized (media-coverage criticism, political positioning/strategy, electoral "
        "exploitation, or general public/political disagreement) — only label an actual claim "
        "being made about the subject itself, not a claim about the debate around it.\n"
        "- Do NOT return vague accusatory framings with no concrete specifics — a label must name "
        "a specific actor, mechanism, statistic, policy, or event; reject labels like 'X ignores Y' "
        "or 'X is overwhelmed by Y' when neither X nor Y nor the mechanism linking them is named.\n"
        f"- If no {domain} misinformation is present, return only []. Nothing else.\n"
        "- The labels must be in french.\n"
        f"Sentences: {sentences}"
    )


def _step2_prompt(label_list: list[str], subject: str = "climate") -> str:
    domain = _DOMAIN_LABELS.get(subject, subject)
    example = _DOMAIN_EXAMPLES.get(subject, "")
    if subject == "climate":
        # Kept byte-for-byte identical to the pre-existing behavior: climate's clustering
        # quality was not part of the "insecurity" precision fix and must not change.
        return (
            f"You are merging a list of French {domain}-discussion labels into a shorter, cleaner list.\n"
            "Group labels that share the same core subject and overall message, even if the wording differs.\n"
            "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
            + (f"Example: {example}\n" if example else "")
            + "Rules:\n"
            "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
            "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
            "- Prefer fewer, broader labels over many narrow ones.\n"
            "- Do NOT keep two labels if they could reasonably be covered by one.\n"
            f"Here is the list of labels:\n{label_list}.\n"
            "Produce the final merged list as a JSON array in French, using double quotes. No code fences."
        )
    return (
        f"You are merging a list of French {domain}-discussion labels into a shorter, cleaner list.\n"
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
        + (f"Example: {example}\n" if example else "")
        + "Rules:\n"
        "- Merge two labels only if a French reader would consider them the same statement, not just "
        "the same subject area.\n"
        "- Write each merged (or kept) label as a short, conversational French sentence starting with "
        "its subject.\n"
        "- When in doubt, keep labels SEPARATE rather than merge them — under-merging is preferable to "
        "over-merging.\n"
        f"Here is the list of labels:\n{label_list}.\n"
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
