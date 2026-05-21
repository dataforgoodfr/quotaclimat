SYSTEM_PROMPT = "You are an assistant helping editors to aggregate claims on climate change discussions."


def _step1_prompt(sentences: list[str]) -> str:
    return (
        "Given these sentences from a news transcript, identify "
        "narrative(s)/concepts they express. Generate a concise, meaningful label for each distinct "
        "narrative present. Limit yourself to 1 or 2 labels.\n"
        "Rules:\n"
        '- Return ONLY a JSON list of label strings with double quotes, e.g. ["label 1", "label 2"]. No code fences.\n'
        "- Labels must describe specific claims, not generic categories.\n"
        "- Do NOT return meaningless names such as 'new_label_1' or 'unknown_topic'.\n"
        "- If no climate misinformation is present, return only []. Nothing else.\n"
        "- The labels must be in french.\n"
        f"Sentences: {sentences}"
    )


def _step2_prompt(label_list: list[str]) -> str:
    return (
        "You are merging a list of French climate-discussion labels into a shorter, cleaner list.\n"
        "Group labels that share the same core subject and overall message, even if the wording differs.\n"
        "Be AGGRESSIVE: if several labels all make a similar point about the same topic, collapse them into one.\n"
        "Example: 'Le nucléaire est essentiel pour la décarbonation', "
        "'Le nucléaire est la seule énergie propre', 'Le nucléaire est préférable aux renouvelables' "
        "→ should all merge into a single label like "
        "'Le nucléaire est une énergie décarbonée supérieure aux renouvelables'.\n"
        "Rules:\n"
        "- Merge any labels that share the same subject AND a closely related claim AND are on the same side of a debate.\n"
        "- Write the merged label as a short, conversational French sentence starting with its subject.\n"
        "- Prefer fewer, broader labels over many narrow ones.\n"
        "- Do NOT keep two labels if they could reasonably be covered by one.\n"
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


def _zone3_prompt(candidate: str, close_existing: list[str]) -> str:
    labels_block = "\n".join(f"  - {lb}" for lb in close_existing)
    return (
        "You are an expert in French climate discourse analysis.\n"
        "You are given a candidate label and a list of the most semantically close existing labels "
        "from a taxonomy of French climate claims.\n\n"
        f'Candidate label:\n  "{candidate}"\n\n'
        f"Closest existing labels:\n{labels_block}\n\n"
        "Decide whether the candidate expresses essentially the SAME claim as at least one of the "
        "existing labels.\n"
        "Reply with a single word, no punctuation:\n"
        "  YES — the candidate is already covered by an existing label\n"
        "  NO  — the candidate expresses a distinct claim\n"
    )
