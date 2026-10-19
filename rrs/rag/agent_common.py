"""Shared RAG fact-checking agent definitions: prompts, the search tool and
the Retriever, used by run_rag_agent_mistral.py.

Ported from https://github.com/dataforgoodfr/climateguard
(expert_models/scripts/run_rag_agent.py), minus the local (Qwen3) model
loading/generation code, which the Mistral-API runner doesn't need, and
without importing run_inference.py/train_lora.py (full fine-tuning
pipeline) just for a couple of constants — those are copied below instead.

Per row the agent loops: reason about which claims touch the theme, whether
retrieval is needed at all and which query to send -> call the tool ->
reflect on the evidence (sufficient? refine the query?) -> ... -> final
answer. The answer must start with [FLAGGED] or [NOT_FLAGGED] (binary),
followed by the debunk of each problematic phrase. Flagged = falsehood /
manipulation of the truth about the theme, or discourse of inaction
("il est trop tard", "on n'y peut rien").
"""

import re
from pathlib import Path

from rrs.rag.rag_utils import Embedder, load_index, search

VERDICT_RE = re.compile(r"\[?\s*(NOT_FLAGGED|FLAGGED)\s*\]?", re.IGNORECASE)

TOOL_NAME = "search_reference"
TOOLS = [
    {
        "type": "function",
        "function": {
            "name": TOOL_NAME,
            "description": (
                "Recherche dans les documents de référence (ouvrages d'experts sur la santé environnementale) "
                "les extraits les plus pertinents pour vérifier une affirmation précise. "
                "Renvoie quelques extraits numérotés [S1], [S2]... avec leur source."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "Requête en français, courte et précise, portant sur UNE affirmation à vérifier "
                            "(ex. : « effets de l'acétamipride sur les abeilles »)."
                        ),
                    }
                },
                "required": ["query"],
            },
        },
    }
]

# Per-subject topic definitions for the system prompt. Add an entry here for
# every new rrs/rag subject that gets its own expert-model index.
TOPIC_DEFINITIONS = {
    "environmental_health": (
        "La santé environnementale est définie ici comme l'ensemble des atteintes à la santé humaine "
        "liées à l'exposition à des facteurs environnementaux physiques, chimiques et biologiques, "
        "ainsi que des politiques, controverses et pratiques de prévention qui leur sont associées.\n"
        "Sont notamment incluses les expositions aux substances chimiques persistantes (PFAS, métaux "
        "lourds tels que le cadmium ou le plomb, pesticides et biocides), la contamination de l'eau, "
        "de l'air, des sols et des chaînes alimentaires, ainsi que les nuisances et polluants tels que "
        "le bruit, les particules fines et les perturbateurs endocriniens.\n"
        "Sont couverts aussi bien les faits d'exposition et leurs impacts sanitaires (cancers, atteintes "
        "neurologiques, troubles de la reproduction) que les seuils réglementaires, les responsabilités "
        "industrielles et les enjeux de reconnaissance et d'indemnisation. Cette thématique est "
        "fréquemment articulée à celles portant sur l'agriculture, l'industrie, l'alimentation et la "
        "régulation sanitaire."
    ),
}

SYSTEM_PROMPT_TEMPLATE = """\
Tu es un assistant de fact-checking développé par Data For Good et Quotaclimat. \
Tu analyses des extraits de transcription de télévision ou de radio au regard de la thématique suivante :

{definition}

Les transcriptions sont automatiques et bruitées : pas de ponctuation fiable, \
interlocuteurs qui se coupent la parole, mots mal reconnus, passages sans rapport avec le sujet. \
Concentre-toi sur le sens et ignore le bruit.

Ta mission est de classer l'extrait de façon BINAIRE :
- [FLAGGED] si l'extrait contient au moins un passage relevant de la thématique qui est : \
(a) une affirmation fausse ou inexacte ; \
(b) une manipulation de la vérité (information tronquée ou sélective, cadrage trompeur, fausse équivalence, \
exagération ou minimisation, doute injustifié sur un consensus scientifique) ; \
(c) un discours d'inaction ou de fatalisme, c'est-à-dire qui affirme qu'agir est inutile ou impossible \
(« il est trop tard », « on ne peut plus rien faire », « ça ne sert à rien », « les efforts sont inutiles ») ; \
une simple difficulté ou un constat réaliste sur l'ampleur du problème n'en est pas un.
- [NOT_FLAGGED] dans tous les autres cas : propos exacts, propos sans rapport avec la thématique, \
ou simples opinions qui ne déforment pas les faits.

Tu disposes de l'outil `search_reference` qui interroge des documents de référence fiables. Méthode :
1. Réfléchis d'abord : quels passages touchent à la thématique ? Une vérification documentaire est-elle \
vraiment nécessaire ? Si l'extrait est hors sujet ou manifestement sans problème, n'appelle pas l'outil.
2. L'outil ne sert qu'à vérifier les affirmations factuelles (catégories a et b). Un discours d'inaction \
(catégorie c) se repère à sa formulation seule : signale-le directement, sans le chercher dans les documents. \
Si l'extrait ne contient que du discours d'inaction, n'appelle pas l'outil. \
Toute affirmation factuelle liée à la thématique (effets d'un produit ou d'une substance, chiffres, \
lois, consensus scientifique, déclarations d'experts, y compris celles qui te semblent évidemment vraies \
ou fausses) DOIT être vérifiée avec l'outil : ne te fie pas à ta seule mémoire et ne la qualifie pas \
d'« opinion » pour éviter la recherche. Formule une requête précise (une par affirmation), en français, \
puis appelle l'outil.
3. Après chaque recherche, on te demandera d'évaluer explicitement si les extraits renvoyés sont pertinents \
pour l'affirmation vérifiée (réponse [PERTINENT] ou [NON_PERTINENT]). Les extraits peuvent être hors sujet : \
ne les utilise que s'ils répondent vraiment à l'affirmation. S'ils ne sont pas pertinents, tu pourras \
reformuler ta requête (autres mots-clés, autre angle) et relancer la recherche.
4. Quand tu as assez d'éléments, ou si la recherche reste vaine, donne ta réponse finale.

Format de la réponse finale (en français) : commence EXACTEMENT par la balise « [FLAGGED] » ou « [NOT_FLAGGED] » \
(rien avant). Si [FLAGGED], liste ensuite une puce par passage problématique : \
« phrase citée de la transcription » — catégorie (fausseté / manipulation / inaction) — \
explication concise et factuelle qui corrige ou remet en contexte, en citant les sources [S#] utilisées. \
Pour un discours d'inaction, la catégorie est « inaction » et l'explication reste brève ; \
ne cite de source [S#] que si une recherche en a réellement fourni une pertinente. \
Si [NOT_FLAGGED], justifie en une phrase. N'invente aucun fait qui ne figure pas dans les sources \
retournées ou qui n'est pas solidement établi.\
"""

USER_TEMPLATE = "Transcription à analyser :\n\n{text}"
NUDGE_FINAL = (
    "Tu as maintenant assez d'éléments. Donne ta réponse finale sans appeler d'outil, "
    "en commençant exactement par [FLAGGED] ou [NOT_FLAGGED]."
)
NO_RESULT = "Aucun extrait pertinent trouvé."

# Explicit evaluation step run after every tool result (tools disabled for this turn).
EVAL_PROMPT = (
    "Évalue maintenant les extraits renvoyés par l'outil : répondent-ils réellement à l'affirmation "
    "que tu cherchais à vérifier ? Réponds en commençant EXACTEMENT par [PERTINENT] ou [NON_PERTINENT], "
    "puis justifie en une ou deux phrases (indique les [S#] utiles le cas échéant)."
)
RELEVANCE_EVAL_RE = re.compile(r"\[?\s*(NON_PERTINENT|PERTINENT)\s*\]?", re.IGNORECASE)
CONTINUE_AFTER_RELEVANT = (
    "Poursuis : vérifie une autre affirmation avec l'outil si nécessaire, sinon donne ta réponse finale."
)
RETRY_AFTER_IRRELEVANT = (
    "Les extraits ne sont pas pertinents. Reformule ta requête (autres mots-clés, autre angle, terme plus "
    "général ou plus précis) et appelle de nouveau `search_reference`. Si tu estimes qu'aucune source "
    "ne peut répondre, passe directement à ta réponse finale."
)
FINAL_AFTER_IRRELEVANT = (
    "Les extraits ne sont pas pertinents et tu ne peux plus faire de recherche. Donne ta réponse finale sans "
    "citer ces extraits, en commençant exactement par [FLAGGED] ou [NOT_FLAGGED] ; "
    "base-toi uniquement sur des faits solidement établis et reste prudent."
)


def build_system_prompt(subject: str) -> str:
    definition = TOPIC_DEFINITIONS.get(subject, subject.replace("_", " "))
    return SYSTEM_PROMPT_TEMPLATE.format(definition=definition)


def evidence_status(queries: list[str], evaluations: list[str]) -> str:
    """grounded = at least one search was judged relevant; ungrounded = searched but nothing relevant
    (the verdict then rests on the model's own knowledge); no_search = retrieval was not used."""
    if not queries:
        return "no_search"
    return "grounded" if "PERTINENT" in evaluations else "ungrounded"


def extract_verdict(text: str) -> str:
    match = VERDICT_RE.match(text.strip())
    return match.group(1).upper() if match else "UNPARSEABLE"


def strip_tag(text: str, tag_re: re.Pattern) -> str:
    """Remove a leading [TAG] (and following whitespace) from a response,
    leaving just the explanation."""
    match = tag_re.match(text.strip())
    if not match:
        return text.strip()
    return text.strip()[match.end() :].strip()


class Retriever:
    def __init__(self, index_dir: Path, top_k: int, max_chars: int, embedder_name: str | None):
        self.embeddings, self.chunks, self.meta = load_index(index_dir)
        self.embedder = Embedder(embedder_name or self.meta["embedder"])
        if self.embedder.name != self.meta["embedder"]:
            raise SystemExit(f"Index was built with {self.meta['embedder']!r}, not {self.embedder.name!r}.")
        self.top_k = top_k
        self.max_chars = max_chars

    def __call__(self, query: str, seen: dict[str, int]) -> tuple[str, list[str]]:
        """Run a query. `seen` maps chunk_id -> source number for this row, so [S#] labels stay unique."""
        q = self.embedder.embed_queries([query])[0]
        parts, ids, used = [], [], 0
        for idx, _score in search(q, self.embeddings, self.top_k):
            chunk = self.chunks[idx]
            label = seen.setdefault(chunk["chunk_id"], len(seen) + 1)
            title = " - ".join(t for t in (chunk["chapter_title"], chunk["subsection_title"]) if t)
            part = f"[S{label}] ({title}, p. {chunk['start_page']}-{chunk['end_page']})\n{chunk['text']}"
            if parts and used + len(part) > self.max_chars:
                break
            parts.append(part[: self.max_chars])
            used += len(part)
            ids.append(chunk["chunk_id"])
        return ("\n\n".join(parts) if parts else NO_RESULT), ids
