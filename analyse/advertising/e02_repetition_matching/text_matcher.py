"""
Text Matcher — Regroupement de segments par similarité de transcription
=======================================================================
Détecte les répétitions d'un même contenu (spots pub, jingles, météo…)
dans une semaine de transcriptions ASR.

Stratégie :
  1. Normalisation légère du texte (casse, ponctuation, espaces)
  2. Vectorisation TF-IDF sur des n-grammes de *caractères*
     → robuste aux erreurs ASR (substitution, insertion, suppression de mots)
     car deux mots proches partagent beaucoup de trigrammes/pentagrammes.
  3. Similarité cosinus par batch (matrices sparse) → économique en RAM
  4. Seuillage → graphe de paires similaires
  5. Composantes connexes → groupes de segments au contenu identique/répété

Dépendances :
    pip install scikit-learn scipy numpy
"""

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

import numpy as np
from scipy.sparse import csr_matrix
from scipy.sparse.csgraph import connected_components
from sklearn.feature_extraction.text import TfidfVectorizer

# ─────────────────────────────────────────────
#  Structures de données
# ─────────────────────────────────────────────


@dataclass
class TextSegment:
    """Un segment audio avec son texte transcrit."""

    text: str
    start_sec: float = 0.0
    end_sec: float = 0.0


@dataclass
class SegmentGroup:
    """Un groupe de segments au contenu répété (ou singleton)."""

    group_id: int
    segments: List[TextSegment]
    canonical_text: str  # texte du segment le plus long du groupe

    @property
    def occurrence_count(self) -> int:
        return len(self.segments)

    @property
    def is_repeated(self) -> bool:
        return self.occurrence_count > 1

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "canonical_text": self.canonical_text,
            "segments": [
                {
                    "start_sec": s.start_sec,
                    "end_sec": s.end_sec,
                    "text": s.text,
                }
                for s in self.segments
            ],
        }


# ─────────────────────────────────────────────
#  Matcher principal
# ─────────────────────────────────────────────


class TextMatcher:
    """
    Regroupe des segments textuels par similarité, tolérant aux erreurs ASR.

    Exemple d'usage
    ---------------
    segments = [
        TextSegment(id=0, text="Bienvenue sur TF1...", start_sec=0, end_sec=30),
        TextSegment(id=1, text="Bienvenue sur TF1...", start_sec=0, end_sec=30),
        ...
    ]
    matcher = TextMatcher(similarity_threshold=0.85)
    groups = matcher.fit_transform(segments)
    matcher.print_summary(groups)
    matcher.export_json(groups, "repetitions.json")
    """

    def __init__(
        self,
        similarity_threshold: float = 0.85,
        # ↑ Score cosinus minimum pour considérer deux segments comme le même contenu.
        # 0.85 → bonne tolérance aux petites erreurs ASR (quelques mots changés).
        # Monter à 0.92+ pour être plus strict (moins de faux positifs).
        # Descendre à 0.70–0.75 si les transcriptions sont très bruitées.
        min_chars: int = 50,
        # ↑ Longueur minimale du texte (caractères) pour être indexé.
        # Les segments < 50 chars donnent des similarités peu fiables.
        # Mettre à 20 pour indexer les très courts jingles/slogans.
        ngram_range: tuple = (3, 5),
        # ↑ Plage de n-grammes de *caractères* pour la vectorisation.
        # (3, 5) = trigrammes à pentagrammes.
        # Plus robuste que les n-grammes de mots car insensible aux mots coupés/altérés.
        # Réduire à (2, 4) pour les textes très courts (< 100 chars).
        # Augmenter à (4, 6) si trop de faux positifs entre textes différents.
        max_features: int = 50_000,
        # ↑ Taille max du vocabulaire TF-IDF.
        # 50 000 est suffisant pour une semaine de transcriptions.
        batch_size: int = 500,
        # ↑ Segments traités par batch lors du calcul de similarité.
        # Réduit la consommation RAM : chaque batch produit une matrice
        # (batch_size × n_segments) en dense, le reste reste sparse.
        # Réduire à 100–200 si la RAM est limitée.
    ):
        self.similarity_threshold = similarity_threshold
        self.min_chars = min_chars
        self.ngram_range = ngram_range
        self.max_features = max_features
        self.batch_size = batch_size

        self._vectorizer = TfidfVectorizer(
            analyzer="char_wb",
            # char_wb = n-grammes de caractères avec padding aux bords de mots
            # (mieux que "char" brut pour éviter les artefacts d'espace)
            ngram_range=ngram_range,
            max_features=max_features,
            sublinear_tf=True,
            # log(1 + tf) : compense les textes longs qui auraient sinon des TF élevés
        )

    # ── Normalisation ────────────────────────────────────────────────────────

    def _normalize(self, text: str) -> str:
        """
        Normalisation légère avant vectorisation.
        On garde les mots, on écrase la casse et la ponctuation.
        On ne stemme PAS : les n-grammes de caractères s'en chargent naturellement.
        """
        text = text.lower()
        text = re.sub(r"[^\w\s]", " ", text)  # ponctuation → espace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    # ── Pipeline principal ───────────────────────────────────────────────────

    def fit_transform(self, segments: List[TextSegment]) -> List[SegmentGroup]:
        """
        Calcule les groupes de segments similaires.

        Paramètres
        ----------
        segments : List[TextSegment]
            Tous les segments de la semaine, avec leur texte transcrit.

        Retourne
        --------
        List[SegmentGroup]
            Groupes triés par nombre d'occurrences décroissant.
            Inclut les singletons (groupes de 1) pour conserver tous les segments.
            Filtrez avec `[g for g in groups if g.is_repeated]` pour les répétitions.
        """
        # ── 1. Filtrage des segments trop courts ─────────────────────────────
        valid_segs = [s for s in segments if len(s.text.strip()) >= self.min_chars]
        skipped = len(segments) - len(valid_segs)

        if not valid_segs:
            print("Aucun segment valide (textes trop courts ou vides).")
            return []

        texts = [self._normalize(s.text) for s in valid_segs]
        n = len(texts)

        if skipped:
            print(f"      {skipped} segments ignorés (< {self.min_chars} chars)")

        # ── 2. Vectorisation TF-IDF ──────────────────────────────────────────
        print(
            f"[1/3] Vectorisation TF-IDF : {n} segments "
            f"(n-grammes de caractères {self.ngram_range})..."
        )
        X = self._vectorizer.fit_transform(texts)
        print(f"      Matrice sparse : {X.shape[0]} segments × {X.shape[1]} features")

        # ── 3. Similarité cosinus par batch ──────────────────────────────────
        print(
            f"[2/3] Calcul des paires similaires "
            f"(seuil={self.similarity_threshold}, batch={self.batch_size})..."
        )
        rows, cols = [], []

        for start in range(0, n, self.batch_size):
            end = min(start + self.batch_size, n)
            batch = X[start:end]

            # (batch_size × n_features) @ (n_features × n) → (batch_size × n)
            # On matérialise en dense uniquement le batch courant
            sim = (batch @ X.T).toarray()

            for bi in range(end - start):
                i = start + bi
                # Triangle supérieur seulement (évite les doublons)
                above_threshold = np.where(sim[bi] >= self.similarity_threshold)[0]
                js = above_threshold[above_threshold > i]
                if len(js):
                    rows.extend([i] * len(js))
                    cols.extend(js.tolist())

        n_pairs = len(rows)
        print(f"      {n_pairs} paires similaires trouvées")

        # ── 4. Composantes connexes → groupes ────────────────────────────────
        print("[3/3] Regroupement par composantes connexes...")

        if n_pairs > 0:
            # Graphe non orienté : on symétrise les paires
            all_rows = np.array(rows + cols, dtype=np.int32)
            all_cols = np.array(cols + rows, dtype=np.int32)
            data = np.ones(len(all_rows), dtype=np.float32)
            adj = csr_matrix((data, (all_rows, all_cols)), shape=(n, n))
        else:
            adj = csr_matrix((n, n), dtype=np.float32)

        n_components, labels = connected_components(adj, directed=False)

        # ── 5. Construction des SegmentGroup ─────────────────────────────────
        group_map: Dict[int, List[TextSegment]] = defaultdict(list)
        for local_idx, group_label in enumerate(labels):
            group_map[group_label].append(valid_segs[local_idx])

        groups = []
        for group_id, segs in group_map.items():
            # Texte canonique = segment avec le plus de caractères (le plus complet)
            canonical = max(segs, key=lambda s: len(s.text)).text
            groups.append(
                SegmentGroup(
                    group_id=group_id,
                    segments=segs,
                    canonical_text=canonical,
                )
            )

        # Tri par nombre d'occurrences décroissant
        groups.sort(key=lambda g: -g.occurrence_count)

        n_repeated = sum(1 for g in groups if g.is_repeated)
        print(
            f"      {n_components} groupes au total, "
            f"dont {n_repeated} avec répétitions (≥ 2 occurrences)"
        )
        return groups

    # ── Affichage ────────────────────────────────────────────────────────────

    def print_summary(self, groups: List[SegmentGroup], top_n: int = 20):
        """Affiche un résumé lisible des groupes répétés."""
        repeated = [g for g in groups if g.is_repeated]

        print("\n" + "═" * 65)
        print("  RÉPÉTITIONS DÉTECTÉES")
        print("═" * 65)
        print(f"  Groupes avec répétitions : {len(repeated)}")
        total_repeated_segs = sum(g.occurrence_count for g in repeated)
        print(f"  Segments concernés       : {total_repeated_segs}")
        print()

        if not repeated:
            print(
                "  Aucune répétition trouvée — essayez de baisser similarity_threshold."
            )
        else:
            print(f"  {'Grp':>5}  {'×':>4}  {'Texte (80 chars)':}")
            print("  " + "─" * 62)
            for g in repeated[:top_n]:
                preview = g.canonical_text[:75].replace("\n", " ")
                print(f"  #{g.group_id:04d}  ×{g.occurrence_count:3d}  {preview}…")
            if len(repeated) > top_n:
                print(f"  … et {len(repeated) - top_n} autres groupes répétés")

        print("═" * 65)

    # ── Export ───────────────────────────────────────────────────────────────

    def export_json(
        self,
        groups: List[SegmentGroup],
        path: str,
        repeated_only: bool = True,
    ):
        """
        Exporte les groupes en JSON.

        Paramètres
        ----------
        repeated_only : bool
            Si True (défaut), n'exporte que les groupes avec ≥ 2 occurrences.
        """
        to_export = [g for g in groups if not repeated_only or g.is_repeated]
        data = [g.to_dict() for g in to_export]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"    Export JSON : {path} ({len(data)} groupes)")


# ─────────────────────────────────────────────
#  Utilitaire : chargement depuis JSON de segments
# ─────────────────────────────────────────────


def load_segments_from_json(path: str, text_key: str = "text") -> List[TextSegment]:
    """
    Charge une liste de TextSegment depuis un fichier JSON.

    Attend un tableau JSON avec au moins les champs :
        - `text` (ou la clé indiquée par text_key)
        - `start_sec` / `end_sec` (optionnels)

    Exemple de fichier attendu :
    [
      {"start_sec": 0.0, "end_sec": 32.5, "text": "..."},
      ...
    ]
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    segments = []
    for i, item in enumerate(data):
        text = item.get(text_key, "")
        segments.append(
            TextSegment(
                text=text,
                start_sec=float(item.get("start_sec", 0.0)),
                end_sec=float(item.get("end_sec", 0.0)),
            )
        )
    return segments


# ─────────────────────────────────────────────
#  Point d'entrée CLI minimaliste
# ─────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Regroupe des segments textuels par similarité (détection de répétitions)"
    )
    parser.add_argument(
        "input",
        help="Fichier JSON d'entrée (tableau de segments avec champ 'text')",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Seuil de similarité cosinus [défaut: 0.85]",
    )
    parser.add_argument(
        "--min-chars",
        type=int,
        default=50,
        dest="min_chars",
        help="Longueur minimale de texte pour être indexé [défaut: 50]",
    )
    parser.add_argument(
        "--ngram-min",
        type=int,
        default=3,
        dest="ngram_min",
        help="N-gramme de caractères minimal [défaut: 3]",
    )
    parser.add_argument(
        "--ngram-max",
        type=int,
        default=5,
        dest="ngram_max",
        help="N-gramme de caractères maximal [défaut: 5]",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        dest="batch_size",
        help="Taille des batchs de calcul [défaut: 500]",
    )
    parser.add_argument(
        "--text-key",
        default="text",
        dest="text_key",
        help="Nom du champ texte dans le JSON [défaut: text]",
    )
    parser.add_argument(
        "--out-json",
        default="repetitions.json",
        dest="out_json",
        help="Fichier JSON de sortie [défaut: repetitions.json]",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Exporter aussi les singletons (non-répétitions)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Nombre de groupes à afficher dans le résumé [défaut: 20]",
    )
    args = parser.parse_args()

    print(f"Chargement : {args.input}")
    segments = load_segments_from_json(args.input, text_key=args.text_key)
    print(f"  {len(segments)} segments chargés")

    matcher = TextMatcher(
        similarity_threshold=args.threshold,
        min_chars=args.min_chars,
        ngram_range=(args.ngram_min, args.ngram_max),
        batch_size=args.batch_size,
    )

    groups = matcher.fit_transform(segments)
    matcher.print_summary(groups, top_n=args.top)
    matcher.export_json(groups, args.out_json, repeated_only=not args.all)


if __name__ == "__main__":
    main()
