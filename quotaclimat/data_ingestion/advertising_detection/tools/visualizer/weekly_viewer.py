"""
weekly_viewer.py
================
Génère un fichier HTML autonome de visualisation des fragments
classifiés (pub, contenu, jingle…).

Usage programmatique :
    from quotaclimat.data_ingestion.advertising_detection.tools.visualizer.weekly_viewer import generate_weekly_viewer
    from quotaclimat.data_ingestion.advertising_detection.e05_classify_fragments import Fragment

    generate_weekly_viewer(
        output_path="report.html",
        fragments=fragments,       # list[Fragment]
        params_summary={"channel": "TF1", "threshold": 0.15, ...},
    )

Dépendances : aucune (stdlib uniquement)
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from ...advertising_detection.e05_classify_fragments import Fragment

TEMPLATE_PATH = Path(__file__).parent / "weekly_viewer.html"


def _build_player_url(channel: str, start_epoch: float, end_epoch: float) -> str:
    """Construit l'URL du player Mediatree pour un fragment."""
    start_cts = int(start_epoch)
    end_cts = int(end_epoch)
    position_cts = start_cts
    return (
        f"https://keywords.mediatree.fr/player/"
        f"?fifo={channel}&start_cts={start_cts}&end_cts={end_cts}&position_cts={position_cts}"
    )


def generate_weekly_viewer(
    output_path: str | Path,
    fragments: list[Fragment],
    annotations: list[dict] | None = None,
    params_summary: Optional[dict] = None,
) -> None:
    """
    Génère un fichier HTML autonome de visualisation des fragments.

    Arguments :
        output_path     : chemin du fichier HTML à générer
        fragments       : liste de Fragment (issus de FragmentsClassifier)
        annotations     : liste optionnelle d'annotations (testimony_table)
                          Chaque annotation : {type: str, start: float (epoch), end: float (epoch)}
        params_summary  : dictionnaire de paramètres à afficher dans le rapport
    """
    output_path = Path(output_path)
    params_summary = params_summary or {}
    annotations = annotations or []

    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Template HTML introuvable : {TEMPLATE_PATH}")

    if not fragments:
        raise ValueError("Aucun fragment à visualiser.")

    # ── Tri des fragments par date ────────────────────────────
    fragments = sorted(fragments, key=lambda f: f.start_date)

    # ── Calcul des tailles de groupes ─────────────────────────
    group_counts: dict[str, int] = defaultdict(int)
    group_members: dict[str, list[float]] = defaultdict(list)
    for frag in fragments:
        if frag.group_id:
            group_counts[frag.group_id] += 1
            group_members[frag.group_id].append(frag.start_date.timestamp())

    # ── Traitement des fragments ──────────────────────────────
    processed_fragments = []
    time_min = float("inf")
    time_max = float("-inf")

    for idx, frag in enumerate(fragments):
        abs_start = frag.start_date.timestamp()
        abs_end = frag.end_date.timestamp()
        duration = abs_end - abs_start

        time_min = min(time_min, abs_start)
        time_max = max(time_max, abs_end)

        group_size = group_counts.get(frag.group_id, 1) if frag.group_id else 1
        member_starts = group_members.get(frag.group_id, []) if frag.group_id else []

        # Chunks internes du fragment
        processed_chunks = []
        if frag.chunks:
            for chunk in frag.chunks:
                processed_chunks.append({
                    "absStart": round(chunk.start_sec, 2),
                    "absEnd": round(chunk.end_sec, 2),
                    "duration": round(chunk.end_sec - chunk.start_sec, 3),
                    "rms": round(chunk.energy_mean, 4),
                    "sc": round(chunk.spectral_centroid, 1),
                    "zcr": round(chunk.zcr_mean, 4),
                })

        player_url = _build_player_url(frag.channel, abs_start, abs_end)

        processed_fragments.append({
            "id": idx,
            "absStart": round(abs_start, 2),
            "absEnd": round(abs_end, 2),
            "duration": round(duration, 3),
            "channel": frag.channel,
            "classification": frag.classification,
            "groupId": frag.group_id,
            "groupSize": group_size,
            "chunks": processed_chunks,
            "playerUrl": player_url,
            "memberStarts": [round(s, 2) for s in member_starts],
        })

    # ── Annotations ────────────────────────────────────────────
    processed_annotations = []
    for ann in annotations:
        start = ann.get("start", 0)
        end = ann.get("end", 0)
        if hasattr(start, "timestamp"):
            start = start.timestamp()
        if hasattr(end, "timestamp"):
            end = end.timestamp()
        processed_annotations.append({
            "type": ann.get("type", "INCONNU"),
            "start": start,
            "end": end,
        })

    # ── Densité par 15 min ─────────────────────────────────────
    if processed_fragments and time_min < time_max:
        bin_size = 900
        n_bins = int((time_max - time_min) / bin_size) + 1
        density_total = [0] * n_bins
        density_ad = [0] * n_bins
        for frag in processed_fragments:
            b = int((frag["absStart"] - time_min) / bin_size)
            if 0 <= b < n_bins:
                density_total[b] += 1
                if frag["classification"] in ("new_ad", "already_known_ad"):
                    density_ad[b] += 1
        density = {
            "binSize": bin_size,
            "total": density_total,
            "ad": density_ad,
        }
    else:
        density = {"binSize": 900, "total": [], "ad": []}

    # ── Stats par classification ───────────────────────────────
    classification_counts: dict[str, int] = defaultdict(int)
    for frag in processed_fragments:
        classification_counts[frag["classification"]] += 1

    unique_groups = set(
        f["groupId"] for f in processed_fragments if f["groupId"]
    )

    # ── Payload ────────────────────────────────────────────────
    payload = {
        "fragments": processed_fragments,
        "annotations": processed_annotations,
        "density": density,
        "params": params_summary,
        "timeRange": {
            "min": time_min if time_min != float("inf") else 0,
            "max": time_max if time_max != float("-inf") else 0,
        },
        "stats": {
            "totalFragments": len(processed_fragments),
            "totalGroups": len(unique_groups),
            "totalAnnotations": len(processed_annotations),
            "classificationCounts": dict(classification_counts),
        },
    }

    # ── Injection dans le template ─────────────────────────────
    with open(TEMPLATE_PATH, encoding="utf-8") as f:
        html = f.read()

    placeholder = '<script id="embedded-data" type="application/json">null</script>'
    if placeholder not in html:
        raise ValueError(
            "Placeholder introuvable dans le template HTML. "
            "Vérifiez que weekly_viewer.html est intact."
        )

    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = html.replace(
        placeholder,
        f'<script id="embedded-data" type="application/json">{payload_str}</script>',
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n[WeeklyViewer] Rapport généré : {output_path.absolute()}")
    print(f"  Taille         : {size_mb:.1f} Mo")
    print(f"  Fragments      : {len(processed_fragments)}")
    for cls, count in sorted(classification_counts.items()):
        print(f"    {cls:<20} : {count}")
    print(f"  Groupes        : {len(unique_groups)}")
    print(f"  Annotations    : {len(processed_annotations)}")
    print("\n  → Ouvrez dans Firefox ou Chrome.\n")
