"""
weekly_viewer.py
================
Génère un fichier HTML autonome de visualisation hebdomadaire
des segments audio/vidéo détectés, avec grouping et annotations.

Usage programmatique :
    from analyse.advertising.tools.visualizer.weekly_viewer import generate_weekly_viewer

    generate_weekly_viewer(
        output_path="week_report.html",
        grouping=report,           # dict issu de MetaMatcherPipeline.build_report()
        parts=[                    # liste de parties (chunks d'enregistrement)
            {
                "start_date": 1709560800,   # epoch début
                "end_date":   1709604400,   # epoch fin
                "segments": [...],          # liste de Segment.to_dict() ou to_alternary_dict()
                "media_url": "https://..."  # URL mp4 / mp3
            },
            ...
        ],
        annotations=[              # testimony_table
            {"type": "PUBLICITE", "start": 1709561000, "end": 1709561300},
            ...
        ],
        params_summary={"channel": "TF1", "threshold": 0.15, ...},
    )

Dépendances : aucune (stdlib uniquement)
"""

import json
from pathlib import Path
from typing import Optional

TEMPLATE_PATH = Path(__file__).parent / "weekly_viewer.html"


def _normalize_segment(seg: dict) -> dict:
    """Normalise les clés courtes {s, e, rms, sc, zcr} vers les clés longues."""
    return {
        "start_sec": seg.get("start_sec", seg.get("s", 0.0)),
        "end_sec": seg.get("end_sec", seg.get("e", 0.0)),
        "rms": seg.get("rms", seg.get("energy_mean", 0.0)),
        "sc": seg.get("sc", seg.get("spectral_centroid", 0.0)),
        "zcr": seg.get("zcr", seg.get("zcr_mean", 0.0)),
    }


def _build_group_lookup(grouping: dict) -> dict:
    """
    Construit un dictionnaire segment_index -> {group_id, group_size, classification}.
    À partir du rapport de grouping issu de MetaMatcherPipeline.build_report().
    """
    lookup = {}
    if not grouping or "groups" not in grouping:
        return lookup
    for group in grouping["groups"]:
        gid = group["group_id"]
        count = group["count"]
        classification = group.get("classification", "")
        duration_mean = group.get("duration_mean", 0)
        duration_std = group.get("duration_std", 0)
        member_ids = []
        for occ in group.get("occurrences", []):
            idx = occ["segment_index"]
            member_ids.append(idx)
        for occ in group.get("occurrences", []):
            idx = occ["segment_index"]
            lookup[idx] = {
                "group_id": gid,
                "group_size": count,
                "classification": classification,
                "duration_mean": duration_mean,
                "duration_std": duration_std,
                "member_ids": member_ids,
            }
    return lookup


def generate_weekly_viewer(
    output_path: str | Path,
    grouping: dict,
    parts: list[dict],
    annotations: list[dict],
    params_summary: Optional[dict] = None,
) -> None:
    """
    Génère un fichier HTML autonome de visualisation hebdomadaire.

    Arguments :
        output_path     : chemin du fichier HTML à générer
        grouping        : rapport issu de MetaMatcherPipeline.build_report()
                          Structure attendue : {total_segments, total_groups, groups: [...]}
        parts           : liste de parties ordonnées chronologiquement
                          Chaque partie : {
                              start_date: float (epoch),
                              end_date: float (epoch),
                              segments: list[dict],  # Segment.to_dict() ou to_alternary_dict()
                              media_url: str          # URL mp4 ou mp3
                          }
        annotations     : liste d'annotations (testimony_table)
                          Chaque annotation : {type: str, start: float (epoch), end: float (epoch)}
        params_summary  : dictionnaire de paramètres à afficher dans le rapport
    """
    output_path = Path(output_path)
    params_summary = params_summary or {}

    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"Template HTML introuvable : {TEMPLATE_PATH}"
        )

    # ── Lookup grouping ────────────────────────────────────────
    group_lookup = _build_group_lookup(grouping)

    # ── Prétraitement des segments ─────────────────────────────
    all_segments = []
    processed_parts = []
    global_idx = 0
    time_min = float("inf")
    time_max = float("-inf")

    for part_idx, part in enumerate(parts):
        part_start = part["start_date"]
        part_end = part["end_date"]
        media_url = part.get("media_url", "")

        time_min = min(time_min, part_start)
        time_max = max(time_max, part_end)

        raw_segments = part.get("segments", [])
        part_seg_ids = []

        for seg_raw in raw_segments:
            seg = _normalize_segment(seg_raw)
            local_start = seg["start_sec"]
            local_end = seg["end_sec"]
            abs_start = part_start + local_start
            abs_end = part_start + local_end

            grp = group_lookup.get(global_idx, {})

            all_segments.append({
                "id": global_idx,
                "absStart": round(abs_start, 3),
                "absEnd": round(abs_end, 3),
                "partIdx": part_idx,
                "localStart": round(local_start, 3),
                "localEnd": round(local_end, 3),
                "duration": round(local_end - local_start, 3),
                "rms": round(seg["rms"], 4),
                "sc": round(seg["sc"], 1),
                "zcr": round(seg["zcr"], 4),
                "groupId": grp.get("group_id", -1),
                "groupSize": grp.get("group_size", 1),
                "classification": grp.get("classification", ""),
                "durationMean": grp.get("duration_mean", 0),
                "durationStd": grp.get("duration_std", 0),
                "memberIds": grp.get("member_ids", []),
            })
            part_seg_ids.append(global_idx)
            global_idx += 1

        processed_parts.append({
            "startDate": part_start,
            "endDate": part_end,
            "mediaUrl": media_url,
            "segmentCount": len(raw_segments),
            "segmentIds": part_seg_ids,
        })

    # ── Groupes pour navigation ────────────────────────────────
    processed_groups = []
    if grouping and "groups" in grouping:
        for g in grouping["groups"]:
            processed_groups.append({
                "groupId": g["group_id"],
                "count": g["count"],
                "durationMean": g.get("duration_mean", 0),
                "durationStd": g.get("duration_std", 0),
                "classification": g.get("classification", ""),
                "memberIds": [occ["segment_index"] for occ in g.get("occurrences", [])],
            })

    # ── Annotations ────────────────────────────────────────────
    processed_annotations = []
    for ann in annotations:
        start = ann.get("start", 0)
        end = ann.get("end", 0)
        # Support datetime objects
        if hasattr(start, "timestamp"):
            start = start.timestamp()
        if hasattr(end, "timestamp"):
            end = end.timestamp()
        processed_annotations.append({
            "type": ann.get("type", "INCONNU"),
            "start": start,
            "end": end,
        })

    # ── Densité par heure ──────────────────────────────────────
    # Pré-calculée en Python pour alléger le JS
    if all_segments and time_min < time_max:
        bin_size = 900  # 15 minutes
        n_bins = int((time_max - time_min) / bin_size) + 1
        density_total = [0] * n_bins
        density_grouped = [0] * n_bins
        for seg in all_segments:
            b = int((seg["absStart"] - time_min) / bin_size)
            if 0 <= b < n_bins:
                density_total[b] += 1
                if seg["groupSize"] > 1:
                    density_grouped[b] += 1
        density = {
            "binSize": bin_size,
            "total": density_total,
            "grouped": density_grouped,
        }
    else:
        density = {"binSize": 900, "total": [], "grouped": []}

    # ── Payload ────────────────────────────────────────────────
    payload = {
        "segments": all_segments,
        "parts": processed_parts,
        "groups": processed_groups,
        "annotations": processed_annotations,
        "density": density,
        "params": params_summary,
        "timeRange": {
            "min": time_min if time_min != float("inf") else 0,
            "max": time_max if time_max != float("-inf") else 0,
        },
        "stats": {
            "totalSegments": len(all_segments),
            "totalGroups": len(processed_groups),
            "totalParts": len(processed_parts),
            "totalAnnotations": len(processed_annotations),
            "groupedSegments": sum(1 for s in all_segments if s["groupSize"] > 1),
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
    print(f"  Segments       : {len(all_segments)}")
    print(f"  Groupés        : {sum(1 for s in all_segments if s['groupSize'] > 1)}")
    print(f"  Groupes        : {len(processed_groups)}")
    print(f"  Parties        : {len(processed_parts)}")
    print(f"  Annotations    : {len(processed_annotations)}")
    print(f"\n  → Ouvrez dans Firefox ou Chrome.\n")
