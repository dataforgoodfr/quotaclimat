"""
generate_report.py  v2
======================
Génère un fichier HTML autonome contenant :
  - Le player (rupture_player_v3.html)
  - L'audio OU la vidéo encodé(e) en base64
  - Les segments JSON  (obligatoire)
  - Les annotations CSV  (optionnel)

Usage :
    # Audio seul
    python generate_report.py flux.mp3

    # Vidéo
    python generate_report.py enregistrement.mp4

    # Avec annotations CSV
    python generate_report.py flux.mp3 --csv annotations.csv

    # Tout spécifier
    python generate_report.py flux.mp4 --segments segments.json --csv pub_list.csv --output rapport.html

Format CSV attendu (séparateur virgule ou point-virgule) :
    type,start,end
    PUBLICITE,00:12:30,00:13:00
    PUBLICITE,00:25:10.5,00:25:40.0
    JINGLE,00:00:05,00:00:12

    Les temps peuvent être en HH:MM:SS, MM:SS ou en secondes décimales.
    La première ligne est automatiquement détectée comme en-tête si elle
    ne contient pas de valeurs temporelles valides.

Dépendances : aucune (stdlib uniquement)
"""

import argparse
import base64
import csv
import io
import json
import mimetypes
import re
import sys
from datetime import datetime
from pathlib import Path

TEMPLATE_PATH = Path(__file__).parent / "player.html"
WARN_SIZE_MB = 80
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".avi"}
VIDEO_PATTERN = re.compile(r"\.(mp4|mov|webm|mkv|avi)(\?|$)", re.IGNORECASE)


def is_url(s: str) -> bool:
    return s.startswith("http://") or s.startswith("https://")


# ─────────────────────────────────────────────
#  Parsing du CSV
# ─────────────────────────────────────────────


def parse_time(s: str):
    """Convertit une chaîne temps en secondes décimales, ou None si invalide."""
    s = s.strip()
    if not s:
        return None
    if ":" in s:
        parts = s.split(":")
        try:
            if len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            if len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def load_csv(path: str) -> list:
    """
    Charge un fichier CSV avec colonnes type, start, end.
    Retourne une liste de dicts {type, start_sec, end_sec}.
    """
    with open(path, encoding="utf-8-sig") as f:
        content = f.read()

    sep = ";" if content.count(";") > content.count(",") else ","
    reader = csv.reader(io.StringIO(content), delimiter=sep)
    rows = [row for row in reader if any(c.strip() for c in row)]

    if not rows:
        return []

    # Détecter si la première ligne est un en-tête
    start_idx = 0
    if len(rows[0]) >= 3 and parse_time(rows[0][1].strip()) is None:
        start_idx = 1

    annotations = []
    for i, row in enumerate(rows[start_idx:], start=start_idx + 1):
        if len(row) < 3:
            continue
        type_val = row[0].strip().strip('"').upper()
        start_sec = parse_time(row[1])
        end_sec = parse_time(row[2])
        if start_sec is None or end_sec is None:
            print(f"  [AVERTISSEMENT] Ligne {i} ignorée (temps invalides) : {row}")
            continue
        annotations.append(
            {
                "type": type_val,
                "start_sec": start_sec,
                "end_sec": end_sec,
            }
        )

    return annotations


# ─────────────────────────────────────────────
#  Encodage média
# ─────────────────────────────────────────────

VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".avi"}


def encode_media(path: Path):
    """
    Retourne (b64_string, mime_type, field_name, is_video).
    field_name = 'video_b64' ou 'audio_b64'
    """
    size_mb = path.stat().st_size / (1024 * 1024)
    is_video = path.suffix.lower() in VIDEO_EXTS

    if size_mb > WARN_SIZE_MB:
        print(f"  [AVERTISSEMENT] Fichier volumineux : {size_mb:.1f} Mo")
        print(f"  Le HTML généré sera lourd ({size_mb:.0f}+ Mo).")
        if is_video:
            print(
                "  Conseil : ffmpeg -i input.mp4 -vf scale=640:-1 -b:a 96k output.mp4"
            )
        else:
            print("  Conseil : ffmpeg -i input.wav -b:a 128k output.mp3")
        answer = input("Continuer quand même ? [o/N] ").strip().lower()
        if answer not in ("o", "oui", "y", "yes"):
            print("Annulé.")
            sys.exit(0)

    mime = mimetypes.guess_type(str(path))[0]
    if mime is None:
        mime = "video/mp4" if is_video else "audio/mpeg"

    with open(path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")

    field = "video_b64" if is_video else "audio_b64"
    return b64, mime, field, is_video


# ─────────────────────────────────────────────
#  Génération
# ─────────────────────────────────────────────


def parse_start_time(s: str) -> float:
    """
    Parse un temps de début en secondes depuis minuit.
    Formats acceptés : HH:MM:SS  |  YYYY-MM-DDTHH:MM:SS  |  YYYY-MM-DD HH:MM:SS
    """
    s = s.strip()
    if "T" in s or (len(s) > 8 and s[4] == "-"):
        dt = datetime.fromisoformat(s)
    else:
        from datetime import date, time as dtime
        parts = s.split(":")
        dt = datetime.combine(date.today(), dtime(int(parts[0]), int(parts[1]), int(float(parts[2]))))
    return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6


def generate_player(
    media_input: str,
    segments: list[dict],
    annotations: list[dict] | None,
    output_path: str | Path,
    params: dict | None = None,
    novelty_peaks: list | None = None,
    start_epoch: float | None = None,
):
    output_path = Path(output_path)
    annotations = annotations or []
    n_steps = 3 if annotations else 2
    media_is_url = is_url(media_input)

    # Vérification template
    if not TEMPLATE_PATH.exists():
        print(f"[ERREUR] Fichier template HTML introuvable : {TEMPLATE_PATH}")
        sys.exit(1)

    # ── Étape 1 : média ──────────────────────────────────────────────
    payload_media = {}

    if media_is_url:
        # Mode URL — on stocke juste l'URL, le navigateur charge le fichier
        url = media_input
        is_video = bool(VIDEO_PATTERN.search(url))
        name = url.split("?")[0].rstrip("/").split("/")[-1] or "media"
        print(f"[1/{n_steps}] URL média détectée : {url}")
        print(f"  Type     : {'vidéo' if is_video else 'audio'}")
        print("  Le fichier ne sera PAS embarqué dans le HTML.")
        payload_media = {
            "media_url": url,
            "media_name": name,
            "is_video": is_video,
        }
    else:
        # Mode fichier local — encodage base64
        media_path = Path(media_input)
        if not media_path.exists():
            print(f"[ERREUR] Fichier média introuvable : {media_path}")
            sys.exit(1)
        print(f"[1/{n_steps}] Encodage du média : {media_path.name}")
        b64, mime, field, is_video = encode_media(media_path)
        media_type = "vidéo" if is_video else "audio"
        print(
            f"  Type : {media_type}  ({mime})  taille base64 : {len(b64) / (1024 * 1024):.1f} Mo"
        )
        mime_key = "video_mime" if is_video else "audio_mime"
        payload_media = {
            field: b64,
            mime_key: mime,
            "media_name": media_path.name,
        }

    # ── Étape 2 : segments ────────────────────────────────────────────
    print(f"[2/{n_steps}] Segments fournis en entrée")
    print(f"  {len(segments)} segments")

    # ── Étape 3 (optionnelle) : annotations ───────────────────────────
    if annotations:
        print("[3/3] Annotations fournies en entrée")
        print(f"  {len(annotations)} annotations")

    # Construction du payload JSON
    payload = {
        **payload_media,
        "segments": segments,
        "annotations": annotations,
        "params": params or {},
        "novelty_peaks": novelty_peaks or [],
        "start_epoch": start_epoch or 0,
    }

    # Injection dans le template
    print(f"\nGénération du rapport HTML : {output_path}...")
    with open(TEMPLATE_PATH, encoding="utf-8") as f:
        html = f.read()

    placeholder = '<script id="embedded-data" type="application/json">null</script>'
    if placeholder not in html:
        print("[ERREUR] Placeholder non trouvé dans le template.")
        print("         Vérifiez que rupture_player_v3.html est dans le même dossier.")
        sys.exit(1)

    payload_str = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    html = html.replace(
        placeholder,
        f'<script id="embedded-data" type="application/json">{payload_str}</script>',
    )
    html = html.replace(
        "<title>Rupture Detector — Audio Lab</title>",
        f"<title>{Path(media_input.split('?')[0]).stem if not media_is_url else media_input.split('/')[-1].split('?')[0]} — Rupture Detector</title>",
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    final_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n✓  Rapport généré  : {output_path}")
    print(
        f"   Taille HTML     : {final_mb:.1f} Mo{' (léger, média chargé à la volée)' if media_is_url else ''}"
    )
    if media_is_url:
        print(f"   URL média       : {media_input}")
    print(f"   Segments        : {len(segments)}")
    if annotations:
        print(f"   Annotations CSV : {len(annotations)}")
    print("\n   → Ouvrez dans Firefox ou Chrome pour l'analyse.\n")


# ─────────────────────────────────────────────
#  Helper to get correct annotation format
# ─────────────────────────────────────────────


def format_annotation(annotations: list[dict], from_date: datetime) -> list[dict]:
    return [
        {
            "type": annotation["type"],
            "start_sec": (annotation["start"] - from_date).total_seconds(),
            "end_sec": (annotation["end"] - from_date).total_seconds(),
        }
        for annotation in annotations
    ]


# ─────────────────────────────────────────────
#  Point d'entrée CLI
# ─────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Génère un rapport HTML autonome (audio/vidéo + segments + annotations CSV)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python generate_report.py radio.mp3
  python generate_report.py https://cdn.example.com/flux.mp3
  python generate_report.py https://cdn.example.com/tv.mp4 --csv pub_list.csv
  python generate_report.py tv.mp4 --segments segments.json --csv pub_list.csv --output rapport_tv.html

Format CSV :
  type,start,end
  PUBLICITE,00:12:30,00:13:00
  JINGLE,00:00:05,00:00:12

  Les temps acceptent : HH:MM:SS  |  MM:SS  |  secondes décimales
  Séparateur : virgule ou point-virgule (auto-détecté)
        """,
    )
    parser.add_argument(
        "media", help="Fichier audio/vidéo local OU URL https:// distante"
    )
    parser.add_argument(
        "--segments",
        default="segments.json",
        help="Fichier JSON des segments [défaut: segments.json]",
    )
    parser.add_argument(
        "--csv", default=None, help="Fichier CSV des annotations manuelles (optionnel)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Nom du rapport HTML [défaut: <nom_media>_report.html]",
    )
    parser.add_argument(
        "--novelty",
        default=None,
        help="Fichier JSON des pics de nouveauté (optionnel, généré par script.py --out-novelty)",
    )
    parser.add_argument(
        "--start-time",
        default=None,
        dest="start_time",
        help="Heure de début de l'enregistrement : HH:MM:SS ou YYYY-MM-DDTHH:MM:SS (affichage heure absolue dans le player)",
    )
    args = parser.parse_args()

    media_input = args.media
    media_is_url = is_url(media_input)

    if media_is_url:
        base_name = Path(media_input.split("?")[0]).name or "media"
        base_stem = Path(base_name).stem or "media"
        output = Path(args.output) if args.output else Path(f"{base_stem}_report.html")
    else:
        media = Path(media_input)
        output = (
            Path(args.output)
            if args.output
            else media.parent / (media.stem + "_report.html")
        )

    segments_path = Path(args.segments)
    if not segments_path.exists():
        print(f"[ERREUR] Fichier segments JSON introuvable : {segments_path}")
        sys.exit(1)

    with open(segments_path, encoding="utf-8") as f:
        segments = json.load(f)

    annotations = []
    if args.csv:
        csv_path = Path(args.csv)
        if not csv_path.exists():
            print(f"[ERREUR] Fichier CSV introuvable : {csv_path}")
            sys.exit(1)
        annotations = load_csv(str(csv_path))

    novelty_peaks = []
    if args.novelty:
        novelty_path = Path(args.novelty)
        if not novelty_path.exists():
            print(f"[ERREUR] Fichier novelty peaks introuvable : {novelty_path}")
            sys.exit(1)
        with open(novelty_path, encoding="utf-8") as f:
            novelty_peaks = json.load(f)

    start_epoch = parse_start_time(args.start_time) if args.start_time else None
    if start_epoch is not None:
        h = int(start_epoch // 3600)
        m = int((start_epoch % 3600) // 60)
        s = int(start_epoch % 60)
        print(f"   Heure de début      : {h:02d}:{m:02d}:{s:02d}")

    generate_player(
        media_input=media_input,
        segments=segments,
        annotations=annotations,
        output_path=str(output),
        novelty_peaks=novelty_peaks,
        start_epoch=start_epoch,
    )


if __name__ == "__main__":
    main()
