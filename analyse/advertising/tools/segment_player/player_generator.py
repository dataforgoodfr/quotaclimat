"""
generate_report.py
==================
Génère un fichier HTML autonome et auto-suffisant contenant :
  - Le player audio (rupture_player_v2.html)
  - L'audio encodé en base64
  - Les segments JSON

Usage :
    python generate_report.py mon_flux.mp3 --segments segments.json
    python generate_report.py mon_flux.wav --segments segments.json --output rapport.html

Dépendances : aucune (stdlib uniquement)
"""

import argparse
import base64
import json
import mimetypes
import sys
from pathlib import Path

TEMPLATE_PATH = Path(__file__).parent / "rupture_player_v2.html"

# Taille max recommandée pour l'audio embarqué (en Mo)
# Au-delà, le fichier HTML devient très lourd.
WARN_SIZE_MB = 50


def embed(audio_path: str, segments_path: str, output_path: str):
    audio_path = Path(audio_path)
    segments_path = Path(segments_path)
    output_path = Path(output_path)

    # ── Vérifications ────────────────────────────────────────────
    if not audio_path.exists():
        print(f"[ERREUR] Fichier audio introuvable : {audio_path}")
        sys.exit(1)
    if not segments_path.exists():
        print(f"[ERREUR] Fichier JSON introuvable : {segments_path}")
        sys.exit(1)
    if not TEMPLATE_PATH.exists():
        print(f"[ERREUR] Template HTML introuvable : {TEMPLATE_PATH}")
        print(
            "         Assurez-vous que rupture_player_v2.html est dans le même dossier."
        )
        sys.exit(1)

    audio_size_mb = audio_path.stat().st_size / (1024 * 1024)
    if audio_size_mb > WARN_SIZE_MB:
        print(f"[AVERTISSEMENT] Fichier audio volumineux : {audio_size_mb:.1f} Mo")
        print(f"                Le HTML généré sera lourd ({audio_size_mb:.0f}+ Mo).")
        print("                Envisagez de convertir en mp3 128k avant d'embarquer.")
        answer = input("Continuer quand même ? [o/N] ").strip().lower()
        if answer not in ("o", "oui", "y", "yes"):
            print("Annulé.")
            sys.exit(0)

    # ── Lecture et encodage de l'audio ───────────────────────────
    print(f"[1/3] Encodage de l'audio : {audio_path.name} ({audio_size_mb:.1f} Mo)...")
    with open(audio_path, "rb") as f:
        audio_bytes = f.read()

    audio_b64 = base64.b64encode(audio_bytes).decode("ascii")
    audio_mime = mimetypes.guess_type(str(audio_path))[0] or "audio/mpeg"

    print(f"      Type MIME : {audio_mime}")
    print(f"      Taille base64 : {len(audio_b64) / (1024 * 1024):.1f} Mo")

    # ── Lecture des segments ──────────────────────────────────────
    print(f"[2/3] Chargement des segments : {segments_path.name}...")
    with open(segments_path, "r", encoding="utf-8") as f:
        segments = json.load(f)

    print(f"      {len(segments)} segments trouvés")

    # ── Construction du payload JSON ─────────────────────────────
    payload = json.dumps(
        {
            "audio_b64": audio_b64,
            "audio_mime": audio_mime,
            "audio_name": audio_path.name,
            "segments": segments,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )

    # ── Injection dans le template HTML ──────────────────────────
    print(f"[3/3] Génération du rapport HTML : {output_path}...")
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        html = f.read()

    # Remplacer le placeholder dans la balise script#embedded-data
    placeholder = '<script id="embedded-data" type="application/json">null</script>'
    replacement = (
        f'<script id="embedded-data" type="application/json">{payload}</script>'
    )

    if placeholder not in html:
        print("[ERREUR] Placeholder non trouvé dans le template HTML.")
        print("         Vérifiez que rupture_player_v2.html contient la balise :")
        print(f"         {placeholder}")
        sys.exit(1)

    html = html.replace(placeholder, replacement)

    # Mise à jour du titre
    html = html.replace(
        "<title>Rupture Detector — Audio Lab</title>",
        f"<title>{audio_path.stem} — Rupture Detector</title>",
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    final_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n✓ Rapport généré : {output_path}")
    print(f"  Taille finale  : {final_size_mb:.1f} Mo")
    print(f"  Segments       : {len(segments)}")
    print("\n  Ouvrez le fichier dans votre navigateur pour l'analyse.\n")


def main():
    parser = argparse.ArgumentParser(
        description="Génère un rapport HTML autonome avec audio embarqué",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples :
  python generate_report.py radio_lundi.mp3
  python generate_report.py radio_lundi.mp3 --segments segments.json
  python generate_report.py radio_lundi.mp3 --output rapport_lundi.html

Workflow complet :
  1. python rupture_detector.py radio_lundi.mp3
        → produit segments.json

  2. python generate_report.py radio_lundi.mp3
        → produit radio_lundi_report.html

  3. Ouvrir radio_lundi_report.html dans Firefox ou Chrome
        """,
    )
    parser.add_argument("audio", help="Fichier audio source (mp3, wav, flac, ogg...)")
    parser.add_argument(
        "--segments",
        default="segments.json",
        help="Fichier JSON des segments [défaut: segments.json]",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Nom du fichier HTML de sortie [défaut: <audio_stem>_report.html]",
    )
    args = parser.parse_args()

    audio_path = Path(args.audio)
    output_path = (
        Path(args.output)
        if args.output
        else audio_path.parent / (audio_path.stem + "_report.html")
    )

    embed(
        audio_path=str(audio_path),
        segments_path=args.segments,
        output_path=str(output_path),
    )


if __name__ == "__main__":
    main()
