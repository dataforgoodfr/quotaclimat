"""
Convert the srt files of every channel folder into the partner CSV layout
(no header, ';' separated): channel_name;program_name;DD/MM/YYYY HH:MM:SS;plaintext

Each srt file is named <YYYYMMDDHHMMSS>.srt: the name gives the start of the
recording, the cue timestamps are offsets from that start.

The rows of each channel are split by month half and written to
<output_root>/<year>/<month>/<1-14|15+>/<first day>_<last day>_<FileName>.csv,
like the files already sent by the partner. Existing files are overwritten.

Usage (from i8n/):
    python3 srt-channel-to-csv.py "csa-belge/tv Bel fr octobre 2026"
    python3 srt-channel-to-csv.py "csa-belge/tv Bel fr octobre 2026" --channels ACTV BX1
"""
import argparse
import csv
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

CUE_TIME = re.compile(
    r"(\d+):(\d{2}):(\d{2})[,.](\d{3})\s*-->\s*(\d+):(\d{2}):(\d{2})[,.](\d{3})"
)
FILENAME_FORMAT = "%Y%m%d%H%M%S"
OUTPUT_DATE_FORMAT = "%d/%m/%Y %H:%M:%S"


def parse_srt(path: Path, encoding: str = "utf-8-sig"):
    """Yield (offset: timedelta, text: str) for each cue of an srt file."""
    content = path.read_text(encoding=encoding, errors="replace")
    for block in re.split(r"\r?\n\s*\r?\n", content.strip()):
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        for i, line in enumerate(lines):
            m = CUE_TIME.search(line)
            if m:
                h, mi, s, ms = (int(x) for x in m.groups()[:4])
                text = " ".join(lines[i + 1 :]).strip()
                if text:
                    yield timedelta(hours=h, minutes=mi, seconds=s, milliseconds=ms), text
                break


# srt folder name -> file name used by the partner in the csv file names
FILE_NAMES = {
    "ACTV": "ACTV",
    "BX1": "BX1",
    "CANALC": "Boukè",
    "CanalZoom": "CanalZoom",
    "LAUNE": "LaUne",
    "MATELE": "MaTélé",
    "NOTELE": "NoTélé",
    "RTC": "Qu4tre",
    "RTL": "RTLTVi",
    "TELEMB": "TéléMB",
    "TELESAMBRE": "Télésambre",
    "TVCOM": "TVCom",
    "TVLUX": "TvLux",
    "VEDIA": "Vedia",
}


def month_half(dt: datetime) -> str:
    return "1-14" if dt.day <= 14 else "15+"


def convert_channel(channel_dir: Path, output_root: Path, program: str = ""):
    channel = channel_dir.name
    csv_channel = channel.upper()  # first column of the partner csv (CanalZoom -> CANALZOOM)
    file_name = FILE_NAMES.get(channel, channel)

    rows = []
    for path in sorted(channel_dir.glob("*.srt")):
        try:
            recording_start = datetime.strptime(path.stem, FILENAME_FORMAT)
        except ValueError:
            print(f"  Skipping {path.name}: name is not YYYYMMDDHHMMSS")
            continue
        for offset, text in parse_srt(path):
            # the srt keeps the second only: drop milliseconds like the partner csv
            rows.append(((recording_start + offset).replace(microsecond=0), text))
    rows.sort(key=lambda r: r[0])

    halves = {}
    for start, text in rows:
        halves.setdefault((start.year, start.month, month_half(start)), []).append((start, text))

    for (year, month, half), half_rows in sorted(halves.items()):
        first, last = half_rows[0][0], half_rows[-1][0]
        output = output_root / str(year) / str(month) / half / f"{first:%Y%m%d}_{last:%Y%m%d}_{file_name}.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            for start, text in half_rows:
                writer.writerow([csv_channel, program, start.strftime(OUTPUT_DATE_FORMAT), text])
        print(f"  {len(half_rows)} rows -> {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_dir", type=Path, help="folder containing one subfolder per channel")
    parser.add_argument("--channels", nargs="*", help="only these channel subfolders (default: all)")
    parser.add_argument("--output-root", type=Path, default=Path("csa-belge"), help="default: csa-belge")
    parser.add_argument("--program", default="", help="program name written in column 2 (not in the srt)")
    args = parser.parse_args()

    channel_dirs = sorted(d for d in args.input_dir.iterdir() if d.is_dir() and any(d.glob("*.srt")))
    if args.channels:
        channel_dirs = [d for d in channel_dirs if d.name in args.channels]
    if not channel_dirs:
        sys.exit(f"No channel folder with .srt files in {args.input_dir}")
    for channel_dir in channel_dirs:
        print(channel_dir.name)
        convert_channel(channel_dir, args.output_root, args.program)
