"""
Convert the srt files of every channel folder into the partner CSV layout
(no header, ';' separated): channel_name;program_name;DD/MM/YYYY HH:MM:SS;plaintext

Each srt file is named <YYYYMMDDHHMMSS>.srt: the name gives the start of the
recording, the cue timestamps are offsets from that start.

Only the cues inside the program grid of the channel (PROGRAM_GRID below) are
kept, and the program column is filled with the name of the program.

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
import unicodedata
from datetime import datetime, time, timedelta
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


# Other folder names seen in the deliveries -> canonical name (key of the dicts below).
# Folder names are compared in upper case, without accents.
ALIASES = {"BOUKE": "CANALC", "QU4TRE": "RTC"}


def canonical_channel(folder_name: str) -> str:
    name = "".join(
        c for c in unicodedata.normalize("NFD", folder_name) if unicodedata.category(c) != "Mn"
    ).upper()
    return ALIASES.get(name, name)


# canonical channel name -> file name used by the partner in the csv file names
FILE_NAMES = {
    "ACTV": "ACTV",
    "BX1": "BX1",
    "CANALC": "Boukè",
    "CANALZOOM": "CanalZoom",
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


# Program grid per srt folder name: (program, weekdays or None for every day, start, end).
# Weekdays: Monday = 0 ... Sunday = 6. Times are local (Belgium), the end is excluded.
EVERY_DAY = None
WEDNESDAY, THURSDAY, SUNDAY = {2}, {3}, {6}

def _jt_18h(program: str):
    return [(program, EVERY_DAY, time(18, 0), time(18, 20))]


PROGRAM_GRID = {
    "LAUNE": [
        ("JT 19h30", EVERY_DAY, time(19, 30), time(20, 10)),
        ("JT 13h", EVERY_DAY, time(13, 0), time(13, 30)),
        ("Jeudi en Prime", THURSDAY, time(20, 0), time(20, 30)),
        # "bi-mensuel": the weeks are not given, every Wednesday is kept
        ("#Investigations", WEDNESDAY, time(20, 15), time(21, 55)),
        ("QR Débat", WEDNESDAY, time(21, 50), time(23, 0)),
    ],
    "RTL": [
        ("RTL Info 13h", EVERY_DAY, time(12, 55), time(13, 25)),
        ("RTL Info 19h", EVERY_DAY, time(19, 0), time(19, 40)),
        ("Face à Buxant", SUNDAY, time(19, 30), time(19, 45)),
    ],
    "ACTV": _jt_18h("Le 18-18"),
    "BX1": _jt_18h("Le 18h"),
    "CANALC": _jt_18h("L'info"),
    "MATELE": _jt_18h("Le journal"),
    "TELESAMBRE": _jt_18h("JT"),
    "TELEMB": _jt_18h("Les infos"),
    "NOTELE": _jt_18h("Le journal"),
    "RTC": _jt_18h("Le JT édition du soir"),
    "TVCOM": _jt_18h("Actu BW"),
    "TVLUX": _jt_18h("Le journal"),
    "VEDIA": _jt_18h("Les infos"),
    "CANALZOOM": _jt_18h("Le JT"),
}


def find_program(channel: str, dt: datetime):
    """Name of the program of the grid broadcast at dt, None if outside the grid.

    When programs overlap (e.g. QR Débat and #Investigations), the one that
    started last wins."""
    matches = [
        (start, program)
        for program, weekdays, start, end in PROGRAM_GRID[channel]
        if (weekdays is None or dt.weekday() in weekdays) and start <= dt.time() < end
    ]
    return max(matches)[1] if matches else None


def month_half(dt: datetime) -> str:
    return "1-14" if dt.day <= 14 else "15+"


def convert_channel(channel_dir: Path, output_root: Path):
    channel = canonical_channel(channel_dir.name)
    if channel not in PROGRAM_GRID:
        print(f"  No program grid for {channel_dir.name}, skipping (add it to PROGRAM_GRID or ALIASES)")
        return
    csv_channel = channel  # first column of the partner csv (CANALC, RTC, CANALZOOM...)
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
            start = (recording_start + offset).replace(microsecond=0)
            program = find_program(channel, start)
            if program:
                rows.append((start, program, text))
    rows.sort(key=lambda r: r[0])

    halves = {}
    for row in rows:
        halves.setdefault((row[0].year, row[0].month, month_half(row[0])), []).append(row)

    for (year, month, half), half_rows in sorted(halves.items()):
        first, last = half_rows[0][0], half_rows[-1][0]
        output = output_root / str(year) / str(month) / half / f"{first:%Y%m%d}_{last:%Y%m%d}_{file_name}.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, delimiter=";")
            for start, program, text in half_rows:
                writer.writerow([csv_channel, program, start.strftime(OUTPUT_DATE_FORMAT), text])
        print(f"  {len(half_rows)} rows -> {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("input_dir", type=Path, help="folder containing one subfolder per channel")
    parser.add_argument("--channels", nargs="*", help="only these channel subfolders (default: all)")
    parser.add_argument("--output-root", type=Path, default=Path("csa-belge"), help="default: csa-belge")
    args = parser.parse_args()

    channel_dirs = sorted(d for d in args.input_dir.iterdir() if d.is_dir() and any(d.glob("*.srt")))
    if args.channels:
        wanted = {canonical_channel(c) for c in args.channels}
        channel_dirs = [d for d in channel_dirs if canonical_channel(d.name) in wanted]
    if not channel_dirs:
        sys.exit(f"No channel folder with .srt files in {args.input_dir}")
    for channel_dir in channel_dirs:
        print(channel_dir.name)
        convert_channel(channel_dir, args.output_root)
