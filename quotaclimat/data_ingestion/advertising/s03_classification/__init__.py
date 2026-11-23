from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[4]

# Before any module of the package reads os.environ, including connect_to_db's defaults.
load_dotenv(ROOT / ".advertising.env")
