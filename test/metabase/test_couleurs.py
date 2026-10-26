import re

import yaml

from quotaclimat.metabase.questions import METABASE_DIR

HEX = re.compile(r"^#[0-9A-F]{6}$")


def test_couleurs():
    with open(METABASE_DIR / "couleurs.yml") as f:
        groups = yaml.safe_load(f)["groupes"]
    for name, group in groups.items():
        assert group["description"] and group["reference"], name
        colors = list(group["couleurs"].values())
        assert all(HEX.match(color) for color in colors), f"{name}: couleur hors format #RRGGBB"
        assert len(set(colors)) == len(colors), f"{name}: deux valeurs ont la même couleur"
