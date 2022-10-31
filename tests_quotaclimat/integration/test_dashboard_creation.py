import sys

sys.path.append(".")
from quotaclimat.Introduction import build_dashboard


def test_dashboard_creation():
    build_dashboard()
