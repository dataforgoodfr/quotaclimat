import sys

from quotaclimat import build_dashboard

sys.path.append(".")


def test_dashboard_creation():
    build_dashboard()
