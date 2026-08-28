"""Turn a Trivy vulnerability report into a minimal OpenVEX file.

VEX (Vulnerability Exploitability eXchange) is a small standard format used to
say, for each vulnerability found by a scanner, whether it actually affects
your software or not. Here we don't yet know, so every vulnerability found by
Trivy is marked as "under_investigation" — a real project would update this
status after reviewing each one (e.g. "not_affected" or "fixed").

Usage:
    python scripts/generate_vex.py <trivy-report.json> <vex.json>
"""
import json
import sys
from datetime import datetime, timezone


def build_vex(trivy_report: dict) -> dict:
    statements = []
    for result in trivy_report.get("Results", []):
        for vuln in result.get("Vulnerabilities", []) or []:
            statements.append({
                "vulnerability": {"name": vuln["VulnerabilityID"]},
                "products": [{"@id": f"pkg:generic/{result.get('Target', 'quotaclimat')}"}],
                "status": "under_investigation",
            })

    return {
        "@context": "https://openvex.dev/ns/v0.2.0",
        "@id": f"https://github.com/quotaclimat/vex-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        "author": "quotaclimat CI",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": 1,
        "statements": statements,
    }


if __name__ == "__main__":
    trivy_report_path, vex_output_path = sys.argv[1], sys.argv[2]

    with open(trivy_report_path) as f:
        trivy_report = json.load(f)

    vex = build_vex(trivy_report)

    with open(vex_output_path, "w") as f:
        json.dump(vex, f, indent=2)

    print(f"Wrote {len(vex['statements'])} VEX statement(s) to {vex_output_path}")
