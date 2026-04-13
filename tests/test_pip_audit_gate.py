import json
import subprocess
import sys


def _run_gate(tmp_path, payload):
    audit_path = tmp_path / "pip-audit.json"
    audit_path.write_text(json.dumps(payload), encoding="utf-8")
    return subprocess.run(
        [sys.executable, "scripts/enforce_pip_audit.py", str(audit_path), "--fail-on", "critical,high"],
        text=True,
        capture_output=True,
        check=False,
    )


def test_pip_audit_gate_allows_medium_report(tmp_path):
    result = _run_gate(
        tmp_path,
        {
            "dependencies": [
                {
                    "name": "pkg",
                    "version": "1.0",
                    "vulns": [{"id": "CVE-1", "severity": "medium", "fix_versions": ["1.1"]}],
                }
            ]
        },
    )
    assert result.returncode == 0
    assert "medium: pkg 1.0 CVE-1" in result.stdout


def test_pip_audit_gate_blocks_high_report(tmp_path):
    result = _run_gate(
        tmp_path,
        {
            "dependencies": [
                {
                    "name": "pkg",
                    "version": "1.0",
                    "vulns": [{"id": "CVE-2", "database_specific": {"severity": "HIGH"}}],
                }
            ]
        },
    )
    assert result.returncode == 1
    assert "high: pkg 1.0 CVE-2" in result.stdout


def test_pip_audit_gate_maps_cvss_score_to_critical(tmp_path):
    result = _run_gate(
        tmp_path,
        {
            "dependencies": [
                {
                    "name": "pkg",
                    "version": "1.0",
                    "vulnerabilities": [{"id": "CVE-3", "ratings": [{"score": 9.8}]}],
                }
            ]
        },
    )
    assert result.returncode == 1
    assert "critical: pkg 1.0 CVE-3" in result.stdout


def test_pip_audit_gate_blocks_cyclonedx_high_rating(tmp_path):
    result = _run_gate(
        tmp_path,
        {
            "vulnerabilities": [
                {
                    "id": "GHSA-1",
                    "ratings": [{"severity": "high"}],
                    "affects": [{"ref": "pkg:pypi/pkg@1.0"}],
                }
            ]
        },
    )
    assert result.returncode == 1
    assert "high: pkg 1.0 GHSA-1" in result.stdout
