from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

BLOCKING_SEVERITIES = {"critical", "high"}


def _severity_from_score(value: Any) -> str | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score >= 9.0:
        return "critical"
    if score >= 7.0:
        return "high"
    if score >= 4.0:
        return "medium"
    if score > 0:
        return "low"
    return None


def _normalize_severity(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip().lower()
        aliases = {"moderate": "medium", "important": "high"}
        if cleaned in {"critical", "high", "medium", "low", "none"}:
            return cleaned
        if cleaned in aliases:
            return aliases[cleaned]
        return _severity_from_score(cleaned)
    if isinstance(value, (int, float)):
        return _severity_from_score(value)
    if isinstance(value, dict):
        for key in ("severity", "level", "rating", "score", "cvss_score", "baseScore"):
            sev = _normalize_severity(value.get(key))
            if sev:
                return sev
    if isinstance(value, list):
        severities = [_normalize_severity(item) for item in value]
        return _max_severity([item for item in severities if item])
    return None


def _max_severity(severities: list[str]) -> str | None:
    order = {"critical": 4, "high": 3, "medium": 2, "low": 1, "none": 0, "unknown": -1}
    if not severities:
        return None
    return max(severities, key=lambda item: order.get(item, -1))


def _package_from_ref(ref: Any) -> tuple[str, str]:
    raw = str(ref or "unknown")
    if raw.startswith("pkg:pypi/"):
        raw = raw.removeprefix("pkg:pypi/")
    if "@" in raw:
        name, version = raw.rsplit("@", 1)
        return name or "unknown", version
    return raw, ""


def _extract_vulnerabilities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dependencies = payload.get("dependencies") or []
    if isinstance(dependencies, dict):
        dependencies = list(dependencies.values())

    findings: list[dict[str, Any]] = []
    for vuln in payload.get("vulnerabilities") or []:
        if not isinstance(vuln, dict):
            continue
        severity = (
            _max_severity(
                [
                    sev
                    for sev in (
                        _normalize_severity(vuln.get("severity")),
                        _normalize_severity(vuln.get("ratings")),
                        _normalize_severity(vuln.get("scores")),
                    )
                    if sev
                ]
            )
            or "unknown"
        )
        affects = vuln.get("affects") or [{}]
        for affected in affects:
            ref = affected.get("ref") if isinstance(affected, dict) else affected
            package, version = _package_from_ref(ref)
            findings.append(
                {
                    "package": package,
                    "version": version,
                    "id": vuln.get("id") or vuln.get("bom-ref") or "unknown",
                    "severity": severity,
                    "fix_versions": vuln.get("fix_versions") or [],
                }
            )
    for dep in dependencies:
        if not isinstance(dep, dict):
            continue
        package = str(dep.get("name") or dep.get("package") or "unknown")
        version = str(dep.get("version") or "")
        vulnerabilities = dep.get("vulns") or dep.get("vulnerabilities") or []
        for vuln in vulnerabilities:
            if not isinstance(vuln, dict):
                continue
            severity_candidates = [
                vuln.get("severity"),
                vuln.get("severities"),
                vuln.get("ratings"),
                vuln.get("cvss"),
                vuln.get("score"),
                vuln.get("cvss_score"),
                (vuln.get("database_specific") or {}).get("severity")
                if isinstance(vuln.get("database_specific"), dict)
                else None,
            ]
            severity = (
                _max_severity([sev for sev in (_normalize_severity(item) for item in severity_candidates) if sev])
                or "unknown"
            )
            findings.append(
                {
                    "package": package,
                    "version": version,
                    "id": vuln.get("id") or vuln.get("vulnerability_id") or vuln.get("fix_versions") or "unknown",
                    "severity": severity,
                    "fix_versions": vuln.get("fix_versions") or vuln.get("fixed_versions") or [],
                }
            )
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail pip-audit JSON only for selected severities.")
    parser.add_argument("audit_json", type=Path)
    parser.add_argument("--fail-on", default="critical,high", help="Comma-separated severities that fail the build.")
    args = parser.parse_args()

    fail_on = {item.strip().lower() for item in args.fail_on.split(",") if item.strip()} or BLOCKING_SEVERITIES
    if not args.audit_json.exists():
        raise SystemExit(f"pip-audit output not found: {args.audit_json}")

    payload = json.loads(args.audit_json.read_text(encoding="utf-8"))
    findings = _extract_vulnerabilities(payload)
    blocking = [item for item in findings if item["severity"] in fail_on]

    if findings:
        print("pip-audit vulnerability report:")
        for item in findings:
            fixed = ",".join(str(v) for v in item["fix_versions"]) or "n/a"
            print(f"- {item['severity']}: {item['package']} {item['version']} {item['id']} fix={fixed}")
    else:
        print("pip-audit: no vulnerabilities reported")

    if blocking:
        print(f"Blocking {len(blocking)} vulnerability/vulnerabilities with severities: {sorted(fail_on)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
