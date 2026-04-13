from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import json
import smtplib
from typing import Any
import urllib.request


@dataclass(frozen=True)
class NotificationResult:
    channel: str
    ok: bool
    detail: str


def _alert_title(alert: dict[str, Any]) -> str:
    return f"[{alert.get('severity', 'alert')}] {alert.get('message', 'Alerta operacional')}"


def _alert_payload(alert: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "ops_alert",
        "tenant_id": alert.get("tenant_id"),
        "severity": alert.get("severity"),
        "metric": alert.get("metric"),
        "value": alert.get("value"),
        "threshold": alert.get("threshold"),
        "message": alert.get("message"),
    }


def _post_json(channel: str, url: str, payload: dict[str, Any], timeout_seconds: float) -> NotificationResult:
    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - configured ops webhook
            status = getattr(response, "status", 200)
        return NotificationResult(channel=channel, ok=200 <= int(status) < 300, detail=f"http_status={status}")
    except Exception as exc:  # pragma: no cover - network failures depend on environment
        return NotificationResult(channel=channel, ok=False, detail=type(exc).__name__)


def _send_email(alert: dict[str, Any], settings: Any, timeout_seconds: float) -> NotificationResult:
    recipients_raw = str(getattr(settings, "ops_alert_email_to", "") or "")
    recipients = [item.strip() for item in recipients_raw.split(",") if item.strip()]
    sender = str(getattr(settings, "ops_alert_email_from", "") or "")
    host = str(getattr(settings, "ops_alert_smtp_host", "") or "")
    if not recipients or not sender or not host:
        return NotificationResult(channel="email", ok=False, detail="email_not_configured")

    msg = EmailMessage()
    msg["Subject"] = _alert_title(alert)
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    body = json.dumps(_alert_payload(alert), ensure_ascii=False, indent=2)
    msg.set_content(body)

    try:
        port = int(getattr(settings, "ops_alert_smtp_port", 587) or 587)
        with smtplib.SMTP(host, port, timeout=timeout_seconds) as smtp:
            if bool(getattr(settings, "ops_alert_smtp_tls", True)):
                smtp.starttls()
            username = str(getattr(settings, "ops_alert_smtp_username", "") or "")
            password = str(getattr(settings, "ops_alert_smtp_password", "") or "")
            if username or password:
                smtp.login(username, password)
            smtp.send_message(msg)
        return NotificationResult(channel="email", ok=True, detail="sent")
    except Exception as exc:  # pragma: no cover - external SMTP is environment-specific
        return NotificationResult(channel="email", ok=False, detail=type(exc).__name__)


def send_alert_notifications(alert: dict[str, Any], settings: Any, *, timeout_seconds: float = 10.0) -> list[NotificationResult]:
    results: list[NotificationResult] = []
    payload = _alert_payload(alert)
    text = _alert_title(alert)

    webhook_targets = [
        ("webhook", getattr(settings, "ops_alert_webhook_url", "")),
        ("slack", getattr(settings, "ops_alert_slack_webhook_url", "")),
        ("teams", getattr(settings, "ops_alert_teams_webhook_url", "")),
    ]
    for channel, url in webhook_targets:
        url = str(url or "").strip()
        if not url:
            continue
        if channel == "slack":
            channel_payload = {"text": text, **payload}
        elif channel == "teams":
            channel_payload = {"text": text, "title": text, **payload}
        else:
            channel_payload = payload
        results.append(_post_json(channel, url, channel_payload, timeout_seconds))

    if bool(getattr(settings, "ops_alert_email_enabled", False)):
        results.append(_send_email(alert, settings, timeout_seconds))

    if not results:
        results.append(NotificationResult(channel="none", ok=True, detail="no_channels_configured"))
    return results
